# -*- coding: utf-8 -*-
"""

生成日历 ::

    gen_schedule                -- 主函数
    gen_check_date              -- 获取归集日
    gen_security_dates          -- 获取所有证券的本金、利息支付日
    gen_date_by_rule            -- 从日期规则表读归集日、本金支付日、利息支付日
    gen_date_series_with_freq   -- 根据频率和起始日、结束日推算支付日序列

"""
import numpy as np
import pandas as pd
from collections import defaultdict
from utils.timeutils import (to_date, to_date2, get_payment_dates, trans_date_class, holiday_adjsut)
from doc import enumerators


def gen_schedule(tranches, product, date_rule=None, df_calendar=None):
    """

    日历主函数, 调用生成各类日期的子函数，并将生成的日期集合在一张表里

    Args:
        tranches (list): 证券基本信息
        product:  项目基本信息
        date_rule (pd.DataFrame): 日期规则表

    Returns:
        tuple: tuple contains

            schedule (pd.DataFrame): 日历
            warn_lst (list): 生成过程中的问题信息

    **逻辑**

    1. 生成各证券的本金支付日、计息日、利息支付日序列 （ ``gen_security_dates`` ）
    2. 将日期整合到 `schedule` , 同时加入一列所有日期的合集 'all_date' , 并将 `schedule` 根据 'all_date' 进行排序。在后续的现金流分配中，
    就是根据这一列进行逐期分配，如果是归集日就就行模拟回款，如果是支付日，则模拟支付对应的证券本息。是否进行模拟回款和支付则根据支付状态列判断。

    3. 生成支付状态列

        * 在 `schedule` 中加入一列 'security_status', 当需要支付任一支证券时就是 1, 否则为 0。 如果是 1，
        则至少有一只证券是需要在这期还本付息的。具体某一支的是否需要支付则根据对应的 `tranche` 实例中的支付日情况判断。如果是计息日则是0.
        * 加入一列 'is_revolving_period' , 对于日期小于循环购买届满日的行为 1， 超出的为 0


    """
    warns_lst = []
    rev_period_end_date = product.revolving_expiry_date

    dates_dict, warning_info = gen_security_dates(tranches, date_rule, df_calendar)
    warns_lst = warns_lst + warning_info
    # 3. 整合日期
    schedule = pd.DataFrame(columns=['all_date'])

    cols_lst = []
    for i in range(len(tranches)):
        cols_lst.append(tranches[i].security_level + "_prin_date")
        cols_lst.append(tranches[i].security_level + "_int_pay_date")

    for key_ in dates_dict:
        pick_out = dates_dict[key_]
        if len(dates_dict[key_]) > 0:
            schedule_temp = pd.DataFrame({'all_date': pick_out, key_: pick_out})
            schedule = schedule.merge(schedule_temp, how='outer', left_on='all_date', right_on='all_date',
                              sort=True)
        else:
            schedule[key_] = float('nan')

    schedule.sort_values(by='all_date', inplace=True, ignore_index=True)

    if product.is_revolving_pool:
        schedule.loc[:, 'is_revolving_period'] = [1 if x <= rev_period_end_date else 0 for x in schedule['all_date']]
    else:
        schedule.loc[:, 'is_revolving_period'] = 0

    schedule.loc[:, 'tax_exp_date'] = schedule.loc[:, cols_lst].T.ffill().iloc[-1, :]  # 在证券端的本金支付日和利息支付日支付税费
    # 当所有支付日都为空（即本月没有证券和税费的支付）时，security_status为0，反之为1
    schedule.loc[:, "security_status"] = schedule.loc[:, 'tax_exp_date'].apply(lambda x: 0 if pd.isna(x) else 1)
    return schedule, warns_lst


def gen_check_date(product, check_dates: list, date_rule, if_rev, recal_cash=False, eoaa=False):
    """
    生成归集日

    Args:
        eoaa: bool, 是否发生加速清偿事件

    Returns:
        np.array: dates(datetime.date) 构成的序列


    **逻辑**

    1. 获取日期规则表中的归集日数据 (用程序 ``gen_date_by_rule`` )

    2. 根据是否重新测算的循环购买分两种情况:

                * 如果不是，此时是要对现金流归集表进行加压和分配，如果现金流归集表的日期跟这里生成的日期不一样，会导致无法正常匹配和分配，因此即使有维护的归集日日期， `仍然用现金流归集表中的归集日`
                * 如果是需要重新测算的循环购买类，此时不需要用到现金流归集表，并且现金流归集表的数据已经是考虑了另一套假设条件的数据，无法保证以新的摊还比例重新测算后的现金流归集表能跟给出的现金流归集表在同一个归集日完成还款。
                  故此时采用的是日期规则表的日期，但是如果没有维护，则会输出预警，同时为了保证程序继续运行，会输出给出的现金流归集表的归集日。如果根本没有有效的现金流归集表，则简单按月度生成日期序列。

    3. 如果有归集日早于初始起算日，则将初始起算日之前的日期全部删除。对应的，在现金流归集表的处理过程中也会将这些日期去掉以保证日期的匹配。


    """
    warn_lst = []
    # 1. 从日期规则表获取归集日（不保证有维护）
    date_series_not_exist, dates = gen_date_by_rule(date_rule, if_rev, '计算日', eoaa=eoaa, security_seq=None)

    # 3. 输出归集日期
    if if_rev and recal_cash: # 如果是循环购买且重新测算现金流的
        if not date_series_not_exist:
            pool_dates = dates  # (1)日期规则表有维护则优先用日期规则表
        else:
            warn_lst.append(('日期缺失', '循环购买要重新测算现金流时，日期规则表没有可用的归集日数据'))
            if len(check_dates) > 0: # （2） 日期规则表没有维护归集日则用现金流归集表中的日期替代
                pool_dates = np.array([to_date2(x) for x in check_dates])
            else:
                pool_dates = gen_pool_date(product.initial_date, product.legal_due_date)  # (3) 如果现金流归集表没有存续期，则简单按月度生成日期
    else:  # （4） 非循环购买的或者循环购买直接用维护的现金流归集表的，归集日直接用现金流归集表给的，避免与现金流归集表的日期不匹配导致加压和分配出错
        if len(check_dates) > 0:
            pool_dates = np.array([to_date2(x) for x in check_dates])
        else:
            pool_dates = np.array([])
    # 归集日里需要有初始起算日，避免处于初始起算日和上市日中间时漏掉几期。eg,假设初始起算日2021.5,2022.1开始簿记建档，
    # 中间每一期都有一次现金流预测，但是如果直接从以估值日，如2021.7月截断，则会漏掉2021.6月的现金流，所以要加初始起算日，这样没有存续报告时，
    # 会将初始起算日作为上一个归集日，将中间的现金流都加起来，作为未来现金流分配的依据
    pool_dates = np.append(pool_dates, product.initial_date)
    # 但是有些项目的初始起算日实际维护成了项目设立日，会导致这个日期晚于首次归集日，此时在使用的时候取首个归集日、初始起算日中较早的点作为起算点（如果有存续期则从存续期对应的日期开始分配）
    pool_dates = np.unique(pool_dates)
    pool_dates = pool_dates[pool_dates >= product.initial_date]  # 只保留晚于初始起算日的，现金流归集表也对应的调整

    return pool_dates, warn_lst


def gen_security_dates(tranches, date_rule, df_calendar=None):
    """
    处理从数据库读取的证券日期序列

    Args:
        tranches (list): 证券基本信息
        date_rule (pd.DataFrame): 日期规则表

    Returns:
        tuple: tuple contains:
               dates_dict (dict): key-证券等级+'_int_date' / '_prin_date'，value-日期序列
               warn_lst (list): 警示信息

    """
    warn_lst = []
    error_lst = []
    interest_dates, principal_dates, count_dates = treat_date_series(date_rule)
    collect_dict = defaultdict(list)
    for i in range(0, len(tranches)):
        seq_ = tranches[i].security_seq
        level_ = tranches[i].security_level
        code_ = tranches[i].security_code
        balance = tranches[i].period_end_balance
        last_date = tranches[i].payment_date

        collect_dict[level_ + "_int_date"] = count_dates[seq_]
        # 如果历史支付日不在其中，则额外加入，以避免提前偿付等情况发生时应付未付本金得不到支付
        int_pay_date = list(set(interest_dates[seq_] + [tranches[i].payment_date]))
        int_pay_date = holiday_adjsut(date_series=int_pay_date, schedule=df_calendar, holiday_rule='forward')
        collect_dict[level_ + "_int_pay_date"] = list(int_pay_date)

        prin_dates = list(set(principal_dates[seq_] + [tranches[i].payment_date]))
        prin_dates = holiday_adjsut(date_series=prin_dates, schedule=df_calendar, holiday_rule='forward')
        if (prin_dates <= last_date).all() and balance > 0:
            error_lst.append(code_)

        collect_dict[level_ + "_prin_date"] = list(prin_dates)

        if len(count_dates[seq_]) == 0 or len(interest_dates[seq_]) == 0 or len(principal_dates[seq_]) == 0:
            warn_lst.append(("日期缺失", f"债券{code_}日期缺失"))

    if len(error_lst) > 0:
        raise ValueError(f"证券{error_lst}未到期，未来没有本金支付日")

    return collect_dict, warn_lst


def treat_date_series(date_series_table):
    """读取到的是字符串，转为数组"""
    interest_dates = defaultdict(list)
    principal_dates = defaultdict(list)
    count_dates = defaultdict(list)
    for idx, row in date_series_table.iterrows():
        long_str = row.accrual_pay_date
        interest_dates[row.security_seq] = [to_date(x) for x in long_str.split(";")] if long_str is not None else []
        long_str2 = row.interest_pay_date
        count_dates[row.security_seq] = [to_date(x) for x in long_str2.split(";")] if long_str2 is not None else []
        long_str3 = row.principal_pay_date
        principal_dates[row.security_seq] = [to_date(x) for x in long_str3.split(";")] if long_str3 is not None else []

    return interest_dates, principal_dates, count_dates


def gen_date_by_rule(date_rule_initial, if_rev, date_name, eoaa=False, security_seq=None):
    """
    从日期规则表提取需要的日期序列

    Args:
        date_rule_initial (pd.DataFrame): 日期规则表
        if_rev (bool): 是否循环购买
        date_name (str): 日期名称
        eoaa (bool): 是否加速清偿，因为加速清偿下证券的支付速度会发生变化，所以是否发生是筛选日期序列的一个条件
        security_seq (str, None): 证券内码

    Returns:
        tuple: tuple contains:
                date_series_not_exist (bool): 是否能够读取到有效的日期序列
                dates (np.array): 日期序列
                postpone_holiday (bool): 遇到节假日是否需要推到下一工作日

    **逻辑**

    1. 读取日期规则表对应的日期, 根据证券端的内码跟日期名称，能够提取到数据，则视作是按券维护的日期；如果不能，则说明是按项目维护的（eg. 归集日).另外，由于有些循环购买，一部分是按项目维护，一部分是按照证券维护，此时要对读取到的按券维护的日期补上按项目维护的那部分
    2. 分以下步骤进一步筛选数据数据：

            1. 如果 ``eoaa=True`` , 则筛选 'collection' 枚举为 '3' 的，即发生加速清偿时的日期。
            2. 如果没有得到有效数据，对于循环购买类的，筛选 'collection' 枚举为 循环购买看枚举为4、5的（分别表示循环期和摊还期）拼在一起
            3. 如果没有得到有效数据，或者是非循环购买类的ABS，则筛选 'collection' 为 '1' 的，即表示无条件


    """

    if date_rule_initial is None:
        return True, np.array([])
    else:
        try:
            date_rule_initial.loc[:, 'date_series'] = date_rule_initial['date_series'].apply(lambda x: x.read())
        except:
            print("无法转化CLOB格式的日期序列")
            return True, np.array([])

    # 1. 读取日期规则表对应的日期,如果是证券端的，先看是不是有单独维护，再看是不是有统一维护（即所有证券都一样）
    dates = np.array([])
    common_ = True
    project_date_rule = date_rule_initial.loc[
        (date_rule_initial['date_project'] == '0') & (
                date_rule_initial['date_name'] == date_name), :]
    if security_seq is not None:
        security_date_rule = date_rule_initial.loc[
                             (date_rule_initial['seq']==security_seq)&(date_rule_initial['date_name']==date_name), :]

        if len(security_date_rule) > 0:
            common_ = False

            if if_rev:
                rule_rev = security_date_rule.loc[security_date_rule['collection']==enumerators.date_cond_dict['not_inamortperiod'], :]
                rule_amo = security_date_rule.loc[security_date_rule['collection']==enumerators.date_cond_dict['inamortperiod'], :]
                if len(rule_rev) == 0:
                    security_date_rule = security_date_rule.append(project_date_rule.loc[project_date_rule['collection']==enumerators.date_cond_dict['not_inamortperiod'], :])
                if len(rule_amo) == 0:
                    security_date_rule = security_date_rule.append(project_date_rule.loc[
                                                                   project_date_rule['collection'] ==
                                                                   enumerators.date_cond_dict['inamortperiod'], :])

            date_rule = security_date_rule

    if common_:
        date_rule = project_date_rule

    date_series_not_exist = False
    if len(date_rule) < 1:
        date_series_not_exist = True

    # 2 筛选后存在需要查找的日期名称，则继续根据日期条件筛选
    if not date_series_not_exist:
        catch_date = False
        if eoaa:  # 如果能找到加速清偿下的计算日；找不到的话就默认即使加速清偿，计算日也不会有变化，跟无条件下一致
            date_rule_1 = date_rule.loc[date_rule['collection'] == enumerators.date_cond_dict['eoaa'], :]
            if len(date_rule_1) > 0:
                dates = date_rule_1.iloc[0, :]['date_series']
                dates = dates.split(',')
                catch_date = True

        if not catch_date:
            if if_rev: # 循环购买看枚举为4、5的
                rule_rev = date_rule.loc[date_rule['collection']==enumerators.date_cond_dict['not_inamortperiod'], :]
                rule_amo = date_rule.loc[date_rule['collection']==enumerators.date_cond_dict['inamortperiod'], :]
                rev_dates = rule_rev.iloc[0, :]['date_series'].split(',') if len(rule_rev) > 0 else []
                amo_dates = rule_amo.iloc[0, :]['date_series'].split(',') if len(rule_amo) > 0 else []
                if len(rule_rev) > 0 or len(rule_amo) > 0:
                    dates = rev_dates + amo_dates
                    catch_date = True

            if not catch_date:
                date_rule_1 = date_rule.loc[date_rule['collection']==enumerators.date_cond_dict['condless'], :]  # 无条件
                if len(date_rule_1) > 0:
                    dates = date_rule_1.iloc[0, :]['date_series'].split(",")
                else:
                    date_series_not_exist = True  # 日期规则表没有可以的日期数据数据
    # 格式
    if not date_series_not_exist:
        dates = np.array([to_date(x) for x in dates])
    dates.sort()
    return date_series_not_exist, dates


def gen_date_series_with_freq(first_pay_date, amort, int_freq, prin_freq, expected_maturity,
                              max_expected_maturity,
                              rev_period_int_freq=None, rev_period_prin_freq=None,
                              rev_period_end_date=None, plan_dates=None, if_rev=False):
    """
    根据付息频率、还本频率生成付息日和还本日，用于在日期规则表没有维护日期时的补充

    Args:
        first_pay_date (datetime.date): 首个付息日， 如果是循环期不付息的证券，但是同个项目下有循环期付息的证券，则要用摊还期内首个付息日作为输入的首次付息日, 以避免进行额外的循环期支付
        amort (str): 摊还方式
        int_freq (str): （非循环购买的）付息频率，or （循环购买的）摊还期付息频率
        prin_freq (str): 还本频率，释义同上
        expected_maturity (datetime.date): 证券security_code对应的预期到期日
        max_expected_maturity (datetime.date): 项目下所有证券的最大预期到期日
        rev_period_int_freq (str): 循环期付息频率
        rev_period_prin_freq (str): 循环期还本频率
        rev_period_end_date (datetime.date): 循环期结束日
        plan_dates (np.array): 摊还计划表
        if_rev (bool): 是否循环购买

    Returns:
        tuple: tuple contains:
                prin_dates (np.array): 本金支付日序列 \n
                int_dates (np.array): 利息支付日序列 \n
                first_pay_date_amo (datetime.date): 摊还期首次支付日 \n
                warn_lst (list): 问题信息

    **逻辑**

    根据摊还方式分以下几种：

    1. 到期一次还本

        * 本金支付日就等于预期到期日
        * 计息日:

             * 如果付息频率等于还本频率，则也是等于预期到期日
             * 如果付息频率不等于还本频率，则根据付息频率和首次付息日通过 ``gen_dates_subfunc`` 生成序列。

    2. 固定摊还

        * 本金支付日为摊还计划表的日期，加上摊还计划最后一天与 `end_date` 按还本频率 'prin_seq' 生成的日期序列（理由同上）
        * 计息日，在还本频率和付息频率相同时，直接等于本金支付日，如果不同，通过 ``gen_dates_subfunc`` 生成序列。 `但是此时可能会产生同一期的本金支付日和计息日不同的情况` ， 因此会返回该信息。

    3. 过手摊还

        1. 非循环购买直接用 ``gen_dates_subfunc`` ，以首次付息日为起始日， `end_date` 为结束日，结合付息频率( `int_feq` )或还本频率( `prin_freq` ) 分别生成计息日和还本日
        2. 循环购买:

                a. 生成计息日

                    * 如果循环期不付息，或者循环期和摊还期付息频率一样，则跟非循环购买采用一样的方法生成计息日序列
                    * 如果循环期付息且循环期和摊还期付息频率不一致，则先从首次付息日开始，循环期截止日为结束日，根据循环期付息频率推出循环期计息日，::

                        gen_dates_subfunc(
                        first_pay_date, rev_period_end_date, rev_period_end_date, rev_period_int_freq, plan_dates, add_one=False)


                    然后从前面生成的最后一个计算日开始, `end_date` 为结束日，根据摊还期付息频率生成摊还期的日期序列。并且更新 `first_pay_date_amo` 为循环期最后一个计息日。

                b. 生成本金支付日: 与计息日推算方法相同

    """
    warn_lst = []
    first_pay_date = to_date(first_pay_date)
    expected_maturity = to_date(expected_maturity)
    freq_to_month = enumerators.freq_to_month
    first_pay_date_amo = first_pay_date
    end_date = max_expected_maturity
    if amort == 'once': # 1 到期一次还本的，还本日就是预期到期日
        prin_dates = np.array([expected_maturity])
        if str(int_freq) == str(prin_freq): # 到期还本付息
            int_dates = prin_dates.copy()
        else:  # 到期还本但是阶段性付息
            int_dates = gen_dates_subfunc(first_pay_date, expected_maturity, end_date, int_freq, plan_dates, add_one=False)

    elif amort == 'fixed': # 2 固定摊还
        prin_dates = np.array([to_date(x) for x in plan_dates])
        if prin_freq in freq_to_month:
            prin_tenor, prin_freq_unit = freq_to_month[prin_freq]
            app_dates = get_payment_dates(start_date=prin_dates[-1], end_date=end_date, tenor=prin_tenor,
                              freq=prin_freq_unit, add_one=False)  # 拓展日期，避免固定摊还期结束后仍会对未清偿完毕的证券进行支付（如果对于固定摊还期结束后仍未支付完毕的不进行补充支付，则支付顺序里不会有相应的支付去向，即使增加了额外的日期也不会有影响）
            prin_dates = np.unique(np.append(prin_dates, app_dates))

        if str(int_freq) == str(prin_freq):
            int_dates = prin_dates.copy()
        else:
            int_dates = gen_dates_subfunc(first_pay_date, expected_maturity, end_date, int_freq, plan_dates, add_one=False)

    elif amort == 'pass-through':  # 3 过手摊还
        if not if_rev: # （1） 非循环购买日期生成比较简单
            int_dates = gen_dates_subfunc(first_pay_date, expected_maturity, end_date, int_freq, plan_dates, add_one=False)
            prin_dates = gen_dates_subfunc(first_pay_date, expected_maturity, end_date, prin_freq, plan_dates, add_one=False)
        else: # （2）循环购买日期分循环期和摊还期生成

            # b. 生成计息区间结束日
            if str(rev_period_int_freq) in ('nan', 'None') or first_pay_date >= rev_period_end_date:  # 循环期不付息时，直接从摊还期开始生成
                int_dates = gen_dates_subfunc(first_pay_date, expected_maturity, end_date, int_freq, plan_dates,
                                              add_one=False) # 此时的first_pay_date本身就是从摊还期开始的
            else:
                if rev_period_int_freq == int_freq:  # 循环期付息，但是频率跟摊还期一致时，不用分别生成
                    int_dates = \
                        gen_dates_subfunc(first_pay_date, expected_maturity, end_date, int_freq, plan_dates,
                                          add_one=False)
                else:  # 分别生成循环期付息日和摊还期付息日，摊还期从循环期最后一个日期开始
                    # 如果循环期结束日不是刚好等于循环期最后一个支付日，则加一个循环期支付日，也就是最后一个循环期支付日可能会比循环期结束日晚一点。摊还期从这个日期开始推算,因为证券支付日总是比对应的资产池计算日晚几天

                    dates_rev = gen_dates_subfunc(
                        first_pay_date, rev_period_end_date, rev_period_end_date, rev_period_int_freq, plan_dates, add_one=False)
                    # 如果是固定日期摊还的，循环期和摊还期的日期会重合，不过这类会有日期规则表，实际上用的是日期规则表维护的日期，如果没有维护前面步骤会输出提示
                    if len(dates_rev) > 0: # 否则不更新
                        first_pay_date_amo = dates_rev[-1]
                    dates_amo = gen_dates_subfunc(first_pay_date_amo, expected_maturity, end_date, int_freq, plan_dates, add_one=False)
                    int_dates = np.unique(np.append(dates_rev, dates_amo))
            # c. 生成还本日
            if str(rev_period_prin_freq) in ('nan', 'None'):
                prin_dates = gen_dates_subfunc(first_pay_date_amo, expected_maturity, end_date, prin_freq, plan_dates, add_one=False)
            else:
                if rev_period_prin_freq == prin_freq:
                    prin_dates = gen_dates_subfunc(first_pay_date, expected_maturity, end_date, prin_freq, plan_dates, add_one=False)
                else:
                    dates_rev = gen_dates_subfunc(
                        first_pay_date, rev_period_end_date, rev_period_end_date, rev_period_prin_freq, plan_dates, add_one=False)

                    if len(dates_rev) > 0: # 否则不更新
                        first_pay_date_amo = dates_rev[-1]
                    dates_amo = gen_dates_subfunc(first_pay_date_amo, expected_maturity, end_date, prin_freq, plan_dates,
                                                  add_one=False)
                    prin_dates = np.unique(np.append(dates_rev, dates_amo))
    else:
        raise ValueError("Unsupported principal amortation type: " + str(amort))

    return prin_dates, int_dates, first_pay_date_amo, warn_lst


def gen_dates_subfunc(first_pay_date, expected_maturity, deadline, freq, fix_dates, add_one=False):
    """
    仅通过日期起始日、结束日和频率推算日期

    Args:
        first_pay_date (datetime.date): 首个付息日
        expected_maturity  (datetime.date): : 预期到期日
        deadline  (datetime.date): : 日期序列截止日。不一定等于预期到期日。
        freq (str): 频率
        fix_dates (np.array): 固定日期序列
        add_one (bool): 如果end_date不是刚好与频率重合,是否多加一个日期

    Returns:
        np.array: dates, 日期序列

    **日期序列**

    根据频率分以下几种:

    1. 频率为 'nan' 或 'None', 相当于维护界面维护了 '无' , 则输出空序列
    2. 频率为 'once' 表示到期一次支付，则日期序列中仅有预期到期日一天
    3. 频率为 'fixed' 则为固定日期支付，一般提取日期规则表维护的计息日，但是如果没有，则会返回空集合
    4. 其他频率则通过 ``utils.timeutils.get_payment_dates`` 生成日期序列

    """
    freq_to_month = enumerators.freq_to_month
    if str(freq) in ('nan', 'None'):
        dates = np.array([])
    elif str(freq) == 'once':
        dates = np.array([expected_maturity])
    elif str(freq) == 'fixed':
        dates = fix_dates.copy() if fix_dates is not None else np.array([])
    elif freq in freq_to_month:
        tenor, freq_unit = freq_to_month[freq]
        dates = get_payment_dates(start_date=first_pay_date, end_date=deadline, tenor=tenor,
                                  freq=freq_unit, add_one=add_one)
    else:
        raise ValueError(f'wrong interest/principal payment frequency{freq}')
    return dates


def gen_pool_date(start_date, end_date):
    """
    简单生成月频日期

    Args:
        start_date (datetime.date): 日期序列起始日
        end_date (datetime.date): 日期序列结束日

    Returns:
        np.array, pool_dates_lst: 月频的日期
    """
    pool_dates_lst = pd.date_range(start_date, end_date, freq='M').tolist()

    pool_dates_lst = np.array([trans_date_class(x) for x in pool_dates_lst])

    return pool_dates_lst


def add_pool_date(schedule, cashflow, is_rev, rev_recal, product, date_rule, eoaa, rev_end_date):
    """
    增加归集日列 'pool_date' 和是否归集日列 'pool_status' ，如果是循环购买重新计算则用 ``gen_check_date`` ； 如果是不重新计算的，用现金流归集表的

    当某一行的 'pool_date' 是归集日时，则设为 1， 否则为 0，作为现金流分配过程中，某一期是否回收现金流的判断依据。
    """

    check_dates = cashflow['date_']
    check_dates = [to_date(x) for x in check_dates]
    if rev_recal and is_rev:
        pool_dates, warns_lst_pool = gen_check_date(product, check_dates, date_rule, is_rev, rev_recal, eoaa)

    else:
        pool_dates = check_dates.copy()

    temp_ = pd.DataFrame({'all_date': pool_dates, 'pool_date': pool_dates})
    schedule = schedule.merge(temp_, on='all_date', how='outer')
    schedule.loc[:, 'pool_status'] = schedule.loc[:, "pool_date"].apply(lambda x: 0 if pd.isna(x) else 1)
    schedule.sort_values(by='all_date', inplace=True)
    if is_rev:
        schedule.loc[:, 'is_revolving_period'] = [1 if x <= rev_end_date else 0 for x in
                                                  schedule['all_date']]
    else:
        schedule.loc[:, 'is_revolving_period'] = 0
    schedule.reset_index(drop=True, inplace=True)

    return schedule