# -*- coding: utf-8 -*-
"""

author: Double Q
"""


import pandas as pd
from sqltemplate_basic import *



def load_all_basic(code: str, trade_date: str):
    """
    读取项目原始数据

    Args:
        code: str, 项目简称或证券代码，必须带市场后缀
        trade_date: str, 交易日期，带单引号的八位数字
        cur:

    Returns:
        tuple: tuple contains:
                 df_product (pd.DataFrame): 项目基本信息，包括项目起始日、首次付息日、入池本金额等信息 \n
                 df_tranches (pd.DataFrame): 证券基本信息，包括证券发行金额、票息、利息类型、最新一期的还本付息情况等信息 \n
                 df_prediction (pd.DataFrame): 现金流归集表 \n
                 df_sequence (pd.DataFrame): 支付顺序表 \n
                 df_plan (pd.DataFrame): 摊还计划表 \n
                 df_other (pd.DataFrame): 一些额外的信息, 包括资产池、证券端的最新报告日、最新资产池归集日、最新证券支付日、资产池调整因子等 \n
                 df_date (pd.DataFrame): 日期规则表 \n
                 contingent_param_dict (dict): 保存日期规则表、历史循环购买、工作日日历等信息 \n
                 df_trigger (pd.DataFrame): 触发时间表 \n
                 events (dict): 已发生的重大事件, key为事件名称，value是bool值表示是否发生，事件名称枚举值见enumerators.py \n
                 security_seq (stt): 证券内码，当 `is_project`==False 时会返回 \n
                 project_seq (stt): 项目内码 \n
                 warns_lst (list): 数据预警信息, 由实例化的LogUtil对象构成


    **逻辑**

        1. 输入证券代码，根据证券代码从 `CSI_ABS_BASIC_INFO` 表查询项目内码。（查询数据的来源见 ``sql_template`` ，此处不赘述，下同）
        2. 根据项目内码查询项目下所有证券的基本信息（分别从ABS数据库和估值数据库）。

          - 增加`delist`列：对比摘牌日`delist_date`与估值日的大小，如果估值日前已摘牌，`delist`列为 `True` ，否则为 `False` ；全部券均为 `True` ,则不继续运行；
          - 如果项目下没有查到证券，报错并返回信息，大概为数据库错误导致信息无法匹配。
          - 浮息债需要提取基准利率，没有维护利率基准报错："浮动利率类型的证券未给定浮动利率基准"；如果过去30天都提取不到数据，会返回报错：
          "浮动利息债缺少{trade_date}日基准利率数据"。注意浮息利率在估值日确认后不会在未来期次根据远期利率更新，这一规则与估值库当前的一致。

        3. 项目基础数据：

           - 根据项目内码查询基本要素数据；

        4. 读取证券端余额信息并与证券基本信息合并成一个dataframe ( ``get_sec_duration_data`` )

           - 没有存续期报告，设 `has_security_duration=False`, 最新报告日 `history_report_date` 、最新计息区间起始结束日 `period_begin_date` 、`period_end_date` 为初始计息起始日 `interest_start_date`，主要便于后续计算利息；剩余本金则用初始本金填充；
           - 有存续期报告：`has_security_duration=True` , 其他信息均为最新一次报告中获取的值，如果有证券的计息区间缺失，则按照没有存续期进行上述字段的补充；剩余本金缺失则用初始本金减去已偿付进行补充；
           - 设置 `has_duration_report` 字段，表示该只证券是否支偿付过本金；

        5. 现金流归集表 ( ``get_prediction`` )：

           - 考虑到非循环现金流归集表可能存在循环购买和非循环购买的表混合，非循环购买统一先提1）非循环购买存续期 2）循环购买存续期 3）非循环购买发行期 4）循环购买发行期
           - 现金流归集表找证券端最新报告日前（小于等于），归集表报告日最新的一期，以保证用于分配证券端本息的现金流是最合适的一期
           - 如果读到的现金流归集表中，归集日都在估值日以前，则视作无效，继续往更早的日期查找，直到初始现金流归集表。
           - 根据是否有存续期现金流归集表设置 `bool` 值 `has_prediction_duration` 。
           - 根据现金流归集表利息列是否有金额判断资产池的现金流是本金还是本息和，设置 `bool` 值 `guess_mixed`
           - 提取初始本金补充前述项目基本信息读取中的缺失值,如果没有缺失则不会补 ( ``get_initial_principal`` )：

             - 根据初始现金流归集表计算资产池初始金额，分别记录归集表的首期期初本金（如果是循环购买只用到这个补充）、本金和、本息和。
             - 如果是本息和，若项目基本信息中提取到了入池本息和余额字段，则直接使用，如果没有，则用初始现金流归集表中得到的值、期初本金/本息和/资产池余额字段中较大者进行填充。
             - 如果是仅本金，若项目基本信息中提取到了入池本金余额字段，则直接使用，如果没有，则用初始现金流归集表中得到的值、期初本金/本息和/资产池余额字段中较大者进行填充。

        6 资产池剩余本金：

           - 用证券端最新报告日尝试读取同报告日的资产池存续信息（如果是clo的，一般直接匹配到，不再进行第二步），
           - 读取报告日在估值日之前的，归集日在证券最新一次计息结束日之前的最新期次资产池存续本金额。（假设了资产端用于支付证券端最近一次利息的回收资金，在证券端披露之前就已回收完成，并且披露了回收后的资产池剩余金额）
           - 确定统计口径，如果维护了“合计”，根据合计的口径确定，如果没有，根据“正常”的口径确定。然后将口径不一致的全部剔除。此时如果同一个项目的存续资产服务报告同时维护了两种口径的数据，则其中一种会被剔除。
           - 查询在证券最新一期报告日之前公布的最新资产池剩余本金数据，枚举为“1-合计”，当合计值缺失时，则为除了枚举 '1', '17', '18', '19', '20', '21' （对应为正常类、关注类等，跟逾期违约值属于两种不同的披露方式，故剔除）以外的枚举值的合计；
           - 如果资产池报告日早于证券报告日，则查询资产池报告日之前的最新一期证券存续期数据，根据该期证券本金之和与最新证券本金之和的差异，对资产池剩余本金进行调整。

        7. 判断是否存在证券端和资产端报告错配的可能性：

            * 如果资产端和证券端最新的报告日不同，且资产池的报告期间结束日早于证券的报告期间起始日，则认为是存在错配，进行资产池的调整
            * 如果判断需要调整，则计算调整因子或者从数据库读取人工维护的调整因子（ ``get_adjust_factor`` ) , 保存在 `df_other` , 计算逻辑为：如果存在错配的可能性，则找到与资产池报告日最接近的证券报告日的数据, 并且证券的计息结束日晚于或者等于资产池计息区间结束日的那一个报告期（适用于证券披露频次高于资产端）。如果没有这样一期报告，则即使根据规则判断需要调整，调整因子也会是0
            计算与最新证券端报告的比值，作为调整比例。调整后，现金流归集表会统一以最近一次证券分配报告对应的支付日作为切割点，只分配这日以后的现金流。请注意现金流归集表支付日列的错误维护会导致现金流计算的错误。

        7. 资产池回收总金额:

            - 根据资产池剩余本金提取过程中，使用的是本金还是本息和数据，提取资产池回收数据中，同样范围的资金中，枚举值为 '合计' 的当期回收数据，加总得到历史回收总金额。

        8. 查询支付顺序数据。
        9. 查询摊还计划数据。
        10. 输出结果。


    """
    is_mysql = False
    warns_lst = []
    contingent_params_dict = {}

    # 0. 连接数据库
    # todo 连接数据库
    # 1. 得到项目序号和证券序号
    sql_seq = get_seqs(code)
    df_bond_seq = sql_read(sql_str=sql_seq, cur=cur, is_mysql=is_mysql, single_return=False)

    if len(df_bond_seq) > 0:
        project_seq = df_bond_seq.iloc[0, 0]
        security_seq = df_bond_seq.iloc[0, 1]
    else:
        raise ValueError(f"未找到证券对应的项目内码,检查 CSI_ABS_BASIC_INFO 表中是否存在证券代码 为 {code} 的证券")

    # 2. 得到证券基本信息
    # 2.1 ABS数据库的证券基本信息
    sql_tranches = tranches_template(project_seq)
    df_tranches = sql_read(sql_tranches, cur, is_mysql, single_return=False)

    if len(df_tranches) < 1:
        raise ValueError("项目下没有证券信息")
    # 2.2 估值库在ABS项目成立前已有的字段
    df_tranches_csi, df_work_schedule = \
        get_csi_data(project_seq, cur, trade_date, dict(zip(df_tranches['security_code'], df_tranches['security_seq'])))

    df_tranches = df_tranches.merge(df_tranches_csi, how='left', on='security_seq')
    df_tranches.sort_values(by='security_level', inplace=True)  # 根据等级排序，通常等级越高支付越优先

    # 2.5 对于附息债，需要提取基准利率，假设此后的利率均为当前最新的基准利率
    df_tranches.loc[:, 'floating_rate_benchmark'] = float('nan')
    for i in range(0, len(df_tranches)):

        if str(df_tranches.loc[i, 'interest_type']) == '2':  # 浮动利率
            retry = 0
            base_exist = False
            if not isnull(df_tranches.loc[i, 'floating_benchmark_period']):
                while retry < 30 and (not base_exist):  # 如果说当日还没有更新这个表格，则读取前一天的(数据库没有工作日相关表格，直接回滚直到找到上一工作日的数据)
                    read_date = (to_date(trade_date.strip("'")) - timedelta(days=retry)).strftime("%Y%m%d")
                    BaseRate = baserate_template(df_tranches.loc[i, 'floating_benchmark_period'], read_date)
                    BaseResult = sql_read(BaseRate, cur, is_mysql, single_return=False)
                    if len(BaseResult) > 0:
                        rate_ = BaseResult.iloc[-1]["target_value"]
                        df_tranches.loc[i, 'floating_rate_benchmark'] = rate_
                        base_exist = True
                    else:
                        retry += 1

                if retry >= 30:
                    raise ValueError(f"浮动利息债缺少{trade_date}日基准利率数据")

            else:
                raise ValueError(f"浮动利率类型的证券{df_tranches.loc[i, 'security_code']}未给定浮动利率基准")

    # 3. 项目基本信息读取
    sql_product = product_template(project_seq)
    df_product = sql_read(sql_product, cur, is_mysql, single_return=False)
    df_product.loc[0, "is_revolving_pool"] = df_product.loc[0, "is_revolving_pool"] == enumerators.IsRevolving['Y']

    if len(df_product) < 1:
        raise ValueError(f"未找到对应的项目基本信息")

    # 4. 读取证券端余额加到df_tranches,与证券基本信息合并
    df_tranches_duration, history_report_date, sec_payment_date, period_end_date, has_security_duration = \
        get_sec_duration_data(project_seq, df_tranches['security_seq'], trade_date,
                              df_product.loc[0, "interest_start_date"],
                              dict(zip(df_tranches['security_seq'], df_tranches['initial_principal'])),
                              cur, calendar=df_work_schedule)

    if len(list(set(list(df_tranches_duration['security_seq'])).difference(list(df_tranches['security_seq'])))) > 0:
        raise ValueError("证券存续信息的证券内码与基本要素中的不匹配，可能是证券基本信息表遗漏证券或者存续期表证券内码错误")  # 内码不匹配会导致得到的本金余额、本期应付金额不对

    df_tranches = df_tranches.merge(df_tranches_duration, on='security_seq', how='left')

    lack_security = df_tranches[pd.isna(df_tranches['initial_principal'])]['security_code']
    if len(lack_security) > 0:
        warns_lst.append(('基本要素缺失', '证券 %s 未维护基本要素' %lack_security ))

    # 5 获取现金流归集表
    df_prediction, prediction_report_date, has_prediction_duration = \
        get_prediction(project_seq, trade_date, history_report_date, df_product.loc[0, 'initial_date'], cur)

    # 5.2 判断资产池的现金流是本金还是本息和
    # todo 根据ABS二级分类确定是否本息和披露
    guess_mixed = (sum(df_prediction['current_interest_due']) < 1) or df_prediction['current_interest_due'].isna().all()
    # 5.3 补充入池本金/本息和信息
    if guess_mixed:
        if isnull(df_product.loc[0, 'initial_principal_interest']) or (df_product.loc[0, 'initial_principal_interest']) < 1:
            initial_principal, warns_lst_1 = get_initial_principal(project_seq, guess_mixed, df_product,
                                                      df_tranches['initial_principal'].sum(), cur)
            df_product.loc[0, ["initial_principal_interest", 'initial_principal']] = initial_principal # 最终统一用initial_principal与现金流进行匹配
            warns_lst = warns_lst + warns_lst_1
        else:
            df_product.loc[0, 'initial_principal'] = df_product.loc[0, "initial_principal_interest"]
    else:  # 利息明确的类型，此时初始本金只是入池资产，利息额外产生，即入池本金<未来资金流入总和
        # （3）非循环购买入池特征中初始本金为空时
        if isnull(df_product.loc[0, 'initial_principal_only']) or (df_product.loc[0, 'initial_principal_only'] < 1):
            initial_principal, warns_lst_1 = get_initial_principal(project_seq, guess_mixed, df_product,
                                                      df_tranches['initial_principal'].sum(), cur)

            df_product.loc[0, ['initial_principal_only', 'initial_principal']] = initial_principal
            warns_lst = warns_lst + warns_lst_1
        else:
            df_product.loc[0, 'initial_principal'] = df_product.loc[0, "initial_principal_only"]

    # 5.4 本息和的情况下，有时候会只维护“现金流入”列（具体四张现金流类的表的字段见表结构的文档），统一并入到current_principal_due列
    if guess_mixed:
        if df_prediction['current_principal_interest_due'].sum() < 0.01:
            # 一些维护在本金字段，填上本息和字段
            df_prediction.loc[:, 'current_principal_interest_due'] = df_prediction['current_principal_due'].fillna(0.) \
                                                                     + df_prediction['current_interest_due'].fillna(0.)
        df_prediction.loc[:, 'current_principal_due'] = df_prediction['current_principal_interest_due']
        df_prediction.loc[:, 'current_interest_due'] = 0.
    df_prediction.drop(columns='current_principal_interest_due', inplace=True)

    # 6. 获取最新的资产池余额
    pool_remaining_principal, default_principal, history_date, pool_report_date, collect_amount, duration_mixed,\
        has_pool_duration = get_pool_duration_data(project_seq, history_report_date, period_end_date,
                                                   df_product.loc[0, 'initial_principal'],
                                                   df_product.loc[0, 'initial_date'], trade_date, guess_mixed, cur)
    # 6.2判断是否可能存在资产池端披露滞后
    adjust_pool_principal = False
    if (not has_security_duration) and has_pool_duration:
        # 一般不会见到这种情况
        adjust_pool_principal = True
    elif has_security_duration:  # 如果证券没有存续期数据，无需进行资产端的调整
        if not has_pool_duration:  # 代表证券端有存续资产端没有存续，此时必须调整,负责会导致高估
            adjust_pool_principal = True
        else:  # 此时，如果report_date 相等，直接视作同一期，不调整，否则比较计息区间起始日和现金流归集日，如果归集日比证券端计息区间起始日还早，则说明前面得到的资产池的报告匹配的是上一个证券端报告期需要调整
            last_interest_begin_date = max([int(x) for x in df_tranches['period_begin_date']])
            if (pool_report_date != history_report_date) and \
                (int(history_date) <= int(last_interest_begin_date)):
                # 如果两个报告日相同，则是同一个报告披露的，则资产池存续和证券存续本金匹配，直接保存数据即可（CLO一般均为如此）
                # 如果报告日不同，且资产池的报告期间结束日早于证券的报告期间起始日，则认为是存在错配，进行资产池的调整
                adjust_pool_principal = True

    # 6.3 如果需要调整，计算调整因子
    if adjust_pool_principal:

        adjust_factor, possible_match_date, if_match = \
            get_adjust_factor(project_seq, df_product.loc[0, 'interest_start_date'],
                              history_date=history_date,
                              period_end_date=period_end_date,
                              has_pool_duration=has_pool_duration,
                              has_sec_duration=has_security_duration,
                              history_report_date=history_report_date,
                              issue_amount=df_tranches['initial_principal'].sum(),
                              remain_amout=df_tranches['period_end_balance'].sum(), cur=cur)

        warns_lst.append(("数据披露问题", f"根据证券端和资产端存续期披露的日期相差较大"))

    else:
        adjust_factor = 1
        possible_match_date = period_end_date

    return adjust_factor, possible_match_date



def get_adjust_factor(project_seq, interest_start_date, history_date, period_end_date, has_pool_duration,
                      has_sec_duration,
                      history_report_date, issue_amount, remain_amout, cur):
    """获取资产池余额调整因子

    """

    possible_match_countdate = period_end_date
    if_match = True
    prin_adjust_factor = 1
    # 找到与资产池报告日最接近的证券报告日的数据, 并且证券的计息结束日晚于或者等于资产池计息区间结束日的那一个报告期（适用于证券披露频次高于资产端）
    if has_pool_duration:
        sql_near_report_date = tranches_next_date(project_seq, history_date)
        near_report_date = sql_read(sql_near_report_date, cur, is_mysql, single_return=True)  #直接用pd.read_sql也行
        if isnull(near_report_date):
            prin_adjust_factor = 1  # 没有后续的证券端报告，不调整
        elif near_report_date != history_report_date:  # 与资产端最近披露日最接近的并不是最新的证券端报告，即资产端最新披露后不止一次披露证券端报告
            if_match = False
            sql_tranches_last = tranches_duration(project_seq, near_report_date)
            df_tranches_last = sql_read(sql_tranches_last, cur, is_mysql, single_return=False)
            df_tranches_last = df_tranches_last.applymap(lambda x: float('nan') if x is None else x)
            df_tranches_last.loc[:, 'total_payment'] = \
                    df_tranches_last['total_payment'].apply(lambda x: 0 if str(x) in ('None', 'nan') else x)

            last_balance_sum = max(0, issue_amount - sum(df_tranches_last['total_payment']) \
                                    if pd.isna(df_tranches_last['period_end_balance']).any()
                                                  else sum(df_tranches_last['period_end_balance']))

            possible_match_countdate = max(df_tranches_last.loc[:, 'period_end_date']) if len(df_tranches_last) > 0 else interest_start_date
            if last_balance_sum > 0:
                prin_adjust_factor = remain_amout / last_balance_sum

            else:
                prin_adjust_factor = 0  # 债券本金都已经是0, 不需要现金流归集表了
        else:
            # 如果资产端披露之后只有这一期证券端披露日，则无法进行进行调整
            prin_adjust_factor = 1

    else:
        if has_sec_duration:
            # 如果没有存续期资产端报告，则直接根据证券端的剩余金额调整即可
            prin_adjust_factor = remain_amout / issue_amount
            possible_match_countdate = interest_start_date + "(计息起始日)"
            if_match = False

    return prin_adjust_factor, possible_match_countdate, if_match


def sql_read(sql_str, cur, is_mysql=False, single_return=False) -> pd.DataFrame:
    """
    根据sql语句读取数据

    Args:
        sql_str (str): sql 语句
        cur (cursor):
        is_mysql(bool): sql语句为mysql，如果是False则是oracle，需要将 `sql_str` 中不符合oracle语法的进行替代
        single_return: True-返回单个值，False-返pd.DataFrame

    """

    if not is_mysql:
        trans_ = {'ifnull': 'nvl', 'IFNULL': 'nvl'}
        for key_ in trans_:
            sql_str = sql_str.replace(key_, trans_[key_])

    if single_return:
        cur.execute(sql_str)
        value_ = cur.fetchone()
        return_ = value_[0]

    else:
        cur.execute(sql_str)
        value_ = cur.fetchall()
        columns_ = cur.description
        return_ = pd.DataFrame(value_, columns=[x[0].lower() for x in columns_])

    return return_



def isnull(x):
    """
    是否是空值

    Args:
        x: 可以是None, float('nan'), NaT 等

    Returns:
        bool: 是否是空值
    """
    if str(x) in ("None", "nan", "none", "", " ", "NaT", "nat"):
        return True
    else:
        return False
