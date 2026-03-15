# -*- coding: utf-8 -*-


import numpy as np
import pandas as pd
from datetime import timedelta
from abs.prepare.sqltemplate.sqltemplate_basic import *
from utils.timeutils import to_date, count_month, holiday_adjsut, try_to_date
from utils.miscutils import ifnull, isnull
from utils.quick_connect import connect_mysql
from utils.sql_util import sql_read
from doc import enumerators
from doc.global_var import global_var as glv


def load_all_basic(code: str, trade_date: str, cur=None):
    """
    读取项目原始数据

    Args:
        code: str, 项目简称或证券代码，必须带市场后缀
        trade_date: str, 交易日期，带单引号的八位数字
        cur: cursor

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

        1. 输入证券代码，根据证券代码从 `CSI_ABS_BASIC_INFO` 表查询项目内码。（查询数据的SQL模板见 ``sqltemplate`` ，此处不赘述，下同）
        2. 根据项目内码查询项目下所有证券的基本信息（分别从ABS数据库和估值数据库）。

          - 增加`delist`列：对比摘牌日`delist_date`与估值日的大小，如果估值日前已摘牌，`delist`列为 `True` ，否则为 `False` ；全部券均为 `True` ,则不继续运行；
          - 如果项目下没有查到证券，报错并返回信息。
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

        6. 初始入池本金/本息和：补充前述项目基本信息读取中的缺失值,如果没有缺失则不会补 ( ``get_initial_principal`` )：

             - 根据初始现金流归集表计算资产池初始金额，分别记录归集表的首期期初本金（如果是循环购买只用到这个补充）、本金和、本息和。
             - 如果 `guess_mixed`是本息和，若项目基本信息中提取到了入池本息和余额字段，则直接使用，如果没有，则用初始现金流归集表中得到的值、期初本金/本息和/资产池余额字段中较大者进行填充。
             - 如果 `guess_mixed`是仅本金，若项目基本信息中提取到了入池本金余额字段，则直接使用，如果没有，则用初始现金流归集表中得到的值、期初本金/本息和/资产池余额字段中较大者进行填充。

        6 资产池剩余本金：

           - 用证券端最新报告日尝试读取同报告日的资产池存续信息（如果是clo的，一般直接匹配到，不再进行第二步），
           - 读取归集日在证券最新一次计息结束日之前的最新期次资产池存续本金额。（假设了资产端用于支付证券端的回收资金，都是在证券端计息结束日之前就已回收完成。）
           - 额外读取存续期资产池调整表中的资产池余额数据作为“合计”的补充数据，口径根据字段含义确定。
           - 确定统计口径，判断以上两步的并集是否维护了“合计”，如果没有，判断是否存在“正常“。然后将有的那个的口径作为提取数据的口径，再将口径不一致的全部剔除。比如根据判断，需要提取本息和，那么就将口径为仅本金的全部剔除。
           - 从前述两步的合并数据中提取最新资产池剩余本金数据，枚举为“1-合计”，当合计值缺失时，则为除了枚举 '1', '17', '18', '19', '20', '21' （对应为正常类、关注类等，跟逾期违约值属于两种不同的披露方式，故剔除）以外的枚举值的合计；
           - 提取其中枚举值为“违约”的，作为当前的剩余违约金额，如果没有则设为0.
           - 另外提取历史资产池回收金额（枚举值为“合计”）加总，以备计算不良贷款的历史回收率。注意如果报告披露滞后，则及结算的RR会偏小。（口径与前面判断的口径一致）

        7. 判断是否存在证券端和资产端报告错配的可能性：

            * 如果资产端和证券端最新的报告日不同，且资产池的报告期间结束日早于证券的报告期间起始日，则认为是存在错配，进行资产池的调整
            * 如果判断需要调整，则计算调整因子 , 保存在 `df_other` , 计算逻辑为：
            如果存在错配的可能性，则找到与资产池报告日最接近的证券报告日的数据, 并且证券的计息结束日晚于或者等于资产池计息区间结束日的那一个报告期（适用于证券披露频次高于资产端）。
            如果没有这样一期报告，则即使根据规则判断需要调整，调整因子也会是0.
            计算最新证券端报告与这一期本金余额的比值，作为调整比例。

        8. 查询支付顺序数据。
        9. 查询摊还计划数据。
        10. 输出结果。

    ..caution::

        因为现金流归集表维护中增加了支付日列，因此统一将历史归集日改成历史支付日，以避免不匹配。但是需要注意此时，加压参数的匹配会受到影响，因为上一归集日和下一归集日之间的天数变了

    TODO:
        现金流归集表的支付日列可能会存在维护错误

    """

    def get_sec_duration_data():
        """
        读取证券存续期信息

        Returns:
            tuple: tuple contains:
            df_tranches_duration: 证券存续信息
            history_report_date: 最近一次证券分配报告披露日
            sec_payment_date: 证券分配报告披露的最近一次支付日，可以是已经支付了的，也可以是在估值日以后
            period_end_date: 证券分配报告披露的最近一次计息期间结束日
            has_security_duration: 是否进行过证券分配

        """
        sql_security_duration = tranches_duration(project_seq, trade_date)
        df_tranches_duration = sql_read(sql_security_duration, cur, is_mysql, single_return=False)
        has_security_duration = True if len(df_tranches_duration) > 0 else False  # 是否有存续期证券报告

        # 内码不匹配会导致得到的本金余额、本期应付金额不对
        if len(set(df_tranches_duration['security_seq']).difference(df_tranches['security_seq'])) > 0:
            raise ValueError("证券存续信息的证券内码与基本要素中的不匹配，可能是证券基本信息表遗漏证券或者存续期表证券内码错误")

        if len(df_tranches_duration) < len(security_seqs):  # 所有证券都需要有本金余额数据，即使没有存续期还款
            diff = list(set(security_seqs).difference(set(df_tranches_duration['security_seq'])))
            df_tranches_duration = df_tranches_duration.append(pd.DataFrame({'security_seq': diff}))

        df_tranches_duration.sort_values(by=['security_seq'], ignore_index=True, inplace=True)
        df_tranches.sort_values(by=['security_seq'], ignore_index=True, inplace=True)

        if not has_security_duration:
            # 没有存续期报告，则报告日设为计息起始日,并将period_begin_balance 和 period_end_balance设为发行本金额，本期应付本金利息、历史偿付本金历史均为0
            history_report_date = interest_start_date
            period_end_date = interest_start_date
            df_tranches_duration.loc[:, 'period_begin_balance'] = df_tranches['initial_principal']
            df_tranches_duration.loc[:, 'period_end_balance'] = df_tranches_duration.loc[:, 'period_begin_balance']
            df_tranches_duration.loc[:, ['period_begin_date', 'period_end_date']] = history_report_date
            df_tranches_duration.loc[:, ["current_principal_due", "current_interest_due", 'total_payment']] = 0.
            df_tranches_duration.loc[:, 'payment_date'] = history_report_date
        else:

            df_tranches_duration = df_tranches_duration.applymap(lambda x: x if not isnull(x) else float('nan'))
            history_report_date = str(
                int(np.nanmax([float(x) for x in df_tranches_duration["report_date"]])))  # 最近一次证券信息披露日
            period_end_date = str(
                int(np.nanmax([float(x) for x in df_tranches_duration["period_end_date"]])))  # 最近一次证券信息披露的计息区间结束日

            # 维护的时候可能是只维护本金偿付，也可能只维护期末余额，需要补充数据

            df_tranches_duration.loc[:, ['total_payment', 'current_principal_due',
                                         'current_interest_due']] = \
                df_tranches_duration[['total_payment', 'current_principal_due', 'current_interest_due']].fillna(0.)
            # 计息区间起始日结束日为空时表示这只券没有还本付息过，设置历史计息区间为初始起始日
            df_tranches_duration.loc[:, ['period_begin_date',
                                         'period_end_date']] = df_tranches_duration[['period_begin_date',
                                         'period_end_date']].fillna(interest_start_date)
            # 判断当期起始本金余额和当期终止本金余额是否为空，如果为空则通过total_payment计算后补上
            df_tranches_duration.loc[:, "period_end_balance"] = \
                df_tranches_duration["period_end_balance"].fillna( \
                df_tranches['initial_principal'] - df_tranches_duration["total_payment"])
            df_tranches_duration.loc[:, "period_begin_balance"] = df_tranches_duration["period_begin_balance"].fillna( \
                df_tranches_duration['period_end_balance'] + df_tranches_duration["current_principal_due"])

            temp_ = np.array(
                [holiday_adjsut(np.array([to_date(x)]), df_work_schedule, holiday_rule='forward')[0].strftime('%Y%m%d')
                 for x in df_tranches_duration['period_end_date'].values])
            df_tranches_duration.loc[:, 'payment_date'] = temp_  # 根据工作日准则后推计息结束日，避免支付日在节假日

        df_tranches_duration.loc[:, 'has_duration_report'] = df_tranches_duration['total_payment'] > 1

        sec_payment_date = max(df_tranches_duration['payment_date']) if has_security_duration else interest_start_date
        df_tranches_duration.loc[df_tranches_duration.period_end_balance < 0, 'period_end_balance'] = 0
        df_tranches_duration.loc[:, 'clear'] = (df_tranches_duration['period_end_balance'] < 0.1) & \
                                        (df_tranches_duration['payment_date'].astype(int) <= int(trade_date))  # 应付未付的证券不算已完成清算

        return df_tranches_duration, history_report_date, sec_payment_date, period_end_date, has_security_duration

    def get_prediction():
        """获取现金流归集表

        Returns
            tuple: tuple contains:
            df_prediction: 匹配的现金流归集表
            prediction_report_date: 现金流归集表披露日期。如果只有发行期的则等于初始起算日、
            has_prediction_duration(bool): 存续期是否有更新过现金流归集表
        """
        # 考虑到非循环现金流归集表可能存在循环购买和非循环购买的表混合，统一先提1）非循环购买存续期 2）循环购买存续期 3）非循环购买发行期 4）循环购买发行期
        # 1 现金流归集表找证券端最新报告日前（小于等于），归集表报告日最新的一期，以保证用于分配证券端本息的现金流是最合适的一期
        sql_prediction_latest_date = prediction_duration_latest_date_template(project_seq, history_report_date)
        prediction_latest_date = sql_read(sql_prediction_latest_date, cur, is_mysql, single_return=True)

        determine = False
        search_time = 0  # 如果超过一定次数直接查初始现金流归集表(存续期披露风格一般一致）
        watershed = history_report_date
        nomore = False
        while (not determine) and (not nomore):
            if search_time > 5:
                nomore = True
            search_time += 1
            if prediction_latest_date is None:  # (1)非循环购买现金流归集表存续里没有数据，查循环购买存续现金流归集表
                sql_prediction_latest_date = prediction_duration_revolving_latest_date_template(project_seq, watershed)
                prediction_latest_date = sql_read(sql_prediction_latest_date, cur, is_mysql, single_return=True)

                if prediction_latest_date is None:  # (2)存续期表找不到，找发行期非循环购买现金流归集表
                    prediction_report_date = initial_date
                    sql_prediction = prediction_issue_template(
                        project_seq)  # 发行的现金流归集表不截取核算日晚于trade_date的，以避免处于初始起算日和首次归集日之间。
                    df_prediction = sql_read(sql_prediction, cur, is_mysql, single_return=False)
                    nomore = True
                    if len(df_prediction) < 1:  # （3）如果现金流归集表是空的，则查循环购买发行期现金流归集表
                        sql_prediction = prediction_issue_revolving_template(project_seq)
                        df_prediction = sql_read(sql_prediction, cur, is_mysql, single_return=False)
                        prediction_report_date = initial_date
                else:  # （3）在循环购买的存续期表中找到了数据，读取 （需要注意循环购买的表同样只保留与非循环相同的字段，否则会因披露随意，导致现金流混乱）
                    prediction_report_date = prediction_latest_date
                    sql_prediction = prediction_duration_revolving_template(project_seq, prediction_latest_date)
                    df_prediction = sql_read(sql_prediction, cur, is_mysql, single_return=False)
            else:  # （4）在非循环购买的存续表中找到了数据，读取作为现金流归集表
                prediction_report_date = prediction_latest_date
                sql_prediction = prediction_duration_template(project_seq, prediction_latest_date)
                df_prediction = sql_read(sql_prediction, cur, is_mysql, single_return=False)

            # (5) 判断下现金流归集表未来期次是不是为空，如果是则检索更早的现金流归集表（因存在一些无效的现金流归集表，归集的都是历史日期对预测无用）
            future_dates = [dt_ for dt_ in df_prediction['date_'] if
                            int(dt_) >= int(trade_date.strip("'"))]  # 未经过处理的时候现金流归集表保存的是YYYYMMDD日期
            if len(future_dates) > 0:
                determine = True  # 当前读取到的现金流归集表即为最终使用的
            else:  # 回滚读更早的
                if prediction_latest_date is not None:
                    watershed = (to_date(prediction_latest_date) - timedelta(days=10)).strftime('%Y%m%d')
                    sql_prediction_latest_date = prediction_duration_latest_date_template(project_seq, watershed)
                    prediction_latest_date = sql_read(sql_prediction_latest_date, cur, is_mysql, single_return=True)

        has_prediction_duration = True if prediction_latest_date is not None else False
        df_prediction = df_prediction.applymap(lambda x: x if not isnull(x) else float('nan'))  # 早前的有一堆None数据
        return df_prediction, prediction_report_date, has_prediction_duration

    def get_initial_principal():
        """如果入池本金/本息和是空的，获取入池本金/本息和补充信息。另外这个补充过程如有发生则记录到数据问题表中备查
        """
        initial_principal = None
        if guess_mixed:
            if not (isnull(df_product.loc[0, 'initial_principal_interest']) or \
                    (df_product.loc[0, 'initial_principal_interest']) < 1):
                initial_principal = df_product.loc[0, "initial_principal_interest"]

        else:  # 利息明确的类型，此时初始本金只是入池资产，利息额外产生，即入池本金<未来资金流入总和
            # （3）非循环购买入池特征中初始本金为空时
            if not (isnull(df_product.loc[0, 'initial_principal_only']) or \
                    (df_product.loc[0, 'initial_principal_only'] < 1)):
                initial_principal = df_product.loc[0, "initial_principal_only"]

        if initial_principal is not None:
            return initial_principal  # 如果已经读到了直接返回

        # 1 根据初始现金流归集表计算资产池初始金额（可以是仅本金也可以是本息和，同时记录guess_mixed进行区分）
        if not df_product.loc[0, 'is_revolving_pool']:
            sql_initial_pool_balance = initial_cashflow_balance(project_seq)
        else:
            sql_initial_pool_balance = initial_cashflow_rev_balance(project_seq)

        df_initial_pool_balance = sql_read(sql_initial_pool_balance, cur, is_mysql, single_return=False)
        df_initial_pool_balance.fillna(0., inplace=True)
        df_initial_pool_balance.loc[0, 'sum_principal_interest'] = \
            max(df_initial_pool_balance.loc[0, 'sum_principal_interest'],
                df_initial_pool_balance.loc[0, 'sum_principal'] + df_initial_pool_balance.loc[0, 'sum_interest'])

        # 2 确定项目的初始入池本金/本息和
        if guess_mixed:  # 本息和：没有明确的利息一说的ABS类型，eg 应收账款、类REITs，对于非循环类的入池本息和就是理论上以后回收的现金流总和
            # （1） 非循环购买入池特征中本息和为空时，用'初始现金流归集表首期期初本金', '初始现金流归集表当期应收本金利息总和', '初始资产池本金余额'中最大的填充
            if not df_product.loc[0, 'is_revolving_pool']:
                complement_data_label = ['初始现金流归集表首期期初本金', '初始现金流归集表当期应收本金利息总和', '初始资产池本金余额',
                                         "期初本金/本息和/资产池余额字段"]
                complement_data = np.array([df_initial_pool_balance.loc[0, 'max_balance'],
                                            df_initial_pool_balance.loc[0, 'sum_principal_interest'],
                                            df_product.loc[0, 'initial_principal_only'],
                                            df_product.loc[0, 'principal_complement']])
            else:  # （2） 循环池不能用现金流归集表现金流和补，不然会把循环购买的部分重复算进去，只能用“余额”类的字段补充
                complement_data_label = ['初始现金流归集表首期期初本金', '初始资产池本金余额', "期初本金/本息和/资产池余额字段"]
                complement_data = np.array([df_initial_pool_balance.loc[0, 'max_balance'],
                                            df_product.loc[0, 'initial_principal_only'],
                                            df_product.loc[0, 'principal_complement']])
            complement_data[complement_data == None] = 0
            ord = np.argmax(complement_data)
            if complement_data[ord] < 1:
                warns_lst.append(("入池本金", "本息和披露模式下，项目未披露初始本息和, 且无法补充"))
            else:
                warns_lst.append(("入池本金", f"本息和披露模式下，项目未披露初始本息和, 用{complement_data_label[ord]}补充"))
            initial_principal = complement_data[ord]

        else:  # 利息明确的类型，此时初始本金只是入池资产，利息额外产生，即入池本金<未来资金流入总和

            if not df_product.loc[0, 'is_revolving_pool']:
                complement_data_label = ['初始现金流归集表首期期初本金', '初始现金流归集表当期应收本金总和', '证券发行本金总和']
                complement_data = np.array([df_initial_pool_balance.loc[0, 'max_balance'],
                                            df_initial_pool_balance.loc[0, 'sum_principal'],
                                            issue_prin])
            else:
                complement_data_label = ['初始现金流归集表首期期初本金', '证券发行本金总和']
                complement_data = np.array([df_initial_pool_balance.loc[0, 'max_balance'],
                                            issue_prin])
            complement_data[np.isnan(complement_data)] = 0
            ord = np.argmax(complement_data)
            if complement_data[ord] < 1:
                warns_lst.append(("入池本金", "本息和披露模式下，项目未披露初始本金, 且初始现金流归集表无有效信息"))
            else:
                warns_lst.append(("入池本金", f"本息分开披露模式下，项目未披露初始本金, 用{complement_data_label[ord]}补充"))
            initial_principal = complement_data[ord]

        return initial_principal

    def get_pool_duration_data():
        """获取与当前的证券报告最接近的历史服务报告及数据

        Returns:
            tuple: tuple contains:
            pool_remaining_principal: 资产池余额总额
            default_principal: 违约本金余额
            history_date: 最近一次历史归集日
            pool_report_date: 服务报告披露日
            collect_amount: 当期已回收本金/本息和
            duration_mixed: 本金和利息是否不能区分
            has_pool_duration: 是否有存续期服务报告

        """
        # 考虑到交易所的私募债券的资产池情况与证券情况的披露没有严格的先后关系
        # 因此目前采用如下步骤，①用security的history_report_date尝试读取资产池存续信息（即假设同时披露，如果是信贷资产证券化的，一般直接匹配到，不再进行第二步），
        # 若无 ②则读取归集日在证券计息结束日之前（<=）的资产池存续本金额。
        # （因已披露未支付的一期证券偿付不会扣减现金流归集表的资金，即假设证券支付用的只有之前归集的资金，因此以早于最近证券计息结束日的作为未来资产池剩余金额，
        # 否则会导致资产池余额偏小）
        sql_pool_latest = pool_duration(project_seq, history_report_date, period_end_date)
        df_pool_latest = sql_read(sql_pool_latest, cur, is_mysql, single_return=False)
        df_pool_latest.loc[:, 'end_principal_balance'] = df_pool_latest['end_principal_balance'].apply(
            lambda x: 0 if isnull(x) else x)
        df_pool_latest = df_pool_latest[df_pool_latest['end_principal_balance']>0]

        # 读取存续期资产特征中的资产池余额作为备用补充数据
        sql_feature = pool_duration_featuretable(project_seq, history_report_date, period_end_date)
        df_feature_latest = sql_read(sql_feature, cur, is_mysql, single_return=False)

        if len(df_feature_latest) > 0:
            df_temp = pd.DataFrame({'loan_range':
                                        df_feature_latest['indicator_name'].apply(lambda x:
                                                                                  {'102':
                                                                                       enumerators.loan_range_dict['prin'],
                                                                                   '103':
                                                                                       enumerators.loan_range_dict['prin_plus_int']}[x]),
                                    'loan_status': '1',
                                    'end_principal_balance': df_feature_latest['indicator_value'],
                                    'report_date': df_feature_latest['report_date'],
                                    'end_date': df_feature_latest['end_date']},
                                   index=range(len(df_feature_latest)))
            if len(df_pool_latest) < 1 or \
                    (to_date(df_feature_latest.loc[0, 'report_date']) > to_date(df_pool_latest.loc[0, 'report_date'])):
                # 如果特征表的数据比资产池存续表的更新，则直接用更新的数据
                df_pool_latest = df_temp
            elif sum(df_pool_latest['loan_status'].isin([enumerators.loan_range_dict['prin_plus_int'],
                                                         enumerators.loan_range_dict['prin']])) < 1:
                # 如果有一个或以上表示在资产池状态-回收情况中维护过了还剩多少本金/本息和，不需要用资产池特征存续补充。
                # 否则表示维护了一些子科目,仍需要补充合计科目
                df_pool_latest = df_pool_latest.append(df_temp).reset_index()

        has_pool_duration = (len(df_pool_latest) > 0)
        if has_pool_duration:
            # 如果找到的日期跟证券端的计息结束日相差过大, 跟下一个日期的距离很近，则找下一个最近的日期作为已发生过的历史日期以增加资产端和证券端存续期金额匹配的概率
            alternative_date = df_pool_latest.loc[0, 'end_date']
            if (to_date(period_end_date) - to_date(alternative_date)).days // 30 >= 9:  # 阈值
                sql_pool_next = pool_duration_next(project_seq, history_report_date, period_end_date)
                df_pool_next = sql_read(sql_pool_next, cur, is_mysql, single_return=False)
                if len(df_pool_next) > 0:
                    df_pool_next.loc[:, 'end_principal_balance'] = df_pool_next['end_principal_balance'].apply(
                        lambda x: 0 if isnull(x) else x)
                    if (to_date(df_pool_next.loc[0, 'end_date']) - to_date(period_end_date)).days // 30 < 3:
                        df_pool_latest = df_pool_next

        # 取出剩余资产池本金余额（剔除违约）、剩余违约本金额
        default_principal = 0
        duration_mixed = guess_mixed
        if has_pool_duration:
            sum_ = df_pool_latest.loc[df_pool_latest['loan_status'] == '1']  # 合计值

            if len(sum_) == 1:
                duration_mixed = sum_['loan_range'].values[0] == enumerators.loan_range_dict['prin_plus_int']
            elif len(sum_) == 2:

                if len(set(sum_['loan_range'])) == 1:
                    if np.ceil(np.mean(sum_['end_principal_balance'])) == np.ceil(
                            sum_['end_principal_balance'].values[0]):
                        duration_mixed = sum_['loan_range'].values[0] == enumerators.loan_range_dict['prin_plus_int']
                    else:
                        raise ValueError("同一期有两条统计口径一致，但是金额不一样的资产池余额合计")

            elif len(sum_) == 0:


                normal_ = df_pool_latest.loc[df_pool_latest['loan_status'] == '2']  # 正常资产

                if len(normal_) == 1:
                    duration_mixed = normal_['loan_range'].values[0] == enumerators.loan_range_dict['prin_plus_int']

                elif len(normal_) == 2:

                    if len(set(normal_['loan_range'])) == 1:
                        if np.ceil(np.mean(normal_['end_principal_balance'])) == \
                                np.ceil(normal_['end_principal_balance'].values[0]):
                            duration_mixed = normal_['loan_range'].values[0] == enumerators.loan_range_dict[
                                'prin_plus_int']
                        else:
                            raise ValueError("同一期有两条统计口径一致，但是金额不一样的资产池正常余额")
                else:
                    raise ValueError("缺少资产池余额合计值和正常余额数据")

            if duration_mixed:
                df_pool_latest_temp = df_pool_latest.loc[
                                      df_pool_latest['loan_range'] == enumerators.loan_range_dict['prin_plus_int'],
                                      :].reset_index()
            else:
                df_pool_latest_temp = df_pool_latest.loc[
                                      df_pool_latest['loan_range'] == enumerators.loan_range_dict['prin'],
                                      :].reset_index()

            # 资产池余额合计
            df_remaining_principal = df_pool_latest_temp.loc[df_pool_latest_temp['loan_status'] == '1',
                                     :].reset_index()  # 枚举 合计
            supplementPoolRemain = sum(df_pool_latest_temp.loc[~(df_pool_latest_temp['loan_status'].isin(
                ['1', '17', '18', '19', '20',
                 '21'])), 'end_principal_balance'].drop_duplicates())  # 除“合计”以及枚举17-21以外的所有枚举值
            if len(df_remaining_principal) > 0:
                pool_remaining_principal = ifnull(df_remaining_principal.loc[0, 'end_principal_balance'],
                                                  supplementPoolRemain)
            else:
                pool_remaining_principal = supplementPoolRemain

            if isnull(pool_remaining_principal) or pool_remaining_principal < 0:
                pool_remaining_principal = 0

            # 剩余违约本金 ( 不大准确，不同机构违约认定不同，有些将违约90天以上的视作违约）
            df_default_principal = df_pool_latest_temp.loc[df_pool_latest_temp['loan_status'] == '10', :].reset_index()
            if len(df_default_principal) > 0:
                default_principal = ifnull(df_default_principal.loc[0, 'end_principal_balance'], 0.)  # 违约金额

            history_date = df_pool_latest_temp.loc[0, 'end_date']  # 设置最后一个归集日history_date
            pool_report_date = df_pool_latest_temp.loc[0, "report_date"]  # 服务报告披露日

            #  当前已回收本金总额，用历史数据加总，不用入池本金减去当前剩余。以避免对于不良贷款类，高估历史实际流入金额
            sql_collect = history_return_amount(project_seq, "'" + history_date + "'", duration_mixed)
            collect_amount = sql_read(sql_collect, cur, is_mysql, single_return=True)
            collect_amount = 0 if collect_amount is None else collect_amount
        else:
            pool_remaining_principal = initial_principal
            history_date = initial_date  # 没有存续期资金归集则设为初始起算日
            pool_report_date = initial_date
            collect_amount = 0

        return pool_remaining_principal, default_principal, history_date, pool_report_date, collect_amount, duration_mixed, \
               has_pool_duration

    def get_adjust_factor():
        """获取资产池余额调整因子

        """

        possible_match_countdate = period_end_date
        if_match = True
        prin_adjust_factor = 1
        # 找到与资产池报告日最接近的证券报告日的数据, 并且证券的计息结束日晚于或者等于资产池计息区间结束日的那一个报告期
        if has_pool_duration:
            sql_near_report_date = tranches_next_date(project_seq, history_date)
            near_report_date = sql_read(sql_near_report_date, cur, is_mysql, single_return=True)
            if isnull(near_report_date):
                prin_adjust_factor = 1  # 没有相应的证券端报告，不调整
            elif near_report_date != history_report_date:  # 与资产端最近披露日最接近的并不是最新的证券端报告，即资产端最新披露日后不止一次披露证券端分配报告
                if_match = False  # 表示原先读到的资产池和证券端报告因数据披露问题存在错配
                sql_tranches_last = tranches_duration(project_seq, near_report_date)
                df_tranches_last = sql_read(sql_tranches_last, cur, is_mysql, single_return=False)
                df_tranches_last = df_tranches_last.applymap(lambda x: float('nan') if x is None else x)
                df_tranches_last.loc[:, 'total_payment'] = \
                    df_tranches_last['total_payment'].apply(lambda x: 0 if str(x) in ('None', 'nan') else x)

                last_balance_sum = max(0, issue_prin - sum(df_tranches_last['total_payment']) \
                    if pd.isna(df_tranches_last['period_end_balance']).any()
                    else sum(df_tranches_last['period_end_balance']))

                possible_match_countdate = max(df_tranches_last.loc[:, 'period_end_date']) if len(
                    df_tranches_last) > 0 else interest_start_date  # 实际上与资产池服务报告匹配的那一期证券分配报告的计息区间结束日

                if last_balance_sum > 0:
                    prin_adjust_factor = remain_amount / last_balance_sum

                else:
                    prin_adjust_factor = 0  # 债券本金都已经是0, 不需要现金流归集表了
            else:
                # 如果资产端披露之后只有这一期证券端披露日，则无法进行进行调整
                prin_adjust_factor = 1

        else:
            if has_security_duration:
                # 如果没有存续期资产端报告，则直接根据证券端的剩余金额调整即可。反之如果资产池和证券端都没有存续报告，那么就是匹配的
                prin_adjust_factor = remain_amount / issue_prin
                possible_match_countdate = interest_start_date + "(计息起始日)"
                if_match = False

        return prin_adjust_factor, possible_match_countdate, if_match

    warns_lst = []
    contingent_params_dict = {}

    # 0. 连接数据库
    global is_mysql
    close_conn = False
    if cur is None:
        conn, is_mysql = connect_mysql()
        cur = conn.cursor()
        close_conn = True
    else:
        is_mysql = glv().get('is_mysql')  # 是不是mysql那个外网数据库

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
    security_codes = df_tranches['security_code']
    security_seqs = df_tranches['security_seq']
    # 2.2 估值库在ABS项目成立前已有的字段读取
    df_tranches_csi, df_work_schedule = \
        get_csi_data(project_seq, cur, trade_date, dict(zip(security_codes, security_seqs)))

    # 全部摘牌则不再进行分配
    if df_tranches_csi['delist'].all():
        raise ValueError("全部证券均已摘牌，不再估值")

    df_tranches = df_tranches.merge(df_tranches_csi, how='left', on='security_seq')
    issue_prin = df_tranches['initial_principal'].sum()  # 发行总金额

    # 2.3 对于浮息债，需要提取基准利率（假设此后的利率均为当前最新的基准利率，不进行远期利率bootstrap）
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
    interest_start_date = df_product.loc[0, "interest_start_date"]
    initial_date = df_product.loc[0, 'initial_date']
    # 4. 读取证券端余额加到df_tranches,与证券基本信息合并

    df_tranches_duration, history_report_date, sec_payment_date, period_end_date, has_security_duration = \
        get_sec_duration_data()

    df_tranches = df_tranches.merge(df_tranches_duration, on='security_seq', how='left')
    df_tranches.sort_values(by='security_level', inplace=True, ignore_index=True)  # 根据等级排序，通常等级越高支付越优先
    remain_amount = df_tranches['period_end_balance'].sum()

    lack_security = df_tranches[pd.isna(df_tranches['initial_principal'])]['security_code']
    if len(lack_security) > 0:
        warns_lst.append(('基本要素缺失', '证券 %s 未维护基本要素' %lack_security ))

    # 5 获取现金流归集表
    df_prediction, prediction_report_date, has_prediction_duration = get_prediction()

    # 5.2 判断资产池的现金流是本金和利息分开的模式还是本息混在一起的模式
    # todo 根据ABS二级分类确定是否本息和披露
    guess_mixed = \
        (sum(df_prediction['current_interest_due']) < 1) or \
        df_prediction['current_interest_due'].isna().all() # 先假设没有利息的是本息和模式
    # 5.3 补充入池本金/本息和信息.并统一归集到字段initial_principal去，只要口径是跟现金流归集表一致就可以。
    initial_principal = get_initial_principal()
    df_product.loc[0, 'initial_principal'] = initial_principal

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
        has_pool_duration = get_pool_duration_data()
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
            get_adjust_factor()

        warns_lst.append(("数据披露问题", f"根据证券端和资产端存续期披露的日期相差较大"))

    else:
        adjust_factor = 1
        possible_match_date = period_end_date

    # 因为现金流归集表加了一列支付日列，将分配起始日(历史归集日）统一改成证券端的上一个计息结束日，以避免归集额外的资金
    virtual_history_date = sec_payment_date

    if duration_mixed != guess_mixed:  # 现金流归集表和存续期资产服务报告存在潜在的口径矛盾需要提示
        warns_lst.append(("资产池余额", f"项目的现金流归集表显示是否本息和为{guess_mixed}，存续期资产余额则显示为{duration_mixed}"))

    # 6.4 如果既没有存续证券报告也没有存续资产池报告，但是却有摘牌证券，此时的计算的结果必然是错误的。对于这种，默认将期末剩余本金额处理成0.
    if (not (has_pool_duration or has_security_duration)) and df_tranches['delist'].any():
        df_tranches.loc[df_tranches['delist'], 'period_end_balance'] = 0
        warns_lst.append(("数据披露问题", "存在证券摘牌，但是无资产池服务报告或证券分配报告"))

    # 7 支付顺序导入
    sql_sequence = sequence_template(project_seq)
    df_sequence = sql_read(sql_sequence, cur, is_mysql, single_return=False)
    if len(df_sequence) < 1:
        raise ValueError("支付顺序缺失")
    
    # 8 摊还计划导入
    sql_plan = plan_template(project_seq)
    df_plan = sql_read(sql_plan, cur, is_mysql, single_return=False)

    abbr_level_dict = dict(zip(df_tranches['security_seq'], df_tranches['security_level']))
    df_plan.loc[:, 'security_level'] = df_plan.loc[:, 'security_seq'].apply(lambda x: abbr_level_dict[x])

    # 9 日期规则表读取
    sql_date_rule = date_rule_template(list(df_tranches['security_seq']), trade_date) # 证券日期序列
    df_date_rule = sql_read(sql_date_rule, cur, is_mysql, single_return=False)
    if len(df_date_rule) == 0:
        raise ValueError("证券日期缺失")
    else:
        if (df_date_rule[['principal_pay_date', 'accrual_pay_date', 'interest_pay_date']].applymap(lambda x: \
                                                                                                   isnull(x))).any().any():
            raise ValueError("证券日期序列存在None值")

    sql_date_rule_initial = read_initial_date_rule(project_seq)  # 日期规则表
    df_date_rule2 = sql_read(sql_date_rule_initial, cur, is_mysql, single_return=False)

    df_date_rule.loc[:, ['principal_pay_date', 'accrual_pay_date', 'interest_pay_date']] = \
        df_date_rule[['principal_pay_date', 'accrual_pay_date', 'interest_pay_date']].applymap(lambda x: \
            x.read())  # BLOB处理成数组

    # 10 触发事件表
    sql_trigger = trigger_template(project_seq)
    df_trigger = sql_read(sql_trigger, cur, is_mysql, single_return=False)

    # 11 重大事项
    # 影响支付顺序的事件包括 ：加速清偿事件，2、违约事件、3、清仓回购事件、4、强制执行事件 5、提前结束循环期事件；
    # 这里将所有事项都列在event_dict中，但是对于支付顺序无影响的事件或影响很小的、无法量化的事件，实质上不会对现金流有影响，后续不会用到
    sql_event = event_template(project_seq, trade_date)
    df_event = sql_read(sql_event, cur, is_mysql, single_return=False)
    event_dict = enumerators.happend_event_dict
    event_lst = df_event.values.tolist()
    event_keys = event_dict.keys()
    event_happen = [True if str(i) in event_lst else False for i in event_keys]
    events = dict(zip(list(event_dict.values()), event_happen))

    # 12 账户资金余额，兑息日之后最近一次期末账户余额（披露上不大明确，计算中可能不能加入账户余额）
    sql_account_remaining = account_remaining_template(project_seq, "'" + sec_payment_date + "'", trade_date)
    df_account = sql_read(sql_account_remaining, cur, is_mysql, single_return=False)
    if len(df_account) > 0:
        account_remain = df_account.loc[0, 'cash_flow_amount']
    else:
        account_remain = 0

    # 13. 读取历史累计违约率
    df_params = get_param([project_seq.strip("'"), ], trade_date=trade_date, return_method='number', cur=cur)
    cdr = df_params.loc[0, 'cdr'] if len(df_params) > 0 else 0
    cum_loss = cdr * df_product.loc[0, 'initial_principal']  # 已违约总金额，包括当前剩余违约本金和已偿还本金

    # 某些类型下不会披露剩余本金，此时现金流归集表未来期次如果超过0期，则加总作为剩余本金
    if pool_remaining_principal < 0.01:
        future_prediction = df_prediction.loc[df_prediction['date_'] > virtual_history_date, :]
        if len(future_prediction) > 0:
            pool_remaining_principal = max(future_prediction['current_principal_due'].fillna(0.).sum(),
                                           max(future_prediction['begin_principal_balance'].fillna(0.)))

    # 14 如果资产池剩余本金已经是0且最近一次已披露的证券本息也支付完毕则不继续计算，否则还会进行最近这一次本息的分配,如果证券支付日晚于当日则会继续运行 把这一次应付未付的金额付掉
    if pool_remaining_principal * adjust_factor < 0.1 and to_date(sec_payment_date) <= to_date(
            trade_date.strip("'")):
        raise ValueError("经调整因子调整后的资产池剩余本金/本息和为0，无法继续现金流分配")
    elif pool_remaining_principal < 0.1 and to_date(sec_payment_date) <= to_date(trade_date.strip("'")):
        raise ValueError("根据资产服务报告，资产池剩余本金/本息和为0，无法继续现金流分配")


    if close_conn:
        cur.close()
        conn.close()

    df_other = pd.DataFrame(data={
        "has_prediction_duration": [has_prediction_duration], # 是否有存续期现金流归集表
        "has_pool_duration": [has_pool_duration], # 是否有资产池存续期报告
        "has_security_duration": [has_security_duration], # 是否有证券端存续期报告
        "prediction_report_date": [prediction_report_date], # 现金流归集表最新披露日
        "pool_report_date": [pool_report_date], # 资产池最新存续报告日
        "history_report_date": [history_report_date], # 证券端最新存续报告日
        "history_date": [history_date], # 最新归集日
        "history_payment_date": [sec_payment_date], # 最近一次支付日
        "history_count_date": [period_end_date],
        "remaining_principal": [float(pool_remaining_principal)], # 资产池剩余资金
        "remaining_principal_no_adjust": [float(pool_remaining_principal)],  # 资产池剩余资金
        "history_report_mismatch": [adjust_pool_principal],  # 是否存在资产服务报告和证券分配报告的期次错配
        "adjust_factor": [adjust_factor], # 资产池调整因子
        "virtual_history_date": [virtual_history_date], # 现金流归集表从这一天开始截取
        "possible_match_count_date": [possible_match_date], # history_date 更可能匹配的证券计息区间结束日
        "mixed_principal": [guess_mixed], # 资产池剩余是否本息和形式
        "mixed_certainty": [guess_mixed == duration_mixed],
        "default_principal_no_adjust": [default_principal],
        "default_principal": [default_principal], # 当前资产池中违约资金金额
        "CDR": [cdr],
        "cum_loss": [cum_loss],
        "account_remain": [account_remain],
        "collect_amount": [collect_amount]
    }, index=[0])

    return df_product, df_tranches, df_prediction, df_sequence, df_plan, df_other, df_trigger, events, df_date_rule,\
            contingent_params_dict, security_seq, project_seq, warns_lst, df_date_rule2, df_work_schedule


def get_csi_data(project_seq, cur, trade_date, security_seqs):
    """
    专门用于在建立ABS数据表之前，估值库已有字段的获取，对于未上市的券也在在此获取

    Args:
        project_seq: 项目序号
        cur:
        trade_date:
        security_seqs(dict): key-证券代码，value-证券内码

    Returns:

    TODO:
        mysql数据库下默认设置为不是利随本清和非累进利率，因为数据库里没有数据表

    """
    sql_cb_seq = get_cb_seq(project_seq)  # 估值库内码
    df_cb_seqs = sql_read(sql_cb_seq, cur, is_mysql, single_return=False)

    df_tranches_csi = pd.DataFrame(columns=['security_code', 'security_seq'])

    cb_seq_list = list(df_cb_seqs['cb_seq'])
    if len(cb_seq_list) > 0:
        if is_mysql:
            sql_tranches_csi = tranches_csi_template(cb_seq_list, 'bond')  # 有个字段在估值库的另一个user下，在外网数据库则是在一个下
            df_tranches_csi = sql_read(sql_tranches_csi, cur, is_mysql, single_return=False)

            df_tranches_csi['int_pay_with_prin'] = False
            df_tranches_csi['if_progressive_rate'] = False

        else:
            sql_tranches_csi = tranches_csi_template(cb_seq_list, 'csi_bond_bs')
            df_tranches_csi = sql_read(sql_tranches_csi, cur, is_mysql, single_return=False)
            sql_how_intcount = pay_with_principal(cb_seq_list)
            df_how_intcount = sql_read(sql_how_intcount, cur, is_mysql, single_return=False)
            df_how_intcount['int_pay_with_prin'] = df_how_intcount['int_pay_with_prin'] == '1'  # 是否利随本清
            df_tranches_csi = df_tranches_csi.merge(df_how_intcount, on='cb_seq', how='left')
            sql_rate = progressive_rates(cb_seq_list)
            df_rate = sql_read(sql_rate, cur, is_mysql, single_return=False)
            df_tranches_csi.loc[:, 'if_progressive_rate'] = False  # 是否利随本清
            df_tranches_csi.loc[:, 'progressive_rates'] = None
            df_rate.sort_values(by=['cf_date', 'cb_seq'], inplace=True)
            for i in range(len(df_tranches_csi)):
                df_rate_bond = df_rate[df_rate['cb_seq'] == df_tranches_csi.loc[i, 'cb_seq']]
                if not (df_rate_bond['coupon_rate'] == df_rate_bond['coupon_rate'].mean()).all() and \
                        df_tranches_csi.loc[i, 'if_progressive_rate']:
                    df_tranches_csi.loc[i, 'if_progressive_rate'] = True

                    df_shift = df_rate_bond[df_rate_bond['coupon_rate'].values - \
                                                         df_rate_bond['coupon_rate'].shift(-1).values!= 0]
                    df_shift.loc[:, 'coupon_rate'] = df_shift['coupon_rate'] / 100
                    df_tranches_csi.loc[i, 'progressive_rates'] = ",".join(df_shift['cf_date']) + ";" + \
                                                                  ",".join(df_shift['coupon_rate'].astype(str))

        # 处理债券代码为带后缀的格式
        df_tranches_csi.loc[:, 'security_code'] = \
            df_tranches_csi[['security_code', 'trade_market']].apply(
                lambda row: str(row[0]) + "." + enumerators.market_suffix[str(row[1])], axis=1)

        try:  # 将估值库信息与ABS基本信息进行匹配，如果没有对应的security_seq,说明估值库里维护了信息的证券在ABS基本信息表有误
            df_tranches_csi.loc[:, 'security_seq'] = [security_seqs[x] for x in df_tranches_csi['security_code']]
        except KeyError as e:
            str_ = ",".join(list(set(df_tranches_csi['security_code']).difference(set(security_seqs.keys()))))
            raise KeyError(f"CSI_ABS_BASIC_INFO 表遗漏证券基本信息，遗漏证券为 {str_},"
                           f"(也可能由不同项目下证券用了同一个项目内码导致，请检查CSI_BOND_ABS_BOND_INFO表中项目内码为{project_seq}的证券)")

    # 部分未上市的证券是在估值库没有，而ABS基本信息中有，此时从补充维护的未上市ABS证券信息表里面读取相关数据
    if len(security_seqs) > len(cb_seq_list):
        try:
            sql_unlist = tranches_csi_template_unlist(list(security_seqs.values()))
            df_tranches_supple = sql_read(sql_unlist, cur, is_mysql, single_return=False)
            df_tranches_csi = df_tranches_csi.append(df_tranches_supple)
            df_tranches_csi.loc[:, 'int_pay_with_prin'] = False
            df_tranches_csi.loc[:, 'if_progressive_rate'] = False
        except:
            print("数据库缺少未上市券表") # 不是每个数据库都有未上市券表
            pass

    # 如果再未上市券信息表里面也缺了这个券，就报错
    lack_info = set(security_seqs.values()).difference(set(df_tranches_csi['security_seq']))
    if len(lack_info) > 0:
        raise ValueError(f"证券{lack_info}未维护估值信息和未上市券信息表")

    df_tranches_csi.loc[:, 'int_pay_with_prin'] = df_tranches_csi.loc[:, 'int_pay_with_prin'].fillna(False)

    df_tranches_csi.loc[:, 'delist'] = [True if (not isnull(x)) and (int(trade_date.strip("'")) > int(x)) else False
                                 for x in df_tranches_csi['delist_date']] # 是否摘牌

    df_tranches_csi.loc[:, 'expire'] = [True if (not isnull(x)) and (int(trade_date.strip("'")) > int(x)) else False
                                 for x in df_tranches_csi['legal_maturity_date']]  # 是否已到预期到期日

    df_tranches_csi.drop(columns=['security_code', 'cb_seq'], inplace=True)

    # 工作日日历导入
    df_work_schedule = None
    if not is_mysql:  # 只有估值库有
        start_date = '19000101'
        end_date = '30000101'
        sql_schedule = date_schedule(start_date, end_date)
        df_work_schedule = sql_read(sql_schedule, cur, is_mysql, single_return=False)
        df_work_schedule.drop_duplicates(subset=['date_'], keep='last', inplace=True, ignore_index=True)
        df_work_schedule.loc[:, 'date_'] = df_work_schedule['date_'].apply(try_to_date)
        df_work_schedule.dropna(how='any', inplace=True)
        df_work_schedule.loc[:, 'is_workday'] = df_work_schedule['is_workday'] == '1'
        df_work_schedule.sort_values(by='date_', ignore_index=True, inplace=True)

    return df_tranches_csi, df_work_schedule


def get_param(project_seqs, trade_date, return_method='series', cur=None):
    """
    获取历史参数值，包括CDR、RR、CPR等，先用 ``get_history_param`` 数据库中的数据，如果没有数据，或者在list中有多个项目内码为空时（超过一半），就用 ``cal_params`` 采用一定的规则计算

    Args:
        project_seqs (list): 可以同时获取多个项目的参数
        trade_date (str): 提取数据的日期
        return_method (str): series-返回历史数据的序列
                             number-返回最新数据
        cur (cursor):

    Returns:
        pd.DataFrame: df_history_param, 列名是参数名 + 项目内码 project_seq + 归集日期 date_
    """
    if_close = False
    if cur is None:
        conn, is_mysql = connect_mysql()
        cur = conn.cursor()
        if_close = True
    else:
        is_mysql = glv().get('is_mysql')

    sql_params = get_history_param(project_seqs, trade_date)
    df_history_param = sql_read(sql_params, cur, is_mysql, single_return=False)
    df_history_param.dropna(how='all', axis=0, inplace=True)

    if len(df_history_param) < 1 or len(set(df_history_param['project_seq'])) / len(project_seqs) < 0.5:
        # TODO 参数表数据到位后删除，应该在数据库中统一计算好
        df_history_param = cal_params(project_seqs, trade_date, cur)

    else:
        df_history_param.loc[:, ['date_', 'initial_date']] = df_history_param[['date_', 'initial_date']].applymap(to_date)
        df_history_param.loc[:, 'age'] = \
            df_history_param[['date_', 'initial_date']].apply(
                lambda row: count_month(row['initial_date'], row['date_']), axis=1)

        df_history_param.loc[:, 'months'] = df_history_param['age'].diff()  # 两次归集日之间的月数
        df_history_param['months'].fillna(df_history_param['age'], inplace=True)
        df_history_param.loc[:, 'smm'] = df_history_param['smm'].fillna(0.)
        df_history_param.loc[:, 'smdr'] = df_history_param['smdr'].fillna(0.)
        df_history_param.loc[:, 'cdr'] = df_history_param['cdr'].ffill().fillna(0.)
        df_history_param.loc[:, 'rr'] = df_history_param['rr'].fillna(0.)
        df_history_param.loc[:, 'ucpr'] = df_history_param['ucpr'].ffill().fillna(0.)
        df_history_param.loc[:, 'cpr'] = 1 - np.power((1 - df_history_param['smm']), 12 / df_history_param['months'])  # 年化早偿率
        df_history_param.loc[:, 'ycdr'] = 1 - np.power((1 - df_history_param['smdr']), 12 / df_history_param['months'])  # 年化违约率

    if len(df_history_param) < 1:
        return df_history_param

    if if_close:
        cur.close()
        conn.close()

    if return_method == 'series':
        return df_history_param
    else:
        return df_history_param.loc[df_history_param.groupby('project_seq')['age'].idxmax(), :].reset_index(drop=True)


def unpack_params(df_history_param):
    """
    将原先保存在同一个表的历史数据，转化为几个DataFrame表格，index为账龄，columns为项目内码，value为参数值

    Args:
        df_history_param (pd.DataFrame): ``get_param`` 中输出的结果参数数据

    Returns:
        tuple: tuple contains:
            df_cdr (pd.DataFrame): 累计违约率，累计违约/初始本金额，index-age（月）, columns-project_seq
            df_ucpr (pd.DataFrame): 无条件年化早偿率
            df_cpr (pd.DataFrame): 条件早偿率
            df_rr (pd.DataFrame): 回收率

    """
    if len(df_history_param) > 0:

        df_history_param.loc[:, 'age'] = df_history_param['age'].apply(lambda x: np.round(x))
        max_age = int(max(df_history_param['age']))
        # 可能会因账龄计算的不准确有重复，舍弃重复值
        df_history_param.drop_duplicates(subset=['project_seq', 'age'], keep='last', inplace=True)

        df_cdr = pd.DataFrame(index=range(0, max_age + 1))  # 生成index是账龄，因为左闭右开所以+1
        df_cpr = pd.DataFrame(index=range(0, max_age + 1))
        df_ucpr = pd.DataFrame(index=range(0, max_age + 1))
        df_rr = pd.DataFrame(index=range(0, max_age + 1))
        df_cdr = df_cdr.merge(pd.pivot_table(df_history_param, index=['age', 'project_seq'])['cdr'].unstack(
            level=-1), how='left', left_index=True, right_index=True)
        df_ucpr = df_ucpr.merge(pd.pivot_table(df_history_param, index=['age', 'project_seq'])['ucpr'].unstack(
            level=-1), how='left', left_index=True, right_index=True)
        df_rr = df_rr.merge(pd.pivot_table(df_history_param, index=['age', 'project_seq'])['rr'].unstack(
            level=-1), how='left', left_index=True, right_index=True)
        df_cpr = df_cpr.merge(pd.pivot_table(df_history_param, index=['age', 'project_seq'])['cpr'].unstack(
            level=-1), how='left', left_index=True, right_index=True)

        # 1. 第0期的参数均为0
        df_ucpr.loc[0, :] = 0
        df_cpr.loc[0, :] = 0
        df_cdr.loc[0, :] = 0

        # 2. 参数理论上不能超过1，也不应该小于0,如果有则剔除，用最近一期的值补上
        df_ucpr[df_ucpr > 1] = float('nan')
        df_cpr[df_cpr > 1] = float('nan')
        df_cdr[df_cdr > 1] = float('nan')
        df_rr[df_rr > 1] = float('nan')

        df_ucpr[df_ucpr < 0] = float('nan')
        df_cdr[df_cdr < 0] = float('nan')
        df_cpr[df_cpr < 0] = float('nan')
        df_rr[df_rr < 0] = float('nan')

        # 3. 补空值，出于简单考虑采用向后填充
        df_ucpr.bfill(inplace=True)
        df_cdr.bfill(inplace=True)
        df_cpr.bfill(inplace=True)
        df_rr.bfill(inplace=True)

        # 4. 将cdr转化为mdr再转化为cdr，以保证cdr单调递增
        mdrs = df_cdr - df_cdr.shift(1)
        mdrs[(mdrs > 0.02) | (mdrs < -1e-4)] = float('nan')
        df_cdr = mdrs.cumsum()
        df_cdr.bfill(inplace=True)

        # 5. 如果最后有一些空行，则去掉
        df_ucpr.dropna(how='all', inplace=True, axis=0)
        df_cdr.dropna(how='all', inplace=True, axis=0)
        df_cpr.dropna(how='all', inplace=True, axis=0)
        df_rr.dropna(how='all', inplace=True, axis=0)
        return df_cdr, df_ucpr, df_cpr, df_rr

    else:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


def cal_params(project_seqs, trade_date, cur=None):
    """
    计算历史违约、早偿、回收率等参数

    Args:
        project_seqs (list): 项目内吗的列表
        trade_date (str): 日期

    Returns:
        pd.DataFrame: df_history_param, 列名是参数指标名 + 项目内码 project_seq + 归集日期 date_


    **公式** ::

        先读取各期的，期末违约本金余额、违约回收金额、早偿金额、期末资产池剩余本金金额、当期总回收本金额、初始本金余额等数据
        CDR = （当期违约本金余额 + 历史违约回收总额） / 初始本金
        RR = 当期及过去几期违约回收总额 / （期末违约本金余额 + 当期及过去几期违约回收总额）
        SMM = 当期早偿回收 / (期末资产池剩余本金金额 + 当期总回收本金额)   # 相当于当期早偿回收 / 期初资产池本金余额
        CPR = 1-(1 - SMM) ^ （1/两个归集日期之间的年化天数)
        SMDR = 当期违约金额 / (期末资产池剩余本金金额 + 当期总回收本金额)
        YCDR = 1-(1 - SMDR) ^ (1/两个归集日期之间的年化天数)
        UCPR = 1-(1 - 当期早偿回收 / 初始本金) ^ (1/两个归集日期之间的年化天数)
        USMM = 当期早偿回收 / 初始本金

    另外，其中的违约余额、违约回收额、早偿额等数据均由 ``GetRawPoolHistoryData`` 根据枚举值提取，这里用到的枚举如下（枚举含义见枚举表）：

        * 违约本金余额： ('7', '8', '9', '13', '22', '23', '25', '28')， 如果得到的结果是空的，用枚举为 ('7', '8', '9', '13', '22', '23', '25', '10') 的数据替代 ，这里的多个枚举指的是枚举值对应的资产池余额状态都可以视作，比如说维护的一些逾期90天、逾期150天、违约都被看作是违约本金余额。并且这里假设了这些枚举值对应的本金余额不会产生交叉
        * 早偿本金余额：['5']， 如果为空则用 ('17', '18') 替代
        * 违约回收金额： ('4', '6', '21')

    """
    close_conn = False
    if cur is None:
        conn, is_mysql = connect_mysql()
        cur = conn.cursor()
        close_conn = True
    else:
        is_mysql = glv().get('is_mysql')
    trade_date = "'" + str(trade_date.strip("'")) + "'"
    # 1. 资产池存续、资产池回收数据
    default_balance_range = ('7', '8', '9', '13', '22', '23', '25', '28')  # 剩余违约本金枚举
    prepay_range = ['5']  # 早偿枚举
    default_return_range = ('4', '6', '21')  # 违约回收
    sql_history_data = GetRawPoolHistoryData(project_seqs, default_balance_range, default_return_range,
                                             prepay_range, trade_date)
    df_history_data = sql_read(sql_history_data, cur, is_mysql, single_return=False)

    # 补充性数据, 应对合并后字典值中，部分含义存在重复（由两个字典合并导致） 或者利用总项和分项具有互补的特性补充数据
    default_balance_range_supple = ('7', '8', '9', '13', '22', '23', '25', '10')
    default_return_range_supple = ('4', '6', '21')
    prepay_range_supple = ('17', '18')
    sql_history_data_supple = GetRawPoolHistoryData(project_seqs, default_balance_range_supple,
                                                    default_return_range_supple, prepay_range_supple, trade_date)
    df_history_data_supple = sql_read(sql_history_data_supple, cur, is_mysql, single_return=False)
    merge_history_data = df_history_data.merge(df_history_data_supple, left_on=['project_seq', 'date_'],
                                               right_on=['project_seq', 'date_'], how='outer',
                                               suffixes=('', '_supple'))
    merge_history_data = merge_history_data.applymap(lambda x: float('nan') if isnull(x) else x)

    merge_history_data.loc[:, 'default_balance'] = \
        merge_history_data['default_balance'].fillna(merge_history_data['default_balance_supple'])
    merge_history_data.loc[:, 'default_return'] = \
        merge_history_data['default_return'].fillna(merge_history_data['default_return_supple'])
    merge_history_data.loc[:, 'prepay'] = merge_history_data['prepay'].fillna(merge_history_data['prepay_supple'])
    merge_history_data.loc[:, 'initial_principal'] = \
        merge_history_data['initial_principal'].fillna(merge_history_data['initial_principal_supple'])
    merge_history_data.loc[:, 'initial_date'] = \
        merge_history_data['initial_date'].fillna(merge_history_data['initial_date_supple'])
    merge_history_data.loc[:, 'end_principal_balance'] = \
        merge_history_data['end_principal_balance'].fillna(merge_history_data['end_principal_balance_supple'])
    merge_history_data.loc[:, 'all_collection'] = \
        merge_history_data['all_collection'].fillna(merge_history_data['all_collection_supple'])

    merge_history_data.sort_values(by=['project_seq', 'date_'], ignore_index=True, inplace=True)

    merge_history_data.loc[:, 'default_balance'] = \
        merge_history_data.groupby('project_seq')['default_balance'].ffill()  # 剩余违约金额如果没有则用上一期的填充，保证违约率单调递增。

    merge_history_data.loc[:, ['prepay', 'default_return']] = merge_history_data[['prepay', 'default_return']].fillna(0.)

    # 2. 计算参数
    df_history_param = pd.DataFrame(
        columns=['project_seq', 'date_', 'initial_date', 'initial_principal', 'cdr', 'ucpr', 'smm', 'mdr', 'rr'])

    if len(merge_history_data) < 1:
        return df_history_param

    df_history_param['cdr'] = \
        (merge_history_data.groupby('project_seq')['default_return'].cumsum() + merge_history_data[
            'default_balance']) / merge_history_data['initial_principal']  # 累计违约率

    df_history_param['rr'] = merge_history_data.groupby('project_seq')['default_return'].cumsum() / \
                             (merge_history_data.groupby('project_seq')['default_return'].cumsum() +
                              merge_history_data['default_balance'])  # 违约回收率

    df_history_param[['project_seq', 'date_', 'initial_date', 'initial_principal']] = merge_history_data[
        ['project_seq', 'date_', 'initial_date', 'initial_principal']]  # initial_date初始起算日，date_历史归集日
    df_history_param.loc[:, ['date_', 'initial_date']] = df_history_param[['date_', 'initial_date']].applymap(to_date)
    df_history_param['age'] = \
        df_history_param[['date_', 'initial_date']].apply(lambda row: count_month(row['initial_date'], row['date_']),
                                                          axis=1)  # 存续月数计算（初始起算日开始）

    df_history_param['months'] = df_history_param['age'].diff()
    df_history_param['months'].where(df_history_param['months'] > 0, np.nan, inplace=True)
    df_history_param['months'].fillna(df_history_param['age'], inplace=True)
    smm = merge_history_data['prepay'] / (
                merge_history_data['all_collection'] + merge_history_data['end_principal_balance'])
    df_history_param['smm'] = smm
    df_history_param['cpr'] = 1 - (1 - smm) ** (1 / df_history_param['months'])
    total_default = merge_history_data.groupby('project_seq')['default_return'].cumsum() + \
                              merge_history_data['default_balance']
    df_history_param['mdr'] = (total_default - total_default.shift(1)) / (
                merge_history_data['all_collection'] + merge_history_data['end_principal_balance'])  # 边际无条件违约率

    df_history_param['ycdr'] = 1 - (1 - df_history_param['mdr']) ** (1 / df_history_param['months'])

    df_history_param['usmm'] = merge_history_data['prepay'] / merge_history_data['initial_principal']
    df_history_param['ucpr'] = \
        1 - (1 - df_history_param['usmm']) ** (1 / df_history_param['months'])# 无条件早偿率

    df_history_param.dropna(subset=['date_'], how='any', inplace=True)
    df_history_param.loc[:, ['initial_principal', 'initial_date']].fillna(method='ffill', inplace=True)
    df_history_param.columns = [x.lower() for x in df_history_param.columns]

    if close_conn:
        cur.close()
        conn.close()

    return df_history_param