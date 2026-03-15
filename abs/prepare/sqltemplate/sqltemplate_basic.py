# -*- coding: utf-8 -*-

"""
SQL模板

Notes:
    * docstring中字段名称的大小写没有严格区分，不影响阅读
    * 后续的计算中会统一将列名全部转为小写
    * 对字段的描述中以格式 " 字段含义 + 字段在数据库的名称 （ + AS + 字段在程序中使用时的名称 ）"

"""
from doc.enumerators import loan_range_dict


def get_seqs(code):
    """通过证券代码找到表格 ``CSI_ABS_BASIC_INFO`` 中对应的项目内码和证券内码

    """
    s = """
        SELECT PROJECT_SEQ, SECURITY_SEQ
        FROM CSI_ABS_BASIC_INFO
        WHERE SECURITY_CODE = '%s'
        """ % code
    return s


def get_cb_seq(project_seq):
    """获取项目内码下对应的估值库内码

    """
    s = """SELECT CB_SEQ
    FROM CSI_BOND_ABS_BOND_INFO
    WHERE PROJECT_SEQ = '%s'
    """ % project_seq
    return s


def product_template(project_seq):
    """
    通过项目内码 ``project_seq`` 获取基本要素数据, 包括:

                * `csi_abs_project_basic_info` 表中的:

                    * 项目简称 `project_abbr`
                    * 初始起算日 `initial_date`
                    * 计息起始日 `interest_start_date`
                    * 首次付息日 `first_pay_date`
                    * 法定到期日 `legal_due_date`
                    * 是否循环购买 `is_recurring_pool` as `is_revolving_pool`
                    * 一级分类 `primary_classification`
                    * 二级分类 `secondary_classification`
                    * 循环购买结束日 `revolving_expiry_date`
                    * 循环购买起始日 `revolving_purchase_date`

                * `csi_abs_assetpool_in_feature` 表中的:

                    * 入池本金余额: `indicator_name` 字段枚举为102的 as `initial_principal_only`
                    * 入池本息和余额: `indicator_name` 字段枚举为103的 as `initial_principal_interest`
                    * 入池本金/本息和余额备用字段: `indicator_name` 字段枚举为50的 as `principal_complement`

    """
    s = '''
        select
            bi.project_abbr, bi.project_seq, bi.initial_date, bi.interest_start_date, 
            bi.first_pay_date, bi.legal_due_date, 
            bi.is_recurring_pool as is_revolving_pool, bi.primary_classification, 
            bi.secondary_classification, bi.revilving_period_in_month as revolving_period_in_month,
            bi.revilving_expiry_date as revolving_expiry_date, bi.s_revilving_purchase_date as start_revolving_purchase_date,
            ai.indicator_value as initial_principal_only, ci.indicator_value as initial_principal_interest, 
            di.indicator_value as contract_principal_only,
            ei.indicator_value as principal_complement
        from csi_abs_project_basic_info bi
        left join(select project_seq, indicator_value 
                 from csi_abs_assetpool_in_feature
                 where indicator_name= '102'
                 and project_seq = {0}) ai
        on bi.project_seq = ai.project_seq
        left join(select project_seq, indicator_value 
                 from csi_abs_assetpool_in_feature
                 where indicator_name= '103'
                 and project_seq = {0}) ci
        on bi.project_seq = ci.project_seq
        left join(select project_seq, indicator_value 
                 from csi_abs_assetpool_in_feature
                 where indicator_name= '100'
                 and project_seq = {0}) di
        on bi.project_seq = di.project_seq
        left join (select project_seq, indicator_value
            from csi_abs_assetpool_in_feature
            where indicator_name = '50'
            and project_seq = {0}) ei
        on bi.project_seq = ei.project_seq
        where bi.project_seq = {0}           
        '''.format("'" + project_seq.strip("'") + "'")

    return s


def tranches_template(project_seq):
    """
    通过项目内码 ``project_seq`` 获取证券要素数据, 包括:

            * `csi_bond_abs_bond_info` 表的:

                * 证券等级 `security_level`
                * 摊还方式 `amort_type`,
                * 非循环购买付息频率 `pay_interest_frequency`
                * 非循环购买还本频率 `pay_principal_frequency`
                * 次级固定资金成本 `sub_fixed_capital_cost`
                * 循环期付息频率 `rev_pay_interest_frequency`
                * 摊还期付息频率 `amo_pay_interest_frequency`
                * 循环期还本频率 `rev_pay_principal_frequency`
                * 摊还期还本频率 `amo_pay_principal_frequency`

    """

    s = '''
        SELECT
            PROJECT_SEQ, SECURITY_LEVEL, SECURITY_SEQ, SECURITY_CODE,
            AMORT_TYPE, PAY_INTEREST_FREQUENCY, PAY_PRINCIPAL_FREQUENCY, 
            SUB_FIXED_CAPTIAL_COST AS SUB_FCC,
            REV_PAY_INTEREST_FREQUENCY,
            REV_PAY_PRINCIPAL_FREQUENCY,
            AMO_PAY_INTEREST_FREQUENCY,
            AMO_PAY_PRINCIPAL_FREQUENCY
        FROM CSI_ABS_BASIC_INFO
        WHERE PROJECT_SEQ = {0}
        '''.format("'" + project_seq.strip("'") + "'")

    return s


def tranches_csi_template(cb_seq_list, db_name):
    """
    通过 ``list(cb_seq)`` 找到项目下所有证券的如下数据:

                * `CSI_BOND_STOCK` 表的:

                    * 利率类型 `CASHING_METHOD` AS `INTEREST_TYPE`
                    * 固定利率 `COUPON_RATE` AS `FIXED_RATE`
                    * 浮动基准类型 `BASE_RATE_TYPE` AS `FLOATING_RATE_TYPE`
                    * 利率基准期限 `BASE_RATE_PERIOD` AS 'FLOATING_BENCHMARK_PERIOD'
                    * 预期到期时间 `EXPIRY_DATE` AS 'LEGAL_MATURITY_DATE'
                    * 摘牌时间 `DELIST_DATE`
                    * 固定利差 `FIXED_SPREAD` AS 'FLOATING_RATE_SPREAD`
                    * 最新张数 `ISSUED_VOL` AS `CURRENT_VOL`  # TODO
                    * 计息基准 `ACCRUAL_DCC` as `daycount`

                `CSI_BOND_STOCK_CODE` 表的:

                    * 证券代码  'BOND_CODE' AS `SECURITY_CODE`
                    * 交易场所 `MARKET` AS `TRADE_MARKET`

                `CSI_BOND_NEWLISTING` 表的：

                    * 发行总额 `INITIAL_PRINCIPAL=ISSUED_VOL*FACE_VALUE`


    Notes:
        估值库中 `CSI_BOND_STOCK` 跟其他数据表不在一个database

    TODO:
        需要建表保存历史张数，然后改这里的CURRENT_VOL提取方式。否则网页回溯计算的结果会是错的
    """

    s = '''
        SELECT
            A.CB_SEQ, A.CASHING_METHOD AS INTEREST_TYPE, C.INITIAL_PRINCIPAL, A.ISSUED_VOL as CURRENT_VOL,
            C.ISSUED_VOL, A.FACE_VALUE as CURRENT_FACE,
            A.ACCRUAL_DCC AS DAYCOUNT, 
            A.COUPON_RATE AS FIXED_RATE, A.BASE_RATE_TYPE AS FLOATING_RATE_TYPE, 
            A.BASE_RATE_PERIOD AS FLOATING_BENCHMARK_PERIOD, A.EXPIRY_DATE AS LEGAL_MATURITY_DATE,
            A.DELIST_DATE,
            A.FIXED_SPREAD AS FLOATING_RATE_SPREAD, B.BOND_CODE AS SECURITY_CODE, B.MARKET as trade_market
        FROM {1}.CSI_BOND_STOCK A
        LEFT JOIN (SELECT CB_SEQ, BOND_CODE, MARKET
                   FROM CSI_BOND_STOCK_CODE
                   WHERE CB_SEQ IN {0}) B
        ON A.CB_SEQ = B.CB_SEQ
        LEFT JOIN (SELECT CB_SEQ, ISSUED_VOL*ISSUED_PRICE as INITIAL_PRINCIPAL, ISSUED_VOL
                    FROM CSI_BOND_NEWLISTING
                    WHERE CB_SEQ IN {0}) C
                    ON A.CB_SEQ = C.CB_SEQ
        WHERE A.CB_SEQ IN {0}
        '''.format(cb_seq_list, db_name.strip("'")).replace("[", "(").replace("]", ")")

    return s


def pay_with_principal(cb_seqs):
    """是否利随本清

                `CSI_BOND_PAY_PERIOD` 表的

                    * 是否利随本清 `ISCLEARBYPOWER`

    """

    s = """ SELECT CB_SEQ, max(ISCLEARBYPOWER) as int_pay_with_prin
    FROM CSI_BOND_PAY_PERIOD
    WHERE CB_SEQ IN {0}
    GROUP BY CB_SEQ
    """.format(cb_seqs).replace("[", "(").replace("]", ")")
    return s

def tranches_csi_template_unlist(security_seq_list):
    """
    未上市券信息读取

    """
    s = '''
           SELECT
               A.SECURITY_SEQ, A.CASHING_METHOD AS INTEREST_TYPE,  ISSUED_VOL*FACE_VALUE AS INITIAL_PRINCIPAL,
               A.ISSUED_VOL AS CURRENT_VOL, A.ISSUED_VOL, A.FACE_VALUE AS CURRENT_FACE,
               A.ACCRUAL_DCC AS DAYCOUNT,
               A.COUPON_RATE AS FIXED_RATE, A.BASE_RATE_TYPE AS FLOATING_RATE_TYPE, 
               A.BASE_RATE_PERIOD AS FLOATING_BENCHMARK_PERIOD, A.EXPIRY_DATE AS LEGAL_MATURITY_DATE,
               A.DELIST_DATE,
               A.FIXED_SPREAD AS FLOATING_RATE_SPREAD
           FROM CSI_BOND_ABS.CSI_UNLISTING_ABS_STOCK A
           WHERE A.SECURITY_SEQ IN {0}
           '''.format(security_seq_list).replace("[", "(").replace("]", ")")
    return s


def baserate_template(floating_benchmark, trade_date):
    """
    输入基准利率期限和日期，从 ``CSI_BOND_BASE_RATE`` 表找到浮动利息证券对应的基准利率值
    """

    s = '''SELECT * FROM CSI_BOND_BASE_RATE \
                    WHERE IMPORT_TIME ={1} AND BASE_RATE_TIME = {0}
                    ORDER BY IMPORT_TIME ASC'''.format("'" + floating_benchmark.strip("'") + "'",
                                                       "'" + trade_date.strip("'") + "'")
    return s


def progressive_rates(cb_seq_list):
    s = """SELECT CB_SEQ, COUPON_RATE, CF_DATE
    FROM CSI_BOND_GZ_CF
    WHERE CB_SEQ IN {0}
    """.format(cb_seq_list).replace("[", "(").replace("]", ")")
    return s

# 最新现金流归集表披露日（非循环），与资产池存续结合起来，
def prediction_duration_latest_date_template(project_seq, security_report_date):
    """
    从非循环购买的存续期现金流归集表（ ``csi_abs_cashflow_duration`` ）中获取最近证券端披露日当天或以前的最新现金流归集表披露日, 一般来说:

                                * CLO每期披露, 此时，得到的日期一般与输入的证券端披露日一样
                                * ABN和交易所ABS只在发行期披露一次，此时输出的是 ``None``
    """

    s = '''
        select max(report_date) as cashflow_latest_date
        from csi_abs_cashflow_duration
        where project_seq = {0}
            and report_date <= {1}
        '''.format("'" + project_seq.strip("'") + "'",  "'" + str(security_report_date).strip("'") + "'")

    return s


def prediction_duration_revolving_latest_date_template(project_seq, security_report_date):
    """
    从循环购买存续现金流归集表( ``csi_abs_cf_issue_loop_duration`` )中获取最近证券端披露日当天或以前的最新现金流归集表披露日, 一般来说:

                                * CLO每期披露, 此时，得到的日期一般与输入的证券端披露日一样
                                * ABN和交易所ABS只在发行期披露一次，此时输出的是 ``None``
    """

    s = '''
        select max(report_date) as cashflow_latest_date
        from csi_abs_cf_issue_loop_duration
        where project_seq = {0}
            and report_date <= {1}
        '''.format("'" + project_seq.strip("'") + "'", "'" + str(security_report_date).strip("'") + "'")
    return s


def prediction_issue_template(project_seq):
    """
    获取 `csi_abs_cashflow_issue` 表中发行期现金流归集表（非循环购买）数据, 包括如下列：

                        * 归集日 `cf_date` as `date_`
                        * 支付日 `pay_date`
                        * 当期本金流入 `cif_principal` as `current_principal_due`
                        * 当期利息流入 `cif_accrual` as `current_interest_due`
                        * 期初本金余额 `s_principal_balance` as `begin_principal_balance`,
                        * 期末本金余额 `e_principal_balance` as `end_principal_balance`
                        * 当期本息和流入 ``ifnull(cif, ifnull(cif_total,ifnull(cif_other, 0)))`` as `current_principal_interest_due`, 即依次取 现金流入_本息和、现金流入_合计、现金流入_其他 作为本息和流入列，如果均为空则该列为 0
    """

    s = '''
        select
            cf_date as date_, pay_date, cf_date as pool_date, cif_principal as current_principal_due, 
            ifnull(cif, ifnull(cif_total,ifnull(cif_other, 0))) as current_principal_interest_due, 
            cif_accrual as current_interest_due, s_principal_balance as begin_principal_balance, 
            e_principal_balance as end_principal_balance
        from csi_abs_cashflow_issue
        where project_seq = {0}
        order by cf_date asc
        '''.format("'" + project_seq.strip("'") + "'")
    return s


def prediction_duration_template(project_seq, report_date):
    """
    输入 `product_duration_latest_date_template` 读取到的报告日, 获取 `csi_abs_cashflow_duration` 表中的存续期现金流归集表（非循环）

    字段含义与 ``prediction_issue_template`` 中一致

    """


    s = '''
        select
            cf_date as date_, pay_date,  cf_date as pool_date,  cif_principal as current_principal_due, 
            ifnull(cif, ifnull(cif_total,ifnull(cif_other, 0))) as current_principal_interest_due, 
            cif_accrual as current_interest_due, begin_principal_balance as begin_principal_balance, 
            end_principal_balance as end_principal_balance
        from csi_abs_cashflow_duration
        where project_seq = {0} and report_date = {1}
        order by cf_date asc
        '''.format("'" + project_seq.strip("'") + "'", "'" + str(report_date).strip("'") + "'")
    return s


def pool_duration(project_seq, security_report_date, last_security_end_date):
    """
     从`csi_abs_duration_status`表中获取最新的资产池余额数据

     **逻辑为** :

         * 先匹配跟证券端最新一期（ ``tranches_duration`` 读取）同时披露的资产端信息，即报告日相同

                * 如果有，则直接返回对应报告日的资产池余额数据即可；
                * 如果没有，读取资产端报告日在估值日之前，归集日在证券最新计息结束日之前（或相同）的那一期资产池存续余额

    Notes:
        数据提取逻辑的前提是假设用于支付当期已宣告未兑付的证券本息的资金在计息结束前就已经回收，如果有项目不是这样，比如滞后了几天，则会因为资产池余额偏高，导致现金流偏多
        但是如果得到的历史归集日早于证券计算日太多，会改用 ``pool_duration_next`` 查询。此时虽然归集日略晚于证券最新计算日，但是由于日期更接近，更可能与证券最新计算日是同一期。

    """

    s = '''
        select project_seq, report_date, start_date, end_date, 
            loan_status, loan_range, end_principal_balance
            from csi_abs_duration_status a
        where project_seq = {0} 
            and report_date = (select
                case when t1.match_num > 0 then {1}
                     when t1.match_num = 0 then (select max(report_date) as report_date
                                                     from csi_abs_duration_status
                                                     where project_seq = {0}
                                                     and end_date <= {2}
                                                     group by project_seq)
                     end
            from(select count(report_date) as match_num
                from csi_abs_duration_status
                where project_seq = {0}
                  and report_date = {1}) t1 )
    '''.format("'" + project_seq.strip("'") + "'", "'" + str(security_report_date).strip("'") + "'",
               "'" + str(last_security_end_date).strip("'") + "'")
    return s


def pool_duration_featuretable(project_seq, security_report_date, last_security_end_date):
    """读取存续特征表中的资产池本金余额和本息和余额数据

    """
    s = """SELECT indicator_name, indicator_value, report_date, end_date
    FROM csi_abs_assets_survive
    WHERE project_seq = {0}
    AND indicator_name in ('102', '103')
    AND report_date = (select
                case when t1.match_num > 0 then {1}
                     when t1.match_num = 0 then (select max(report_date) as report_date
                                                     from csi_abs_assets_survive
                                                     where project_seq = {0}
                                                     and end_date <= {2}
                                                     group by project_seq)
                     end
            from(select count(report_date) as match_num
                from csi_abs_assets_survive
                where project_seq = {0}
                  and report_date = {1}) t1)
    """.format("'" + project_seq.strip("'") + "'", "'" + str(security_report_date).strip("'") + "'",
               "'" + str(last_security_end_date).strip("'") + "'")
    return s


def pool_duration_next(project_seq, security_report_date, last_security_end_date):

    """

    与 ``pool_duration`` 相似，但是是找归集日晚于证券端计息区间结束日的最近一期资产端报告

    **逻辑** :
        当 ``pool_duration`` 找到的归集日跟证券端差9个月以上，则会尝试用这个存续读取，如果读取到的资产池归集日滞后3个月以内，则用这个日期的资产池存续余额，而不用 ``pool_duration`` 提取的

    Notes:
        一般只有交易所ABS中的一部分会遇到这种情况，比较少

    """

    s = '''
        select project_seq, report_date, start_date, end_date, 
            loan_status, loan_range, end_principal_balance
            from csi_abs_duration_status a
        where project_seq = {0} 
            and report_date = (select min(report_date) as report_date
                                                     from csi_abs_duration_status
                                                     where project_seq = {0}
                                                     and end_date > {2}
                                                     group by project_seq)
    '''.format("'" + project_seq.strip("'") + "'", "'" + str(security_report_date).strip("'") + "'",
               "'" + str(last_security_end_date).strip("'") + "'")
    return s


def tranches_duration(project_seq, trade_date):
    """
    从 ``csi_abs_security_duration`` 表中获取证券存续期数据，包括：

            * 最新报告日 `report_date`
            * 计息起始日 `start_date` as `period_begin_date`
            * 计息结束日 `end_date` as `period_end_date`,
            * 当期本金兑付 `cur_cash_principal_amount` as `current_principal_due`,
            * 当期利息兑付 `cur_cash_interest_amount` as `current_interest_due`
            * 期初本金余额 `start_principal_balance` as `period_begin_balance`
            * 期末本金余额 `end_principal_balance` as `period_end_balance`
            * 已兑付本金总额 `total_payment` 由已披露本金支付的总额加总得到
    """

    s = '''
       select 
            a.start_date as period_begin_date, a.end_date as period_end_date, 
            a.start_principal_balance as period_begin_balance,
            a.end_principal_balance as period_end_balance, a.cur_cash_principal_amount as current_principal_due,
            a.cur_cash_interest_amount as current_interest_due, 
            b.report_date, b.security_seq, b.total_payment
       from csi_abs_security_duration a
       right join(select t1.project_seq, t1.security_seq,
                max(t1.report_date) as report_date, t2.total_payment
       from csi_abs_security_duration t1
       left join (
            select project_seq, security_seq, 
            sum(cur_cash_principal_amount) as total_payment
            from csi_abs_security_duration
            where project_seq = {0}
                and report_date <= {1}
            group by project_seq, security_seq
            ) t2
       on t1.security_seq = t2.security_seq
       where t1.project_seq = {0}
       and t1.report_date <= {1}
       group by t1.project_seq, t1.security_seq, t2.total_payment) b
       on a.security_seq = b.security_seq
       and a.report_date = b.report_date
       where a.project_seq = {0}
       and b.report_date <= {1}
    '''.format("'" + project_seq.strip("'") + "'", "'" + trade_date.strip("'") + "'")
    return s


def tranches_next_date(project_seq, pool_end_date):
    """
    某个归集日 ``pool_end_date`` 之后最近的一期证券存续报告日
    """

    s = '''
        select min(report_date) as report_date
        from csi_abs_security_duration
        where project_seq = {0}
          and end_date >= {1}
    '''.format("'" + project_seq.strip("'") + "'", "'" + pool_end_date.strip("'") + "'")
    return s


def sequence_template(project_seq):
    """
    根据项目内码，从 ``csi_abs_paymentrule`` 表中获取支付顺序数据，包括如下列：

                                * 节点编号 `node_no`
                                * 父节点 `parent_node_no`
                                * 节点类型 `node_type`
                                * 分支条件 `branch_condition`
                                * 节点条件 `node_condition`
                                * 资金来源 `money_source`
                                * 资金去向 `money_destination`
                                * 支付上限 `upper_limit`
                                * 支付下限 `lower_limit`

    """
    s = '''
        select distinct
            project_seq,
            node_no, parent_node_no, node_type,
            branch_condition, node_condition,
            money_source, money_destination, upper_limit, lower_limit
        from csi_abs_payment_rule
        where project_seq = {0}
        order by node_no asc
        '''.format("'" + project_seq.strip("'") + "'")
    return s


def plan_template(project_seq):
    """
    根据项目内码获取摊还计划表 ``csi_abs_payback`` 中的数据, 包括列名：

                            * 支付日 'power_date' as 'date_',
                            * 目标本金支付 'target_principal_payment' as 'target_principal_payment',
                            * 目标本金余额 'target_balance'
    """

    s = '''
        select 
            PS.PROJECT_SEQ,
            sp.security_seq, 
            sp.power_date as date_,
            sp.target_principal_payment as target_principal_payment,
            sp.target_balance
        from csi_abs_payback sp
        inner join CSI_BOND_ABS_BOND_INFO PS
            on sp.security_seq = PS.SECURITY_SEQ
        where PS.PROJECT_SEQ = {0}
        '''.format("'" + project_seq.strip("'") + "'")
    return s


def plan_csi_template(cb_seq):

    """
    获取估值库中表 ``csi_bond_gz_cf`` 里，对应的证券的未来现金流的预测

    """

    s = '''SELECT CF_DATE AS DATE_, PRINCIPAL AS TARGET_PRINCIPAL_PAYMENT, COUPON_RATE
    FROM CSI_BOND_GZ_CF 
    WHERE CB_SEQ = {0}
    '''.format("'" + str(cb_seq) + "'")
    return s


def prediction_issue_revolving_template(project_seq):
    """
    从表 ``csi_abs_cf_issue_loop`` 获取发行期现金流归集表（循环购买）数据

    循环类ABS的表格比非循环多了期初本息和余额、期末本息和余额字段，故相对于非循环购买：

            * 期初本金余额  `ifnull(s_principal_balance, s_cash_flow)` as `begin_principal_balance`
            * 期末本金余额 `ifnull(principal_balance, cf_balance)` as `end_principal_balance`

    """

    s = '''
        select distinct
            cf_date as date_, pay_date, cf_date as pool_date, cif_principal as current_principal_due, 
            ifnull(cif, ifnull(cif_total,ifnull(cif_other, 0))) as current_principal_interest_due, 
            ifnull(cif_accrual, 0) as current_interest_due,
            ifnull(s_principal_balance, s_cash_flow) as begin_principal_balance,
            ifnull(principal_balance, cf_balance) as end_principal_balance
        from csi_abs_cf_issue_loop
        where project_seq = {0}
        order by cf_date asc
        '''.format("'" + project_seq.strip("'") + "'")
    return s


def prediction_duration_revolving_template(project_seq, report_date):
    """输入 `product_duration_revolving_latest_date_template` 读取到的报告日, 获取 `csi_abs_cf_issue_loop_duration` 表中的存续期现金流归集表（循环）

    循环类ABS的表格比非循环多了期初本息和余额、期末本息和余额字段，故相对于非循环购买：

                * 期初本金余额  `ifnull(s_principal_balance, s_cash_flow)` as `begin_principal_balance`
                * 期末本金余额 `ifnull(principal_balance, cf_balance)` as `end_principal_balance`
    """

    s = '''
        select distinct
            cf_date as date_, pay_date, cf_date as pool_date, cif_principal as current_principal_due, 
            ifnull(cif, ifnull(cif_total,ifnull(cif_other, 0))) as current_principal_interest_due, 
            ifnull(cif_accrual, 0) as current_interest_due,
            ifnull(s_principal_balance, s_cash_flow) as begin_principal_balance,
            ifnull(principal_balance, cf_balance) as end_principal_balance
        from csi_abs_cf_issue_loop_duration
        where project_seq = {0}
            and report_date = {1}
        order by cf_date asc
        '''.format("'" + project_seq.strip("'") + "'", "'" + str(report_date) + "'")
    return s


def initial_cashflow_balance(project_seq):
    """
    统计非循环购买的初始现金流归集表中的期初本金余额的最大值、当期本金回收列、当期本息和回收的合计值，备用于无法从入池特征获取初始本金时的补充

    Todo:
        考虑删除，从数据维护上解决问题

    """

    s = f'''select max(A.s_principal_balance) as max_balance, 
    sum(A.cif_principal) as sum_principal, 
    sum(A.cif_accrual) as sum_interest,
    sum(ifnull(A.cif, ifnull(A.cif_total,ifnull(A.cif_other, 0)))) as sum_principal_interest
    from csi_abs_cashflow_issue A
    where A.project_seq = {"'" + project_seq.strip("'") + "'"}
    '''
    return s


def initial_cashflow_rev_balance(project_seq):
    """
    统计循环购买的初始现金流归集表中的期初本金余额的最大值用于无法从入池特征获取初始本金时的补充

    Todo:
        考虑删除，从数据维护上解决问题

    """

    s = f'''select max(ifnull(A.s_principal_balance, A.cf_balance)) as max_balance, 
    sum(A.cif_principal) as sum_principal,
    sum(A.cif_accrual) as sum_interest,
    sum(ifnull(A.cif, ifnull(A.cif_total,ifnull(A.cif_other, 0)))) as sum_principal_interest
    from csi_abs_cf_issue_loop A
    where A.project_seq = {"'" + project_seq.strip("'") + "'"}
    '''
    return s


def static_amortization_template(project_seq):
    """
    读取循环购买类的模拟静态池

    Todo:
        考虑删除，因为披露的量的原因，新版本不再使用这些数据

    """

    s = '''
        select start_date as begin_date, 
        principal_balance as begin_principal_balance,
        accrual as static_interest_due, 
        cash_inflow_principal as static_principal_due
        from csi_abs_static_payback_dist
        where project_seq = {0}
        order by start_date 
        '''.format("'" + project_seq.strip("'") + "'")
    return s


def dynamic_params_template(project_seq):
    """
    读取循环购买类的序列型假设参数

    Todo:
        考虑删除，因为披露的量的原因，新版本不再使用这些数据

    """

    s = '''
        select month as duration_month, ifnull(cumulative_default_rate, loss_rate) as dynamic_defaultrate,
        annual_return as dynamic_yield, discount_rate as dynamic_discountrate,
        amortization_ratio as payback_proportion, cumulative_crp as dynamic_prepayrate
        from csi_abs_assumption_month
        where project_seq = {0}
        order by 'month'+0 asc
        '''.format("'" + project_seq.strip("'") + "'")
    return s


def basic_assumption(project_seq):
    """
    读取循环购买类的数值型假设参数

    Todo:
        考虑删除，因为披露的量的原因，新版本不再使用这些数据

    """

    s = f'''
        select assumption_object, assumption_type, indicator_type, indicator_value
        from csi_abs_assumption
        where project_seq = {"'" + project_seq.strip("'") + "'"}
    '''
    return s


def event_template(project_seq, trade_date):
    """
    读取重大事项表 ``csi_abs_absimajiss`` 中已发生的重大事件

    """

    s = '''
    select artype
    from csi_abs_absimajiss
    where project_seq = {0}
    and declaration_date <= {1}
    '''.format("'" + project_seq.strip("'") + "'", "'" + str(trade_date).strip("'") + "'")
    return s


def repurchase_history_template(project_seq, trade_date):
    s = '''
        select distinct
            report_date,
            start_date,
            end_date,
            loop_date,
            incr_principal_balance as revolving_principal,
            incr_prinandint_due as revolving_principal_interest,
            purchase_account_outflow as revolving_cash_out
        from csi_abs_pool_purchase
        where project_seq = {0}
            and report_date <= {1}
        order by end_date asc
        '''.format("'" + project_seq.strip("'") + "'", "'" + str(trade_date).strip("'") + "'")
    return s


def account_remaining_template(project_seq, sec_payment_date, trade_date):
    """
    读取估值日前，最新一次证券端支付日后最新的账户余额
    """

    s = f'''select report_date, start_date, end_date, cash_flow_amount
    from csi_abs_fund_managing_report
    where project_seq = {"'" + project_seq.strip("'") + "'"}
    and report_date = (select min(report_date)
                       from csi_abs_fund_managing_report 
                       where project_seq={"'" + project_seq.strip("'") + "'"}
                       and report_date<={"'" + str(trade_date).strip("'") + "'"}
                       and end_date>={"'" + str(sec_payment_date).strip("'") + "'"})
    and flow_type = '45'
    '''
    return s


def date_rule_template(security_seqs, date_):
    """
    读取日期规则表, 包括:

            * 内码 `seq`
            * 对象 `date_project`
            * 日期名称 `date_name`
            * 条件 `collection`
            * 日期序列 `date_series`

    """

    s = f"""
    select *
    from CSI_BOND_INPUT1.CSI_ABS_DATE_SERIES T
    where (T.security_seq, T.trade_date) in (select security_seq, max(trade_date) 
                         from CSI_BOND_INPUT1.CSI_ABS_DATE_SERIES
                         where security_seq in {security_seqs} 
                         and trade_date <= {date_} 
                         group by security_seq)
    """.replace('[', '(').replace(']', ')')
    return s


def read_initial_date_rule(project_seq):
    s = f"""select date_project, seq, date_name, date_type, collection, date_series
    from CSI_ABS_DATE_RULE 
    WHERE SEQ = '{project_seq}' """
    return s


def trigger_template(project_seq):
    """
    读取触发事件表
    """
    s = f"""
    select *
    from csi_abs_trigger_event
    where project_seq = {"'" + project_seq.strip("'") + "'"}
    """
    return s


def get_history_param(project_seqs, fit_date):
    """
    读取 ``csi_abs_duration_param`` 中的历史参数, 包括

                * 报告日 `report_date` as `date_`
                * 初始起算日 `initial_date`
                * 累计违约率 `cumulative_default_rate` /100  as `cdr`
                * 无条件年化早偿率 `cumulative_crp` /100 as `ucpr`
                * 违约回收率 `default_recovery_rate` /100 as `rr`
                * 条件早偿率 `conditional_crp` /100 as `smm`
                * 条件违约率 `conditional_default_rate`/100 as `smdr`

    TODO:
        数据库这张表还未导入数据，不确定各个字段的具体含义和算法
    """
    s = '''
        select distinct 
            A.project_seq, A.report_date as date_, B.initial_date,
            A.cumulative_default_rate/100 as cdr, A.cumulative_crp/100 as ucpr,
            A.default_recovery_rate/100 as rr, A.conditional_crp/100 as smm, 
            A.conditional_default_rate/100 as smdr
        from csi_abs_duration_param A
        left join (select project_seq, initial_date
                   from csi_abs_project_basic_info) B
        on A.project_seq = B.project_seq
        where A.project_seq in {0}
        and A.report_date <= {1}
        '''.format(project_seqs, "'" + str(fit_date).strip("'") + "'").replace('[', '(').replace(']', ')')
    return s


def GetRawPoolHistoryData(project_seqs, default_balance_range, default_return_range, prepay_range, predict_date):
    """
    输入list(项目内码)，视作违约的本金存续状态的枚举值，视作违约回收的本金回收状态的枚举值，视作早偿回收的本金回收状态的枚举值，日期，从资产池存续状态表 ``csi_abs_duration_status`` 和资产池回收表 ``csi_abs_duration_collection`` 读取违约、早偿、回收金额，用于无法通过调用 ``get_history_param`` 获取足额数据时，计算历史参数。

    **逻辑** :
            * 从资产池存续状态表 ``csi_abs_duration_status`` ， 根据输入的视作违约回收的本金回收状态的枚举值（``default_balance_range(list)``） 读取 `loan_status` 列的值在给定枚举值范围内的，将对应的行的本金余额数据加总作为剩余违约本金余额。 并且将 `loan_status` 限制在仅本金（下同）。
            * 从资产池回收状态表 ``csi_abs_duration_collection`` 根据输入的违约回收枚举值、早偿枚举值，加总作为当期的违约回收金额和早偿金额
            * 读取 ``csi_abs_assetpool_in_feature`` 中枚举值为 '50' 的作为初始本金余额（由于这里是批量读取多个项目的初始本金，没有像 ``load_basic_data`` 一样精细处理）
    """

    from abs.doc.enumerators import loan_range_dict
    s = '''select A.project_seq, A.end_date as date_, 
           A.default_balance, D.default_return, B.prepay, E.end_principal_balance, F.all_collection, 
           C.initial_principal, C.initial_date
           from (select project_seq, end_date, sum(end_principal_balance) as default_balance 
               from csi_abs_duration_status 
		       where project_seq in {0}
                   and loan_range = {5}
                   and loan_status in {1}
                   and report_date <= {4}
		       group by project_seq, end_date) A
           join (select A.project_seq, B.initial_principal, A.initial_date
                      from csi_abs_project_basic_info A
                      left join(select project_seq, indicator_value as initial_principal
                          from csi_abs_assetpool_in_feature
                          where indicator_name= '50'
                          and project_seq in {0}) B
                      on A.project_seq = B.project_seq
                      where A.project_seq in {0}) C
           on A.project_seq = C.project_seq
           join (select project_seq, end_date, 
                 sum(abs(collection_principal)) as prepay
                 from csi_abs_duration_collection 
                 where project_seq in {0}
                     and loan_range = {5}
                     and loan_status in {3}
                     and report_date <= {4}
                 group by project_seq, end_date) B
           on A.project_seq = B.project_seq
           and A.end_date = B.end_date
           join (select project_seq, end_date, 
                 sum(abs(collection_principal)) as default_return
                 from csi_abs_duration_collection
                 where project_seq in {0}
                      and loan_range = {5}
                      and loan_status in {2}
                      and report_date <= {4}
                      group by project_seq, end_date) D
           on A.project_seq = D.project_seq
           and A.end_date = D.end_date
           join (select project_seq, end_date, 
                 sum(end_principal_balance) as end_principal_balance
                 from csi_abs_duration_status
                 where project_seq in {0}
                      and loan_range = {5}
                      and loan_status = '1'
                      and report_date <= {4}
                      group by project_seq, end_date) E
           on A.project_seq = E.project_seq
           and A.end_date = E.end_date
           join (select project_seq, end_date, 
                 sum(abs(collection_principal)) as all_collection
                 from csi_abs_duration_collection
                 where project_seq in {0}
                      and loan_range = {5}
                      and loan_status = '1'
                      and report_date <= {4}
                      group by project_seq, end_date) F
           on A.project_seq = F.project_seq
           and A.end_date = F.end_date
    '''.format(project_seqs, default_balance_range, default_return_range, prepay_range,
               "'" + predict_date.strip("'") + "'", "'" + str(loan_range_dict['prin'].strip("'")) + "'").replace("[", "(").replace("]", ")")
    return s


def history_return_amount(project_seq, history_date, if_mixed):
    """
    用于得到当前已回收的本金总额，仅选择历史资产池回收情况中回收款类别为 '合计' 的。

    Args:
        project_seq (str): 项目内码
        history_date (str): 最近一次资产池披露的归集日, 默认跟资产池存续状态的归集日各期完全一致
        if_mixed (bool) :  与最新一期资产池存续状态表读到的一致，即如果存续状态读到是以本息和方式披露，则为 True，否则为否

    """
    s = ''' select sum(abs(collection_principal)) as all_return
    from csi_abs_duration_collection
    where project_seq = {0}
    and loan_range = {2}
    and loan_status = '1'
    and end_date <= {1}
    '''.format("'" + project_seq.strip("'") + "'", "'" + str(history_date).strip("'") + "'",
               "'" + str(loan_range_dict['prin_plus_int'].strip("'")) + "'" if if_mixed else "'" + str(loan_range_dict['prin'].strip("'")) + "'" )
    return s


def date_schedule(start_, end_):
    """
    获取 `start_` 和 `end_` 日历信息,返回包括 日期 `date_` , 是否节假日 `is_workday`

    Args:
        start_ (str): 日期范围起始
        end_ (str): 日期范围截止

    """

    s = f''' select all_date as date_, if_worked as is_workday
    from csi_bond_work_date
    where all_date >= {start_}
    and all_date <= {end_}
    '''
    return s
