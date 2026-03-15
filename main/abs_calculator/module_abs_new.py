# -*- coding: utf-8 -*-
"""现金流计算器接口

调用 ``abs_calculator`` , 其他为子函数

对计算过程中提到的一些名词解释如下

.. glossary::

    资产池 / 基础资产
        指符合法律法规规定，权属明确，可以产生独立、可预测的现金流且可特定化的财产、财产权利或者财产与财产权利构成的资产组合。

    初始本金余额 / 入池本金金额
        值得是入池基础资产在首次入池、未进行回款时的未偿本金余额

    资产池余额
        指任一时点入池基础资产的未偿本金余额之和

    专项计划期限
        指自专项计划设立日（包含该日）起至专项计划终止日（不包含该日）止的期间。

    回收款
        指从基础资产产生的现金流入，包括本金回收款和利息回收款。

    证券端
        指发行人依据《信托合同》或《基础资产买卖协议》和《发行说明书》向投资者发行的一种资产受益证券，是证明“资产支持证券持有人”享有的相应特定权益的权利凭证。

    募集资金
        指发行人通过销售资产支持证券而募集的认购资金总和。

    循环购买
        在循环期内，收到的回款金额不用于支付证券端本息，而用于继续购买符合标准的资产

    不良贷款
        不良资产证券化以不良贷款作为基础资产，资产池的现金流来源更多依赖于对抵质押物的处置、担保人的追偿、借款人资产的处置，而非正常经营所产生的现金流；

    加压参数
        提到的加压参数特指: 早偿率、违约率、违约回收率、违约延迟月数，循环购买中的摊还比例、循环购买率、收益率、折价率。不包括加压过程中所需的其他的一些参数，如模型类型等

    早偿
        资产池的提早支付本金，由于会影响利息收入，故视作风险

    违约
        资产池的拒付或者延迟支付

    累计违约率 / CDR / Cumulative Default Rate
        资产池在专项计划期限内的违约金额占初始本金的比例

    违约分布 / DP / Default Portions
        资产池每期的无条件违约率占累计违约率的比例

    无条件违约率 / 边际违约率 / MDR / marginal default rate
        资产池每期违约金额占初始本金的比例, 所有期的MDR加总等于CDR

    单期违约率 / 条件违约率 / SMDR / Single Monthly Default Rate
        SMDR = 当期违约金额 / 期初本金余额

    年化违约率 / YCDR / Yearly Conditional Default Rate
        单期违约率的年化值

    单期清偿率 / 单期早偿率 / SMM  / Single Monthly Mortality
        | 算法1: SMM = (部分早偿额 + 全部早偿额) / 期初本金余额 = 1 - (1 - CPR) ** （ACT / 365）  (默认的是这种，现金流计算和模型中都是）
        | 算法2: SMM = (部分早偿额 + 全部早偿额) / （期初本金余额 - 当期计划偿还本金额） = 1 - (1 - CPR) ** （ACT / 365）

    年化早偿率 / CPR / Constant Prepayment Rate
        当年提前偿还的本金占当年末计划应有的未偿还本金的比例，是用来计算早偿率最常用的指标，是以年利率形式表示的SMM。

    无条件早偿率 / 边际早偿率 / USMM
        USMM = 当期早偿金额 / 初始本金余额 = 1 - (1 - USMM) ** (ACT / 365)

    无条件年化早偿率 / UCPR
        USMM 的年化值

    违约回收率（或回收率）
        违约资产通过催收等手段回收的比例，
        回收率 = 违约后回收的本金金额 / 基础资产产生违约时的本金余额

    延迟回收月份
        违约资产在违约后的第几个月能够通过手段回收一部分

    初始起算日
        亦称“封包日”，是指基础资产入池的日期，从该天（含）起，基础资产所产生的本金、利息（如有）及相关利益一并归属发行载体。

    预期到期日
        是指通过对早偿率、违约率的预测，各档资产支持证券本息预期偿付完毕对应的日期。

    法定到期日
        是指资产支持证券法定到期的日期，通常晚于预期到期日两年，是为处置资产预留清算时间。

    计算日 / 归集日 / 核算日
        定期归集基础资产现金流入的日期，一般为月度或季度末月的最后一日。

    计息日
        是指计算持有资产支持证券利息的日期。

    支付日
        是指支付资产支持证券利益的日期。

    收款期间
        指自一个计算日起（不包含该日）至下一个计算日（包含该日）之间的期间，其中第一个“收款期间”应自初始起算日（包含该日）起至第一个计算日（包含该日）结束。

    计息期间
        是指自一个计息日（含该日）至下一个计息日（不含该日）之间的期间。

    服务机构报告日 / 资产端报告日
        是指资产服务机构按照服务合同的约定向发行人和评级机构出具《贷款/资产服务报告》的日期

    加速清偿事件
        指出现某些特殊情况时，通过改变现金流支付顺序从而保障优先级投资人利益的一种信用触发机制。

    违约事件
        是指出现某些特殊情况时，通过合并本金账户和收益账户，加速优先级证券本息兑付的一种信用触发机制。

    收益账
        指发行人在信托账户或专项计划账户下设立的，用于核算利息回收款的子账。

    本金账
        指发行人在信托账户或专项计划账户下设立的，用于核算本金回收款的子账。

"""
from main.abs_calculator.main_cashflow import main_calculation
from abs.prepare.data_load.load_basic_data import load_all_basic
from abs.prepare.data_preprocess.dataFullandAdjust import data_full

from main.abs_calculator.input_standardize import calculator_input_preprocess
from abs.calculate.valuation.valuation import project_valuation
from abs.prepare.data_load.load_yield_curve import main_spot_curve
from utils.quick_connect import connect_mysql
from abs.calculate.valuation.format import (warns_table, data_match_table, valuation_checked, \
    valuation_result_standardize)


def abs_calculator(params: dict, cal_valuation=False):
    """
    ABS现金流计算器主函数

    param: 所有输入参数构成的dict，key为参数名，value为参数值，包含的参数如下
    cal_valuation (bool): 是否计算估值，如果是测试用，则True计算估值，如果是生产，则为False,不进行折现
    _________
    参数
    _________

    **通用参数**

    * security_code(str): 证券代码
    * trade_date(str): 估值日期, 格式YYYYMMDD, 如 "20210122"
    * cur(cursor): 不需要输入，用于批量测试中减少重复连接数据库
    * is_security (bool): 计算范围

                * `True` - 对单个证券进行估值
                * `False` - 输出同项目下所有证券的估值

    **现金流加压所需参数**
    * scenario_type(str):

                        * `user` - 自定义参数加压
                        * `model` - 模型加压，即用模型估计参数后加压

                        Notes:
                            参数 ``scenario_type`` 、 ``model_type``、 ``module_type`` 决定了需要输入的其他参数。具体各选项下需要输入什么参数见后面的示例

    * model_type(str, None): 模型选择（以下同时对模型所需要输入的参数进行了说明，参数具体含义见后文说明）

                        * 当 ``scenario_type=model`` 时

                            * `linear_model` - 线性回归模型::

                                >> scenario_type = 'model'
                                >> model_type = 'linear_model'
                                >> RR = 20  # 由于回归模型预测的是早偿、违约，不预测回收率，所以需要额外输入
                                >> DPP = 2
                                >> bayes = True
                                >> suoe = True
                                >> split_default = False
                                >> begin_default_recover = False
                                # 也可以输入 RRs 替代 RR 和 DPP
                                >> RRs = pd.Series([30, 20], index=[3, 5])

                            * `sigmoid_model` - 逻辑回归模型::

                                # 入参与线性回归模型一致，除了改动模型类型
                                >> model_type = 'sigmoid_model'

                            * `markov_model` - 马尔可夫转移矩阵::

                                # 由于转移矩阵中考虑了所有的状态（违约、逾期、早偿、违约回收、正常回收等），故不需要额外输入压力参数
                                >> scenario_type = 'model'
                                >> model_type = 'markov_model'
                                >> n_markov = False
                                >> suoe = True
                                >> split_default = False
                                >> begin_default_recover = False


                            * `extrapolate_model` - 资产池外推模型::

                                >> scenario_type = 'model'
                                >> model_type = 'extrapolate_model'
                                >> suoe = True
                                >> split_default = False
                                >> begin_default_recover = False

                                # 由于资产池外推只能得到累计违约率和违约分布序列，故仍需要自己假设早偿率和违约回收率
                                # 模式一
                                >> RR = 20
                                >> DPP = 2
                                >> CPR = 1

                                # 模式二
                                >> RRs = pd.Series([30, 20], index=[3, 5])
                                >> CPRs = pd.Series([3, 2, 1, 1, 1], index=[1, 2, 3, 4, 5])
                                >> param_match_method = 'remain'

                        * 当 ``scenario_type=user`` 时
                            * `None` - 不用模型

    * bayes (bool): `model_type=linear_model` 线性回归，或者 `model_type=sigmoid_model` 逻辑回归时是否用贝叶斯估计更新模型参数，默认 `False`
    * n_markov (bool): 马尔可夫转移矩阵中( ``model_type=markov_model`` )用到的转移矩阵数量

                * `True` - 根据可比资产池历史数据为不同账龄估计不一样的转移矩阵
                * `False` - 假设存续期次不同，转移概率不变，即估计单一的转移矩阵

    * module_type(str): 当  ``scenario_type=user``  输入入参模式（详情见excel输入参数表), 不同模式下必须输入的参数不同（如下），参数含义见后文

                            * `static_normal1` 现金流计算器-非循环购买-非不良贷款-数值模式1 ::

                                    >> CDR = 1
                                    >> CPR = 2
                                    >> RR = 20
                                    >> DPP = 2
                                    >> minus_CDR = True
                                    >> split_default = False
                                    >> begin_default_recover = False

                                    >> recal_cashflow = False  # 该模式下固定


                            * `static_normal2` 现金流计算器-非循环购买-非不良贷款-数值模式2 ::

                                    >> YCDR = 2
                                    >> CPR = 2
                                    >> RR = 20
                                    >> DPP = 2
                                    >> split_default = True
                                    >> begin_default_recover = False

                                    >> recal_cashflow = False  # 该模式下固定

                            * `series_normal1` 现金流计算器-非循环购买-非不良贷款-序列模式1 ::

                                    >> CDR = 1
                                    >> DPs = pd.Series([0.5, 0.3, 0.1, 0.1], index=[1, 2, 3, 4])
                                    >> CPRs = pd.Series([3, 2, 1, 1], index=[1, 2, 3, 4])
                                    >> RRs = pd.Series([30, 20], index=[3, 5])
                                    >> minus_CDR = True
                                    >> param_match_method = 'remain'
                                    >> dp_match_perfectly = False
                                    >> split_default = False
                                    >> begin_default_recover = False
                                    >> recal_cashflow = False  # 该模式下固定

                            * `series_normal2` 现金流计算器-非循环购买-非不良贷款-序列模式2 ::

                                    >> YCDRs = pd.Series([0.2, 0.1, 0.1, 0.05], index=[1, 2, 3, 4])
                                    >> CPRs = pd.Series([3, 2, 1, 1], index=[1, 2, 3, 4])
                                    >> RRs = pd.Series([30, 20], index=[3, 5])
                                    >> param_match_method = 'remain'
                                    >> split_default = False
                                    >> begin_default_recover = False
                                    >> recal_cashflow = False  # 该模式下固定

                            * `static_npl_recal` 现金流计算器-非循环购买-不良贷款 ::

                                    >> RR = 20
                                    >> portion = 0
                                    >> recal_cashflow = True  # 该模式下固定

                            * `series_rev_recal` 现金流计算器-循环购买-数值模式 ::

                                    >> CDR = 0.1
                                    >> CPR = 3
                                    >> RR = 20
                                    >> DPP = 3
                                    >> PP = 20
                                    >> RP = 100
                                    >> YR = 5

                                    Examples2:
                                    >> CDR = 0.1
                                    >> CPR = 3
                                    >> RR = 20
                                    >> DPP = 3
                                    >> PP = 20
                                    >> RP = 100
                                    >> DR = 98


                            * `static_rev_recal` 现金流计算器-循环购买-序列模式 ::

                                    Examples1:
                                    >> CDRs = pd.Series([1, 1, 2, 3, 3], index=[1, 2, 3, 4, 5])
                                    >> CPRs = pd.Series([3, 2, 1, 1, 0.5], index=[1, 2, 3, 4, 5])
                                    >> RRs = pd.Series([30, 20], index=[3, 5])
                                    >> PPs = pd.Series([40, 30, 20, 5, 5], index=[1, 2, 3, 4, 5])
                                    >> RP = 100
                                    >> YRs = pd.Series([5, 6, 7, 9, 11], index=[1, 2, 3, 4, 5])

                                    Examples2:
                                    >> CDRs = pd.Series([1, 1, 2, 3, 3], index=[1, 2, 3, 4, 5])
                                    >> CPRs = pd.Series([3, 2, 1, 1, 0.5], index=[1, 2, 3, 4, 5])
                                    >> RRs = pd.Series([30, 20], index=[3, 5])
                                    >> PPs = pd.Series([40, 30, 20, 5, 5], index=[1, 2, 3, 4, 5])
                                    >> RP = 100
                                    >> DRs = pd.Series([99, 99, 98, 95, 95], index=[1, 2, 3, 4, 5])

    * recal_cashflow(bool): 循环购买类型或者不良贷款类型是否要利用加压参数重新测算现金流

                         * `True` - 循环购买下重新测算现金流，不良贷款中将现金流回复为100%回收后重新加压，
                         * `False` - （默认）循环购买和不良贷款中不进行加压，循环购买直接分配摊还期现金流，不良贷款直接分配现金流

    * CDR (float, int, None): 累计违约率(单位: %)
    * CPR (float, int, None): 年化早偿率(单位: %)
    * RR (float, int, None): 违约回收率(单位: %)
    * DPP (int, None): 延迟回收月份(单位: 月)
    * PP (float, int, None): 月摊还比例(单位: %)
    * YR (float, int, None): 资产池收益率(单位: %)
    * RP (float, int, None): 循环购买率(单位: %)
    * DPs (pd.Series, None): 违约分布. 如下：


                                *  ``dp_match_perfectly=False`` 时, 表示输入的违约分布是个基于月的假设，需要根据各个归集日距离上个归集日实际的天数进行调整

                                    * ``param_match_method='remain'`` 时， 此时输入的 ``DPs = pd.Series([10, 5, 4], index=[1, 2, 3]) ``指的是，从最新披露的归集日开始的第一个的违约分布是10%， 第二个月是5%

                                        * 当 ``minus_CDR=True`` 时， ``DPs`` 各期的绝对值不重要， 关键是相对关系，因为在计算上，会在输入的累计违约率中扣除当前累计违约率，得到的值实际上是未来的无条件违约率总和，将这个值分配在未来的每一期得到的就是每一期的无条件违约率。此时需要分配这笔无条件违约率的期次是一定的，只要确定每一期的能分到的比例即可，因此只看相对关系。
                                        * 当 ``minus_CDR=False`` 时，此时需要看绝对关系，因为此时认为，即使已经发生了一些违约，并且与一开始对于已发生违约那几期的无条件违约率的假设背离，但是还是认为未来期次中对无条件违约率的假设值不变。而无条件违约率=CDR * DP[i], 故此时对 ``DP`` 要求严格输入绝对值

                                    * ``param_match_method='all'`` 时，此时输入的 ``DPs`` 总和为 100%，假设为 ``DPs = pd.Series([30, 20, 20, 10, 5, 5, 6, 4], index=[1, 2, 3, 4, 5, 6, 7, 8]) ``, 即，DP序列的第一期对应着项目设立后的首个归集日。此时相比于前面的，只是多了一步数据截取，即去掉了历史的归集日。

                                * ``dp_match_perfectly=True`` 时，是给模型使用的，需要严格匹配现金流归集表，比如，``DPs = pd.Series([10, 5, 4], index=[1, 2, 3])`` 就表示现金流归集表中未来第一个归集日的违约金额占初始本金额的 5%。此时不会再根据资产池归集表之间的实际天数对DP序列进行处理

    * CPRs (pd.Series, None): 年化违约率序列,
    * RRs (pd.Series, None): 违约回收率序列, 表示违约后的不同月份会有违约回收流入, index是延迟回收月份, value是对应时间的回收率 (单位: %)
    * DR (float, int, None): 折价率，单笔循环买入资产支付金额占入池金额的比例，与YRs不兼容 (单位: %)
    * CDRs (pd.Series, None): 累计违约率序列, 适用于循环购买中的动态假设, 指的是资产池存续月数（index)与累计违约率(value, 单位: %)的关系
    * PPs (pd.Series, None): 摊还比例序列, index-月, value-假设的月摊还比例 (单位: %)
    * YRs (pd.Series, None): 收益率序列, 假设资产在存续期的每个月的利息率不同(单位: %)
    * DRs (pd.Series, None): 折价率序列, 单笔循环买入资产存续月数(index)与折扣率(value, 单位: %)的关系, 与YRs不兼容
    * YCDR (float, int, None): 年化违约率
    * YCDRs (pd.Series, None): 年化违约率序列, index-月，value-假设的年化违约率 (单位: %)
    * portion (float, int, None): (不良贷款用), 表示不良贷款的延迟回收比例，主要用于评级的加压 (单位: %)
    * param_match_method (str): 当用到序列型的加压参数时，如何匹配加压参数序列的假设月份和未来现金流归集表;

            * `remain` - 将参数从上一归集日之后开始匹配 （默认）
            * `all` - 将参数从初始起算日开始匹配

            Note:
                与 ``RRs`` 无关（如有输入），因为违约回收率统一指的是从违约时点开始，跟资产池存续时间无关

    * minus_CDR (bool): 是否从输入的 ``CDR`` 中考虑当期已发生的累计违约率的影响

            * `True` - 从输入的 ``CDR`` 扣除已发生CDR，并且从违约分布中，截出剩余期限内的 ``DPs`` 重新调整到新的总和为1的违约分布 DPs_new，即剩余期限内第 ``i`` 期的违约率为（ ``CDR`` - current_CDR) * ''DPs_new[i]''
            * `False` - 为之前交付版本，未考虑已发生的CDR，此时剩余期限内第 ``i`` 期的违约率为 ``CDR`` * ``DPs[i]``。这一方法下，相当于当对无条件违约率存在一个假设时，不管至今实际已经违约了多少，未来的无条件违约率不受影响

            Note:
                不管是哪种模式，CDR*DP得到的都是相对于初始本金而言的无条件违约率。用年化违约率时不需要考虑这个字段
                如果是 ``module_type=static_normal1``，虽然没有输入 ``DPs``，实际上会根据现金流归集表每期的回款比例计算 ``DPs`` ， 因此还是会用到 ``minus_CDR`` 参数

    * begin_default_recover (bool): 是否对当前剩余违约金额计算违约回收

            * 当 ``split_default=True`` 时，选择当前的违约金额是否要算违约回收。
            * 当 ``split_default=False`` 时，该指标无效，因为已经将当前剩余违约金额视作了正常存续的资产

    * dp_match_perfectly (bool): 当有输入违约分布 ``DPs`` 时，``DPs``是否与最新现金流归集表完全匹配

            * `True` - 输入的 ``DPs`` 跟每一期的核算日是匹配的,直接乘以累计违约率得到现金流归集表每一期对应的累计违约率值（实际算法还需要结合 ``minus_CDR`` 考虑）

                Notes:
                    此时 ``param_match_method`` 对序列参数中的 ``DP`` 无效，因为 ``DP`` 已经与现金流给i举报

            * `False` （默认） - 输入值是以月为单位的,需要根据归集日之间的具体日期匹配到每个核算日

    * suoe (bool): 早偿对后续现金流的影响

            * `True` - 用早偿缩额法
            * `False` - 用早偿缩期法

    * split_default (bool): 是否从最新剩余本金中扣除当前剩余违约金额

             * `True` - 从读取的资产池剩余本金中扣除当前剩余违约本金，再用于调整现金流归集表的首期期初本金余额
             * `False` - 不从资产池剩余本金中扣除当前剩余违约本金，相当于当前剩余违约本金视作正常类，在后续加压过程中会对这部分模拟违约、早偿的行为

    * interest_method (str): 循环购买测算现金流(recal_cashflow=True)中利息的计算方式

            * `yield_` - 收益率( ``YR`` 或者 ``YRs`` )*剩余本金计算利息
            * `discount` - 循环购买支出/折现率（ ``DR`` 或者 ``DRs`` )计算入池资产金额、

    * same_type (bool): (仅scenario_type='model'时) 选择可比项目时用同二级分类(True) 还是同发起人（False); 回归模型写死为用同二级分类

    **现金流分配和折现所需参数**

    exist_tax_exp: bool, 是否计算税费

                        * `True` - 考虑税费，需要对应的输入 ``exp_rate`` , ``tax_rate``
                        * `False` - 不考虑税费，即使输入了 ``exp_rate`` , ``tax_rate`` 也作为 0 处理

    exp_rate(float, int): 费率(单位: %)
    tax_rate(float, int): 税率(单位: %)
    coupon_rate_change(float, int): 票面利率变动多少个 bp (单位: bp）, 主要用于评级模块的压力测试

    day_count_method (str): 利息天数计算方式, 主要影响应计利息，算头不算尾的应计利息比算尾不算头的多一天。如果是完整的计息区间，则不会影响.

                        * 'begin' - 算头不算尾
                        * 'end' - 算尾不算头


    add_remain_account (bool): 是否在现金流分配的过程中，额外加入当前账户余额用于证券端偿付

                                * `True` - 是
                                * `False` (默认） - 否，默认为否，因为账户余额的披露很不规范

    value_method (str, dict(str: str)): 折现方法

                        * ``is_security=True`` 输入str，
                        * ``is_security=False``

                                        1. 输入dict（key为证券代码，value为枚举中的str),相当于假设同一项目下不同证券可以用不同的方法折现
                                        2. 输入str, 此时认为项目下所有证券折现方式一样

                        枚举值:

                         * ``yield_`` - 到期收益率折现
                         * ``curve`` - 收益率曲线折现

    curve_name (str, dict(str: str)): 收益率曲线名称，填法与 ``value_method`` 一致，仅当证券采用收益率曲线折现模式时输入，输入曲线的名称需要与估值数据库保存的一致。
    input_type (str, dict(str: str)): 计算依据，填法与 ``value_method`` 一致，

                        * `yield_` - 输入到期收益率算估值，同时需满足 ``value_method=='yield_'``
                        * `spread` - 输入利差算估值，同时需满足 ``value_method=='curve'``
                        * `dirty_price` -输入全价反算利差（ ``value_method=='curve'`` ）或到期收益率（（ ``value_method=='yield_'`` ）)
                        * `clean_price`- 输入净价反算利差（ ``value_method=='curve'`` ）或到期收益率（（ ``value_method=='yield_'`` ）)

    input_value(float, dict(str: float)): ``input_type`` 对应的值

                        * ``input_type='yield_'`` - 输入的是到期收益率, 单位为%
                        * ``input_type='spread'`` - 输入的是利差，单位是BP
                        * ``input_type='clean_price' or input_type='dirty_price'`` 是净价/全价，单位为元

    compensate(bool): 是否自动补齐不足额的优先级还款，资金来源为账户剩余金额和扣除次级已支付的金额

    Examples::

        # 单个证券折现或者项目下所有证券折现参数一致时
        >> value_method = 'yield_'
        >> input_type = 'yield_'
        >> input_value = 3
        >> cal_derivatives = True
        >> exist_tax_exp = False
        >> coupon_rate_change = 0.
        >> add_remain_account = False
        >> day_count_method = 'begin'

        # 项目下折现参数不一致时
        >> value_method = {'2089394': 'curve', '2089395': 'yield_', '2089396': 'yield_'}
        >> input_type = {'2089394': 'spread', '2089395': 'yield_', '2089396': 'curve'}
        >> input_value = {'2089394': 0.5, '2089395': 4, '2089396': 5}
        >> curve_name = {'2089394': 'cc_ll_gz'}
        >> cal_derivatives = False
        >> exist_tax_exp = True
        >> tax_rate = 3.26
        >> exp_rate = 0.1
        >> coupon_rate_change = 0.
        >> add_remain_account = False
        >> day_count_method = end


    Returns:
         tuple: tuple contains:
                         * pool_result_upload(pd.DataFrame): 加压后现金流归集表（上传版本） \n
                         * security_result_upload(pd.DataFrame): 证券端所有证券的预测现金流（上传版本） \n
                         * security_result(pd.DataFrame): 证券端所有证券的预测现金流（中文版本） \n
                         * pool_result(pd.DataFrame): 加压后现金流归集表（中文版本） \n
                         * df_assumptions(pd.DataFrame): 预测的压力参数 \n
                         * df_other(pd.DataFrame): 用于结果检查的一些信息 \n
                         * df_factor(pd.DataFrame): 如果是模型，模型的因子系数值，根据模型不一样，表格结构不一样 \n
                         * df_warns_info(pd.DataFrame): 项目可能出现的数据问题（不针对单个证券） \n
                         * derivative_results(pd.DataFrame): 估值衍生指标
                         * valuation_results(pd.DataFrame): 项目下所有证券估值结果 \n


    _________
    逻辑
    _________
        1. ``calculator_input_preprocess`` 将提取输入的dict中的参数，并进行处理
        2. ``load_all_basic`` 读取现金流计算所需的数据
        3. ``data_full`` 预处理数据.
        4. ``main_spot_curve`` 读取折现曲线（如为曲线折现模式）
        5. ``main_calculation`` 现金流加压与分配
        6. ``security_valuation`` 或 ``project_valuation`` 对证券或项目下所有证券进行现金流折现及衍生指标计算
        7. ``result_check`` 根据一些规则检查输出结果及异常结果提示
        8. ``cf_upload`` 用于上传的证券端现金流数据

    TODO:
        1. 以上参数上部分为默认值，配置在 `doc.conf` 中，如需选用其他方式可在配置文件修改，如需对特定项目单独修改，则在入参中进行规定。
        包括：bayes, n_markov, split_default, begin_default_recover,
        add_remain_account, dp_match_perfectly, day_count_method, param_match_method, minus_CDR, cpr_type，
    """

    # 1. 参数处理

    (security_code, trade_date, value_method, curve_name,\
        scenario_type, model_type, bayes, n_markov, input_type, input_value, coupon_rate_change, recal_cashflow,\
        CDR, CPR, RR, exist_tax_exp, exp_rate, tax_rate, DPP, suoe, PP, YR, RP, upload,\
        DPs, CPRs, RRs, DR, CDRs, PPs, YRs, DRs, YCDR, YCDRs, portion, module_type, interest_method, is_security,
     cur, split_default, begin_default_recover, add_remain_account, dp_match_perfectly,
     day_count_method, param_match_method, minus_CDR, cpr_type, same_type, compensate) = calculator_input_preprocess(params)

    # 2. 项目数据读取

    close_conn = False
    if cur is None:
        conn, is_mysql = connect_mysql()
        cur = conn.cursor()
        close_conn = True
    df_product, df_tranches, df_prediction, df_sequence, df_plan, df_other, df_trigger, events, df_date_rule, \
    contingent_param_dict, security_seq, project_seq, warns_lst, df_date_rule_initial, df_calendar = \
        load_all_basic(security_code, trade_date, cur=cur)

    security_seqs = dict(zip(df_tranches['security_code'], df_tranches['security_seq']))

    # 3 数据处理
    df_product, df_tranches, df_prediction, df_sequence, df_plan, df_other, df_trigger, df_schedule, \
    warns_lst2 = \
        data_full(df_product, df_tranches, df_prediction, df_sequence, df_plan, df_other, df_trigger, events=events,
                  df_date=df_date_rule, if_recal=recal_cashflow,
                  split_default=split_default, df_daterule=df_date_rule_initial, df_calendar=df_calendar)

    # 4 加压现金流归集表、分配现金流


    security_result_upload, pool_result_upload, security_result_cn, pool_result_cn, tranches_obj, \
    df_assumptions, df_factor, distribution_info = \
        main_calculation(df_product=df_product, df_tranches=df_tranches, df_prediction=df_prediction,
                         df_sequence=df_sequence, df_plan=df_plan, df_other=df_other, df_trigger=df_trigger,
                         events=events, df_schedule=df_schedule,
                         project_seq=project_seq, trade_date=trade_date,
                         exist_tax_exp=exist_tax_exp, exp_rate=exp_rate, tax_rate=tax_rate,
                         scenario_type=scenario_type, coupon_rate_change=coupon_rate_change,
                         recal_cashflow=recal_cashflow, suoe=suoe,
                         module_type=module_type, model_type=model_type, bayes=bayes, n_markov=n_markov,
                         CDR=CDR, CPR=CPR, RR=RR, DPP=DPP, RP=RP, DPs=DPs, CPRs=CPRs, RRs=RRs, CDRs=CDRs, PPs=PPs,
                         YRs=YRs, DRs=DRs, YCDR=YCDR, YCDRs=YCDRs, portion=portion,
                         param_match_method=param_match_method,
                         interest_method=interest_method, day_count_method=day_count_method, minus_CDR=minus_CDR,
                         begin_default_recover=begin_default_recover, add_remain_account=add_remain_account,
                         dp_match_perfectly=dp_match_perfectly, split_default=split_default, cur=cur,
                         cpr_type=cpr_type, same_type=same_type, compensate=compensate)

    valuation_results_upload, derivative_results_upload, derivative_results = None, None, None
    if cal_valuation:
        # 5. 计算PV
        securities = df_tranches['security_code']
        n = len(securities)
        input_types = dict(zip(securities, [input_type] * n)) if not isinstance(input_type, dict) else input_type
        input_values = dict(zip(securities, [input_value] * n)) if not isinstance(input_value, dict) else input_value
        value_methods = dict(zip(securities, [value_method] * n)) if not isinstance(value_method,
                                                                                    dict) else value_method
        curves = dict(zip(securities, [None] * n))
        for vm in value_methods:
            if value_methods[vm] == 'curve':
                try:
                    if not isinstance(curve_name, dict) and (curve_name is not None):
                        curve = main_spot_curve(trade_date, curve_name, cur)
                        curves = dict(zip(securities, [curve] * n))
                        break
                    elif curve_name is None:
                        raise ValueError('曲线折现情况下未填入曲线名称')
                    elif type(curve_name) == dict:
                        curves[vm] = main_spot_curve(trade_date, curve_name[vm], cur)
                    else:
                        raise TypeError("错误的曲线名称输入")
                except (Exception, ValueError, KeyError) as e:
                    raise ValueError(f"折现曲线提取错误:曲线名{curve_name},日期{trade_date}")

        try:
            valuation_results, derivative_results = project_valuation(trade_date, tranches_obj, pool_result_upload,
                                                                      input_types, input_values, value_methods, curves)
            valuation_results_upload, derivative_results_upload = \
                valuation_result_standardize(valuation_results, derivative_results, security_seqs)

        except (Exception, ValueError, KeyError) as e:
            raise ValueError(f"折现估值模块错误:{e}")

    if close_conn:
        cur.close()
        conn.close()

    # 6 计算一些衍生指标
    df_date_match = data_match_table(df_other, trade_date, project_seq)
    df_warns_info = warns_table(project_seq, trade_date, warns_lst, warns_lst2)
    df_checked, df_checked_project = valuation_checked(derivative_results, df_other, distribution_info, df_tranches, tranches_obj,
                      is_npls=df_product.loc[0, 'is_npls'], security_seqs=security_seqs)

    if is_security:
        security_result_upload = security_result_upload[security_result_upload['SECURITY_SEQ'] == security_seq]
        df_checked = df_checked[df_checked['SECURITY_SEQ'] == security_seq]

    return pool_result_upload, security_result_upload, pool_result_cn, security_result_cn, df_warns_info, \
           df_assumptions, df_factor, df_date_match, valuation_results_upload, df_checked, derivative_results_upload, df_checked_project

