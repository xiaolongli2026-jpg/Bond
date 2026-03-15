# -*- coding: utf-8 -*-
"""
静态池加压模块，不包含不良贷款加压

Notes:
    文档中提到的所有参数都能在 ``LoanPool`` 或者所在类方法的参数描述中找到，不会重复解释

"""

import numpy as np
import pandas as pd

from calculate.asset.s_param_match import match_cashflow_dp
from utils.timeutils import to_date2
from typing import Union


class LoanPool(object):

    """
    初始化资产池

    Args:
        pool_start_date (datetime.date): 资产池收款起始日。对应发行说明书的初始起算日或者存续期的下一归集期间起始日，
        也即已经完成归集的最近一个归集日（如果资产池滞后，则对应着已经披露收益分配公告的最近一次证券支付日。

        original (pd.DataFrame): 基准现金流预测，必须包含以下列。其第一行对应的就是未来第一次归集日：

                                    * "date_", 归集日
                                    * "begin_principal_balance", 期初本金余额
                                    * "current_principal_due", 当期应收本金
                                    * "current_interest_due", 当期应收利息
                                    * "end_principal_balance" 期末本金余额

        initial_principal (float): 初始本金，用于计算违约金额
        initial_date (datetime.date): 初始起算日
        begin_default (float): 当期剩余违约本金
        pool_start_CDR (float): 当前计算时刻已发生的CDR
        begin_default_recover (bool): 是否对当前剩余违约金额计算违约回收
        split_default (bool): 是否从最新剩余本金中扣除当前剩余违约金额

    Attributes:

        daycounts (np.array): 各个归集日与上一归集日之间的天数，首个为第一个归集日与 `pool_start_date` 之间的天数，（n, )
        year_fracs (np.array): 各个归集日与上一归集日之间的年化天数，以 `daycounts`/365得到, (n, )， year_fracs[i] 表示对应着现金流归集表第 `i` 期的年化天数
        original_PR_OPB (np.array): 由当期利息回收除以期初本金余额得到，是计算加压后现金流利息的依据。**注：如果期初本金为0但是利息不为0，则利息率会变成0，加压后对应的行不会有利息** ，（n, )
        total_days (int): 现金流归集表中最后一个归集日与初始起算日之间的天数
        remain_days (int): 现金流归集表中最后一个归集日与 `pool_start_date`  之间的天数
        num_periods (int): 现金流归集表的总期数，即现金流归集表的行数

    """

    def __init__(self, pool_start_date, original, initial_principal, initial_date, begin_default=0, pool_start_CDR=0.,
                 begin_default_recover=False, split_default=False):

        self.pool_start_date = to_date2(pool_start_date)
        self.begin_default_recover = begin_default_recover
        self.split_default = split_default

        # 筛选所需列
        self.original = original[["date_", "begin_principal_balance",
                                  "current_principal_due", "current_interest_due", "end_principal_balance"]]

        # 首个计算日所在行号
        first_calculate_row = 0

        self.initial_principal = initial_principal
        self.begin_principal = self.original.loc[0, 'begin_principal_balance']
        self.begin_default = begin_default
        self.pool_start_CDR = pool_start_CDR

        last_date = self.original.date_.max()
        self.total_days = (last_date - initial_date).days
        self.remain_days = (last_date - pool_start_date).days

        self.num_periods = len(self.original)

        OPB = self.original['begin_principal_balance'].values  # 期初本金余额序列
        valid_loc = ~((OPB == 0) | (np.isnan(OPB)))  # OPB序列的有效值
        self.original_IR_OPB = np.zeros([self.num_periods, ])
        self.original_PR_OPB = np.zeros([self.num_periods, ])
        self.original_IR_OPB[valid_loc] = self.original['current_interest_due'].values[valid_loc] / OPB[valid_loc]
        self.original_PR_OPB[valid_loc] = self.original['current_principal_due'].values[valid_loc] / OPB[valid_loc]

        self.all_dates = self.original.date_

        # 计算各个归集日与上一归集日之间的天数
        daycounts = np.zeros(self.num_periods)
        last_date = None
        for i, this_date in enumerate(self.all_dates):
            if i > first_calculate_row:
                daycounts[i] = (this_date - last_date).days
            elif i == first_calculate_row:
                daycounts[i] = (this_date - self.pool_start_date).days
            last_date = this_date

        self.daycounts = daycounts
        self.year_fracs = daycounts / 365.  # 早偿与违约金额每期计算比例：ACT/365

    def scenario_analysis(self, CPR: Union[float, int], CDR: Union[float, int], DPs: np.ndarray, RR: Union[float, int],
                          DPP: int, well_distributed: bool, suoe: bool, CPR_type: str,
                          minus_CDR: bool):
        """
        非循环非不良数值入参模式1

        Args:
            CPR (float): conditional prepayment rate, 年化早偿率
            CDR (float): cumulative default rate, 累计违约率
            RR (float): recycle rate, 违约回收率
            DPP (float): delay period, 回收延迟月数
            well_distributed (bool): True-将累计违约率均匀分布在每一期, False-根据现金流归集表每期的应收本金额，分配违约金额
            suoe (bool): 采用缩额法(True)还是缩期法(False)计算早偿后现金流
            CPR_type (str):

                * type1-SMM=当月提前还款本金/当月期初未偿本金
                * type2-SMM=当月提前还款本金/(当月期初未偿本金-当月计划偿还)

            minus_CDR (bool): 是否从输入的 ``CDR`` 中考虑当期已发生的累计违约率的影响

        Returns:
            tuple: tuple contains:
                after (pd.DataFrame): 加压后的现金流归集表  \n
                assumptions (pd.DataFrame): 匹配到每一期的参数假设


        **计算逻辑**

        1. 假设值：

        .. code-block:: python
            :linenos:

                 CDR = 0.02
                 pool_start_CDR = 0.005
                 daycounts = np.array([30, 30, 30, 30, 30])
                 original['current_principal_due'] = np.array([3000, 1000, 500, 300, 200])
                 original['begin_principal_balance'] = np.array([5000, 2000, 1000, 500, 200])
                 initial_principal = 10000
                 begin_principal = 5000
                 total_days = 210
                 remain_days = 150  # sum(daycounts)
                 year_fracs = daycounts / 365.
                 CPR = 0.01


        2. 预期违约金额

            * 如果 ``minus_CDR=True``

                * 如果 ``well_distributed=False``

                .. code-block:: python
                    :linenos:

                        # 此时，相当于会考虑当期实际累计违约率的影响，并且累计违约率在存续期内的分布情况与现金流归集表一致
                        remain_CDR = max(CDR - pool_start_CDR, 0)  -> 0.015
                        DP = original['current_principal_due'] / begin_principal -> array([0.6 , 0.2 , 0.1 , 0.06, 0.04]) # DP总和等于1，长度与现金流归集表一致，不包括历史值
                        DP = DP / DP.sum()  #  归一化处理，正如主函数中对入参的介绍，此时只看 ``DP`` 的相对值
                        plan_DA = DP * remain_CDR * initial_principal -> array([90., 30., 15.,  9.,  6.])
                        # 此时，sum(plan_DA)/initial_principal + pool_start_CDR = 0.02

                * 如果 ``well_distributed=True``


                |

                .. code-block:: python
                    :linenos:

                        # 此时，相当于会考虑当期实际累计违约率的影响，并且累计违约率在存续期内均匀分布
                        remain_CDR = max(CDR - pool_start_CDR, 0) -> 0.015
                        DP = daycounts / remain_days -> array([0.2, 0.2, 0.2, 0.2, 0.2])  # DP总和等于1，长度与现金流归集表一致，不包括历史值
                        plan_DA = DP * remain_CDR * initial_principal -> array([30., 30., 30., 30., 30.])


            * 如果 ``minus_CDR=False``

                * 如果 ``well_distributed=False``

                .. code-block:: python
                    :linenos:

                        # 此时，不考虑当期实际累计违约率的影响，并且累计违约率在存续期内的分布情况与现金流归集表一致

                        DP = original['current_principal_due'] / initial_principal -> array([0.3 , 0.1 , 0.05, 0.03, 0.02]) # DP总和小于1，长度与现金流归集表一致，不包括历史值
                        plan_DA = DP * CDR * initial_principal -> array([60., 20., 10.,  6.,  4.])
                        # 此时，sum(plan_DA)/initial_principal + pool_start_CDR = 0.15 ，显然不等于输入的 `CDR`

                * 如果 ``well_distributed=False``

                .. code-block:: python
                    :linenos:

                        # 此时，不考虑当期实际累计违约率的影响，并且累计违约率在存续期内均匀分布
                        DP = daycounts / total_days -> array([0.14285714, 0.14285714, 0.14285714, 0.14285714, 0.14285714]) # DP总和小于1，长度与现金流归集表一致，不包括历史值
                        plan_DA = DP * CDR * initial_principal -> array([28.57142857, 28.57142857, 28.57142857, 28.57142857, 28.57142857])
                        # 此时 sum(DP * CDR * initial_principal) / initial_principal + pool_start_CDR = 0.019285714285714285


        3. 单期早偿率

            .. code-block:: python
                :linenos:

                # 年化早偿率（CPR）是计算每期早偿金额的重要参数。如果给出的是年化的早偿率，我们需要转化为每月或者是每个季度的早偿率。
                SMM = 1. - (1. - CPR) ** year_fracs -> array([0.00082571, 0.00082571, 0.00082571, 0.00082571, 0.00082571])


        4. 现金流加压


            将以上2.3中计算的单期早偿率和预期违约金额结合起来得到最后的加压后现金流归集表，包括实际违约金额、早偿金额和正常回款金额等数据。
            `2` 中计算的是金额， `3` 中计算的是比率，是因为这一模块下早偿是条件参数需要逐期根据当期期初本金余额计算，
            违约是无条件参数与当期期初本金无关，直接计算出所有期的预期违约金额更方便。

            .. code-block:: python
                :linenos:

                    after = self.cal_cashflow(plan_DA=plan_DA, SMMs=SMM, RRs=np.array([RR]), DPPs=np.array([DPP]),
                                  CPR_type=CPR_type, suoe=suoe, CDR_type='cumulative')
                    assumptions = pd.DataFrame({'DATE': self.original['date_'], 'SMMs': SMM, 'DP': DP}) # assumption 只是为了将匹配到每期的参数保存下来

        """

        if CPR < 0 or CPR > 1 or CDR < 0 or CDR > 1 or RR < 0 or RR > 1 or DPP < 0:  # 压力参数需符合要求
            raise ValueError("Invalid scenario params: CPR={0}, CDR={1}, RR={2}, DPP={3}".format(
                CPR, CDR, RR, DPP))

        # 1 违约事件分布 DP 和各期的违约金额plan_Dstaticpools_cdrs_initialA
        if minus_CDR:
            if DPs is not None:
                DP = DPs
            elif well_distributed: # 均匀分布
                DP = self.daycounts / self.remain_days
            else: # 与现金流归集表分布一致
                DP = self.original['current_principal_due'] / self.begin_principal  # 总和为1

            DP = DP / DP.sum()
            remain_CDR = max(CDR - self.pool_start_CDR, 0)
            plan_DA = DP * remain_CDR * self.initial_principal
        else:  # 此时存续期DP总和不应该为1
            if well_distributed:
                DP = self.daycounts / self.total_days
            else:
                DP = self.original['current_principal_due'] / self.initial_principal

            plan_DA = DP * CDR * self.initial_principal

        SMM = 1. - (1. - CPR) ** self.year_fracs

        # 2 现金流加压
        after = self.cal_cashflow(plan_DA=plan_DA, SMMs=SMM, RRs=np.array([RR]), DPPs=np.array([DPP]),
                                  CPR_type=CPR_type, suoe=suoe, CDR_type='cumulative')

        assumptions = pd.DataFrame({'DATE': self.original['date_'], 'SMMs': SMM, 'DP': DP})
        return after, assumptions

    def dynamic_scenario_analysis(self, SMMs: np.ndarray, DPPs: np.ndarray, RRs: np.ndarray, suoe: bool,
                                  CPR_type: str, minus_CDR: bool, CDR: (float, int), DPs: np.ndarray):
        """
        非循环非不良序列入参模式1

        Args:
            SMMs (np.ndarray, (n, )): 当期早偿率序列, 长度需要与现金流归集表一致，且期限匹配（为计算器输入序列通过 ``s_param_match.match_cashflow_cond_params`` 处理后的结果）
            CDR (float): 累计违约率
            DPs (np.ndarray, (n, )): 违约率分布,长度需要与现金流归集表一致，且期限匹配（minus_CDR为False时若是发行说明书现金流，DPs之和应该为1，若是存续期则会小于1；minus_CDR为True时，DPs总和不重要，只需要考虑输入的未来DPs之间的大小关系）
            DPPs (np.ndarray, (n, )): 延迟回收月, 假设违约后每个月回收多少
            RRs (np.ndarray, (n, )): recycle rate, 违约回收率，长度与 ``DPPs`` 一致
            suoe (bool): 采用缩额法(True)还是缩期法(False)计算早偿后现金流
            CPR_type (str): type1-SMM=当月提前还款本金/当月期初未偿本金, type2-SMM=当月提前还款本金/(当月期初未偿本金-当月计划偿还)
            minus_CDR (bool): 是否从输入的 ``CDR`` 中考虑当期已发生的累计违约率的影响


        Returns:
            tuple: tuple contains:
                after (pd.DataFrame): 加压后的现金流归集表  \n
                assumptions (pd.DataFrame): 匹配到每一期的参数假设


        **计算逻辑**


            从输入参数来看，相较于 ``scenario_analysis`` ，这里的参数都是通过 ``s_param_match`` 中的函数处理过的，可以直接匹配现金流归集表的序列。
            故通过 DPs 和 CDR ，计算得到 ``plan_DA`` 后使用 ``cal_cashflow`` 计算最终现金流即可

                *如果 ``minus_CDR=True``

                .. code-block:: python
                    :linenos:

                        remain_CDR = max(CDR - pool_start_CDR, 0)
                        # DP总和等于1，长度与现金流归集表一致，不包括历史值
                        plan_DA = DP * remain_CDR * initial_principal
                        after = self.cal_cashflow(plan_DA=plan_DA, SMMs=SMMs, RRs=RRs, DPPs=DPPs, CPR_type=CPR_type, suoe=suoe)

                * 如果 ``minus_CDR=False``

                .. code-block:: python
                    :linenos:

                        # DP总和小于1，长度与现金流归集表一致，不包括历史值
                        plan_DA = DP * CDR * initial_principal
                        after = self.cal_cashflow(plan_DA=plan_DA, SMMs=SMMs, RRs=RRs, DPPs=DPPs, CPR_type=CPR_type, CDR_type='cumulative', suoe=suoe)
                        # 不需要输出assumption, 因为不是在这个程序内部将假设参数与现金流归集表的每一个日期进行匹配

        """
        SMMs = np.array(SMMs)
        DPs = np.array(DPs)

        if SMMs .shape != (self.num_periods,) or DPs.shape != (self.num_periods,):  # 向量长度
            raise ValueError("Invalid param shape: CPRs: {0}, DPs: {1}. Expected shape: ({2},)".format(
                SMMs.shape, DPs.shape, self.num_periods))

        if minus_CDR:
            remain_CDR = max(CDR - self.pool_start_CDR, 0)
            DPs = DPs / DPs.sum()  # 重新分配，使得DPs总和为1，只反映了剩余未实现CDR的分布关系
            plan_DA = DPs * remain_CDR * self.initial_principal
        else:
            plan_DA = DPs * CDR * self.initial_principal

        after = self.cal_cashflow(plan_DA=plan_DA, SMMs=SMMs, RRs=RRs, DPPs=DPPs, CPR_type=CPR_type,
                                  CDR_type='cumulative', suoe=suoe)
        return after

    def scenario_analysis_ycdr(self, CPR: (float, int), YCDR: (float, int), RR: (float, int),
                          DPP: int, suoe: bool, CPR_type: str):
        """
        非循环非不良数值入参模式2

        Args:
            YCDR: conditional default rate, 年化违约率
            CPR: conditional prepayment rate, 年化早偿率
            RR: recycle rate, 违约回收率
            DPP: delay period, 回收延迟月数
            suoe (bool): 采用缩额法(True)还是缩期法(False)计算早偿后现金流
            CPR_type (str): type1-SMM=当月提前还款本金/当月期初未偿本金, type2-SMM=当月提前还款本金/(当月期初未偿本金-当月计划偿还)

        Returns:
            tuple: tuple contains:
                after (pd.DataFrame): 加压后的现金流归集表  \n
                assumptions (pd.DataFrame): 匹配到每一期的参数假设


        **计算逻辑**


        1. 每期的条件违约率


            年化违约率属于条件假设，不需要考虑历史累计违约率是多少，采用跟早偿一样的方式计算每期的条件违约率

            .. code-block::python
                :linenos:

                    SMDRs = 1. - (1. - YCDR) ** year_fracs


        2. 现金流加压


            这一模式与 ``scenario_analysis`` 中的区别在于违约率，每期早偿率的计算与  ``scenario_analysis`` 一样

            .. code-block:: python
                :linenos:

                    after = self.cal_cashflow(SMMs=SMMs, RRs=np.array([RR]), DPPs=np.array([DPP]),
                                        SMDRs=SMDRs, CPR_type=CPR_type, CDR_type='constant', suoe=suoe)
                    assumptions = pd.DataFrame({'DATE': self.original['date_'], 'SMMs': SMM, 'DP': DP}) # assumption 只是为了将匹配到每期的参数保存下来

        """

        if CPR < 0 or CPR > 1 or YCDR < 0 or YCDR > 1 or RR < 0 or RR > 1 or DPP < 0:  # 压力参数需符合要求
            raise ValueError("Invalid scenario params: CPR={0}, YCDR={1}, RR={2}, DPP={3}".format(
                CPR, YCDR, RR, DPP))

        SMDRs = 1. - (1. - YCDR) ** self.year_fracs
        SMMs = 1. - (1. - CPR) ** self.year_fracs
        # 2 现金流加压
        after = self.cal_cashflow(SMMs=SMMs, RRs=np.array([RR]), DPPs=np.array([DPP]),
                     SMDRs=SMDRs, CPR_type=CPR_type, CDR_type='constant', suoe=suoe)
        assumptions = pd.DataFrame({'DATE': self.original['date_'], 'SMMs': SMMs, 'SMDRs': SMDRs})
        return after, assumptions

    def dynamic_scenario_analysis_ycdr(self, SMMs: np.ndarray, SMDRs: np.ndarray,
                                       DPPs: np.ndarray, RRs: np.ndarray,
                                       suoe: bool, CPR_type: str):
        """
        非循环非不良序列入参模式2

        Args:
            SMMs (np.array): 当期早偿率序列, 与现金流归集表逐期匹配匹配（为计算器输入序列处理后的结果，下同）, 长度 (n, )
            SMDRs (np.array): 当期条件违约率序列 (n, )
            DPPs (np.array): 延迟回收月数, 假设违约后每个月回收多少 (m, )
            RRs (np.array):  违约回收率，长度与DPPs一致
            suoe (bool): 采用缩额法(True)还是缩期法(False)计算早偿后现金流
            CPR_type (str): type1-SMM=当月提前还款本金/当月期初未偿本金, type2-SMM=当月提前还款本金/(当月期初未偿本金-当月计划偿还)

        Returns:
            pd.DataFrame: assumptions: 匹配到每一期的参数假设


        **逻辑**


            这一模式的加压参数与现金流归集表逐期匹配，直接使用 ``cal_cashflow`` 加压

            .. code-block:: python
                :linenos:

                    after = self.cal_cashflow(SMMs=SMMs, SMDRs=SMDRs, RRs=RRs, DPPs=DPPs, CPR_type=CPR_type,
                                  suoe=suoe, CDR_type='constant')

        """

        SMMs = np.array(SMMs)

        if SMMs .shape != (self.num_periods,) or SMDRs.shape != (self.num_periods,):  # 向量长度
            raise ValueError("Invalid param shape: CPRs: {0}, SMDRs: {1}. Expected shape: ({2},)".format(
                SMMs.shape, SMDRs.shape, self.num_periods))

        after = self.cal_cashflow(SMMs=SMMs, SMDRs=SMDRs, RRs=RRs, DPPs=DPPs, CPR_type=CPR_type,
                                  suoe=suoe, CDR_type='constant')
        return after

    def cal_cashflow(self, SMMs: np.ndarray, RRs: np.ndarray, DPPs: np.ndarray, CPR_type: str, CDR_type, suoe: bool,
                     plan_DA: np.ndarray=None, SMDRs: np.ndarray=None):
        """
        计算加压后的现金流归集表

        Args:
            plan_DA (np.array): 预计违约金额序列 (n, )
            SMMs (np.array): 每期的条件早偿率 (n, )
            RRs (np.array): 违约回收率，与回收延迟月份一一对应 (m, )
            DPPs (np.array): 回收延迟月数 (m, )
            CPR_type (str): 早偿计算方式, 'type1' - smm * 期初本金余额， 'type2' - smm * (期初本金余额 - 预计当期本金回收)
            SMDRs (np.array): 每期的条件违约率 (n, )
            CDR_type (str):  cumulative-表示根据累计违约率加压, 需对应输入预计违约率序列 plan_DA,
                             constant-表示根据年化违约率加压, 需对应输入每期的违约金额占期初本金的比例 SMDRs
            suoe (bool): 采用缩额法(True)还是缩期法(False)计算早偿后现金流

        Returns:
            pd.DataFrame, after: 加压后现金流归集表

        **逻辑**


        1. 符号定义

            * `OPB` - 期初本金余额序列, (n, )， `opb` - 预测单期期初本金，首期为当前最新的剩余本金余额，如果 ``split_default=True`` 则在数据处理 ``data_full`` 中剔除掉了当期已违约；否则表示包括了当期违约金额并将其视为正常。
            * `PR` - 当期本金回收金额序列, (n, ), `pr` - 预测单期本金回收, `plan_pr` - 初始现金流归集表规定的某一期本应偿付的金额
            * `IR` - 当期利息回收金额序列, (n, ), `ir` - 预测单期利息回收
            * `CPB` - 期末本金余额序列 (n, ),
            * `DA` - 违约金额, (n, ), `da` - 预测单期实际违约金额, `plan-da` - 仅用加压参数计算的、不考虑与 `opb` 和 `pa` 产生矛盾时的调整的预期违约金额
            * `PA` - 早偿金额序列, `pa` - 预测单期实际早偿金额, `plan_pa` - 用加压参数计算的、不考虑与 `opb` 和 `pa` 产生矛盾时调整的预期违约金额
            * `BDA` - 期初违约本金余额, (n, )， 其中，如果 ``split_default=True`` ,BDA首期等于 ``begin_default`` , 否则则为0
            * `EDA` - 期末违约本金余额, (n, )， 等于 期初违约本金余额+当期违约-当期违约回收
            * `RA` - 违约回收金额, (n, )

        2. 逐期计算当期的早偿、违约、正常回收金额，假设当前为第 ``i`` 期

            1. 预期违约金额:

            .. code-block:: python
                :linenos:

                    # 如果 ``CDR_type = 'cumulative'`` ,即累计违约率计算违约金额
                    plan_da = expected_da(opb, plan_da=plan_DA[i], type_='cumulative')

                    # 如果 ``CDR_type = 'constant'`` ,即年化违约率计算违约金额
                    plan_da = expected_da(opb, mdr=SMDRs[i], type_='constant')


            2. 当期预期早偿金额

            .. code-block:: python
                :linenos:

                plan_pa = expected_pa(opb, SMMs[i], plan_pr, type_=CPR_type)  # SMMs[i] 表示现金流归集表中第 `i` 个归集日的条件早偿率


            3. 结合预期违约金额、预期早偿金额、当期本金回收，使用 ``cal_cashflow_1period`` 函数计算当期实际的违约、早偿、回收

            .. code-block:: python
                :linenos:

                pr, da, pa = cal_cashflow_1period(opb, plan_pr=plan_pr, plan_da=plan_da, plan_pa=plan_pa,
                                                  pr_opb=pr_opb, suoe=suoe)


        3. 利息固定为期初本金余额的比例，并且认为其中的违约金额是不会付息的，当期早偿金额在这一期仍会付息。

            .. code-block:: python
                :linenos:

                IR = (OPB - DA) * self.original_IR_OPB


        4. 违约回收金额 RA

            .. code-block:: python
                :linenos:

                ages = (daycounts / 30).cumsum()  # 指的是归集日与上一归集日 ``pool_start_date`` 之间的月数
                DA_copy = DA.copy()
                DA_copy[0] = DA_copy[0] + BDA[0] if self.begin_default_recover else DA_copy[0]  # 如果要对当期已违约金额进行回收，则会将其跟现金流归集表中第一期违约的本金加在一起算违约回收。
                RA = cal_RA(RRs, DPPs, DA_copy, ages, num_periods)

        5. 计算利息
        利息等于（期初本金余额-当期违约）*利息率

        """
        # 0 检查参数
        if ((CDR_type == 'cumulative') and (plan_DA is None)) or ((CDR_type == 'constant') and (SMDRs is None)):
            raise ValueError(f"违约率类型{CDR_type}与输入参数不符")

        # 分别计算期初本金余额OPB、本期应收本金PR、本期应收利息IR、期末本金余额CPB、提前偿还金额PA、违约金额DA、回收金额RA#
        # 1 初始化序列
        n = self.num_periods
        OPB, PR, IR, CPB, PA, DA, BDA, EDA = np.zeros(n), np.zeros(n), np.zeros(n), np.zeros(n), \
                                                 np.zeros(n), np.zeros(n), np.zeros(n), np.zeros(n)
        OPB[0] = self.begin_principal
        BDA[0] = self.begin_default if self.split_default else 0  # 可以不考虑当期的违约本金余额
        plan_PR = self.original['current_principal_due'] # 计划本金回款

        # 2 逐期计算
        for i in range(n):

            opb = OPB[i]  # 小写代表本期金额
            plan_pr = plan_PR[i]
            pr_opb = self.original_PR_OPB[i]

            if opb > 0:

                plan_da = self.expected_da(opb, plan_da=plan_DA[i], type_=CDR_type) if CDR_type == 'cumulative' else \
                          self.expected_da(opb, smdr=SMDRs[i], type_=CDR_type)

                plan_pa = self.expected_pa(opb, smm=SMMs[i], plan_pr=plan_pr, type_=CPR_type)
                pr, da, pa = self.cal_cashflow_1period(opb, plan_pr=plan_pr, plan_da=plan_da, plan_pa=plan_pa,
                                                       pr_opb=pr_opb, suoe=suoe)

                PA[i] = pa
                DA[i] = da
                PR[i] = pr
                if i < self.num_periods - 1:
                    OPB[i + 1] = max(opb - pr - da, 0)  # 计算下一期的期初本金，也就是本期的期末本金
                else:
                    # 最后一期仍有本金剩余的时候，记录该笔剩余
                    CPB[i] = max(opb - pr - da, 0)

        # 3 计算违约回收, 回收率为0时没必要
        ages = (self.daycounts / 30).cumsum()  # 简化计算账龄
        DA_copy = DA.copy()
        DA_copy[0] = DA_copy[0] + BDA[0] if self.begin_default_recover else DA_copy[0]  # 如果要对当前剩余违约本金模拟回收的话，则加入当前剩余本金
        RA = self.cal_RA(RRs=RRs, DPPs=DPPs, DA=DA_copy, ages=ages, total_period=self.num_periods)

        EDA = BDA[0] + DA.cumsum() - RA.cumsum()
        BDA[1:] = EDA[0: -1]

        IR = (OPB - DA) * self.original_IR_OPB  # 计算利息金额
        CPB[:-1] = OPB[1:]  # 每一期的期末本金都是下一期的期初本金

        after = pd.DataFrame(data={'date_': self.all_dates,
                                   'begin_principal_balance': OPB,
                                   'current_principal_due': PR,
                                   'current_interest_due': IR,
                                   'end_principal_balance': CPB,
                                   'prepay_amount': PA,
                                   'default_amount': DA,
                                   'recycle_amount': RA,
                                   'begin_default_balance': BDA,
                                   'end_default_balance': EDA
                                   })
        return after

    @staticmethod
    def cal_RA(RRs, DPPs, DA, ages, total_period):

        """
        计算违约回收序列，由于非循环购买的违约回收仅与违约金额有关，不需要跟早偿、违约一样考虑相互关系及对未来金额的影响，故在现金流归集表生成完毕后直接一次性生成即可

        Args:
            RRs (np.array): 违约回收率序列, 即输入的违约回收率序列中的 `values`, (m, )
            DPPs (np.array): 延迟回收月数序列, 即输入的违约回收率序列中的 `index`, (m, )
            DA (np.array): 违约金额序列, 如果 ``begin_default_cover = True`` 则输入中会额外在第一行加入当前违约金额 ``begin_default``,  (n, )
            ages (np.array): 每个归集日距离上一归集日 ``pool_start_date`` 的月份数,  (n, )
            total_period (int): 现金流归集表总期数 n

        Returns:
            np.array: RA: 违约回收金额序列


        **逻辑**


            * 生成 (m, n) 的回收时间序列, 列对应的是违约时间，行对应的是某笔违约的延迟回收月份，如::

                >> ages = np.array([1, 2, 3, 4, 5, 6])
                >> DPPs = np.array([1, 2, 3])
                >> repay_ages = np.array([[2, 3, 4, 5, 6, 7], [3, 4, 5, 6, 7, 8], [4, 5, 6, 7, 8, 9]])

            * 生成 (m, n) 的回收金额序列, 列对应的是违约时间，行对应的是延迟回收月份， 如::

                >> DA = np.array([10000, 20000, 30000, 30000, 20000, 10000])
                >> RRs = np.array([0.3, 0.2, 0.1])
                >> repay_amounts = np.array([[3000. 6000. 9000. 9000. 6000. 3000.], [2000. 4000. 6000. 6000. 4000. 2000.], [1000. 2000. 3000. 3000. 2000. 1000.]])

            * 以上两个数组的一一对应，每一期的回收金额由位于这一期的账龄（距离 ``pool_start_date`` 的天数）与上一归集日的账龄之间的 ``repay_ages`` 对应位置的 ``repay_amounts`` 加总获得::

                >> np.array([    0.  3000.  8000. 14000. 17000. 15000.])


        Notes:
            计算中不考虑超出现金流归集表最后一个归集日的违约回收

        """

        RA = np.zeros(total_period)
        if RRs.sum() > 0:
            len_rr = len(DPPs)
            repay_ages = np.tile(ages, (len_rr, 1)) + DPPs.reshape(len_rr, 1)
            repay_ages[np.isnan(repay_ages)] = float('inf')
            repay_amount = np.tile(RRs.reshape(len_rr, 1), (1, total_period)) * DA.reshape(1, total_period)

            last_age = 0
            for i in range(0, total_period):
                this_age = ages[i]
                if str(this_age) != 'nan':
                    RA[i] = sum(repay_amount[(repay_ages > last_age) & (repay_ages <= this_age)])
                    last_age = this_age
        return RA

    @staticmethod
    def expected_pa(opb: float, smm: float, plan_pr: float, type_):
        """
        计算预计早偿金额

        Args:
            opb (float): 期初本金额
            smm (float): 当期早偿率
            plan_pr (float): 计划还款额
            type_ (str):

                    * type1 (默认) - pa = opb * smm, 对应的输入 ``smm``
                    * type 2 - pa =（opb-plan_pr）*SMM

        Returns:

            float, pa: 预期早偿金额

        """
        if type_ == 'type1':
            pa = opb * smm  # 早偿金额=期初剩余本金*SMM
        elif type_ == 'type2':
            pa = (opb - plan_pr) * smm  # 早偿金额=（期初剩余本金-当期应收本金）*SMM
        else:
            raise ValueError("错误的早偿计算方式枚举值")
        return pa

    @staticmethod
    def expected_da(opb: float, type_: str, smdr: float = 0., plan_da: float = 0.):
        """
        计算单期的预期违约金额

        Args:
            opb (float): 期初本金额
            smdr (float): single monthly default rate 当期违约率, 用于计算 ``type_=constant`` 模式下当期违约额
            plan_da (float): cumulative模式下预计违约额
            type_ (str): cumulative-累计违约率型, 需对应输入预计违约率序列,
                        constant-年化违约率型, 需对应输入每期的违约金额占期初本金的比例 SMDRs

        Returns:
            float: da: 预期违约金额
        """

        if type_ == 'cumulative':
            da = min(opb, plan_da)  # 实际违约金额为预计违约金额和期初剩余本金中的较小值
        elif type_ == 'constant':
            da = smdr * opb

        return da

    @staticmethod
    def cal_cashflow_1period(opb, plan_pr: float, plan_da: float, plan_pa: float, pr_opb: float, suoe: bool):
        """
        | 输入预期违约金额、预期早偿金额、当期本金回收，计算当期实际的违约、早偿、回收, 使得期初期末本金和三者匹配。
        | 这么做主要是因为三者的值的计算具有独立性，比如根据累计违约率和违约分布算的当期可能的违约金额结合初始本金计算，未考虑当期实际的期初本金余额；
        当期本金回收在缩期下，即使加压后当期实际期初本金小于原始现金流归集表对应期次的期初本金余额。等等.
        最终可能会导致违约金额、早偿金额、当期本金回收的总和超过资产池期初本金余额，这显然不合理，故需要将三者的值调整到总和小于等于资产池期初本金余额

        Args:
            opb (float): 期初本金
            plan_pr (float): 预计当期回收(包含了早偿)
            plan_da (float): 预计当期违约
            plan_pa (float): 预计当期早偿
            pr_opb (float): 原始现金流归集表中当期本金回收与期初本金额的比
            suoe (bool): 是否用缩额法

        Returns:

            tuple: tuple contains:
                pr (float):  实际的正常本金流入+早偿流入  \n
                da (float):  实际的早偿流入 \n
                pa (float):  实际的当期违约金额


        **逻辑**


        * ``suoe=True`` 表示当存在早偿时，剩余贷款还是按照原定支付比例支付，而不是原定金额支付。

            .. code-block:: python
                :linenos:

                    pr = pa + pr_opb * (opb - pa - da)


            但是如果 ``pa + da > opb`` ，则需要先对 `pa`, `da` 进行调整，这里采用的是等比调整

            .. code-block:: python
                :linenos:

                    port_ = opb / (da + pa)
                    da = da * port_
                    pa = pa * port_


        * ``suoe=False`` 表示没有早偿的贷款还是按照原定支付金额支付。

            .. code-block:: python
                :linenos:

                    pr = plan_pr - da + pa


            但是如果违约的假设给的比较极端，导致 ``pr < 0`` , 此时由于 `plan_pr` 是给定的值（原始现金流归集表规定），不方便对 `pa`, `da` 采用比例调整，
            故采用较为粗暴的方法优先保证违约率假设的实现

            .. code-block:: python
                :linenos:

                    da = plan_pr + pa
                    pr = 0


            此外，如果 ``pr > opb - da`` 同样不合理，回收金额不可能超过资产池余额，由于上述原因，同样需要采用非比例调整

            .. code-block:: python
                :linenos:

                    pr = opb - da
                    pa = opb - da - min(opb - da, plan_pr)


        Notes:
            通过这一步的调整，最终得到的早偿、违约金额与输入的预期值有出入属于正常现象，特别是在给出的假设加压参数较为极端（如极高的违约率早偿率）、或者现金流归集表的最后几期时。

        """
        pr, da, pa = plan_pr, plan_da, plan_pa
        if suoe:
            if da + pa > opb:  # 20230113保证当期回收+违约总和不会超过期初本金
                port_ = opb / (da + pa)
                da = da * port_
                pa = pa * port_
            pr = pa + pr_opb * (opb - pa - da)  # 缩额法计算本金回收
        else:
            pr = plan_pr - da + pa
            if pr < 0:  # 当期回收不能小于0
                da = plan_pr + pa
                pr = 0
            elif pr > opb - da:  # 也不能超过剩余可回收的总额
                pr = opb - da
                pa = opb - da - min(opb - da, plan_pr)
        return pr, da, pa

    # 回归模型
    def scenario_analysis_regression(self, cdr_coef: pd.DataFrame, ucpr_coef: pd.DataFrame, df_cdr_factors: pd.DataFrame,
                                     df_ucpr_factors: pd.DataFrame, model_type: str, RRs: pd.Series, suoe: bool):
        """
        用于回归模型的压力参数预测， 因为回归模型存在需要随着现金流更新的参数，所以不能一次性生成压力参数序列后输入 ``cashflowPressure`` ，而是逐期生成

        Args:
            cdr_coef (pd.DataFrame) : 累计违约率回归模型系数
            ucpr_coef (pd.DataFrame) : 无条件早偿率回归模型系数
            df_cdr_factors (pd.DataFrame): 累计违约率回归模型因子
            df_ucpr_factors (pd.DataFrame): 无条件早偿率回归模型因子
            model_type (str): 模型类型

                                * `linear_model` - 线性回归模型
                                * `sigmoid_model` - 逻辑回归模型

            RRs (pd.Series):  违约回收率
            suoe (bool): True - 缩额法， False - 缩期法

        Returns:
            pd.DataFrame, after: 加压后现金流归集表


        **逻辑**

        1. 符号定义

            * `OPB` - 期初本金余额序列, (n, )， `opb` - 预测单期期初本金，首期为当前最新的剩余本金余额
            * `PR` - 当期本金回收金额序列, (n, ), `pr` - 预测单期本金回收, `plan_pr` - 初始现金流归集表规定的某一期本应偿付的金额
            * `IR` - 当期利息回收金额序列, (n, ), `ir` - 预测单期利息回收
            * `CPB` - 期末本金余额序列 (n, ),
            * `DA` - 违约金额, (n, ), `da` - 预测单期实际违约金额, `plan-da` - 仅用加压参数计算的、不考虑与 `opb` 和 `pa` 产生矛盾的预期违约金额
            * `PA` - 早偿金额序列, `pa` - 预测单期实际早偿金额, `plan_pa` - 禁用加压参数计算的、不考虑与 `opb` 和 `pa` 产生矛盾的预期违约金额
            * `BDA` - 期初违约本金余额, (n, )
            * `EDA` - 期末违约本金余额, (n, )
            * `RA` - 违约回收金额, (n, )
            * `cdr_factors` - pd.DataFrame, 保存的累计违约率模型因子数据，列名是因子，行名是月
            * `ucpr_factors` - pd.DataFrame, 保存的无条件早偿率模型因子数据，列名是因子，行名是月
            * `cdr_coef` - dict, 累计违约率回归模型系数
            * `ucpr_coef` - dict, 无条件早偿率回归模型系数
            * `this_ucpr` - float, 本期无条件年化早偿率
            * `last_ucpr` - float, 上一期的无条件年化早偿率
            * `last_cdr` - float, 上一期的累计违约率
            * `this_cdr` - float, 上一期的累计违约率
            * `initial_principal` - float, 初始本金额
            * `usmm` - float, 单期的无条件早偿率 = 单期早偿金额 / 初始本金额
            * `mdr` - float,  default rate, 单期的边际无条件违约率 = 单期违约金额 / 初始本金额

        2. 逐期计算当期的早偿、违约、正常回收金额，假设当前为第 ``i`` 期

            a. 根据回归系数和因子值计算当期无条件早偿率，计算中限制了无条件早偿率的增长上限，每期增长不能超过标准差的0.1%:

            .. code-block:: python
                :linenos:

                this_ucpr0 = calculate(ucpr_coef, ucpr_factors.loc[i, :], model_type)
                this_ucpr = min(1, last_ucpr + 0.001 * this_std, max(0, this_ucpr0))


            b. 根据回归系数和因子值计算当期无条件违约率:

            .. code-block:: python
                :linenos:

                this_cdr0 = calculate(cdr_coef, cdr_factors.loc[i, :], model_type)
                this_mdr = max(0, this_cdr0 - last_cdr) # 无条件违约率


            c. 计算预期早偿金额, 与自定义参数加压不同，模型预测的早偿率是无条件年化早偿率，需要转化为单期无条件早偿率（具体见研究报告）:

            .. code-block:: python
                :linenos:

                usmm = 1 - (1 - this_ucpr) ** self.year_fracs[i]
                plan_pa = self.initial_principal * usmm


            d. 预期违约金额, 这里不再用到违约分布，而是用到累计违约率的绝对增长值 `MDR` ，很容易可以理解 `MDR` 和违约分布 `DP` 的关系

            .. code-block:: python
                :linenos:

                    plan_da = self.initial_principal * this_mdr


            e. 结合预期违约金额、预期早偿金额、当期本金回收，使用 ``cal_cashflow_1period`` 函数计算当期实际的违约、早偿、回收

            .. code-block:: python
                :linenos:

                pr, da, pa = cal_cashflow_1period(opb, plan_pr=plan_pr, plan_da=plan_da, plan_pa=plan_pa,
                                                  pr_opb=pr_opb, suoe=suoe)

            f. 记录本期的各项压力参数值


            .. code-block:: python
                :linenos:

                self.params.loc[i, 'CPRs'] = 1 - (1 - pa/opb) ** (1 / self.year_frac[i])
                self.params.loc[i, 'UCPRs'] = this_ucpr
                self.params.loc[i, 'MDRs'] = this_mdr
                self.params.loc[i, 'USMMs'] = usmm
                self.params.loc[i, 'SMMs'] = usmm * initial_principal/opb


            g. 更新因子
                因子 `prob` , `previous` 需要重计算，定义见因子构造模块

        3. 违约回收、利息回收的计算与自定义参数加压 ``cal_cashflow`` 下一致


        Notes:
            由于回归模型的预测是按月，故不管现金流归集表原来是什么频率，在这个模块中只能输入月频

        """
        from abs.calculate.assetmodel.regression_model_predict import calculate
        self.RRs = RRs
        self.suoe = suoe
        cdr_factors = df_cdr_factors.copy()  # 保存的模型因子，部分因子会更新
        ucpr_factors = df_ucpr_factors.copy()
        ucpr_factor = df_ucpr_factors.columns
        cdr_factor = df_cdr_factors.columns
        self.params = pd.DataFrame(np.zeros([self.num_periods, 7]),
                                   columns=['CPRs', 'UCPRs', 'MDRs', 'CDRs', 'DP', 'USMMs', 'SMMs'])  # 保存参数
        self.params.loc[:, 'date_'] = self.all_dates
        n = self.num_periods
        OPB, PR, IR, CPB, PA, DA, BDA, EDA = np.zeros(n), np.zeros(n), np.zeros(n), np.zeros(n), \
                                                 np.zeros(n), np.zeros(n), np.zeros(n), np.zeros(n)

        OPB[0] = self.begin_principal
        BDA[0] = self.begin_default if self.split_default else 0
        last_cdr = self.pool_start_CDR
        last_ucpr = 0  # 不大影响下一期的CPR计算
        plan_PR = self.original['current_principal_due']
        for i in range(n):

            opb = OPB[i]
            if opb > 0:
                pr_opb = self.original_PR_OPB[i]
                plan_pr = plan_PR[i]

                # 无条件年化早偿率
                this_ucpr0 = calculate(ucpr_coef, ucpr_factors.loc[i, :], model_type)

                # 限制无条件早偿率范围上限为ucpr0+0.001std,其中std为前三期ucpr标准差
                if i > 2:
                    this_std = self.params.loc[i - 3: i, 'UCPRs'].std(ddof=1)
                    this_ucpr = min(1, last_ucpr + 0.001 * this_std, max(0, this_ucpr0))
                else:
                    this_ucpr = min(1, max(0, this_ucpr0))

                self.ucpr = this_ucpr
                self.last_ucpr = this_ucpr

                year_frac = self.year_fracs[i]
                usmm = 1 - (1 - this_ucpr) ** year_frac

                # 累计违约率
                this_cdr0 = calculate(cdr_coef, cdr_factors.loc[i, :], model_type)

                # 限制边际累计违约率的下限
                this_mdr = max(0, this_cdr0 - last_cdr)

                # 计算现金流
                # 违约金额，不能超过期初现金余额
                plan_da = self.initial_principal * this_mdr
                # 早偿金额，不超过期初本金余额
                plan_pa = self.initial_principal * usmm
                pr, da, pa = self.cal_cashflow_1period(opb, plan_pr=plan_pr, plan_da=plan_da, plan_pa=plan_pa,
                                                       pr_opb=pr_opb, suoe=self.suoe)

                PR[i], DA[i], PA[i] = pr, da, pa

                if i < self.num_periods - 1:
                    OPB[i + 1] = max(opb - pr - da, 0)  # 计算下一期的期初本金，也就是本期的期末本金
                else:
                    # 最后一期仍有本金剩余的时候，记录该笔剩余
                    CPB[-1] = max(opb - pr - da, 0)

                # 年化条件早偿率
                if year_frac > 0:
                    smm = usmm * self.initial_principal / opb
                    this_cpr = 1 - (1 - smm) ** (1 / year_frac)
                else:
                    this_cpr = 0
                    smm = 0

                self.params.loc[i, 'CPRs'] = this_cpr
                self.params.loc[i, 'UCPRs'] = this_ucpr
                self.params.loc[i, 'MDRs'] = this_mdr
                self.params.loc[i, 'USMMs'] = usmm
                self.params.loc[i, 'SMMs'] = smm

                last_cdr = this_cdr0
                last_ucpr = this_ucpr

                # 更新因子
                if i < n - 1:
                    # 重新计算Previous 和 POPB
                    popb = OPB[i + 1] / self.original.loc[i + 1, 'begin_principal_balance']
                    previous = OPB[i + 1] / self.initial_principal
                    if 'popb' in ucpr_factor:
                        ucpr_factors.loc[i + 1, 'popb'] = popb

                    if 'previous' in ucpr_factor:
                        ucpr_factors.loc[i + 1, 'previous'] = previous

                    if 'popb' in cdr_factor:
                        cdr_factors.loc[i + 1, 'popb'] = popb

                    if 'previous' in cdr_factor:
                        cdr_factors.loc[i + 1, 'previous'] = previous

        ages = (self.daycounts / 30).cumsum()  # 简化计算账龄
        DA_copy = DA.copy()
        DA_copy[0] = DA_copy[0] + BDA[0] if self.begin_default_recover else DA_copy[0]
        RA = self.cal_RA(np.array(list(self.RRs.values)), np.array(list(self.RRs.index)),
                         DA_copy, ages, self.num_periods)

        EDA = BDA[0] + DA.cumsum() - RA.cumsum()
        BDA[1:] = EDA[0: -1]

        IR = (OPB - DA) * self.original_IR_OPB  # 计算利息金额
        CPB[:-1] = OPB[1:]  # 每一期的期末本金都是下一期的期初本金

        after = pd.DataFrame(data={'date_': self.all_dates,
                                   'begin_principal_balance': OPB,
                                   'current_principal_due': PR,
                                   'current_interest_due': IR,
                                   'end_principal_balance': CPB,
                                   'prepay_amount': PA,
                                   'default_amount': DA,
                                   'recycle_amount': RA,
                                   'begin_default_balance': BDA,
                                   'end_default_balance': EDA
                                   })

        self.params.drop(index=self.params.loc[self.params[['CPRs', 'UCPRs', 'MDRs']].sum(axis=1)==0, :].index, inplace=True)
        if len(self.params) > 0:
            self.params.loc[:, 'CDRs'] = self.params['MDRs'].cumsum() + self.pool_start_CDR
            self.params.loc[:, 'DP'] = self.params['MDRs'] / max(self.params['CDRs'])
        return after



