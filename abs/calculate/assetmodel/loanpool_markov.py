# -*- coding: utf-8 -*-
"""
马尔可夫转移矩阵的现金流加压，由于每期都要结合转移后的状态数据和原始现金流归集表数据计算具体每个状态资金的金额，故需要逐期计算。
并且由于各状态金额由转移概率计算，不涉及加压参数，故不能调用自定义加压参数加压时的模块完成计算，需要单独编写。


说明 ::

    predict_allperiod   -- 现金流加压的主要函数
    predict_oneperiod   -- 对单期各项本金回收、违约、早偿的预测
    trans_matrix_match  -- 为单期选择合适的转移概率参数
    promatrix           -- 将序列参数转化为矩阵的形式


"""

import numpy as np
import pandas as pd
from utils.timeutils import age_


class LoanPoolMarkov(object):
    """

    Args:
        project_seq (str): 项目内吗
        status (pd.Series): 最新一期披露的状态数据
        transfer_prob (pd.DataFrame): 转矩概率
        cashflow (pd.DataFrame): 最新现金流归集表
        initial_default (float): 当前剩余违约本金余额
        last_pool_date (datetime.date): 最新历史归集日
        initial_date (datetime.date): 初始起算日
        n_markov (bool): 转移概率数量

                        * `True` - 每一期根据已存续月数匹配不同的转移概率矩阵
                        * `False` - 项目在存续期以相同的转移概率计算后续状态的金额

        suoe (bool): 缩额还是缩期法
        split_default (bool): 当前剩余违约本金余额是够已经从最新现金流归集表的期初本金余额中扣除
        begin_default_recover (bool): 是否要考虑当前剩余违约本金余额的回收

    """

    def __init__(self, project_seq, status, transfer_prob, cashflow, initial_default, last_pool_date, initial_date,
                 n_markov, suoe, split_default, begin_default_recover):

        self.before = cashflow
        self.num_periods = len(self.before)
        OPB = self.before['begin_principal_balance'].values
        valid_loc = ~((OPB == 0) | (np.isnan(OPB)))
        self.original_IR_OPB = np.zeros([self.num_periods, ])
        self.original_PR_OPB = np.zeros([self.num_periods, ])
        self.original_IR_OPB[valid_loc] = self.before['current_interest_due'].values[valid_loc] / OPB[valid_loc]
        self.original_PR_OPB[valid_loc] = self.before['current_principal_due'].values[valid_loc] / OPB[valid_loc]

        self.project_seq = project_seq
        self.n_markov = n_markov
        self.status = status
        self.initial_default = initial_default
        self.last_check_age = age_(initial_date, last_pool_date)

        use_single_matrix = True
        if 'age' in transfer_prob.columns:
            if not transfer_prob['age'].isna().all():
                use_single_matrix = True
        self.use_single_matrix = use_single_matrix

        self.df_prob_collect = transfer_prob
        self.s_cols = ['t_n', 't_o_1_30', 't_o_31_60', 't_o_61_90', 't_d', 'a_p', 'a_o', 'a_d']
        self.suoe = suoe

        self.age_series = self.before['age']
        self.date_series = self.before['date_']
        self.plan_principal_payments = self.before['current_principal_due']

        self.predict_status = pd.DataFrame(columns=self.s_cols)  # 根据转移矩阵预测的违约早偿等情况
        self.predict_status.loc[:, ['date_', 'age']] = self.before[['date_', 'age']].values
        self.split_default = split_default
        self.begin_default_recover = begin_default_recover

    def predict_allperiod(self):
        """
        逐期测算

        Returns:
            tuple: tuple contains:
                  * after (pd.DataFrame): 加压后的现金流归集表
                  * predict_status (pd.DataFrame): 直接用转移矩阵算出来的各期的预计各状态的本金金额，未考虑实际期初本金余额的限制。


        **逻辑**


        1. 符号定义

            * `OPB` - 期初本金余额序列, (n, )， `opb` - 预测单期期初本金，首期为当前最新的剩余本金余额。如果 ``split_default=True`` 则在数据处理 ``data_full`` 中剔除掉了当期已违约；否则表示包括了当期违约金额并将其视为正常正常。
            * `PR` - 当期本金回收金额序列, (n, ), `pr` - 预测单期本金回收, `plan_pr` - 初始现金流归集表规定的某一期本应偿付的金额
            * `IR` - 当期利息回收金额序列, (n, ), `ir` - 预测单期利息回收
            * `CPB` - 期末本金余额序列 (n, ),
            * `DA` - 违约金额, (n, ), `da` - 预测单期实际违约金额, `plan-da` - 仅用加压参数计算的、不考虑与 `opb` 和 `pa` 产生矛盾的预期违约金额
            * `PA` - 早偿金额序列, `pa` - 预测单期实际早偿金额, `plan_pa` - 禁用加压参数计算的、不考虑与 `opb` 和 `pa` 产生矛盾的预期违约金额
            * `BDA` - 期初违约本金余额, (n, )， 其中，如果 ``split_default=True`` ,BDA首期等于 ``begin_default`` , 否则则为0
            * `EDA` - 期末违约本金余额, (n, )
            * `RA` - 违约回收金额, (n, )
            * `last_status` -  上一期期末状态，其中，第一期的时候，如果 ``begin_default_recover=True`` , 则将违约本金余额状态金额设置为 ``initial_default`` , 否则设置为 0。

        2. 逐期计算

            1. 选择转移矩阵 `matrix_`

                * 如果前后两个归集日之间只有一个月，则直接用 ``trans_matrix_match`` 选择对应的转移概率参数
                * 如果前后两个归集日之间有多个月，则用 ``trans_matrix_match`` 选择多个转移概率参数，并将对应的转移矩阵累乘

            2. 计算单期各项回收::

                pr, da, pa, dr, ir, cpb, next_status = self.predict_oneperiod(last_status, matrix_, opb)

            3. 以 `next_status` 作为下一期的输入状态继续算下一期


        """
        n = self.num_periods
        OPB, PR, IR, CPB, PA, DA, BDA, EDA, RA = np.zeros(n), np.zeros(n), np.zeros(n), np.zeros(n), \
                                                 np.zeros(n), np.zeros(n), np.zeros(n), np.zeros(n), np.zeros(n)

        OPB[0] = self.before.loc[0, 'begin_principal_balance']
        BDA[0] = self.initial_default if self.split_default else 0 # BDA[0]的设置不影响违约回收，begin_default_recover的影响已经体现在了初始状态last_status里面

        # 如果不考虑当前已违约的偿还，则将初始违约金额设成0
        if self.begin_default_recover:
            self.status['t_d'] = self.initial_default
        else:
            self.status['t_d'] = 0
        last_status = self.status[self.s_cols].values

        last_check_age = self.last_check_age
        for idx in range(0, self.num_periods):
            self.period_num = idx
            this_check_age = self.age_series[self.period_num]
            transfer_num = this_check_age - last_check_age
            if transfer_num > 1:
                matrix_ = np.eye(8)
                for x in range(last_check_age+1, this_check_age+1):
                    p_1 = self.trans_matrix_match(x)
                    matrix_1 = self.promatrix(p_1)
                    matrix_ = matrix_ @ matrix_1
            else:
                p_ = self.trans_matrix_match(this_check_age)
                matrix_ = self.promatrix(p_)
            opb = OPB[self.period_num]
            pr, da, pa, dr, ir, cpb, next_status = self.predict_oneperiod(last_status, matrix_, opb)

            PR[self.period_num] = pr
            IR[self.period_num] = ir
            CPB[self.period_num] = cpb
            PA[self.period_num] = pa
            DA[self.period_num] = da
            RA[self.period_num] = dr
            last_check_age = this_check_age
            if self.period_num + 1 <= self.num_periods:
                self.predict_status.loc[self.period_num, self.s_cols] = next_status
            if self.period_num + 1 < self.num_periods:
                OPB[self.period_num + 1] = cpb
            last_status = next_status

        EDA = BDA[0] + DA.cumsum() - RA.cumsum()
        BDA[1:] = EDA[:-1]

        after = pd.DataFrame(data={'date_': self.date_series,
                                   'age': self.age_series,
                                   'begin_principal_balance': OPB,
                                   'current_principal_due': PR,
                                   'current_interest_due': IR,
                                   'end_principal_balance': CPB,
                                   'prepay_amount': PA,
                                   'default_amount': DA,
                                   'recycle_amount': RA,
                                   'begin_default_balance': BDA,
                                   'end_default_balance': EDA})

        return after, self.predict_status
        
    def predict_oneperiod(self, last_staus, transform_matrix, opb):
        """
        用某一期的各状态数据和转移概率计算下一期预测状态，并根据当期期初本金余额进行调整

        Args:
            last_staus (np.array): 上一期的状态
            transform_matrix (np.array): 转移矩阵
            opb (float): 当期期初本金余额

        Returns:
             tuple: tuple contains:
                    * pr (float): 当期回款（除违约回收）总额
                    * da (float): 当期违约本金额
                    * pa (float): 当期早偿金额
                    * ra (float): 当期违约回收
                    * ir (float): 当期利息回收
                    * cpb (float): 当期期末本金余额
                    * ps (np.array): 当期期末状态


        **逻辑**

        1. 用上一期状态乘以转移矩阵得到下一期状态::

                ps = last_staus @ transform_matrix


        2. 根据如下规则计算指标 :

                * 定义：`ps[0]` 正常本金余额， `ps[1]` 逾期1-30天本金余额， `ps[2]` 逾期31-60天本金余额， `ps[3]` 逾期61-90天本金余额， `ps[4]` 违约本金余额， `ps[5]` 早偿本金总和， `ps[6]` 逾期回收本金总和,  `ps[7]` 违约回收本金总；`ls` 则为上一期的状态，也即当期开始状态转移前的状态。
                * 新增违约 ``da = ps[4] + (ps[7] - ls[7]) - ls[4]``  # 即当期本金余额增长 + 当期违约回收额增长
                * 当期早偿 ``pa = ps[5] - ls[5]``  # 状态包含的是此前回收的总和，并不仅是当期的值，故需要用当期减去上一期
                * 当期违约回收 ``da = ps[7] - ls[7]``
                * 当期逾期回收 ``overdue_return = ps[6] - ls[6]``

        3. 计算正常回收金额

            由于现金流归集表对当期正常本金回收有规定，不应无视现金流归集表的信息完全采用转移概率计算当期正常回收。故当期正常回收计算如下：

                * ``suoe=True`` , 早偿缩额法下，计算正常回收::

                        healthy_return =  ps[0] * pr_opb  # pr_opb 为原始现金流归集表中，当期本金回收与当期期初本金余额的比值

                * ``suoe=False`` , 早偿缩期法下，正常回收应为::

                        healthy_return = min(plan_pr, ps[0])  # plan_pr 为原始现金流归集表中，这一期的绝对本金回收额

            最后计算 `pr` , 在自定义加压中，`pr` 指的是正常回收与早偿回收的总和，但是在这个模型中将逾期和逾期回收单独拆分出来了。为了与自定义加压下的现金流归集表口径一致，故有::

                pr =  healthy_return + pa + overdue_return

        4. 应收利息::

                ir = (opb - da) * ir_opb # ir_opb 为原始现金流归集表中当期利息回收与当期期初本金余额的比值

        5. 当期期末状态

            显然，在状态转移过程中，实际上忽略了正常本金余额能够正常回收的概率，而在第3步才计算了当期应正常回收的金额，故需要将这边金额从转移后的状态中扣除，才可以用于下一期的状态转移::

                ps[0] = ps[0] - healthy_return

        6. 逾期金额的处理

            此外，如果在最后一期，逾期本金逾期并未完成回收，则将这笔金额视作违约，加到当期新增违约，以防止现金流归集表中的余额与最新历史资产池余额对不上及与自定义加压模式中的口径不一致::

                            da = da + sum(ps[1: 4])

        """
        ps = last_staus @ transform_matrix
        ps = ps.flatten()
        ls = last_staus.flatten()

        da = ps[4] + (ps[7] - ls[7]) - ls[4]  # 新增违约
        pa = ps[5] - ls[5]  # 新增早偿
        ra = ps[7] - ls[7]  # 违约回收
        overdue_return = ps[6] - ls[6]  # 逾期回收

        #  如果是最后一期的话，将逾期金额都算到违约里去
        if self.period_num == self.num_periods - 1:
            delay_ = sum(ps[1:4])
            da += delay_
            ps[1:4] = 0

        plan_pr = self.plan_principal_payments[self.period_num]
        pr_opb = self.original_PR_OPB[self.period_num]
        if self.suoe:
            healthy_return = ps[0] * pr_opb  # 正常回收
            pr = healthy_return + pa + overdue_return  # 跟现金流计算器的逻辑不一样，因为多分出了逾期这一项
        else:
            healthy_return = min(plan_pr, ps[0])
            pr = healthy_return + pa + overdue_return
        ps[0] = ps[0] - healthy_return
        ir = (opb - da) * self.original_IR_OPB[self.period_num]

        cpb = opb - pr - da  # 余额里不能考虑违约金额
        if cpb < 0:
            print('pause')
        return pr, da, pa, ra, ir, cpb, ps


    def trans_matrix_match(self, age):
        """
        匹配转移矩阵，如果是 ``n_markov=True`` , 根据当期的账龄匹配转移矩阵中对应账龄的(如果没有查到，则用更早期次的；
        如果晚于拟合得到的最大账龄的转移矩阵，则后面的期次一直用最后一个转移矩阵；如果早于拟合得到的最早的一期转移矩阵，则用首个转移矩阵）；
        如果 ``n_markov=False`` 则只会有一个矩阵

        Args:
            age (int): 存续期次，即距离初始起算日的月数（四舍五入）

        Returns:
            np.array: 匹配的转移概率矩阵

        """
        if self.use_single_matrix or (not self.n_markov):
            match_result = self.df_prob_collect.copy()
            match_result.sort_values(by=['age', 'prob_type'])
            match_result.drop_duplicates(subset=['prob_type'], keep='last', inplace=True)
        else:
            min_age = self.df_prob_collect['age'].min()
            max_age = self.df_prob_collect['age'].max()
            if age in self.df_prob_collect['age']:
                match_result = self.df_prob_collect.loc[self.df_prob_collect['age'] == age, :]
            else:
                if age > max_age:
                    match_result = self.df_prob_collect.loc[self.df_prob_collect['age'] == max_age, :]
                elif age < min_age:
                    match_result = self.df_prob_collect.loc[self.df_prob_collect['age'] == min_age, :]
                else:
                    last_age = age - 1
                    while (last_age not in self.df_prob_collect['age']):
                        last_age -= 1

                    match_result = self.df_prob_collect.loc[self.df_prob_collect['age'] == last_age, :]

        if len(match_result) != 6:
            raise ValueError("转移概率参数数量不对")
        else:
            match_result.sort_values(by=['age'], inplace=True, ascending=True)
            return match_result['prob_num'].values


    @staticmethod
    def promatrix(p_):
        """将序列的转移概率数据转化为矩阵形式"""
        matrix = [[1-p_[0]-p_[1], p_[0], 0, 0, 0, p_[1], 0, 0], 
                     [0, 0, p_[2], 0, 0, 0, 1-p_[2], 0],
                     [0, 0, 0, p_[3], 0, 0, 1-p_[3], 0],
                     [0, 0, 0, 0, p_[4], 0, 1-p_[4], 0],
                     [0, 0, 0, 0, p_[5], 0, 0, 1-p_[5]],
                     [0, 0, 0, 0, 0, 1, 0, 0],
                     [0, 0, 0, 0, 0, 0, 1, 0],
                     [0, 0, 0, 0, 0, 0, 0, 1]]

        trans_matrix = np.array(matrix)

        return trans_matrix
        