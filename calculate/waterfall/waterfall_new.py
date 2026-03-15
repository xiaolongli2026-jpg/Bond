# -*- coding: utf-8 -*-
"""
现金流瀑布，需要结合 ``Tranche`` 一起理解，比较复杂一些。 主要关注：

说明 ::

    Waterfall                   --   类函数，适用于不重新测算循环购买ABS现金流的所有情况。
    Waterfall.run_all_periods   --   逐期模拟现金流入和现金流分配过程。主要看这个，其他的 method 服务于这个
    Waterfall.period_pool       --   period_pool 为 run_all_periods 模拟流入的过程，现金流为现金流归集表获取
    Waterfall_rev               --   仅用于重新测算循环购买现金流的情况，继承了 Waterfall，但是修改了 period_pool
    Waterfall_rev.period_pool   --   为循环购买计算每期的现金流入和循环购买、税费等。相当于对于循环购买来说，继承了 Waterfall 的现金流分配模块，但是替换了其中的模拟现金流入模块


"""
import math
import re
import numpy as np
from datetime import date
from collections import defaultdict
from typing import Union

import pandas as pd

from utils.operatorutils import and_, or_
from utils.timeutils import count_months

from calculate.asset.loanpool import LoanPool
from doc.enumerators import trigger_event_dict, cond_pairs


class Waterfall(object):

    ignored_target = ("tax", "exp", "tax,exp", "exp,tax", 'int_col', 'ignore')  # 不处理的节点

    # 为同支付顺序上的数据添加优先级.有时候次级超额收益虽然跟次级本金在同一行,但是实际上应该要先付完本金再考虑超额收益
    priority_ = {'sub[d]*.exret': 2}  # 没有列举的则平等为0.数字不重要,体现顺序就可以
    complement_ = {'sub[d]*.int': False, 'sub[d]*.exret': False, 'incentive_fee': False}

    items = {'exp_due': 0., 'tax_due': 0., 'return_decline': 0., 'vacant_period': 0, 'asset_under_debt_period': 0,
             'reinv_due': 0, }

    def __init__(self, tranches_obj, schedule, payment_sequence, trigger_events, trigger_rules, is_revolving,
                 initial_principal, pred_start_date, history_date, exp_rate, tax_rate, pay_exp_tax,
                 int_col, prin_col, last_cdr, cumsum_loss):
        """
        模拟现金流回收和分配

        Args:
            tranches_obj: list of Tranche object, 其中的计息日、还本日的长度(n)与日历必须一致
            schedule (pd.DataFrame): 日历 （n, )
            payment_sequence (pd.DataFrame): 支付顺序表
            trigger_events (dict): 估值日实际已发生的重大事项情况
            trigger_rules (pd.DataFrame): 触发事件表
            is_revolving (bool): 是否循环购买
            initial_principal (float): 入池资产池本金金额
            pred_start_date (int): 开始分配的归集日（最新历史归集日的下一期）处于日历的第几行，从这一行开始模拟资产池本金回收。注意各个证券的 `Tranche` 实例在分配开始前也处于这一行
            history_date (datetime.date): 最新历史归集日
            exp_rate (float): 费率
            tax_rate (float): 税率
            pay_exp_tax (bool): 是否计算税费
            int_col (float): 收入帐账户余额
            prin_col (float): 本金帐账户余额
            last_cdr (float): 当前的累计违约率
            cumsum_loss (float): 当前的累计违约金额

        Attributes:
            ignored_target (tuple): 不进行支付的节点去向，如果在维护中有新增节点去向，如暂时不进行处理，可以在这个列表加入，或者在 ``enumerators.target_pairs`` 映射为 'ignore'
            priority_ (dict): 为节点去向设置优先级，key为节点去向的正则表达式，value为优先级，数字越大优先级越低，不在其中的统一设置优先级为0。当前仅设置了超额收益为次优先，如果有其他劣后支付的，需要在字典中增加。
            complement_ (dict): 节点去向是否补充支付（即支付来源账户的钱不够，用其他账户补足），key为节点去向的正则表达式，value为布尔值，不在其中的统一设置为 `True` ,即需要补充支付。当前仅设置次级超额收益、次级期间收益和超额激励设置为 `False`
            period_num (int): 处于日历的第几行，随着逐期分配的进行逐渐增加，最后等于 `n-1`。即逐期模拟时，`period_num` 就是当前模拟到了第几期
            items (dict): 一些分配过程中用到的属性的集合，由于会不断更新，因此保存在dict，而不是设置为类属性。包括:

                              * int_col (float): 收益账余额
                              * prin_col (float): 本金帐余额
                              * total_col (float): 如果不分收益账和本金帐时，使用的混合账户本金余额
                              * pool_principal (float): period_num 期期末资产池余额
                              * cum_paid_diff (float): 总差额支付
                              * cum_int_to_prin (float): 收益账向本金帐转移总额，初始为0
                              * cum_prin_to_int (float): 本金账向收益账转移总额，初始为0
                              * cum_loss (float): 总违约金额，初始为 `cumsum_loss`
                              * cdr (float): 累计违约率，初始为 `last_cdr`
                              * incentive_fee_due (float): 应付超额激励，设置为无穷大，最终实际付多少由支付上限和账户余额决定
                              * 各重大事项, 可以为'eoaa','eod', 'server_dismiss','eoce','fbb'等（详细的枚举见 ``enumerators``），
                              * exp_due (float): 应付费，初始设置为 0。根据 `exp_liabilities` 的定义，每期模拟时都会将当期的 `exp_due` 记录到 `exp_liabilities` 中
                              * tax_due (float): 应付税，初始设置为 0
                              * return_decline (float): 收入变化 = 1 - 当期本息回收总和 / 上期本息回收总和，初始设为 0。用于触发事项的判定，有条件规定，收入下降超过多少即触发事件。
                              * vacant_period (int): 记录账户资金连续沉淀超过入池本金的期次，初始为0。如果有一期没有超过，则清零重新算。 用于触发事项的判定。
                              * asset_under_debt_period (int): 记录资产连续低于负债的情况（即资产池期末本金余额 / 证券端本金余额小于1），初始设为 0。如果有一期没有超过，则清零重新算。 用于触发事项的判定。
                              * reinv_due (float): 应进行的循环购买支出
                              * exp_clear (bool): 是否能够足额支付税费

                              具体有那些看重大事项发生情况、支付顺序、触发事件表有哪些，并且在模拟分配过程中会持续更新事项。对于某些不存在于三表中的事项，即使设置了也用不到。
        _last_pool_period_num (int): 上一个归集日所在行。因为日历由归集日和支付日交叉构成，因此上一行不一定是上一归集日。这里记录上一归集日所在行
        last_check_date (datetime.date): 上一归集日
        total_periods (int): 日历总长度，等于 `n`
        __events_list (list): 用于保存重大事项触发情况，list中的元素为各个模拟期的重大事项构成的dict。
        child_dict (dict): 每个节点对应的子节点
        mixed (bool): 是否为混合账户，初始为否，如果在支付顺序中遇到支付来源为 `total_col` ，则会转为是，并且将本金账和利息帐的资金转到混合账户中。一旦认定为混账账户，则不可逆，即不会再有资金从本金帐和收益账转入转出。
        tax_payments (np.array): 支付税额记录 (n,)
        exp_payments (np.array): 支付费额记录 (n,)
        reinv_payments (np.array): 循环购买支出记录 (n,)
        incentive_fee_payments (np.array): 支付超额激励记录 (n,)
        tax_liabilities (np.array): 应付税额记录，tax_payments 为实付 (n,)
        exp_liabilities (np.array): 应付费额记录 (n,)
        success_nodes (dict): 顺利进入过的节点信息, 用于检查是否存在无法满足节点条件导致的证券资金里不足

        Notes:
              * 如果有新增的节点条件、触发事件条件维护方式，则参考 `items` 中 'return_decline' ， 'vacant_period' 的设置，将新条件公式中的因素中，非对应到单个证券、优先级证券的因素都设置在 `items` 中。
              * 当前、上一日期，都是针对所处的模拟期次 `period_num` 而言
        """
        self.payment_sequence = payment_sequence
        self.__initialize_events(trigger_events)
        self.trigger_rules = trigger_rules
        self.pred_start_date = pred_start_date

        self.tranches_obj = tranches_obj # a list of all Tranche objects
        self.is_revolving = is_revolving

        self.last_cdr = last_cdr
        self.initial_principal = initial_principal

        self.last_check_date = history_date
        self.tax_rate = tax_rate if pay_exp_tax else 0.
        self.exp_rate = exp_rate if pay_exp_tax else 0.

        self.total_periods = len(schedule)
        self.security_status = schedule["security_status"] == 1
        self.pool_status = schedule["pool_status"] == 1
        self.revolving_period = schedule["is_revolving_period"] == 1
        self.all_dates = schedule['all_date']
        self.pool_dates = schedule['pool_date']
        self.schedule = schedule
        # 筛选出other类的节点，这是起点
        self.first_node = list(payment_sequence[payment_sequence["node_type"] == "other"].itertuples())[0]

        # 找到每个节点的子节点（们）
        self.child_dict = defaultdict(list)
        for nd in self.payment_sequence.itertuples():
            self.child_dict[nd.parent_node_no].append(nd)

        self.items['int_col'] = int_col
        self.items['prin_col'] = prin_col
        self.items['total_col'] = 0
        self.items['pool_principal'] = initial_principal

        self.mixed = False  # 本金账收入账是否混合，初始值为False
        self.items['cum_paid_diff'] = 0.
        self.items['cum_int_to_prin'] = 0.
        self.items['cum_prin_to_int'] = 0.

        self.items['cum_loss'] = cumsum_loss
        self.items['cdr'] = last_cdr
        self.items['incentive_fee_due'] = float('Inf') # 具体付多少由支付上限和账户余额决定
        self.success_nodes = defaultdict(bool)  # 是否成功地进入过节点
        self.success_branches = defaultdict(bool)  # 记录进入的分支情况

        self.tax_payments, self.exp_payments = \
            np.full([self.total_periods, ], 0.), np.full([self.total_periods, ], 0.) # 实付税费

        self.reinv_payments, self.incentive_fee_payments = \
            np.full([self.total_periods, ], 0.), np.full([self.total_periods, ], 0.)  # 循环购买支出, 激励费用

        self.tax_liabilities = np.full([self.total_periods, ], 0.)  # 应付税
        self.exp_liabilities = np.full([self.total_periods, ], 0.)  # 应付费

        self.__events_list = []
        self.infos = []
        self.period_num = 0

        self._last_pool_period_num = 0  # 上一个资产池归集日的编号

    def __initialize_events(self, events):
        """初始化事件是否发生: 由于事件发生与否主要影响分支的选择，因此将支付顺序中的节点条件都提取出来，
        然后将不带 'not' 的设置为否（即默认事件没有发生）,并保存在类属性 `items` 里。然后根据 `events` 中的重大事项发生情况进行更新.如果是不影响分支条件的事项直接忽略"""
        valid_branches = np.unique(self.payment_sequence[['branch_condition', 'node_condition']].astype(str))
        for br in valid_branches:
            if "," not in br: # 单个条件
                if 'not' not in br:
                    self.items[br] = False
                else:
                    self.items[br] = True
            else:  # 如果支付顺序的节点条件有多个
                str_ = re.findall('\((.*)\)', br)[0]
                list_ = str_.split(',')
                for x in list_:

                    if 'not' not in br:
                        self.items[x] = False
                    else:
                        self.items[x] = True
        self.update_events(events)

    def update_events(self, new_events):
        """
        根据新的事件情况更新类属性 ``items`` 中保存的事件发生情况，只更新不带 'not' 的类型，带 'not' 的类型会在判断节点条件时，
        自动设置为与事件发生情况相反。比如 ``eoaa=True`` , 则 ``not(eoaa)=False`` . 另外. 假设事件一旦发生就不可逆

        Args:
            new_events (dict): key-事件名称， value-bool, 是否发生

        """
        for e in new_events.keys():
            if e in self.items:
                self.items[e] = self.items[e] or new_events[e]  # 更新事件
            else:
                self.items[e] = new_events[e]  # 新增事件

    def run_all_periods(self, prediction, amortPeriodOnly=False):
        """

        现金流分配主要函数

        Args:
            prediction (pd.DataFrame): 现金流归集表，需要与schedule长度相同, 并且date_列与日历的pool_date列一一对应.按照日期升序排列
            amortPeriodOnly (bool): 是否只在摊还期时，模拟账户资金的收入和支出:

                * `False` （默认）- 不管在循环期还是摊还期，都会将现金流归集表的应收本金和应收利息列加入到账户余额中，并且证券端的支付需要从账户资金中扣除对应金额。证券端是否足额支付取决于账户资金是否足够。
                * `True` - 适用于循环购买不重新测算的情况。仅在摊还期进行前述操作，在循环期，则账户余额不变，如果需要支付优先级利息，则直接认为可以足额支付，并记录当期实付利息等于应付利息。

        **逻辑**

        从日历第一行开始进行如下操作：

        1. 当 ``period_num < pred_start_date `` 时跳过。从 `pred_start_date` 行开始正式模拟现金流归集。
        2. 在 `items` 中加入 'inamortperiod' 和 'not(inamortperiod)'，两者刚好相反，分别表示当前处于摊还期、当前处于循环期。同时设置 `if_deduct_col` ，表示是否扣减账户资金。
        如果 ``amortPeriodOnly=True`` 且当前处于循环期（ ``items['not(inamortperiod)']=True`` ）,则为否，其他时候均为是。
        3. 设置 `schedule["security_status"]` 设置 `status` ，表示当期是否是证券支付日（本金或利息支付日）。再根据 `schedule["pool_status"]` 设置 `pstatus`, 表示当期是否是归集日
        4. 如果 ``pstatus=True`` , 当 ``if_deduct_col=False`` 时，不对资产池进行任何操作，因为不会增减账户资金，同时也不能模拟税费的支付。
        当 ``if_deduct_col=True`` 时则需要模拟资金流入(调用 ``simulate_return`` )。具体为:

            * ``period_pool`` 返回 `row` ,保存了当期的应收本金（current_principal_due)、应收利息(current_interest_due)、
                应付税(tax_paid_liability)、应付费（exp_paid_liability）数据、违约回收recycle_amount，再投资收入（reinvest_income，目前全部设为0） 。
            * 如果 ``mixed=True`` , 则将 ``row.current_interest_due + row.reinvest_income + row.current_principal_due + row.recycle_amount`` 加入到混合账户中。
            否则将 ``row.current_interest_due + row.reinvest_income`` 加入到收入帐，``row.current_principal_due + row.recycle_amount`` 加入到本金帐
            * 支付税费：首先将 `row.tax_paid_liability` 和 `row.exp_paid_liability` 分别加入到应付税和应付费中，
            然后用 ``pay_exp_tax`` 完成税费支付。 因为税费简化计算了，因此不运行支付顺序，直接提前扣除
            * 更新相关数据，包括 `items` 中的 'return_decline', 'cum_loss', 'cdr', 'pool_principal'
            * 如果当前处于循环期，则还需要更新应付循环购买金额，即在应付循环购买金额中加入 `row.current_revolving_out` 。
            如果得到的应付循环购买金额不为0，后续在跑支付顺序时会模拟循环购买支出。

        5. 运行支付顺序 （具体见 ``run_from_node`` )，此时当 `status` 为 `True` 时会用账户里的资金分配现金流，如果为 `False` ，不会支付
        6. 判断触发事项情况。（仅当证券还未完全偿付本金时）

            * 通过 ``self.evaluate_condition`` 来对触发事项中的触发条件是否满足进行判断， ::

                self.trigger_rules['cond_result'] = \
                [self.evaluate_condition(cond_expr) for cond_expr in self.trigger_rules['trigger_condition']]

            * ``Trigger.events`` 判断最终触发的重大事项， 然后用 ``self.update_events`` 更新 `items` 中保存的重大事项发生情况。
            如果触发的重大事项刚好是支付顺序的分支条件，则下一模拟期就会跑支付顺序中不同的分支。

        7. 将所有证券都推到下一期 （证券日期与资产池日期需同步,即tranche实例和waterfall实例所在行需要一致）::

                        self.tranches_obj.push_tranches_to_next_period()

        Notes:
            重大事项一旦触发，即使往后不再满足条件，也不会改为未发生该重大事项。

        """
        self.amortPeriodOnly = amortPeriodOnly and self.is_revolving  # 如果是非循环购买,无论如何都需要模拟资产池归集的过程
        self.prediction = prediction
        self.last_period_cash = 0   # 上一期回收本金金额
        for i in range(0, self.total_periods):# 对于每一期，跑一次 waterfall
            if i < self.pred_start_date:
                continue

            self.period_num = i
            self.status = self.security_status[self.period_num]  # 证券端是否支付本金或者利息
            self.pstatus = self.pool_status[self.period_num]  # 资产池是否归集资金

            self.items['not(inamortperiod)'] = self.revolving_period[self.period_num]  # 是否在循环期
            self.items['inamortperiod'] = not self.revolving_period[self.period_num]  # 是否在摊还期
            self.if_deduct_col = not (self.amortPeriodOnly and self.items['not(inamortperiod)'])  # not(在循环期且不扣减资金)

            # 1. 模拟回款行为
            self.simulate_return()

            # 2. 从第一个节点开始跑 waterfall
            self.run_from_node(self.first_node)

            # 3. 每一期之后，更新每个证券的应付数据, tranche object 也会变
            if self.period_num < self.total_periods - 1:  # 到了最后一期不不需要再计算下一期的证券端应付
                self.tranches_obj.push_tranches_to_next_period(self.period_num + 1)

            # 4 更新上一资产池归集日的日期
            self._last_pool_period_num = self.period_num if self.pstatus else self._last_pool_period_num  # 上一归集日所在行
            self.last_check_date = self.all_dates[self.period_num] if self.pstatus else self.last_check_date  # 上一归集日

    def trigger_judge(self):
        """
        触发事件判断

        """

        asset_debt_ratio = self.items['pool_principal'] / self.tranches_obj.all_sec.balance \
            if self.tranches_obj.all_sec.balance > 0 else float('Inf')  #  资产余额比证券本金余额总和
        # 记录资产"连续"低于负债的情况,如果发生则次数加一,如果这次没有发生,则清空次数
        if asset_debt_ratio < 1:
            self.items['asset_under_debt_period'] = self.items[
                                                        'asset_under_debt_period'] + 1 if asset_debt_ratio < 1 else 0

        # 记录账户资金连续沉淀超过初始本金的情况
        if self.account > self.initial_principal:
            self.items['vacant_period'] += 1
        else:
            self.items['vacant_period'] = 0

        # 每跑完一期更新状态，判断是否触发事件
        if self.tranches_obj.all_sec.balance > 0:  # 如果已经还完了，不更新触发事件
            self.trigger_rules['cond_result'] = \
                [self.evaluate_condition(cond_expr) for cond_expr in self.trigger_rules['trigger_condition']] # 条件判断

            events = Trigger.events(self.trigger_rules, self.all_dates[self.period_num])
            self.update_events(events)  # 更新属性中的事件发生与否,以改变运行分支
            events.update({'date_': self.all_dates[self.period_num]})
            self.__events_list.append(events)  # 保存,用于最后输出

    def simulate_return(self):
        """
        模拟资产池单期回款行为.同时记录收入变化和更新后的累计违约率, 资产池余额, 累计违约本金额

        """
        # 对于不重新计算的循环购买，其循环期没有账户流入,其他情况均正常流入（注意,循环购买在循环期没有资金流入时,各项指标的计算上会存在问题）
        if self.pstatus:
            row = self.period_pool()
            if self.if_deduct_col:
                if self.mixed:
                    self.add_to_col("total_col", row.current_interest_due + row.reinvest_income + \
                                    row.current_principal_due + row.recycle_amount)
                else:
                    self.add_to_col("int_col", row.current_interest_due + row.reinvest_income)
                    self.add_to_col("prin_col", row.current_principal_due + row.recycle_amount)

                # 支付税费
                self.items['tax_due'] = self.items['tax_due'] + row.tax_paid_liability  # 应付税
                self.items['exp_due'] = self.items['exp_due'] + row.exp_paid_liability  # 应付费
                self.pay_exp_tax()

                # 记录前后两期收入变化 （只考虑正常本金利息回收,不考虑违约回收）

                self.items['return_decline'] \
                        = 1 - (row.current_interest_due + row.current_principal_due) / self.last_period_cash \
                        if self.last_period_cash > 0 else 0

                self.last_period_cash = row.current_interest_due + row.current_principal_due
                self.items['cum_loss'] += row.default_amount
                self.items['cdr'] += row.default_amount / self.initial_principal
                self.items['pool_principal'] = row.end_principal_balance

    def period_pool(self):
        """
        找到当前 `period_num` 对应的归集日的回收金额，并计算税费 ::

            税 = 税率 * 当期利息回收 * 距离上一归集日的天数 / 365
            费 = 费率 * 期初本金余额 * 距离上一归集日的天数 / 365

        """
        row = self.prediction.loc[self.period_num, :]

        if self.pstatus:  # 不是归集日的话是不需要回款也不需要计算税费id
            row.exp_paid_liability = \
                row.begin_principal_balance * self.exp_rate * (row.date_ - self.last_check_date).days / 365
            row.tax_paid_liability = \
                row.current_interest_due * self.tax_rate * (row.date_ - self.last_check_date).days / 365
            self.exp_liabilities[self.period_num] = row.exp_paid_liability
            self.tax_liabilities[self.period_num] = row.tax_paid_liability
        return row

    def run_from_node(self, node):
        """
        对日历中需要支付证券端的行，完整跑一边支付顺序

        Args:
            node: 第一个节点所在行


        **逻辑**

        1. 根据节点类型:

                * 如果节点类型是 'other' 或 'type'： 跳过，不进行操作
                * 如果节点类型是 'branch' （即分支条件），则用 ``self.evaluate_condition(node.branch_condition)`` 判断节点条件是否成立，如果不成立则不进入这一分支，停止运行。而如果条件满足，则进入分支。
                * 如果节点类型是 'pay' ， 则表示'支付'。此时先判断节点条件是否成立，如果成立，则用 ``transfer_money`` 模拟支付行为:

                        * 如果节点条件是空值，表示无条件，即无条件支付节点
                        * 如果节点条件有值， ``self.evaluate_condition(node.node_condition)`` 判断节点条件是否成立


        2. 找到子节点，重新运算 ``run_from_node`` ，直到支付顺序中选择的分支的最后一个节点。其中，如果有两个子节点，则会分别进入两个子节点，
        并对这两个子节点及其后续子节点都逐个运行 ``run_from_node`` ，直到各自分支的最后一个节点，或者直到出现新的 branch（分支条件） 并且条件没有被满足。
        """

        if node.node_type in ("other", "judge"):
            pass
        elif node.node_type == "branch":
            if (str(node.branch_condition) not in ("nan", "None")) and \
                    (not self.evaluate_condition(node.branch_condition)):
                return "branch condition failed"  # 不进入该分支
            else:
                self.success_branches[node.branch_condition] = True  # 进入过某个分支就变成True

        elif node.node_type == "pay":

            try:
                in_ = self.evaluate_condition(node.node_condition) \
                    if not (str(node.node_condition) in ("nan", "None")) else True

                if not (str(node.money_destination) in ("nan", "None")):
                    lst_ = node.money_destination.split(",")
                    for x in lst_:
                        self.success_nodes[x] = in_ or self.success_nodes[x]  # 进入过一次节点就是True

            except:
                raise ValueError(f'未知的节点条件{node.node_condition}')
            else:
                if in_:
                    self.transfer_money(node.money_source, node.money_destination, node.upper_limit)

        # 进入子节点，若无子节点则会停止
        for child_node in self.child_dict[node.node_no]:
            self.run_from_node(child_node)

    def transfer_money(self, source, target, upper_limit):

        """
        模拟支付

        Args:
            source (str): 资金来源
            target (str): 资金去向
            upper_limit (str, float): 支付上限


        **逻辑**

        1. 如果 `target` 在 `ignored_target` 中，则不分配
        2. 如果是账户间的资金流转，即`` source == "int_col" and target == "prin_col"`` ,
        则用 ``self.transfer_betweem_col(source, target, upper_limit)`` 进行账户间的资金转移
        3. 如果资金来源和去向相同，则跳过
        4 ``self.eveluate_multi_express`` 获取有效去向及应付金额 `target_pay_list` （不考虑账户余额限制）
        5. 账户余额的处理:

            * 如果 ``self.if_deduct_col=True`` 则需要扣减账户金额，此时账户余额也会影响实际支付额，用 ``self.eveluate_actual_payment`` 完成应付测算和模拟支付
            * 如果 ``self.if_deduct_col=False`` ，直接对 `target_pay_list` 中的金额进行全额支付，不考虑账户资金余额的限制
        """
        # 当本金账收入账同时作为资金来源时，进行本金账收入账混合。此过程不可逆
        if (not self.mixed) and (source == "total_col"):
            self.transfer_betweem_col('prin_col', 'total_col')
            self.transfer_betweem_col('int_col', 'total_col')
            self.mixed = True

        # 税费已在支付证券本息前统一扣除，此处不处理。本金账补足收益账也不处理，因为会在支付证券时体现
        if source == target:
            return 0
        elif source == "int_col" and target == "prin_col":
            self.transfer_betweem_col(source, target, upper_limit)
        else:

            target_list_valid, target_pay_list = self.eveluate_multi_express(target, upper_limit)

            if len(target_list_valid) < 1:
                return 'no_target'

            if not self.if_deduct_col:
                if sum(target_pay_list) > 0:
                    for (tar, amt) in zip(target_list_valid, target_pay_list):
                        self.pay_single_target(tar, amt)  # 直接支付,不扣减账户资金

            else:
                self.eveluate_actual_payment(target_list_valid, target_pay_list, source)  # 扣减账户资金并支付

    def eveluate_actual_payment(self, target_list_valid, target_pay_list, source):
        """ 估计考虑到账户上限时候的实付金额

        * 根据支付去向优先级进行分组，如果所有去向优先级一样，则只有一组；如果优先级不一样，则同一优先级的分在一组。
        此外，如果没有支付上限和有支付上限的科目在同一行，会人为让无上限的优先级滞后一档,不然资金会都到无上限的去向
        * 根据支付优先级依次计算应付金额
        * 计算组内总待付金额为 `target_due_sum`
        * 如果组内支付去向的待付金额都是有限值，则计算各个待付金额占总待付金额的比例
        * 如果组内支付去向待付金额为无穷大（一般是账户有多少资金支付多少金额），如果有两个无穷大的，则各为50%，以此类推
        * 取 `target_due_sum` 和 `source` 对应账户的资金余额中较小的值为 `amount_sum` , 即为实际可以支付的金额，乘以前面得到的比例，
        即为各个支付去向，从 `source` 账户中得到支付的金额。
        * 显然，如果 `amount_sum==target_due_sum` , 则各个去向都得到了足额支付。
        但是如果  `amount_sum<target_due_sum` 且 `source` 为收入帐时,此时需要考虑是否用其他账户补充支付。
        先每个支付去向是否需要补充支付（ 由 `set_complement` 确定），如果需要，则计算待付金额跟已付金额的差异作为待补充支付金额，如果不需要，则待补充支付金额为0。
        * 如果需要补充支付。将需要补充的金额加总和本金账户余额中较小值为 `make_up_sum` .计算各个待补充项目的占比（如果是同一行有多个去向，有些有上限，有些没有，则只支付没有上限的。），乘以 `make_up_sum` 即为所有支付去向得到本金帐的补充支付。
        * 模拟支付行为， 通过 ``self.pay_single_target`` 为每个去向支付等同于以上计算得到的可支付金额。

        """
        target_priorities = self.set_target_priority(target_list_valid, target_pay_list)  # 支付优先级
        if_complement = self.set_complement(target_list_valid)
        target_number = len(target_list_valid)
        sets = {}
        # 根据优先级和是否补足进行划分
        index_list = range(0, target_number)
        if len(set(target_priorities)) == 1:
            sets[0] = index_list
        else:
            keys = list(set(target_priorities))
            for k in keys:
                sets[k] = [x for x in index_list if target_priorities[x] == k]

        # 对每个优先级组进行处理
        for i in sets.keys():
            index_level = sets[i]
            level_target_due = [target_pay_list[n] for n in index_level]
            level_complement = [if_complement[n] for n in index_level]
            level_target = [target_list_valid[n] for n in index_level]
            target_due_sum = sum(level_target_due)
            if target_due_sum >= 0.01:
                make_up_sum = 0
                col_amount = self.evaluate_col(source)
                amount_sum = min(col_amount, target_due_sum)

                # 如果存在无穷大的待付金额，则按比例分配
                if math.isinf(target_due_sum):
                    portion_ = [1/target_number] * target_number
                else:
                    # 计算每个支付去向占总待付金额的比例
                    portion_ = [x / target_due_sum for x in level_target_due]

                # 计算每个支付去向的实际支付金额
                amount_list = [x * amount_sum for x in portion_]

                # 如果实际可支付金额小于总待付金额，并且来源是利息账户，则考虑补充支付，是本金账户、混合账户则不用管
                if (target_due_sum > amount_sum) and (source == 'int_col'):
                    compensate_num = [due_ - amt if cpt is True else 0 \
                                      for (amt, due_, cpt) in zip(amount_list, level_target_due, level_complement)]
                    make_up_sum = min(sum(compensate_num), self.evaluate_col("prin_col"))
                    if make_up_sum > 0:
                        # 计算补充支付的比例
                        if math.isinf(make_up_sum):
                            portion2_ = [1 if x > 0 else 0 for x in compensate_num]
                            portion2_ = [x / sum(portion2_) for x in portion2_]
                        else:
                            portion2_ = [x / sum(compensate_num) for x in compensate_num]
                        # 更新实际支付金额
                        amount_list = \
                            [make_up_sum * port + am for (port, am) in zip(portion2_, amount_list)]

                # 对每个支付去向进行支付
                for (tar, amt) in zip(level_target, amount_list):  # 支付
                    self.pay_single_target(tar, amt)

                # 如果存在补足，则从本金账户转到收入账户
                self.transfer_betweem_col('prin_col', source, make_up_sum)  # 如果存在补足则从本金账户转到利息账户去 如果不需要补 金额是0
                # 扣减来源账户的金额
                self.deduct_from_col(source, make_up_sum + amount_sum)

    def eveluate_multi_express(self, targets_express, limit_express):

        """ 对支付去向的金额进行估计

            * 先将 `target` 以 ',' 为分割点进行分割，得到支付去向的列表 `target_list` , 如果支付去向只有一个，则列表中只有一个元素，
            如果有多个去向（eg. sub.prin, sub.exret), 则会有两个元素，以此类推。
            * 对支付上限 `upper_limit` 做一样的操作，得到 `upper_limit_list` 。 如果其中的元素数量与 `target_list` 一样，
            则会将两者中的元素一一对应，得到各个去向各自的支付上限。 但是如果不一样，则无法一一匹配，假设全部支付去向的支付上限均为无穷大（即无上限）。
            * 使用 ``self.evaluate_express`` 逐一估计 `upper_limit_list` 的数值。 跟应付进行对比后，取其中较小的值作为各支付去向的待付金额 `target_pay_list`
            * 其中，在 `self.ignore` 中的去向会被剔除

        """
        target_list = targets_express.split(",")
        upper_limit_list = str(limit_express).split(",") if str(limit_express) not in ('nan', 'None') else []
        if len(upper_limit_list) != len(target_list):
            upper_limit_list = [float('Inf')] * len(target_list)

        target_list_valid = []
        upper_limit_list_valid = []
        for x in range(len(target_list)):
            if target_list[x] not in self.ignored_target and 'ignore' not in target_list[x]:
                target_list_valid.append(target_list[x])
                upper_limit_list_valid.append(upper_limit_list[x])

        target_due_list = [self.evaluate_due(tar) for tar in target_list_valid]
        limit_result_list = [self.evaluate_express(lim_) for lim_ in upper_limit_list_valid]
        target_pay_list = [min(tar, lim) for tar, lim in zip(target_due_list, limit_result_list)]
        return target_list_valid, target_pay_list

    def set_target_priority(self, targets, target_pay_list):
        """
        人工设置目标的支付优先级，当两个或三个目标处于同一行时，会根据优先级修改先后顺序。目前只有一个滞后项，即次级超额收益。当次级超额收益与其他项在同一行时，默认会滞后支付。
        |如果要设置其他滞后的支付去向，则在类属性 `priority_` 中加入对应的滞后项的正则表达式。另外，如果同一行有些有上限，有些没上限，则将没上限的优先级往后调

        Args:
            targets: 支付去向
            target_pay_list: 支付去向预期金额

        """
        priorities = [0] * len(targets)
        for i in range(len(targets)):
            tar_ = targets[i]
            tar_pay = target_pay_list[i]
            for key_ in self.priority_.keys():
                if re.fullmatch(key_, tar_):
                    priorities[i] = self.priority_[key_]
                    break

            if math.isinf(tar_pay):
                priorities[i] += 1  # 无上限的优先级往后调
        return priorities

    def set_complement(self, targets):
        """
        为支付去向设置”是否用其他账户补充支付字段“，除了下面字典中形式的去向外，全部补充支付。比如当收入帐不足以支付优先级利息时，用本金帐自动补足。
        |complement_ = {'sub[d]*.int': False, 'sub[d]*.exret': False, 'incentive_fee': False}，分别表示次级期间收益，次级超额收益和超额激励。
        如果有其他不需要补充支付的项则在类属性 `complement_` 中加入支付去向的正则表达式

        Args:
            targets (list): 支付去向

        """
        # 初始化补充支付标志列表，默认为True
        complement = [True] * len(targets)
        for i in range(len(targets)):
            tar_ = targets[i]
            # 遍历complement_字典中的正则表达式？？？self.priority_
            for key_ in self.priority_.keys():
                if re.fullmatch(key_, tar_):
                    # 如果找到匹配的正则表达式，更新补充支付标志
                    complement[i] = self.complement_[key_]
                    break
        return complement

    def transfer_betweem_col(self, source, target, upper_limit='Inf'):
        """
        账户之间转移资金

        Args:
            source (str): 转出账户
            target (str): 转入账户
            upper_limit (str, float): 转移金额上限，如果没有维护则表示全部转移

        """

        source_balance = self.evaluate_col(source)
        upper_bound = float("inf") if str(upper_limit) in ("nan", "None") else self.evaluate_express(upper_limit)
        amount = min(source_balance, upper_bound)

        if amount >= 0.01:
            self.deduct_from_col(source, amount)
            self.add_to_col(target, amount)
            count_cumsum = "cum_"+source.replace("_col", "")+"_to_"+target.replace("_col", "")
            if count_cumsum not in self.items:
                self.items[count_cumsum] = 0
            self.items[count_cumsum] += amount

    def add_to_col(self, col, amount):
        """ 增加归集账户金额。发生在期初现金归集、本金账收益账互转、分帐户转入混合账户等场合

        Args:
            col (str): 账户名称
            amount (float): 需要转入账户 `col` 中的金额

        """
        assert str(amount) != 'nan', "The transferred amount is NaN"
        assert amount >= -0.01, "The transferred amount is negative"
        try:
            self.items[col] += amount
        except (KeyError, AttributeError):
            raise ValueError("Cannot find the account: " + col)

    def deduct_from_col(self, col, amount):
        """
        扣减账户余额。发生在证券兑付、税费支付、本金账收益账互转、分帐户转入混合账户等场合

        Args:
            col (str): 账户名称
            amount (float): 需要从账户 `col` 中扣减的金额

        """

        assert amount > -0.01, "The transferred amount is negative"
        try:
            self.items[col] = self.items[col] - amount
        except (KeyError, AttributeError):
            raise ValueError("Cannot find the account: " + col)
        assert self.items[col] > -0.01, "The {0} balance is negative after transfer".format(col)

    def pay_single_target(self, target, amount):
        """模拟单个节点的支付，如果是不带"."的是资产池属性，带”.“的是证券端属性，分别调取 ``pay_pool_fee`` 和 ``pay_security`` 进行应付金额的扣减，和已付金额的记录。
         如果节点去向为'ignore' 则忽略这一节点。

         Args:
             target(str): 支付去向
             amount(str): 支付金额

        """
        if 'ignore' in target:
            return 0
        else:
            attr_list = target.split(".")
            if len(attr_list) == 1:
                # 资产池属性，调用pay_pool_fee
                self.pay_pool_fee(attr_list[0], amount)
            elif len(attr_list) == 2:
                # 证券属性，调用pay_security
                self.pay_security(attr_list[0], attr_list[1], amount)
            else:
                raise ValueError(f"无法处理的资金去向{target}")

        #  记录当前支付去向是否成功支付的标志
        self.items['pay_'+target+'_success'] = True

    def pay_pool_fee(self, target, amount):
        """
         `pstatus` 为 `True` 时，支付资产池相关的超额奖励、循环购买资金等(税费由于提前扣除了，不需要考虑 ）。
         通过从应付金额 `self.items[target + '_due']` 中扣除 amount，然后在支出序列中对应期次加入已支付金额以记录数据。

        Args:
            target (str): 支付去向 ，如 rev-循环购买支出,
            amount (float): 需支付金额

        """
        if self.pstatus and amount > 0: # 如果不是核算日就不支付资产池端费用
            due_count = self.items[target + '_due']
            if due_count >= amount:
                exec("self.%s_payments[self.period_num]+=amount" %target)
                self.items[target + '_due'] -= amount
            else:
                raise ValueError(f"向{target}支付超过应付的金额")

    def pay_security(self, subord, attr, amount):
        """
        证券本息支付，如果当前处于证券支付日( `status` 为 `True` )，并且此节点条件得到了满足，则支付证券端。
        调取 ``Tranche`` 实例中的 ``receive_int`` / ``receive_prin`` / ``receive_fcc`` / ``receive_exret`` 。
        则实例中会从对应的应付金额中扣除 amount, 然后在支出序列的对应期次上增加支付的金额

        Args:
            subord (str): 证券等级
            attr (str): 支付项，包括 int, prin, fcc, exret
            amount (float): 需要支付的金额

        """
        # 获取对应等级的Tranche对象
        tr = getattr(self.tranches_obj, subord)

        if self.status:  # 确保是在证券的支付日
            # 调用Tranche对象的相应支付方法
            getattr(tr, 'receive_' + attr)(amount)

    def pay_exp_tax(self):
        """
        由于当前对税费的计算比较简单，而一般税费都是在支付证券端之前扣除，因此不处理税收和费用的支付节点，直接在每期进行证券端的分配前先行扣除税和费，
        然后再运行支付顺序，并忽略支付顺序中去向 'tax','exp'


        **逻辑**


        1. 如果不是混合账户：

                * 先扣除收入帐（应付金额： `self.items['tax_due'] + self.items['exp_due']` ）
                * 如果收入帐不足以支付应付税费，则用本金帐补充支付
                * 如果仍不足以支付，则假设已付金额中，税费按照各自的比例支付，剩余的作为应收税和应收费留在 `self.items` 中，留待下一期支付。
                * 更新税费支付序列 `self.tax_payments, self.exp_payments` ， 即将当期已付的税费各自加到对应的那期

        2. 如果是混合账户：

                * 从混合账户中扣除应付税费
                * 如果不足以支付，则假设已付金额中，税费按照各自的比例支付，剩余的作为应收税和应收费留在 `self.items` 中，留待下一期支付。
                * 更新税费支付序列 `self.tax_payments, self.exp_payments ` ， 即将当期已付的税费各自加到对应的那期

        """

        amount = self.items['tax_due'] + self.items['exp_due']
        if amount < 0.01:
            return 0

        if not self.mixed:
            int_col = self.evaluate_col("int_col")
            int_pay = min(amount, int_col)
            self.deduct_from_col("int_col", int_pay)
            shortage = amount - int_pay
            if shortage > 0:
                prin_col = self.evaluate_col("prin_col")
                prin_pay = min(shortage, prin_col)
                self.deduct_from_col("prin_col", prin_pay)
                shortage -= prin_pay
        else:
            total_col = self.evaluate_col("total_col")
            total_pay = min(amount, total_col)
            self.deduct_from_col("total_col", total_pay)
            shortage = amount - total_pay

        tax_pay = (amount - shortage) * self.items['tax_due'] / amount
        exp_pay = (amount - shortage) * self.items['exp_due'] / amount
        self.tax_payments[self.period_num] += tax_pay
        self.exp_payments[self.period_num] += exp_pay
        self.items['tax_due'] -= tax_pay
        self.items['exp_due'] -= exp_pay
        self.items['exp_clear'] = self.exp_clear  # 税费是否足额支付

    def evaluate_due(self, item):
        """ 计算某个支付去向本期的应付，如果是映射成 'ignore' 的则忽略，否则用 ``evaluate_single`` 获取应付值，应付值统一以'_due' 结尾，比如 exp_due, a.int_due"""
        if 'ignore' in item:
            return 0
        else:

            return self.evaluate_single(item + "_due")

    def evaluate_col(self, col):
        """ 获取本金账（prin_col)或收入账(int_col)或者混同账户（total_col)的余额 """
        assert col in ("int_col", "prin_col", "total_col"), col + " is not a valid account name."
        v = self.items[col]
        return v

    def evaluate_express(self, expr: Union[str, float, int]):
        """既可以用于计算数值，也可以用于判断逻辑值。 支持符号包括:

        1.   加减乘除 ( ) %
        2.   < > = >= <=  ： 如果有则输出的是 bool 值，否则是 float

        """
        if isinstance(expr, (float, int)):
            return expr
        elif isinstance(expr, str) and expr.replace(".", "").isdigit():
            return float(expr)
        else:
            if re.search(r'\W', expr) is None:
                return self.evaluate_single(expr)
            else:
                expr = "(" + expr + ")"
                op_priority = { '>': 1, '=': 1, '<': 1,
                               '+': 2, '-': 2, '*': 3, '/': 3}
                op_stack, var_stack = [], []
                n = len(expr)
                word = ''
                i = 0
                while i < n:
                    c = expr[i]
                    if c == '(':
                        op_stack.append(c)

                    elif c == ')':
                        # 计算
                        var_stack.append(self.evaluate_single(word))
                        word = ''
                        while op_stack[-1] != '(':
                            var_stack, op_stack = self.__calc(var_stack, op_stack)
                        op_stack.pop()

                    elif c in op_priority.keys():
                        if word != '':
                            var_stack.append(self.evaluate_single(word))
                            word = ''
                        while op_stack and op_stack[-1] != '(':
                            if op_priority[op_stack[-1]] >= op_priority[c]:
                                var_stack, op_stack = self.__calc(var_stack, op_stack)
                            else:
                                break
                        op_stack.append(c)

                    else:
                        if c == '%':  # 处理百分号
                            word = str(float(word) / 100)
                        else:
                            word = word + c

                    i += 1
                if len(var_stack) > 1:
                    raise ValueError("evaluate_express函数有误")
                return var_stack[0]

    @staticmethod
    def __calc(num_stack, op_stack):
        """
        evaluate_express调用， 用于计算单个公式, 支持的符号包括 + - * / > >= <= < =

        Args:
            num_stack （list): 数值栈
            op_stack (list): 符号栈

        Returns:
            tuple: tuple contains:
                  * num_stack （list): 弹出已完成运行的数值，并压入计算结果的数值栈
                  * op_stack (list): 弹出以完成运算的符号后的符号栈
        """
        op = op_stack.pop()
        num2 = num_stack.pop()
        num1 = num_stack.pop()
        if op == '+':
            ans = num1 + num2
        elif op == '-':
            ans = num1 - num2
        elif op == '*':
            ans = num1 * num2
        elif op == '/':
            ans = num1 / num2
        elif op == '>':
            ans = num1 > num2
        elif op == '>=':
            ans = num1 >= num2
        elif op == '<':
            ans = num1 < num2
        elif op == '<=':
            ans = num1 <= num2
        elif op == '=':
            ans = num1 == num2
        else:
            raise ValueError("错误的支付上限计算符号")
        num_stack.append(ans)
        return num_stack, op_stack

    def evaluate_condition(self, expr) -> bool:
        """ 判断一个有 and / or / not 的复合表达式。用于分支条件和节点条件的判断。仅支持逻辑运算，不支持数值计算，
        公式中如果有类似于 a+b>c的表达式，则视作一个子表达式，通过 ``evaluate_express`` 转化为单个值（bool)。 将所有子表达式转化后，
        通过这个函数输出最终的布尔值，也即输入条件表达式的判断结果"""
        if str(expr) in ('nan', 'None'):
            return True  # 表示无条件进入节点

        expr = 'and(' + expr + ')'
        cond_stack = [[]]  #
        op_stack = []
        word = ''
        n = len(expr)
        i = 0
        while i < n:
            c = expr[i]
            if c == '(':
                op_stack.append(word)
                cond_stack.append([])
                word = ''
            elif c == ")":
                op = op_stack[-1]
                cond_stack[-1].append(self.evaluate_express(word))
                cond = cond_stack[-1]
                word = ''
                if op == 'and':
                    v = and_(cond)
                elif op == 'or':
                    v = or_(cond)
                elif op == 'not':
                    v = not and_(cond)
                else:
                    raise ValueError(f"不支持的逻辑符号{op}")
                op_stack.pop()
                cond_stack.pop()

                cond_stack[-1].append(v)

            elif c == ",":
                cond_stack[-1].append(self.evaluate_express(word))
                word = ''
            else:
                word = word + c
            i += 1
        return cond_stack[-1][0]

    def evaluate_single(self, condition_or_item) -> Union[float, bool, int]:
        """
        返回单个属性的值，如果是比较复杂的带逻辑符号（and, or, not) 或者带运算符号的 （+-*/) 则由 ``evaluate_express`` 计算

        Args:
            condition_or_item (str): 需要获取的属性值

        **逻辑**

            * 如果 `condition_or_item` 是数值(float,int)(eg. 5, 4.5),或者是字符串但是每个字符都是数字(eg. '3')，则返回数值 float
            * 如果 `condition_or_item` 是字符串，分为以下情况:

                    * 没有 '.' 有两种情况：

                            1. 恒定值：用于表达一些与项目本身的属性无关，表示一个不变的值的字符串。出现恒定值的情况包括：

                                    * 在处理支付顺序中，所有节点条件都进行了映射，部分节点条件由于后来规则的修改，不再不作为是否进入节点的依据，比如int_day表示位于计息日，
                                    由于是否计息在 ``Tranche`` 中完成判断，故该节点不需要作为判断依据，设为 'eternal_True'。即无论如何都会进入节点，
                                    因此会恒定返回 `True` ,  ``run_from_node`` 中遇到该节点一定会进入

                                    * 在触发事项中既包含了敲入敲出的上下限，但是不一定两者都有，有些只有下限的，则会将上限设为无穷大，比如'and(cdr>0.1,cdr<Inf)' , 这里的 `Inf` 就表示无穷大。

                                    包括::

                                    bool_dict = {"True": True, "False": False, '': True, 'inf': float('Inf'), 'Inf': float('Inf'),
                                    'nan': float('nan'), 'eternal_True': True}


                            2. 表示是项目通用的属性（eg.in_amort_period), 这类属性保存在了 `self.items` 中，从中提取并返回，返回结果可能是 float 也可能是 bool。

                    * 有一个 '.' , 则表示证券属性，将'.'前后的值拆分开来，则分别表示证券等级和证券保存在 ``Tranche`` 中的属性，如 A1.int_due。 故从证券实例的集合 `self.tranches_obj` 读取对应等级的实例，提取实例的属性值（如 int_due)并返回。

                    * 有超过一个 '.' ，目前仅是为了处理三种条件类型：A1.AfterIntDates.6，A1.BeforeIntDates.6，A1.OnIntDates.6，即为了判断当前支付日期跟某个支付日（这里的例子是第6个计息日）的先后关系。此时认识从 `self.tranches_obj` 读取对应等级的实例（如A1)，并输入参数值6到 method 中（如 ``AfterIntDates``）
        Notes:
            * 如果后续需要拓展支付顺序中的维护规则、触发事项条件，必须保证其表达式中的各项都能由以上方法读取到（保证项目属性在这个类中存在，证券属性在 ``Tranche`` 实例中存在）。读取不到会返回 '支付顺序中维护的去向不存在'。
            * 不能设置会更新的属性与恒定值的表达符号一样。

        TODO:
            超过一个 '.' 的属性获取待验证，目前没有数据
        """

        if isinstance(condition_or_item, (float, int)):
            result = condition_or_item
            return result
        elif isinstance(condition_or_item, str) and condition_or_item.replace(".", "").isdigit():
            result = float(condition_or_item)
            return result
        else:
            attr_list = condition_or_item.split(".")
            if len(attr_list) == 1:  # 项目条件
                bool_dict = {"True": True, "False": False, '': True, 'inf': float('Inf'), 'Inf': float('Inf'),
                             'nan': float('nan'), 'eternal_True': True}
                if condition_or_item in bool_dict.keys():
                    result = bool_dict[condition_or_item]
                else:

                    try:
                        result = self.items[attr_list[0]]  # 资产池属性

                    except (AttributeError) as err:
                        raise ValueError(f"支付顺序中维护的去向不存在{condition_or_item}")

            elif len(attr_list) == 2:  # 单个证券的条件

                try:

                    tr = getattr(self.tranches_obj, attr_list[0])

                except (AttributeError) as err:

                    raise ValueError(f"支付顺序中的证券等级与基本要素的矛盾,检查证券基本信息中是否有等级为{attr_list[0]}的证券")

                else:

                    try: # prin_due
                        result = getattr(tr, attr_list[1])  # 证券端属性

                    except (AttributeError) as err:

                        raise ValueError(f"支付顺序中维护的去向不存在{condition_or_item},检查{attr_list[1]}是否符合要求")

            elif len(attr_list) > 2:  #需要输入参数更新的属性，第一个是证券等级，第二个是指标名称（对应函数名），后面为参数更新需要的指标

                add_vars = attr_list[2:]
                tr = getattr(self.tranches_obj, attr_list[0])
                result = getattr(tr, attr_list[1])(*tuple(add_vars))

            else:
                raise ValueError("无法解析支付顺序中的表达式: " + condition_or_item)

            return result


    @property
    def cdr(self):
        """期末累计违约率=总违约金额/入池本金"""
        return self.items['cum_loss'] / self.initial_principal

    @property
    def exp_clear(self):
        """费用是否完成支付,根据应付费用是否为 0 判断"""
        return self.items['exp_due'] < 0.01

    @property
    def account(self):
        """账户沉淀资金余额，为本金帐和收入帐总和"""
        return max(self.items['int_col'] + self.items['prin_col'], self.items['total_col'])

    def prediction_after(self):
        """
        用于返回包含税费、超额激励, 期末账户资金余额, 事件发生情况的现金流归集表

        Returns:
            pd.DataFrame: df
        """
        df = self.prediction.copy()
        df['tax_paid'] = self.tax_payments
        df['exp_paid'] = self.exp_payments
        df['incentive_fee'] = self.incentive_fee_payments
        df['tax_paid_liability'] = self.tax_liabilities
        df['exp_paid_liability'] = self.exp_liabilities

        future_events = self.events_result()  # 合并事件发生情况到现金流归集中
        if len(future_events) > 0:
            event_to_number = dict(zip(trigger_event_dict.values(), trigger_event_dict.keys()))
            events = list(future_events.columns)
            if 'date_' in events:
                events.remove('date_')
            events_type = np.array([event_to_number[x] for x in events])

            future_events.loc[:, 'event'] = future_events[events].apply(
                lambda x: ",".join(events_type[x.values]), axis=1)

            df = df.merge(future_events[['date_', 'event']], on='date_', how='left')

        df = df.loc[df[['current_principal_due', 'current_interest_due',
                        'recycle_amount', 'prepay_amount',
                        'default_amount', 'tax_paid', 'exp_paid']].sum(axis=1) > 0.1, :].copy()
        return df

    def events_result(self):
        """
        用于返回模拟过程中的重大事项触发情况

        Returns:
            pd.DataFrame: 重大事项发生情况
        """
        df = pd.DataFrame(self.__events_list)

        return df

    def write_info(self, info_tag, info_detail):
        """记录过程中的提示信息"""
        self.infos.append((info_tag, info_detail))


class Waterfall_rev(Waterfall):

    def __init__(self, tranches_obj, schedule, payment_sequence, trigger_events, trigger_rules, is_revolving,
                 initial_principal, latest_principal, pred_start_date, initial_date,
                 history_date, tax_rate, exp_rate, pay_exp_tax,
                 int_col, prin_col, last_cdr, cumsum_loss, begin_default, begin_default_recover, split_default):
        """
        用与循环购买的资产池现金流测算和现金流分配

        Args:
            latest_principal (float): 最新资产池剩余金额
            initial_date (datetime.date): 初始起算日
            begin_default (float): 最新违约本金余额
            begin_default_recover (bool): 是否考虑当前违约本金余额的回收
            split_default (bool): 最新资产池剩余金额中是否扣除了最新违约本金余额

        Attributes:
            principals (np.array): column-第几次循环购买,index-循环买入资产在存续期本金回收序列 (n, ) 。当前最新资产余额作为首期（1）循环买入资产
            interests (np.array):  column-第几次循环购买,index-循环买入资产在存续期利息回收序列 (n, )
            recycles (np.array):  column-第几次循环购买,index-循环买入资产在存续期违约回收本金序列 (n, )
            defaults (np.array):  column-第几次循环购买,index-循环买入资产在存续期各期的违约本金序列 (n, )
            prepays (np.array):  column-第几次循环购买,index-循环买入资产在存续期早偿回收序列 (n, )
            begin_balances (np.array):  column-第几次循环购买,index-循环买入资产在存续期期初本金余额的序列 (n, )
            end_balances (np.array):  column-第几次循环购买,index-循环买入资产在存续期期末本金余额的序列 (n, )

        Notes:
            使用这个模块进行现金流分配时，要求支付顺序的支付去向中必须有循环购买支付这一项。
        """
        super().__init__(tranches_obj, schedule, payment_sequence, trigger_events, trigger_rules, is_revolving,
                 initial_principal, pred_start_date, history_date, exp_rate, tax_rate, pay_exp_tax,
                         int_col, prin_col, last_cdr, cumsum_loss)

        self.principals = np.zeros((self.total_periods, 1))
        self.interests = np.zeros((self.total_periods, 1))
        self.recycles = np.zeros((self.total_periods, 1))
        self.defaults = np.zeros((self.total_periods, 1))
        self.prepays = np.zeros((self.total_periods, 1))
        self.begin_balances = np.zeros((self.total_periods, 1))
        self.end_balances = np.zeros((self.total_periods, 1))
        self._repurchase_times = 0
        self.begin_default = begin_default
        self.begin_default_recover = begin_default_recover
        self.split_default = split_default
        self.initial_date = initial_date
        self.latest_principal = latest_principal

        if not ('reinv' in self.payment_sequence['money_destination'].values):
            raise ValueError("循环购买类的支付顺序中没有循环购买支付项")

    def get_data_params(self, CDRs, PPs, RP, CPRs, RRs, suoe, CPR_type, type_, YCDRs=None, YRs=None, DRs=None):
        """
        将所有加压参数设置为类属性，同'时将当前的本金余额视作第一次循环购买支出，用 ``s'elf.pool_set`` 设置资产池，假设其循环购买日即为生成日历中的上一历史归集日（如果当前处于首个归集日之前，则假设循环购买日为初始起算日），计算其在后续每一期的回收金额，然后保存在类属性 `principals` ， `interests` 等中。

        Args:
            CDRs (pd.Series): 累计违约率序列
            PPs (pd.Series): 摊还比例序列，总和为 1
            RP (float, int): 循环购买比例
            CPRs (pd.Series): 早偿率序列
            RRs (pd.Series): 违约回收率序列，index-延迟回收月数，value-违约回收率。总和不超过1
            YCDRs (pd.Series): 年化违约率序列，与 `CDRs` 二选一
            YRs (pd.Series): 收益列序列
            DRs (pd.Series):  折价率序列，与 `YRs` 二选一
            suoe (bool): 用早偿缩额法还是缩期法
            type_ (str): 'yield_' - 使用 `YRs` 计算资产池利息， 'discount' - 使用 `DRs` 计算入池资产金额
            CPR_type (str): 早偿计算方式, 'type1' - `smm` * 期初本金余额， 'type2' - `smm` * (期初本金余额 - 当期本金回收)

        """

        self.RP = RP
        self.CDRs = CDRs
        self.YCDRs = YCDRs
        self.CDR_type = 'cumulative' if CDRs is not None else 'constant'
        self.PPs = PPs
        self.longevity_limit = max(PPs.index)  # 一笔循环购买,最迟需要几个月摊还完毕
        self.BPs = pd.Series(float('nan'),
                             index=range(0, max(self.total_periods, self.longevity_limit) + 1))

        # 使用 [::-1] 对序列进行反转，意味着将序列从尾部到头部进行翻转
        # cumsum() 这是Pandas中的累积求和函数
        # shift(1) 将序列中的每个元素向下移动一个位置。
        # 通过反转和累积求和，我们可以得到每个月末相对于初始本金的剩余比例
        cs = PPs[::-1].cumsum().shift(1)
        cs[self.longevity_limit] = 0
        self.BPs[cs.index] = cs.values  # 表示每个月结束时的资产池剩余应占初始买入金额的比例, 第0期应该为1,最后一期为0
        self.BPs[0] = 1
        self.BPs.sort_index(inplace=True)   # 对 self.BPs 进行索引排序。
        self.BPs.ffill(inplace=True)   # 对 Series 中的 NaN 值进行前向填充

        self.CPRs = CPRs
        self.CPR_type = CPR_type
        self.RRs = RRs.values
        self.DPPs = RRs.index.values
        self.YRs = YRs
        self.DRs = DRs
        self.type_ = type_
        self.suoe = suoe

        last_repur_period = self.pred_start_date - 1
        while (not self.pool_status[last_repur_period]) and last_repur_period >=0:
            last_repur_period -= 1

        self.history_period = last_repur_period  # 将上一个归集日作为作为当前本金余额的摊还起始期次. 如果没有存续期,则这个日期会是初始起算日(在日历生成的时候已经加入了初始起算日,因此前述的步骤一定会找到一个归集日)

        # 将当前的本金余额作为首个循环购买资产，并计算其后续回收金额
        self.pool_set(self.latest_principal, last_repur_period)
        # 如果要考虑当前的剩余违约资产,则额外加入一笔违约回收
        if self.begin_default_recover:
            ages_ = count_months(self.all_dates[last_repur_period], self.pool_dates)
            ages_[ages_ < 0] = float('nan')
            DA = np.full(ages_.shape, 0.)
            DA[last_repur_period] = self.begin_default
            add_RA = LoanPool.cal_RA(RRs=self.RRs, DPPs=self.DPPs, DA=DA, ages=ages_, total_period=self.total_periods)
            self.recycles[:, self._repurchase_times] = self.recycles[:, self._repurchase_times] + add_RA

    def transfer_money(self, source, target, upper_limit):
        super().transfer_money(source, target, upper_limit)
        self.items['reinv_due'] = self.reinv_due  # 每次支付了一次证券本金利息等项目后都经计算一下当前的账户余额多少能用来循环购买

    def period_pool(self):
        """
        获取模拟期的现金流入

        **逻辑**

        1. 由于每一期在支付顺序支付成功某一项时，都会在 `items` 中设置 ``items['pay_'+target+'_success'] = True`` ,
        表示某一项支付成功。因此，如果循环购买支出在上一次成功支付了，则会有 `items['pay_reinv_success'] = True` ,
         并且在 `items` 中的 'reinv_payments' 的上一期记录了该笔支出。于是，对于成功进行的循环购买，
         在下一期通过 ``self.pool_set`` 计算存续期的本金回收、利息回收等。 如果没有支付成功，则相当于上一期没有进行循环购买。
        2. 将类属性 `principals` , `interests` 等有关现金流入的项目中，每次循环购买的第 `period_num` 个（即模拟到的期次）数字相加，
        得到的就是资产池在这一期的总回收金额。并以 ``Waterfall.period_pool`` 同样的方法计算税费
        """
        if self.items.get('pay_reinv_success', False):  # 表示上一期支付过循环购买支出

            repur_period = self._last_pool_period_num
            reinv_amount = self.reinv_payments[repur_period]
            self.pool_set(reinv_amount, repur_period)
            self.items['pay_reinv_success'] = False

        # 计算当前期的本金回收和利息回收
        current_principal_due = np.nansum(self.principals[self.period_num]) # 包含了早偿
        current_interest_due = np.nansum(self.interests[self.period_num])

        # self.principals[self.period_num]和self.interests[self.period_num]分别存储了当前归集日的本金回收和利息回收。
        # self.begin_balances[self.period_num]和self.end_balances[self.period_num]分别存储了当前归集日的期初和期末本金余额。
        # self.defaults[self.period_num]和self.recycles[self.period_num]分别存储了当前归集日的违约本金和违约回收金额。
        # tax_paid_liability和exp_paid_liability分别计算了当前归集日的税和费用负债。
        # 以下部分实现了报告中第14页的公式逻辑
        # 这个公式用于计算在特定时间点t的现金流入量，其中考虑了多个资产池 ii 的摊还比例,累计违约率CDRi、违约回收率RR、每期的违约本金PPi
        begin_principal_balance = np.nansum(self.begin_balances[self.period_num])
        end_principal_balance = np.nansum(self.end_balances[self.period_num])
        default_amount = np.nansum(self.defaults[self.period_num])
        recycle_amount = np.nansum(self.recycles[self.period_num])

        # 计算税和费用
        tax_paid_liability = \
            current_interest_due * self.tax_rate * (self.all_dates[self.period_num] - self.last_check_date).days / 365
        exp_paid_liability = \
            begin_principal_balance * self.exp_rate * (self.all_dates[self.period_num] - self.last_check_date).days / 365

        # 更新税费记录
        self.exp_liabilities[self.period_num] = exp_paid_liability
        self.tax_liabilities[self.period_num] = tax_paid_liability

        row = pd.Series({'date_': self.all_dates[self.period_num],
                         'current_principal_due': current_principal_due,
                         'begin_principal_balance': begin_principal_balance,
                         'current_interest_due': current_interest_due,
                         'end_principal_balance': end_principal_balance,
                         'default_amount': default_amount,
                         'recycle_amount': recycle_amount,
                         'tax_paid_liability': tax_paid_liability,
                         'exp_paid_liability': exp_paid_liability,
                         "reinvest_income": 0,
                         })
        return row

    def pool_set(self, reinv_amount, repur_period):
        """
        为新增循环购买计算后续的现金流情况, 其中得到的本金回收、利息回收、期初期末本金余额等序列都与日历长度一样，并且逐行对应。只不过回收现金流只在循环购买日后的归集日有。

        Args:
            reinv_amount (float): 循环购买金额
            repur_period (int): 循环购买日期在日历中第几行


        **逻辑**

        1. 计算单笔循环购买的日期，与其后续归集日之间的月数
        2. 分别计算后续各个归集日的摊还比例 ( 通过 ``self.cal_PPs(ages_)`` )、smms ( 通过 ``self.cal_SMMs(ages_, self.CPRs)`` )
        3. 违约率:

            * 如果是用累计违约率计算（即输入 `CDRs` ）, 则通过该笔循环购买最后一个回收日距离循环购买日的月数，从 `CDRs` 序列中选择对应的累计违约率。比如假设该笔循环购买在10.5个月后完成归集，则选择 `CDRs` 中对第11个月的假设条件，作为存续期的累计违约率。
            * 如果是用年化违约率计算（即输入 `YCDRs` ), 则跟早偿率一样，计算各期的条件值 （ ``self.cal_SMMs(ages_, self.YCDRs)`` ）

        4. 收益率:

            * 如果是用收益率计算利息 （即输入 `YRs` ），则 ``self.cal_YRs(ages_)`` 得到每期的收益率，利息即等于对应期次收益率乘以期初本金
            * 如果是用折价率算 （即输入 `DRs` ), 此时相当的于没有利息，但是入池资产总额=循环购买支出 / 折价率

        5. 将以上参数全部输入 ``self.single_pool_pressure`` 即得到单笔循环购买后续的回收金额
        """
        self._repurchase_times += 1

        repur_date = self.all_dates[repur_period]
        ages_ = count_months(repur_date, self.pool_dates)
        ages_[ages_ < 0] = float('nan')
        pool_month = np.nanmax(ages_)

        if np.isnan(pool_month):
            return 0  # 后面没有足够的日期了不能进行摊还

        pool_month = min(pool_month, self.longevity_limit)
        this_pps = self.cal_PPs(ages_)  # 摊还比例

        if self.CDR_type == 'cumulative':
            this_cdr = self.CDRs[min(np.ceil(pool_month), max(self.CDRs.index))]  # 累计违约率
        else:
            this_cdr = 0
        this_smdrs = self.cal_SMMs(ages_, self.YCDRs) if self.CDR_type == 'constant' else None  # 条件违约率

        this_smms = self.cal_SMMs(ages_, self.CPRs)  # 条件早偿率

        if self._repurchase_times == 1:
            this_dr = 1  # 第一笔指的是资产池剩余金额，因为已经入池了，因此不需要再用到折现率
        else:
            this_dr = self.DRs[min(np.ceil(pool_month), max(self.DRs.index))] if self.type_ == 'discount' else 1.  # 折价率

        this_yrs = self.cal_YRs(ages_) if self.type_ == 'yield_' else None  # 资产池收益率序列

        OPB, CPB, PR, PA, DA, IR, RA = self.single_pool_pressure(reinv_amount, repur_period,
                                                                 this_pps, this_smms, ages_,
                                                                 self.RRs, self.DPPs, this_cdr, this_smdrs,
                                                                 this_yrs, this_dr, self.CDR_type,
                                                                 self.CPR_type, self.type_, self.suoe)

        self.principals = np.insert(self.principals, self.principals.shape[1], PR, axis=1)
        self.interests = np.insert(self.interests, self.interests.shape[1], IR, axis=1)
        self.begin_balances = np.insert(self.begin_balances, self.begin_balances.shape[1], OPB, axis=1)
        self.end_balances = np.insert(self.end_balances, self.end_balances.shape[1], CPB, axis=1)
        self.defaults = np.insert(self.defaults, self.defaults.shape[1], DA, axis=1)
        self.prepays = np.insert(self.prepays, self.prepays.shape[1], PA, axis=1)
        self.recycles = np.insert(self.recycles, self.recycles.shape[1], RA, axis=1)

    @staticmethod
    def single_pool_pressure(reinv_amount, reinv_period, pps, smms, ages_, RRs, DPPs,
                             cdr, smdrs, yrs, dr, CDR_type, CPR_type, interest_type_, suoe):
        """

        Args:
            reinv_amount (float): 循环购买支出金额
            reinv_period (int): 本次循环购买日在日历的第几行
            pps (np.array): 摊还比例序列
            smms (np.array): 单期条件早偿率序列
            ages_ (np.array): 每一个归集日与某一次循环购买日期之间的月数，如果归集日早于循环购买日，则为空值
            RRs (np.array): 违约回收率
            DPPs (np.array): 违约回收率对应的延迟回收月数
            cdr (float): 累计违约率
            smdrs (np.array): 单期条件违约率序列
            yrs (np.array): 年收益率序列
            dr (float): 折价率
            CDR_type (str): 'yield_' - 使用 `YRs` 计算资产池利息， 'discount' - 使用 `DRs` 计算入池资产金额
            CPR_type (str): 早偿计算方式, 'type1' - smm * 期初本金余额， 'type2' - smm * (期初本金余额 - 当期本金回收)
            interest_type_ (str): yield_ - 需要根据利息率计算利息， discount-用折价率计算入池金额，不需要计算利息
            suoe (bool):

        Returns:
            tuple: tuple contains:
                    * OPB (np.array): 单笔循环购买支付存续期的期初本金余额序列
                    * CPB (np.array): 单笔循环购买支付存续期的期末本金余额序列
                    * PR (np.array): 单笔循环购买支付存续期的本金回收序列
                    * PA (np.array): 单笔循环购买支付存续期的早偿回收序列
                    * DA (np.array): 单笔循环购买支付存续期的违约本金序列
                    * IR (np.array): 单笔循环购买支付存续期的利息回收序列
                    * RA (np.array): 单笔循环购买支付存续期的违约回收序列


        **逻辑**

        1. 如果是折价计算的模式，则令入池资产总额 ``reinv_amount = reinv_amount / dr``
        2. 逐期计算，假设滚动到了第 `i` 期:

                * 计算本金回收为 ``reinv_amount * pps[i]``
                * 计算预期早偿金额 :  `` LoanPool.expected_pa(opb, smm=smms[i], plan_pr=plan_pr, type_=CPR_type)`` . 其中 `opb` 为第 `i` 期期初本金余额
                * 计算预期违约金额 : 如果是用累计违约率计算的，则预期违约金额等于 ``LoanPool.expected_da(opb, plan_da=plan_pr*cdr, type_=CDR_type)`` ,
                如果是用条件违约率计算，则为 ``LoanPool.expected_da(opb, smdr=smdrs[i], type_=CDR_type)`  .注意此时违约金额的计算跟非循环下不完全一样
                * 然后计算实际的本金回收、违约金额、早偿金额 ::

                     LoanPool.cal_cashflow_1period(opb, plan_pr=plan_pr, plan_da=plan_da, plan_pa=plan_pa, pr_opb=pr_opbs[i], suoe=suoe)

        3. 计算违约回收，同样利用 ``Loanpool`` , 其中的 ``cal_RA`` 用于一次生成违约序列对应的违约回收金额。
        4. 计算利息，如果是有 `YRs` 的模式，则利息=（期初本金-当期违约)*收益率*年华天数=(`OPB` - `DA`) * `yrs` * `ages_` / 12。否则，利息为0,其收益来源于本金回收总和超过循环购买支出的部分。
        """

        common_series = np.full(ages_.shape, 0.)
        OPB, CPB, PR, DA, PA = common_series.copy(), common_series.copy(), common_series.copy(), \
                               common_series.copy(), common_series.copy()

        if interest_type_ == 'discount':
            reinv_amount /= dr

        total_periods = len(ages_)
        CPB[reinv_period] = reinv_amount  # 期末本金余额, 循环购买那一期的期末余额即为循环购买金额
        bps = 1 - np.nancumsum(pps) + pps  # 期初剩余本金
        nan_row = np.isnan(ages_)
        pr_opbs = np.divide(pps, bps, where=bps>0)
        pr_opbs[nan_row] = 0
        last_period = reinv_period
        for i in range(reinv_period + 1, total_periods):
            pp, smm = pps[i], smms[i]

            if ~np.isnan(ages_[i]):
                opb = CPB[last_period]
                OPB[i] = opb
                plan_pr = pp * reinv_amount
                plan_pa = LoanPool.expected_pa(opb, smm=smm, plan_pr=plan_pr, type_=CPR_type)  # 早偿金额
                if CDR_type == 'cumulative':
                    plan_da = LoanPool.expected_da(opb, plan_da=plan_pr*cdr, type_=CDR_type)
                else:
                    plan_da = LoanPool.expected_da(opb, smdr=smdrs[i], type_=CDR_type)

                pr, da, pa = LoanPool.cal_cashflow_1period(opb, plan_pr=plan_pr, plan_da=plan_da, plan_pa=plan_pa,
                                                            pr_opb=pr_opbs[i], suoe=suoe)
                PA[i] = pa
                DA[i] = da
                PR[i] = pr
                CPB[i] = max(opb - pr - da, 0)

                if CPB[i] == 0:
                    break

                last_period = i
            else:
                OPB[i] = CPB[last_period]
                CPB[i] = CPB[last_period]

        RA = LoanPool.cal_RA(RRs=RRs, DPPs=DPPs, DA=DA, ages=ages_, total_period=total_periods)

        if interest_type_ == 'yield_':
            IR = (OPB - DA) * yrs  # 早偿也有利息
            IR[np.isnan(IR)] = 0
        else:
            IR = common_series.copy()

        return OPB, CPB, PR, PA, DA, IR, RA

    def cal_PPs(self, ages_):
        """
        计算每笔循环购买后续每个归集日的回款比例（based on initial PPs assumption），如果归集日距离循环购买日期的月数不是整数，则采用插值法计算。
        比如第2月期末应剩余75%， 第3月期末应剩余50%，则归集日为第2.5个月时，期末应该剩余62.5%。结合上一个归集日的期末应剩余比例，
        则得到这一个归集日应返回的金额占循环购买入池金额的比例。

        Args:
            ages_ (np.array): 每一个归集日与某一次循环购买日期之间的月数，如果归集日早于循环购买日，则为空值

        Returns:
            np.array: 每个归集日应回款占此次循环购买入池金额的百分比

        """
        BPs = self.BPs.copy()
        result_BPs = np.full(self.total_periods, float('nan'))   # 初始化结果数组

        for period_ in range(0, self.total_periods):
            x = ages_[period_]  # 当前归集日与循环购买日之间的月数
            if ~np.isnan(x):
                if x > self.longevity_limit:  # 检查是否超出最长寿命限制
                    result_BPs[period_] = 0
                    break

                if x in BPs.index:  # 如果月数在索引内，直接使用摊还比例
                    result_BPs[period_] = BPs[x]
                else:  # 使用线性插值计算回款比例
                    floor_x, ceil_x = int(np.floor(x)), int(np.ceil(x))  # 找到最接近的两个摊还比例的索引
                    floor_y, ceil_y = BPs[floor_x], BPs[min(ceil_x, self.longevity_limit)]  # 获取这两个索引对应的摊还比例
                    result_BPs[period_] = self.linear_insert(x, floor_x, ceil_x, floor_y, ceil_y)

        # 计算每期的摊还比例变化并归一化
        result_BPs = pd.Series(result_BPs)
        result_PPs = result_BPs.shift(1).ffill().bfill() - result_BPs.ffill().bfill()
        result_PPs = result_PPs.fillna(0.) / result_PPs.sum(skipna=True)
        return np.array(result_PPs)

    def cal_SMMs(self, ages_, year_posits):
        """
        将年化早偿率或者年化违约率转化为单期条件假设。通过将距离买入日的月数向上取整，得到对应的假设条件，然后根据与上个归集日之间的月数，计算当期的假设值。
        比如当前距离上一归集日1个月，距离循环购买日2.5个月，假设第3个月的年化早偿率为2%， 则当期的 smm = 1 - （1 - 2%）** （1/12）

        Args:
            ages_ (np.array): 每一个归集日与某一次循环购买日期之间的月数，如果归集日早于循环购买日，则为空值
            year_posits (np.array): 假设序列，index-月份，value-假设值

        Returns:
            np.array: 处理后的条件假设序列
        """
        result_SMMs = np.full(self.total_periods, float('nan'))   # 初始化结果数组
        last_d = 0   # 上一个有效归集日的月数
        for i in range(0, self.total_periods):
            d_ = ages_[i]  # 当前归集日与循环购买日之间的月数
            if ~np.isnan(d_) and d_ > 0:  # 检查是否为有效归集日
                y_posit = year_posits[min(int(np.ceil(d_)), max(year_posits.index))]   # 获取对应的年化条件假设值
                result_SMMs[i] = 1. - (1. - y_posit) ** ((d_-last_d) / 12)   # 计算单期条件早偿率
                last_d = d_   # 更新上一个有效归集日的月数

        result_SMMs[np.isnan(result_SMMs)] = 0
        return result_SMMs

    def cal_YRs(self, ages_):
        """
        获取单笔循环购买在后续每个归集日的收益率。通过对 `ages_` 向上取整选择 `YRs` 中对应的假设条件。比如当归集日与循环购买日距离3.5个月时，则取 `YRs` 中，对于第4个月的假设收益率

        Args:
            ages_ (np.array): 每一个归集日与某一次循环购买日期之间的月数，如果归集日早于循环购买日，则为空值

        Returns:
            np.array: 每一个归集日的利息收益率,已经乘以相应期间年化天数，可直接用于乘以剩余本金
        """
        result_YRs = np.full(self.total_periods, float('nan'))  # 初始化结果数组
        last_d = 0  # 上一个归集日与循环购买日之间的月数
        for i in range(0, self.total_periods):  # 当前归集日与循环购买日之间的月数
            d_ = ages_[i]

            if ~np.isnan(d_) and d_ > 0:  # 检查是否为有效归集日
                # 找到对应的年化收益率，计算当前归集日的收益率
                result_YRs[i] = self.YRs[min(np.ceil(d_), max(self.YRs.index))] * (d_ - last_d) / 12
                last_d = d_  # 更新上一个归集日的月数

        result_YRs[np.isnan(result_YRs)] = 0
        return result_YRs


    @staticmethod
    def linear_insert(x, floor_x, ceil_x, floor_y, ceil_y):
        """线性插值，如果超过范围，则用上下限"""
        if x >= ceil_x:
            return ceil_y
        elif x <= floor_x:
            return floor_y
        else:
            return (ceil_y - floor_y) / (ceil_x - floor_x) * (x - floor_x) + floor_y

    @property
    def reinv_due(self):
        """
        应付循环购买的金额:

            * 如果为混合账户，则为混合账户余额乘以循环购买比例 ``self.evaluate_col('total_col') * self.RP``
            * 如果本金账户和收益账户分开，则为本金账户余额加收益帐余额乘以循环购买比例

        """
        if self.mixed:
            return self.evaluate_col('total_col') * self.RP
        else:
            return (self.evaluate_col('int_col') + self.evaluate_col('prin_col')) * self.RP

    def prediction_after_rev(self):
        """用于返回最终的加压后现金流归集表，通过将各次循环购买的各期本金回收、利息回收等逐期相加得到资产池的本金回收、利息回收等"""
        prediction_result = pd.DataFrame()
        prediction_result.bfill(inplace=True)
        prediction_result['current_principal_due'] = self.principals.sum(axis=1)
        prediction_result['current_interest_due'] = self.interests.sum(axis=1)
        prediction_result['default_amount'] = self.defaults.sum(axis=1)
        prediction_result['recycle_amount'] = self.recycles.sum(axis=1)
        prediction_result['prepay_amount'] = self.prepays.sum(axis=1)
        prediction_result['end_principal_balance'] = self.end_balances.sum(axis=1)
        prediction_result['begin_principal_balance'] = self.begin_balances.sum(axis=1)
        prediction_result['current_revolving_out'] = self.reinv_payments
        prediction_result['end_default_balance'] = np.nancumsum(prediction_result['default_amount']) - \
                                                   np.nancumsum(prediction_result['recycle_amount'])

        prediction_result['begin_default_balance'] = prediction_result['end_default_balance'].shift(1)
        prediction_result.reset_index(drop=True, inplace=True)
        prediction_result.loc[0, 'begin_default_balance'] = 0
        if self.split_default:
            prediction_result[['end_default_balance', 'begin_default_balance']] = \
                prediction_result[['end_default_balance', 'begin_default_balance']] + self.begin_default

        prediction_result['tax_paid'] = self.tax_payments
        prediction_result['exp_paid'] = self.exp_payments
        prediction_result['incentive_fee'] = self.incentive_fee_payments
        prediction_result['tax_paid_liability'] = self.tax_liabilities
        prediction_result['exp_paid_liability'] = self.exp_liabilities
        prediction_result['date_'] = self.schedule['pool_date']
        prediction_result['reinvest_income'] = 0
        prediction_result = prediction_result[prediction_result.index > self.history_period]  # 只保留下个归集日及以后的，历史日期不保留
        future_events = self.events_result()
        if len(future_events) > 0:
            event_to_number = dict(zip(trigger_event_dict.values(), trigger_event_dict.keys()))
            events = list(future_events.columns)
            if 'date_' in events:
                events.remove('date_')
            events_type = np.array([event_to_number[x] for x in events])
            future_events.loc[:, 'event'] = future_events[events].apply(
                lambda x: ",".join(events_type[x.values]), axis=1)
            prediction_result = prediction_result.merge(future_events[['date_', 'event']], on='date_', how='left')

        prediction_result = \
            prediction_result.loc[prediction_result[['current_principal_due', 'current_interest_due',
                                                     'recycle_amount', 'prepay_amount',
                                                     'default_amount', 'tax_paid', 'exp_paid',
                                                     'current_revolving_out']].sum(axis=1) > 0.1, :].copy()

        return prediction_result


class Trigger:

    @classmethod
    def events(cls, trigger, current_date):
        """
        输入的触发事件表已判断触发条件是否满足，但是部分触发事件规定，仅在某时间段内满足触发条件才会触发重大事项。这里判断是否在规定时间内触发了事件，并输出

        Args:
            trigger (pd.DataFrame): 触发事件表，已根据当期项目的各数据更新了触发条件是否满足
            current_date (datetime.date): 当前日期

        Returns:
            dict: events: key-重大事项名称，value-是否最终触发

        """
        events = {}
        for i, c in trigger.iterrows():
            in_date = date(2000, 1, 1) if isinstance(c.start_date, float) else c.start_date
            out_date = date(3000, 1, 1) if isinstance(c.end_date, float) else c.end_date

            if current_date > in_date and current_date < out_date:
                events[c.event_type] = c.cond_result

        return events

