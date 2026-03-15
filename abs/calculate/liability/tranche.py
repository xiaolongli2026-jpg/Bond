
"""

**名词释义**

.. glossary::

    次级期间收益
        期间收益是或有收益，一般来说，为每期收益账的最后一个分配项。具体而言，“收入分账户”中的资金在完成税收、中介机构费用、优先档利息、本金账累计转移额和违约额的偿付之后，若仍有剩余，会根据约定的期间收益率向次级档证券持有人进行支付，常见的期间收益一般在2%~5%之间

    次级预期资金成本 / 固定资金成本
        次级档预期资金成本一般在次级档证券本金清偿完毕后、超额收益分配前进行支付。产品发行之初，发起机构和次级投资者通常会基于特定假设对收益率水平达成一致预期，并在此基础上设定次级档预期资金成本。由于超额收益通常由贷款服务机构和次级投资人按比例获取，且超额收益和期间收益本身有较大不确定性，因此预期资金成本很大程度上保证了次级档证券的基本收益水平。

    次级超额收益
        在次级档预期资金成本偿付完成后，如资产池仍有现金回款，则将按照一定比例在贷款服务机构和投资者之间进行分配，其中，分配给贷款服务机构的部分，被成为“浮动服务报酬”，分配给次级档证券的部分，被称为次级档证券的“超额收益”

    次级收益
        由次级期间收益、次级预期资金成本、次级超额收益共同构成，也可能是三者的自由组合。
"""

import numpy as np
import pandas as pd

from utils.timeutils import count_year


class Tranche(object):
    """
    单个证券的实例，用于现金流分配，随着现金流分配的过程逐期推进，该实例中的数据也会逐步更新。

    Args:
        security_id (str): 证券代码
        subordination (bool): 证券等级
        notional (float): 名义本金
        remaining_balance (float): 剩余面值, 包括已披露未支付的部分
        coupon_rates (np.ndarray): 利率，如果是累进税率则序列的值可能不一样，如果是非累进，则都是一样的
        daycount (str): 计息方式
        current_vol (int): 当前剩余张数
        initial_interest_date (datetime.date): 计息起始日
        last_period_num (int): 最新一次披露的支付日在prin_dates、int_dates的第几行，当存在已披露未支付时需要
        begin_period_num (int): 资产池的未来第一期
        last_payment_date (datetime.date): 上一付息日
        last_period_end_date (datetime.date): 上一计息区间截止日
        mat_date (datetime.date): 预期到期日
        legal_maturity (datetime.date): 法定到期日
        prin_dates (np.ndarray): 本金支付日序列，与现金流归集表和日历长度一致，非支付日的则对应的行为nan  （n, )
        int_dates (np.ndarray): 利息支付日序列，与现金流归集表和日历长度一致，非支付日的则对应的行为nan  （n, )
        amort_type (str): 摊还方式
        trade_after_payment (bool): 最新一次披露的应付本息， 在估值日是否已经支付
        amort_plan (pd.DataFrame): 摊还计划表（或有）
        int_due (float): 应付未付利息， 当上一次披露的应付利息支付日在估值日之后时，仍有未支付的部分，则考虑进来
        prin_due (float): 应付未付本金
        int_pay_with_prin (bool): 是否利随本清（需要是固定摊还）


    **属性**

    固定值

    * is_sub (bool): 是否是次级证券, `True` - 是， `False` - 否
    * _pass_through (bool): 是否是过手摊还， `True` - 是， `False` - 否
    * expect_expiry_date (datetime.date): 该证券的预期到期日，等于 ``mat_date``
    * begin_balance (float): 开始运行现金流分配之前的本金余额，也即估值时点的剩余面值

    可变值: 随着现金流的分配，值会变化的属性

    * period_num (int): 表示模拟过程中证券处于的行数，刚初始化时是等于 `last_period_num` , 开始现金流瀑布之前需要推到 `begin_period_num` ， 模拟过程中则需要与现金流瀑布 ``waterfall`` 中的 `period_num` 同步
    * actual_expiry_date (datetime.date): 实际到期日，初始设为预期到期日，后续在剩余面值清零的时候更新为清零的日期
    * total_periods (int): (说明中也用 `n` 表示)需要跑的期次，不等于支付日的次数。为了使资产端收集和证券端分配能够一起跑，这个总其次需要跟 ``water_fall`` 中的期次长度一致，也即期次是同时包含了资产端归集日和证券端支付日的日期数量总和。
    * int_payments (np.array): 实付利息 （n, )
    * prin_payments (np.array): 实付本金 （n, )
    * int_due_payments (np.array): 各期的应付利息 （n, )
    * balance_records (np.array): 证券在各期付完本金后的剩余本金 （n, )
    * prin_due_payments (np.array): 各期的应付本金（仅固定摊还，其他的没有应付本金的概念） （n, )
    * target_balances (np.array): 目标本金余额序列,如果是过手摊还,那么在预期到期日的目标本届余额是0.没有目标余额的期次都是nan  （n, )
    * last_int_date: 当前计息区间的起始日, 首期为最新一期报告披露的计息结束日，即使没有完成支付。
    * int_dates (np.array): 计息日，如果 ``workday_rule='actual'`` ，则为输入参数中的 `int_dates` , 否则为 `int_count_dates` , 前者为经过了节假日调整的支付日，后者则为未经过节假日调整的计息日
    * prin_dates (np.array): 还本日
    * int_pay_dates (np.array): 付息日
    * int_due (float): 应付利息, 如果当期结束完成了足额偿付则期末为 0
    * period_int_date  (datetime.date):  当前计息期间的结束日，如当前非计息日则为 nan
    * period_prin_date (datetime.date): 当前本金支付日，如当前非本金支付日则为 nan
    * period_pay_date (datetime.date): 当前计息区间的支付日，如当前非利息支付日则为 nan

    Note:
        在开始运行 ``waterfall`` 之前，每个证券的实例都要将应付未付的本息付掉（默认为能付清，并且不会扣减未来现金流归集表的本金收入），然后将 `period_num` 推到与下一归集日同一期，这样在 ``waterfall`` 进行分配时，资产端和证券端的期次完全匹配。

    TODO:
        目前利随本清都是固定摊还，如果有过手摊还的利随本清则暂时无法覆盖
    """

    def __init__(self, security_id, subordination, notional, remaining_balance, coupon_rates, daycount, current_vol,
                 initial_interest_date, last_period_num, begin_period_num, last_period_end_date, last_payment_date, mat_date,
                 legal_maturity, prin_dates, int_dates, int_pay_dates, amort_type,
                 trade_after_payment, int_due, prin_due, compensate, int_pay_with_prin, amort_plan=None):

        self.is_sub = False
        self._after_plan = False
        self._after_expect_maturity = False
        self._after_legal_maturity = False
        self._clear = False
        self._pass_through = True
        self._due = False
        self._reach_target_balance = True
        self._target_balance = 0
        self.int_pay_with_prin = int_pay_with_prin

        self.security_id = security_id
        self.subordination = subordination  # 在这个产品里，该 Tranche 的级别

        self.notional = notional  # notional of the security, constant
        self.begin_balance = remaining_balance  # 记录跑 waterfall 之前的本金余额
        self.balance = remaining_balance  # balance of the principal, changing over time

        self.coupon_rates = coupon_rates
        self.daycount = daycount.upper()
        self.current_vol = current_vol
        self.expect_expiry_date = mat_date  # 预期到期日
        self.actual_expiry_date = mat_date  # 实际到期日，后面会更新，
        self.legal_maturity = legal_maturity

        self.prin_dates = prin_dates
        self.int_dates = int_dates  # 计息日
        self.int_pay_dates = int_pay_dates

        self.total_periods = len(prin_dates)
        # 记录每期证券端现金流入
        self.int_due_payments = np.full([self.total_periods, ], 0.)  # 应付利息
        self.prin_due_payments = np.full([self.total_periods, ], float('nan')) # 固定摊还的时候记录应付本金
        self.target_balances = np.full([self.total_periods, ], float('nan'))
        i = len(prin_dates) - 1
        while i >= 0:
            if str(prin_dates[i]) != 'nan':
                break
            i -= 1
        self.target_balances[i] = 0  # 预期到期日的目标余额设为0
        self.final_prindate = i

        self.int_payments = np.full([self.total_periods, ], 0.)   # 实付利息
        self.prin_payments = np.full([self.total_periods, ], 0.)
        self.int_complement = np.full([self.total_periods, ], 0.) # 利息补足金额 , 完成资产池分配后,如果有补充支付的行为, 则会记录在这里
        self.prin_complement = np.full([self.total_periods, ], 0.) # 本金补足金额
        self.balance_records = np.full([self.total_periods, ], float('nan'))

        self.initial_interest_date = initial_interest_date

        self.last_int_date = last_period_end_date  # 当前计息区间的起始日（即已披露的下一次）,如果没有存续期则为计息起始日
        self.last_payment_date = last_payment_date

        self.period_num = last_period_num
        self.period_int_date = int_dates[last_period_num]  # 当前计息期间的结束日
        self.period_prin_date = prin_dates[last_period_num]
        self.period_pay_date = int_pay_dates[last_period_num] # 当前计息区间的支付日
        self._int_due = int_due
        self._prin_due = prin_due

        self.begin_period_num = begin_period_num # 用于区分应付未付和需要从资产池扣款的金额
        self.compensate = compensate
        self.complement_amount = 0 # 补充支付总金额，仅当compensate为True时，为不能足额支付的本息进行补足，资金来源为次级证券和账户余额，如果没有补足则一直是0
        self.amort_type = amort_type.lower()  # fixed, target-balance, pass-through,once

        if self.amort_type in ["fixed", "target-balance"]:
            self._pass_through = False
            amort_plan.reset_index(drop=True, inplace=True)
            self.amort_plan = amort_plan  # for fixed or target-balance tranches
            self.amort_plan_dates = list(amort_plan["date_"]) # 需要调整到与支付日一致，
            self.amort_plan_end_date = self.amort_plan_dates[-1]

        #将应付的未付的已宣告本金和利息提前扣除，将期次推到与下一个归集日一致
        if not trade_after_payment:
            self.receive_int(self._int_due)
            self.receive_prin(self._prin_due)

        self.balance_records[self.period_num] = self.balance
        if begin_period_num > last_period_num:
            diff_ = begin_period_num - last_period_num
            while diff_ > 0:
                self.next_period()  # 将期次推到与现金流归集表一致
                diff_ -= 1
        else:
            self.__count_interest_due()  # 先记录未来第一次应付利息

    def next_period(self):
        """如果 `period_num` 行属于计息日，则计算这个计息日跟上一个计息日之间的利息，加到 `int_due` 中，
        并且更新上一计息日 `last_int_date` 为 `period_num` 对应行的计息日。
        如果 `period_num` 行是空值，则当期是归集日，不是计息日。此时后推一期是为了保证证券段和资产池回收的模拟同步，实际上不对证券实例做任何操作。"""
        self.period_num += 1

        if self.period_num < self.total_periods:

            self.period_prin_date = self.prin_dates[self.period_num]  # 更新本金支付日

            if str(self.period_int_date) not in ("nan", "None"):  # 如果上一行有计算日，就将其作为上一计算日，否则不更新, 保留的为最近计算日
                self.last_int_date = self.period_int_date

            self.period_int_date = self.int_dates[self.period_num]  # 更新计息日
            self.period_pay_date = self.int_pay_dates[self.period_num] # 更新支付日
            self.__count_interest_due()

    def __count_interest_due(self):
        """
        计算当期应付利息, 不是利随本清时公式为：证券余额*计息期间年化应计天数*利息率；利随本清下为当期应付本金额 * 初始计息起始日至今年华天数 * 利息率

        """
        if str(self.period_int_date) not in ('nan', 'None'):  # 如果这一行是付息日，则计算应付利息并累加在应付未付的利息中.同时,记录在逐期应计利息序列,等待利息支付日进行支付
            if self.int_pay_with_prin:
                target_principal = self.target_prin_payment
                period_int = count_year(self.initial_interest_date, self.period_int_date, self.daycount) * \
                             target_principal * self.coupon_rates[self.period_num]

            else:
                period_int = count_year(self.last_int_date, self.period_int_date, self.daycount) * \
                             self.balance * self.coupon_rates[self.period_num]

            self.int_due_payments[self.period_num] = period_int
            self._int_due += period_int

    def receive_int(self, amount):
        """模拟利息回收 从应付利息中扣除已付"""
        if self.if_pay_int:
            if self._int_due - amount > -0.01:
                # update int_due, record the cashflow
                # 更新应付利息和实付利息记录
                self._int_due -= amount
                self.int_payments[self.period_num] += amount

                if self.compensate and self.period_num >= self.begin_period_num:  # 如果是历史应付未付的，不进行补足，按照披露的付
                    # 如果需要补足支付，调用makeup_int方法
                    self.makeup_int()
            else:
                 raise ValueError(f"证券{self.security_id}在支付日日{self.period_pay_date}的流入金额超过应付利息")
        else: # 非偿付日下
            if amount >= 1e-2:
                raise ValueError(f"证券{self.security_id}在非利息支付日有利息流入, 期次为{self.period_num}")

    def receive_prin(self, amount):
        """模拟本金回收 从证券本金中扣除应付/实付本金"""

        if self.if_pay_prin:
            
            if amount <= self.balance + 0.01:  # 检查支付金额是否在合理范围内
                # update balance, record the cashflow
                # 更新本金余额和实付本金记录
                self.balance -= amount
                self.prin_payments[self.period_num] += amount

                if self.compensate and self.period_num >= self.begin_period_num:
                    # 如果需要，进行本金补足逻辑
                    self.makeup_prin()  # 如果是有支付目标的，则自动补足

                # 更新本金余额记录
                self.balance_records[self.period_num] = self.balance

                # 如果本金余额接近零，则更新实际到期日
                if self.balance < 0.01:  # 完成还款了,则记录下证券实际到期日
                    self.actual_expiry_date = self.period_prin_date

            else:
                # 如果支付金额超过了本金余额，抛出异常
                raise ValueError(f"证券{self.security_id}在本金支付日{self.period_prin_date}的流入金额超期初本金余额")
        else:
            # 如果不是本金支付日且有本金流入，抛出异常
            if amount >= 1e-2:  # 小于0.01时忽略
                raise ValueError(f"证券{self.security_id}在非本金支付日有本金流入, 期次为{self.period_num}")

    @property
    def if_pay_int(self) -> bool:
        """当前是否支付利息, 根据 `period_pay_date` 是否是日期得到，如果是空值(nan), 则返回False, 当期不付息"""
        return str(self.period_pay_date) not in ('nan', 'None')

    @property
    def if_pay_prin(self) -> bool:
        """当期是否支付本金, 根据 `period_prin_date` 是否是日期得到，如果是空值(nan), 则返回False, 当期不还本"""
        return str(self.period_prin_date) not in ('nan', 'None')

    @property
    def on_int_date(self) -> bool:
        """用于判断支付顺序的节点条件 `A1.OnIntDates.n` , 即当期是否刚好是该证券的第 `n` 个计息日"""
        return self._on_int_date

    @on_int_date.setter
    def on_int_date(self, split_date_num):
        if not hasattr(self, 'split_date'):
            self.split_date = self.int_pay_dates[~np.isnan(self.int_pay_dates)][split_date_num-1]
        self._on_int_date = self.period_pay_date == self.split_date

    @property
    def before_int_date(self):
        """用于判断支付顺序的节点条件 `A1.BeforeIntDates.n` , 即当期是否早于是该证券的第 `n` 个计息日"""
        return self._before_int_date

    @before_int_date.setter
    def before_int_date(self, split_date_num):
        if not hasattr(self, 'split_date'):
            self.split_date = self.int_pay_dates[~np.isnan(self.int_pay_dates)][split_date_num - 1]
        self._before_int_date = self.period_pay_date >= self.split_date

    @property
    def after_int_date(self):
        """用于判断支付顺序的节点条件 `A1.BeforeIntDates.n` , 即当期是否晚于是该证券的第 `n` 个计息日"""
        return self._after_int_date

    @after_int_date.setter
    def after_int_date(self, split_date_num):
        if not hasattr(self, 'split_date'):
            self.split_date = self.int_pay_dates[~np.isnan(self.int_pay_dates)][split_date_num - 1]
        self._after_int_date = self.period_pay_date < self.split_date

    @property
    def int_due(self) -> float:
        """当期应付利息，如果处于支付日，则返回 `self._int_due` ，如果不是，则返回0 """
        if self.if_pay_int:
            return self._int_due
        else:
            return 0.

    @property
    def prin_due(self) -> float:
        """ 当期应付本金，如果处于还本日，则返回当前的本金余额，如果不是，则返回0。具体还多少在 ``waterfall`` 中会根据支付上限判断，
        特别是固定摊还的.因为有些证券除了固定摊还,在固定摊还期没有还完的情况下会继续还款,因此不能简单的将固定摊还证券的应付本金直接根据摊还计划表确认"""
        if self.if_pay_prin:
            return self.balance
        else:
            return 0.

    @property
    def target_prin_payment(self) -> float:
        """ 对于固定摊还证券，获取当期的本金计划还款额，用于 ``waterfall`` 中调取计算本金支付上限:

                * 如果当期是摊还计划表中的还本日（ ``self.due==True`` ) 返回摊还计划表中对应行的当期应付本金，
                * 如果当期不是摊还计划表中的还本日则返回 0。相当于视作过手摊还

        Note:
            * 如果是过手摊的话，统一返回当期期初的本金余额，以避免支付顺序错误维护了支付上限
            * 不是支付日的还本金额统一为0, 相当于假设了如果在规定的固定摊还日没有还完，
            后续的应付本金会一直是0,除非支付顺序其他行显示有类似于过手摊还的补充支付安排。结合触发事件表，此时可能会触发违约

        """
        # assume amort_plan_dates is a list
        if self._pass_through:
            return self.balance
        else:
            if self.due:
                # 考虑到摊还计划表的频率和证券还本付息频率不一定相等，这里查询一下
                n = self.amort_plan_dates.index(self.period_prin_date)
                target_pay = self.amort_plan["target_principal_payment"][n]
                self.prin_due_payments[self.period_num] = target_pay
                return target_pay
            else:
                return 0.

    @property
    def target_balance(self):
        """ 对于固定摊还证券，获取当期的目标本金，用于 ``waterfall`` 中调取计算本金支付上限:

            * 如果当期是摊还计划表中的还本日（ ``self.due==True`` ) 返回摊还计划表中对应行的当期目标本金余额, 则计算支付上限时应付的本金额即为：当期期初本金余额-当期目标本金余额（ `self.target_balance-self.balance` )
            * 如果当期不是则返回上一个摊还日的目标本金余额。 此时在计算支付上限当期应付本金余额时:

                    * 如果在上一个摊还日时正常完成了还款，则当期应付本金 = 当期期初本金余额 - 当期目标本金余额（即上一期的目标本金余额） = 0
                    * 如果在上一个摊还日时未正常完成还款，则当期应付本金等于上一摊还日应付但是因为资金不够未足额支付的应付本金。

            * 如果当期不仅不是还本日，并且晚于最晚的摊还日 （ ``self.after_plan=True`` ）, 此时返回 0。也就是过了固定摊还阶段后，变成跟过手摊还一样支付上限即为本金余额，有多少还多少。

        Notes:
            * 如果过手摊还的话则统一返回 0 ，此时即使支付顺序错误地维护了支付上限，也不会产生影响
            * 跟规定每期摊还本金额的情况不同，规定目标余额时，允许在后续期次补充支付，直到达到目标本金余额
        """
        if self._pass_through:
            tb = 0
        else:
            if self.due:
                n = self.amort_plan_dates.index(self.period_prin_date)
                self._target_balance = self.amort_plan["target_balance"][n]
                tb = self._target_balance
            elif self.after_plan:
                tb = 0.
            else:
                tb = self._target_balance

            self.target_balances[self.period_num] = tb # 目标余额

        return tb

    @property
    def due(self) -> bool:
        """对于固定摊还的证券，当前是否处于摊还计划中的还款日，根据当期日期是否在摊还计划表中得到。如果是过手摊还，则恒为否（False) """
        if not self._pass_through:
            self._due = self.period_prin_date in self.amort_plan_dates
        return self._due

    @property
    def not_due(self) -> bool:
        """对于固定摊还的证券，当前是否不处于摊还计划中的还款日，根据当期日期是否在摊还计划表中得到。"""
        return not self.due

    @property
    def after_plan(self) -> bool:
        """对于固定摊还的证券， 当前日期是否晚于预期到期日（即固定摊还计划中最后一个本金摊还日） """
        if (not self._after_plan) and (not self._pass_through):
            self._after_plan = self.period_prin_date > self.amort_plan_end_date if self.if_pay_prin else False
        return self._after_plan

    @property
    def clear(self) -> bool:
        """是否完成了本金还款(本金余额为0）"""
        if not self._clear:
            self._clear = abs(self.balance) < 0.01
        return self._clear

    @property
    def not_clear(self):
        """是否仍有剩余本金"""
        return not self.clear

    @property
    def eternal_True(self) -> True:
        return True

    @property
    def ignore(self):
        return 0

    @property
    def prin_clear(self) -> bool:
        """对于固定摊还类，当期本金是否足额清偿（即达到当期的目标本金余额）"""
        if self._pass_through:  # 过手没有还款预期
            return True
        else:
            return self.reach_target_balance

    @property
    def int_clear(self) -> bool:
        """当期利息是否足额清偿（即当期期末应付利息是否达到0）"""
        return self.int_due < 0.01

    @property
    def after_expect_maturity(self):
        """是否已经到了（或超过）预期到期日"""
        if not self._after_expect_maturity:
            self._after_expect_maturity = self.period_pay_date >= self.expect_expiry_date if self.if_pay_int else False
        return self._after_expect_maturity

    @property
    def after_legal_maturity(self):
        """是否已经到了（或超过）法定到期日"""
        if not self._after_legal_maturity:
            self._after_legal_maturity = self.period_pay_date >= self.legal_maturity if self.if_pay_int else False
        return self._after_legal_maturity

    @property
    def reach_target_balance(self):
        """固定摊还证券是否达到了目标本金余额, 只在 ``self.due = True`` 时进行更新，否则中间的日期会错误的更新。如果过了固定摊还期但是没有完成还款，该指标还是维持在最后一个固定摊还日的状态不进行更新"""
        if self.due:
            self._reach_target_balance = (self.balance - self.target_balance) < 1
        return self._reach_target_balance

    @property
    def expect_maturity_target_clear(self) -> bool:
        """预期到期日时（或以后）证券余额达到目标余额（固定摊还证券）"""
        return self.reach_target_balance and self.after_expect_maturity

    @property
    def expect_maturity_prin_clear(self):
        """预期到期日时（或以后），固定摊还的证券的本金支付总和是否达到了摊还期计划表规定的各期应付本金总和（因为在处理摊还计划表时对摊还计划表的目标本金支付和目标本金余额进行了互相填充，因此判断是否达到目标本金余额也一样）"""
        return self.expect_maturity_target_clear

    @property
    def legal_maturity_balance_clear(self) -> bool:
        """是否在法定到期日完成了还款"""
        return self.after_legal_maturity and self.clear

    def makeup_int(self):
        """补齐应付未付利息"""

        self.int_complement[self.period_num] += self.int_due  # 支付过了利息，但是如果不足，则额外补充支付
        self.int_payments[self.period_num] += self.int_due
        self.complement_amount += self._int_due
        self._int_due = 0

    def makeup_prin(self):
        """补齐应付未付本金"""
        if self._pass_through:
            if self.period_num == self.final_prindate:
                self.prin_complement[self.period_num] += self.balance
                self.prin_payments[self.period_num] += self.balance
                self.complement_amount += self.balance
                self.balance = 0
        else:
            target = self.target_balance
            if self.balance > target:
                amount = self.balance - target
                self.complement_amount += amount
                self.balance -= amount
                self.prin_complement[self.period_num] += amount
                self.prin_payments[self.period_num] += amount


class SubTranche(Tranche):

    def __init__(self, security_id, subordination, notional, remaining_balance, coupon_rates, fcc, daycount, current_vol,
                 initial_interest_date, last_period_num, begin_period_num, last_period_end_date, last_payment_date,
                 mat_date, legal_maturity, prin_dates, int_dates, int_pay_dates, amort_type,
                 amort_plan, rev_status, trade_after_payment, prin_due=0., int_due=0.):
        """次级证券

        Args:
            rev_status (np.adarray): 日历中是否循环购买期的列，1表示循环购买期, 0表示摊还期
            coupon_rates (np.adarray): 次级预期期间收益
            fcc (float): 次级固定资金成本
            继承了 ``Tranche`` , 故其他入参与 ``Tranche`` 中一致

        Attributes:
            fcc (float): 固定资金成本率
            fcc_due (float): 应付固定资金成本
            fcc_payments (np.array): 固定资金成本支付情况序列 （n,)
            exret_payments (np.array): 超额收益支付情况序列 (n,)


        """
        self.rev_status = rev_status
        self.fcc = fcc  # fixed capital cost
        self.fcc_due = 0.

        super().__init__(security_id, subordination, notional, remaining_balance, coupon_rates, daycount, current_vol,
                         initial_interest_date, last_period_num, begin_period_num, last_period_end_date,
                         last_payment_date, mat_date, legal_maturity, prin_dates, int_dates, int_pay_dates, amort_type,
                         trade_after_payment=trade_after_payment, amort_plan=amort_plan,
                         int_due=int_due, prin_due=prin_due, compensate=False, int_pay_with_prin=False)  # 次级不补

        self.fcc_payments = np.full(self.total_periods, 0.)
        self.exret_payments = np.full(self.total_periods, 0.)
        self.is_sub = True

    def next_period(self):
        """将次级证券推到下一期，在继承 ``Tranche.next_period`` 的同时，有以下特征:

            * 先清空上一期的次级应付期间收益，即假设次级的期间收益即使没还完也不会作为应付金额在以后进行支付。
            相反，优先级的利息如果未足额支付会一直保留在应付金额中（ `self._int_due` )
            * 继承 ``Tranche.next_period`` 将次级证券推到下一期，此时计算了当期的期间收益
            * 如果下一期刚好是计息日，则次级还需要额外计算固定资金成本，计算方法为 （公式见研究报告）::

                self.fcc_due += max((count_year(self.last_int_date, self.period_int_date, self.daycount) *\
                             self.balance * self.fcc - self.int_payments[self.period_num]), 0)



        """

        self._int_due = 0  # 次级应付期间收益每期结束时清零
        super().next_period()

        if self.period_num < self.total_periods:  # 次级需要额外计算一下FCC
            self.count_fcc_due()

    def count_fcc_due(self):
        """
        计算次级固定资金成本

        """
        if str(self.period_int_date) not in ('nan', 'None'):
            self.fcc_due = 0 # 固定资金成本不递延
            self.fcc_due += max((count_year(self.last_int_date, self.period_int_date, self.daycount) *\
                             self.balance * self.fcc - self.int_payments[self.period_num]), 0)
        
    def receive_exret(self, amount):
        """次级超额收益，具体需要支付多少由 ``waterfall`` 决定。将支付的金额加到 `self.exret_payments` 对应的期次上"""
        self.exret_payments[self.period_num] += amount

    def receive_fcc(self, amount):
        """应付固定资金成本的支付，具体支付多少在 ``waterfall`` 决定。将支付的金额加到 `self.fcc_payments` 对应的期次上"""
        if amount <= self.fcc_due:
            self.fcc_due -= amount
            self.fcc_payments[self.period_num] += amount
        else:
            raise ValueError("The inflow is larger than fcc due.")

    @property
    def exret_due(self) -> float:
        """应付超额收益:

            * 如果是摊还期，并且是还本日期，则返回无穷大，因为超额收益一般没有上限，账户有多少余额还多少。
            如果对超额收益有限制要求，则需要在支付顺序维护合规的支付上限。
            * 否则返回 0.

        """
        # 次级超额收益频率与还本频率一致 ，但是次级超额收益只在摊还期支付

        is_rev = self.rev_status[self.period_num]
        if self.if_pay_int and str(is_rev) == '0':
            return float("inf")
        else:
            return 0.

    @property
    def fcc_clear(self) -> bool:
        """是否完成固定资金成本的支付， 根据应付固定资金成本( `self.fcc_due` )是否为 0 判断"""
        return self.fcc_due < 0.01

    def roll_back(self, amount):
        """
        扣减已经分配过程支付的本金利息, 用于补充优先级的本息支付(服务于现金流补齐逻辑)

        """
        if amount <=0:
            return 0

        int_copy = self.int_payments.copy()
        prin_copy = self.prin_payments.copy()
        exret_copy = self.exret_payments.copy()
        fcc_copy = self.fcc_payments.copy()
        total_payment = sum(int_copy) + sum(prin_copy) + sum(exret_copy) + sum(fcc_copy) # 总支付

        int_copy[:self.begin_period_num] = 0
        exret_copy[:self.begin_period_num] = 0
        fcc_copy[:self.begin_period_num] = 0
        prin_copy[:self.begin_period_num] = 0
        cumsum_reverse = np.nancumsum(int_copy[::-1]) + np.nancumsum(prin_copy[::-1]) + \
            np.nancumsum(exret_copy[::-1]) + np.nancumsum(fcc_copy[::-1])


        remain_ = 0
        n = len(cumsum_reverse)
        if amount >= cumsum_reverse[-1]:
            remain_ = amount - cumsum_reverse[-1]
            self.int_payments[self.begin_period_num:] = 0
            self.prin_payments[self.begin_period_num:] = 0
            self.fcc_payments[self.begin_period_num:] = 0
            self.exret_payments[self.begin_period_num:] = 0
        elif amount < cumsum_reverse[0]:
            portion_ = 1 - amount / cumsum_reverse[0]
            self.prin_payments[-1] = self.prin_payments[-1] * portion_
            self.int_payments[-1] = self.int_payments[-1] * portion_
            self.exret_payments[-1] = self.exret_payments[-1] * portion_
            self.fcc_payments[-1] = self.fcc_payments[-1] * portion_
        else:
            floor_index = np.argwhere(cumsum_reverse < amount)[-1][0]
            ceil_index = np.argwhere(cumsum_reverse > amount)[0][0]
            temp_ = amount - cumsum_reverse[floor_index]
            portion_ = 1 - temp_ / (cumsum_reverse[ceil_index] - cumsum_reverse[floor_index])
            self.prin_payments[n-ceil_index-1] = self.prin_payments[n-ceil_index-1] * portion_
            self.int_payments[n-ceil_index-1] = self.int_payments[n-ceil_index-1] * portion_
            self.exret_payments[n-ceil_index-1] = self.exret_payments[n-ceil_index-1] * portion_
            self.fcc_payments[n-ceil_index-1] = self.fcc_payments[n-ceil_index-1] * portion_

            self.int_payments[n-floor_index-1:] = 0
            self.prin_payments[n-floor_index-1:] = 0
            self.fcc_payments[n-floor_index-1:] = 0
            self.exret_payments[n-floor_index-1:] = 0


        # 确认一下更新后支付序列的金额是否对的上
        if abs(total_payment - sum(self.int_payments) - sum(self.prin_payments) - sum(self.fcc_payments) -
                sum(self.exret_payments) - (amount - remain_)) > 0.1:
            raise ValueError("扣减后金额与扣减金额的和不等于次级初始支付")

        setattr(self, 'deduct_amount', amount - remain_)

        return remain_


class TrancheCollect:

    def __init__(self, *args):

        """
        联合证券信息

        Args:
            *args: Tranche/Subtranche 实例，有几只证券，*arg中就有几个实例

        Attributes:
            priority_levels (list): 优先级证券的等级（eg.a1, a2)，根据等级中是否有 'a' 判断是否属于优先级
            sub_levels (list): 次级证券的等级（eg.sub1,sub2,sub), 根据实例中的 `is_sub` 属性是否为是判断是否是次级证券


        """
        self.priority_levels = []
        self.sub_levels = []
        self.all_levels = []
        for tr_obj in args:
            level = tr_obj.subordination
            setattr(self, level, tr_obj)
            self.all_levels.append(level)
            if tr_obj.is_sub:
                self.sub_levels.append(level)
            elif 'a' in level:
                self.priority_levels.append(level)  # 剩余的是夹层

    def push_tranches_to_next_period(self, current_num):
        """将所有证券都推到下一期，同时重新实例化以更新 `self._priority` 和 `self._all_sec` 数据"""

        for tr_level in self.all_levels:
            if getattr(self, tr_level).period_num < current_num:  # 发现一些披露上有问题或者没有及时维护的会导致资产池日期比证券端日期早
                getattr(self, tr_level).next_period()

        self._priority = self.Combine([getattr(self, level) for level in self.priority_levels])
        self._all_sec = self.Combine([getattr(self, level) for level in self.all_levels])


    def move_cash(self, account_remain):
        """
        将补充支付优先级的金额从次级和账户余额中扣掉，先扣减账户余额，不足的时候从最后一期次级开始扣

        Returns:
            account_remain (float): 账户余额
            excess_ (float): 补充支付超次级本息超额收益和的部分, 即没有办法补充支付的部分

        """
        sum_ = 0
        for level in self.priority_levels:
            tr = getattr(self, level)
            makeup_sum = getattr(tr, 'complement_amount')
            sum_ += makeup_sum  # 补充的总金额

        if sum_ < 0.1:
            return account_remain, 0

        # 加总需要补充支付的金额,从账户余额和补充来源扣款
        if account_remain > 0:
            minus_ = min(sum_, account_remain)
            sum_ -= minus_
            account_remain -= minus_

        excess_ = 0
        if sum_ > 0:
            balances = []
            for level in self.sub_levels:
                tr = getattr(self, level)
                begin_balance = getattr(tr, 'begin_balance')
                balances.append(begin_balance)

            portion_ = [x / sum(balances) * sum_ for x in balances]
            i = 0
            for level in self.sub_levels:
                tr = getattr(self, level)
                remain_ = getattr(tr, 'roll_back')(portion_[i] + excess_)
                excess_ = remain_
                i += 1

        setattr(self, 'excess_payment', excess_)  # 补充支付的金额中，账户余额、次级证券都无法覆盖的部分
        return account_remain, excess_


    @property
    def priority(self):
        """所有优先级证券对 ``Combine`` 实例化，用于 ``waterfall`` 中触发事件判断时对优先级证券需要共同满足的条件进行计算"""
        try:
            return self._priority
        except:
            self._priority = self.Combine([getattr(self, level) for level in self.priority_levels])
            return self._priority

    @property
    def all_sec(self):
        """项目下所有证券对 ``Combine`` 实例化，用于 ``waterfall`` 中对所有证券需要共同满足的条件进行计算"""

        try:
            return self._all_sec
        except:
            self._all_sec = self.Combine([getattr(self, level) for level in self.all_levels])
            return self._all_sec


    class Combine:

        def __init__(self, tr_obj_list):
            """
            用于计算所有证券属性联合构成的指标，主要用于触发事项中，对需要所有证券都满足条件才能触发的事件的判断

            Args:
                tr_obj_list (list): 由前面 Tranche、SubTranche实例化后打包成的列表
            """
            self.tr_obj_list = tr_obj_list

        @classmethod
        def initialize(cls, *args):
            cls.__init__(*args)

        @property
        def clear(self) -> bool:
            """是否所有证券本金都清偿完毕"""
            return self.clear_cal('clear')

        @property
        def int_clear(self) -> bool:
            """是否所有证券的应付利息都按时足额偿还"""
            return self.clear_cal('int_clear')

        @property
        def expect_maturity_target_clear(self):
            """是否所有证券在预期到期日的证券余额都达到目标余额（如果是固定摊还）"""
            return self.clear_cal('expect_maturity_target_clear')

        @property
        def expect_maturity_prin_clear(self):
            """预期到期日时，当期本金支付是否达到当期应付本金（固定摊还）"""
            return self.clear_cal('expect_maturity_target_clear')

        @property
        def legal_maturity_balance_clear(self):
            """法定到期日时，当期本金支付是否达到当期应付本金（固定摊还）"""
            return self.clear_cal('legal_maturity_balance_clear')

        @property
        def prin_clear(self):
            """是否所有证券的本金都按时足额偿还（固定摊还）"""
            return self.clear_cal('prin_clear')

        def clear_cal(self, attr) -> bool:
            """用与判断是否所有证券的某个属性（ `attr` ）都是 `True` """
            clear = True
            for tr in self.tr_obj_list:
                clear = clear and (getattr(tr, attr))
            return clear

        @property
        def target_balance(self) -> float:
            """所有证券的目标本金余额总和"""
            return sum([tr.target_balance for tr in self.tr_obj_list])

        @property
        def balance(self) -> float:
            """所有证券的本金余额总和"""
            return sum([tr.balance for tr in self.tr_obj_list])

        @property
        def notional(self) -> float:
            """所有证券的初始面值总额"""
            return sum([tr.notional for tr in self.tr_obj_list])

        @property
        def int_payments(self) :
            """各期利息/期间收益支付总额,即各个证券实例中保存的 `self.int_payments` 序列逐期加总，不包括次级证券的超额收益、固定资金成本 """
            return np.array([tr.int_payments for tr in self.tr_obj_list]).T.sum(axis=1)

        @property
        def prin_payments(self):
            """各期还本总额序列，即各个证券实例中保存的 `self.prin_payments` 序列逐期加总"""

            return np.array([tr.prin_payments for tr in self.tr_obj_list]).T.sum(axis=1)

        @property
        def fcc_payments(self):
            """次级固定资金成本支付总额序列"""
            fccs = np.array([tr.fcc_payments for tr in self.tr_obj_list if tr.is_sub])
            if len(fccs) > 0:
                return fccs.T.sum(axis=1)
            else:
                return 0.

        @property
        def exret_payments(self):
            """次级超额收益支付总额序列"""
            exrets = np.array([tr.exret_payments for tr in self.tr_obj_list if tr.is_sub])
            if len(exrets) > 0:
                return exrets.T.sum(axis=1)
            else:
                return 0.
