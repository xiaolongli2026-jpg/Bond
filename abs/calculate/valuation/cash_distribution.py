# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from collections import defaultdict
from utils.timeutils import to_date2, count_year
from utils.operatorutils import and_
from calculate.liability.tranche import Tranche, SubTranche, TrancheCollect
from calculate.waterfall.waterfall_new import Waterfall, Waterfall_rev


def calculate_abs(df_product, df_tranches, df_prediction, df_sequence, df_plan, df_other, df_trigger, events, df_schedule,
                  trade_date, coupon_rate_change, add_remain_account,
                  compensate, day_count_method, begin_default_recover=False, split_default=False,
                  CDRs=None, PPs=None, RP=0, CPRs=None, RRs=None, YCDRs=None, YRs=None, DRs=None, suoe=True,
                  cpr_type='type1', type_='yield_', recal_cashflow=False, exist_tax_exp=False,
                  exp_rate=0, tax_rate=0,) -> tuple:
    """
    功能: 1. 循环购买重新测算的模块的现金流加压和分配；2.其他测算模块的现金流分配

    Args:
        见 ``module_abs_new.abs_calculator``

    Returns:
        tuple:  见 ``m下·ain_cashflow.main_calculation``


    **逻辑**

    1. 如果不是循环购买重新测算的类型，将日历和现金流归集表进行匹配，使得每行都相互对应。可能会出现一些没有归集日、本金回收的行，此类行属于证券端的支付日而非归集日。
    对于非循环购买，找到下一归集日在第几行，即现金流归集表日期中归集日期晚于最新披露的证券支付日的第一行 （设为 `pred_start_period` ）。
    此时现金流归集表和日历的长度是一样的，只有这样才能让waterfall实例和tranche实例在现金流模拟的过程中是同步的。

    2. 如果是循环购买重新测算的类型，现金流归集表不会被用到。此时找到日历中归集日期晚于最新披露的证券支付日的第一行 （设为 `pred_start_period` ），
    并将证券支付日视作上一归集日，将当前剩余资产池本金额从这个日期开始进行摊还并模拟循环购买行为。
    注意，此时即使有实际的历史归集日也不会参考，不然当这个日期滞后时会导致模拟归集时，一部分在历史支付日之前被归集，产生误差。
    当然，将历史归集日改为历史支付日会使得各期回收金额产生变化，但是不良影响会相对小一些。\n
    另外，如果在日期规则表中维护的这个日期序列没有未来期次，则没办法继续往后算。此时只分配掉应付未付的证券本息，不会运行现金流瀑布

    3. 找到所有证券端最新历史支付日在日历中所在的行，记为 `sec_start_period` 。 如果前述得到的证券支付日晚于估值日，设置 `trade_after_payment` 为 `False` ,
    表示有应付未付金额，如早于等于估值日，则设置为 `True`。对于存在应付未付的证券，即使现金流归集表没有金额了，最终结果中也会在证券的未来现金流中保存这一期。
    如果现金流归集表未来有回收金额，则回收的金额只用于支付这一期应付未付金额以后的证券本息，这一期则是默认能支付掉但是不扣除账户余额。\n
    另外如果证券没有支付过本息，历史支付日保存的是计息起始日，则 `sec_start_period` 会被设为0，即从日历表的第一行开始进行模拟支付。

    4. ``initial_tranche_object`` 初始化证券对象，实例中的一个重要属性是未来每期的本金支付、利息支付金额。
    5. 现金流瀑布: 用于模拟从资产池回收金额到证券端分配现金流的过程。一般情况下现金流每期的应回收金额由输入的加压后现金流归集表给出。
    但是如果是循环购买重新测算现金流的类型，则会根据假设参数，在waterfall实例中一边测算每期应收本金、利息、违约金额、早偿金额，一边模拟循环购买、还本付息和证券本息支付。
    其中，如果 `add_remain_account` 为 True, 则 ``Waterfall`` 初始的本金帐余额 `prin_col` 设置为读取到的账户余额。

    ..caution::
        资产池余额为0时会直接清空现金流归集表。但是有时候资产池余额是根据证券余额调整的，如果证券端完成了本金支付，但是资产池尚未清算，未来仍有可能支付次级超额收益。此时这样的做法相当于减少了超额收益的现金流。\n
        如果证券端报告是滞后的，那么可能会出现结果中的现金流早于估值日的情况。

    """
    # 0. preparation
    distribution_info = defaultdict(list)  #
    trade_date = to_date2(trade_date)

    product = list(df_product.itertuples(index=False))[0]
    tranches = list(df_tranches.itertuples(index=False))
    other = list(df_other.itertuples(index=False))[0]
    initial_date = product.initial_date
    initial_principal = product.initial_principal
    is_revolving_pool = product.is_revolving_pool
    history_date = to_date2(other.virtual_history_date)
    trigger_events = events
    cum_loss = other.cum_loss
    cdr = other.CDR
    remaining_principal = other.remaining_principal
    default_principal = other.default_principal
    has_security_duration = other.has_security_duration
    pass_wf = True if remaining_principal < 0.01 else False # 如果是有应付未付的债券但是现金流归集表没有了的，会再额外分配一次，但是不经过现金流归集表

    if remaining_principal < 0.01:
        df_prediction = pd.DataFrame()  # todo 资产池余额为0则清空现金流归集表。不绝对准确，有时候资产池余额是根据证券余额调整的，实际资产池的情况并不明确，并不能获知是否有余额可用于支付超额收益

    # 1. 最近一次已宣告兑付日所在期数 sec_start_period（当前可能已支付也可能已披露未支付），逐券找日期
    sec_start_periods = {}
    trade_after_payments = {}  # 是否已支付
    for tr in tranches:
        last_sec_payment_date = tr.payment_date
        not_non_dates = df_schedule.loc[~df_schedule[tr.security_level + '_int_pay_date'].isna(), :]
        row_ = not_non_dates.loc[not_non_dates[tr.security_level + '_int_pay_date'] == last_sec_payment_date, :]
        # 找到最接近披露的支付日的那个日期
        if len(row_) > 0:
            spp = row_.index[0]
            sec_start_periods[tr.security_level] = spp

            # False 表示支付在估值日后，此时需要将这笔算进证券端的现金流，但是不需要扣除资产池的本金
            # True 表示已宣告本息在估值日之前就已经支付，不需要再考虑
            if trade_date < last_sec_payment_date and has_security_duration:
                trade_after_payments[tr.security_level] = False
            else:
                trade_after_payments[tr.security_level] = True
        else:
            sec_start_periods[tr.security_level] = 0  # 此时表示没有支付过证券的本息，因为payment_date是补的计息起始日，并不在支付日列里面。
            trade_after_payments[tr.security_level] = True

    # 1.2如果日历中最后一个证券支付日等于或者早于历史支付日（已经完成支付，非应付未付），则理应不继续计算。
    if df_schedule.loc[~df_schedule['tax_exp_date'].isna(), 'tax_exp_date'].max() <= to_date2(df_other.loc[0, 'history_payment_date']) \
        and and_(trade_after_payments.values()):
        if df_tranches['period_end_balance'].sum() < 0.1:
            raise ValueError("证券已完成最后一次支付，即历史支付日晚于等于支付日序列中最后一个，需检查是否应摘牌证券未维护摘牌日")
        else:
            raise ValueError("证券的最近历史支付日晚于等于支付日序列最后一个，但是本金仍有剩余，需检查日期序列是否证券")

    # 2. 分为循环购买重新测算型和其他类型，循环购买重新测算型不用原来的现金流归集表
    if (is_revolving_pool and recal_cashflow):
        df_temp = df_schedule.loc[~df_schedule['pool_date'].isna(), :].copy()
        future_dates = df_temp.loc[df_temp['pool_date'] > history_date, :].copy()  # 从上一个证券端兑付日开始截取, 找到未来第一期归集日, 如果目前没有存续期报告,则histroy_date等于证券利息起算日
        pass_wf = pass_wf or (len(future_dates) < 1)
        pred_start_period = future_dates.index[0] if len(future_dates) > 0 else max(df_temp.index)

    else:  # 此时的日历和现金流归集表都只保留trade_date以后的日期
        if len(df_prediction) > 0:

            df_prediction.loc[:, 'reinvest_income'] = 0.
            df_prediction = df_prediction.merge(df_schedule[['pool_date', 'all_date']], left_on='date_',
                                                right_on='pool_date', how='outer')

            df_prediction.sort_values(by='all_date', ignore_index=True, inplace=True)
            if len(df_schedule) != len(df_prediction):
                raise ValueError('日历中的归集日列与现金流归集表不一致')

            #  找到下一归集日在第几行(历史报告日的下一日，报告不是最新该日期也不是）,需要超过当前披露的支付日
            future_prediction = df_prediction.loc[~df_prediction['date_'].isna(), :]

            pred_start_period = future_prediction.loc[future_prediction['date_'] > history_date, 'date_'].index[0]\
                                if len(future_prediction) > 0 else max(df_schedule.index)
            pass_wf = pass_wf or (len(future_prediction) < 1)

            # 由于prediction经过了加压，所以将schedule中超过最后归集日的归集日进行处理
            df_schedule.loc[df_prediction['date_'].isna(), 'pool_status'] = 0
        else:
            pass_wf = True
            pred_start_period = 0

    # 3. 初始化各个 Tranche 对象
    tranches_obj = initial_tranche_object(df_schedule, tranches, product, product.interest_start_date,
                                          trade_after_payments, sec_start_periods,
                                          coupon_rate_change, trade_date, pred_start_period, df_plan, day_count_method,
                                          compensate)

    # 4. 现金流瀑布
    # 4.1 分配现金流
    if pass_wf:  # 现金流归集表已经是空的时，不再进行现金流分配,但是还是需要返还tranche实例,这样不会把应付未付的金额漏掉
        return tranches_obj, df_prediction, distribution_info

    not_minus_account = (is_revolving_pool and (not recal_cashflow)) #仅循环购买且不重新计算现金流时，循环期不扣账户余额
    if add_remain_account:
        prin_col = other.account_remain
    else:
        prin_col = 0

    if product.is_revolving_pool and recal_cashflow:
        wf = Waterfall_rev(tranches_obj=tranches_obj, schedule=df_schedule, payment_sequence=df_sequence,
                           trigger_events=trigger_events, trigger_rules=df_trigger, is_revolving=is_revolving_pool,
                           initial_principal=initial_principal, latest_principal=remaining_principal,
                           pred_start_date=pred_start_period, initial_date=initial_date, cumsum_loss=cum_loss,
                           history_date=history_date, exp_rate=exp_rate, tax_rate=tax_rate, pay_exp_tax=exist_tax_exp,
                           begin_default=default_principal, last_cdr=cdr,
                           begin_default_recover=begin_default_recover, prin_col=prin_col, int_col=0,
                           split_default=split_default)

        wf.get_data_params(CDRs=CDRs, PPs=PPs, RP=RP, CPRs=CPRs, RRs=RRs, YCDRs=YCDRs, YRs=YRs,
                           DRs=DRs, suoe=suoe, type_=type_, CPR_type=cpr_type)

        wf.run_all_periods(df_prediction, amortPeriodOnly=False)
        prediction = wf.prediction_after_rev()
    else:

        wf = Waterfall(tranches_obj=tranches_obj, schedule=df_schedule, payment_sequence=df_sequence,
                       trigger_events=trigger_events, trigger_rules=df_trigger, is_revolving=is_revolving_pool,
                       initial_principal=initial_principal, pred_start_date=pred_start_period,
                       history_date=history_date, exp_rate=exp_rate, tax_rate=tax_rate, pay_exp_tax=exist_tax_exp,
                       cumsum_loss=cum_loss, last_cdr=cdr, prin_col=prin_col, int_col=0)

        wf.run_all_periods(df_prediction, amortPeriodOnly=not_minus_account)

        prediction = wf.prediction_after()

    final_account = wf.account
    run_nodes_info = wf.success_nodes

    # 5. 补足证券端的不足金额.注意这一步跟前述分配是分离的,因此最后出来的pool_result跟security_result中的证券分配金额可能会有出入
    if compensate:

        final_account, excess_payment = tranches_obj.move_cash(final_account)

        # 如果有应付未付金额，检查一下次级的应付未付是否被错误的扣掉了
        for tr in tranches:
            level_ = tr.security_level
            tr_obj = getattr(tranches_obj, level_)
            trade_after_payment = trade_after_payments[level_]
            sec_start_period = sec_start_periods[level_]
            prin_payments = getattr(tr_obj, 'prin_payments')
            int_payments = getattr(tr_obj, 'int_payments')
            if not trade_after_payment:
                if not (prin_payments[sec_start_period] == tr.current_principal_due and
                        int_payments[sec_start_period] == tr.current_interest_due):
                    raise ValueError("证券现金流结果中，应付未付金额错误")

    if final_account > 0.1:
        distribution_info[df_product.loc[0, 'project_seq']].append('截止清算，账户仍有余额 ，请按照如下顺序检查（1）节点存在性或条件是否满足，'
                                                                   '（2）现金流归集表的支付日是否超出证券最晚支付日（可能是因为证券支付日与现金流归集表的支付日列不匹配导致），'
                                                                   '（3）支付顺序中次级是否有超额收益'
                                                                   '（4）次级是否维护了利息支付日')

    for tr in tranches:
        if tr.period_end_balance < 0.01:
            continue  # 如果已经完成偿还了则无所谓有没有节点

        seq_ = tr.security_seq
        code_ = tr.security_code
        level_ = tr.security_level
        if level_.lower() + '.prin' not in run_nodes_info.keys():
            info_ = f'证券 {code_} 本金节点不存在'  # 利息节点的存在问题不检查，这里只是用于在本金无法偿付完毕时进行原因定位用。
            distribution_info[seq_].append(info_)
        elif not run_nodes_info[level_.lower() + '.prin']:
            info_ = f"证券 {code_} 本金节点条件无法满足"  # 只检查本金节点就可以，因为一般只有本金节点有条件
            distribution_info[seq_].append(info_)

    del wf
    return tranches_obj, prediction, distribution_info


def initial_tranche_object(df_schedule, tranches, product, interest_start_date, trade_after_payments, sec_start_periods,
                           coupon_rate_change, trade_date, pred_start_period, df_plan, day_count_method, compensate):
    """
    该函数主要用于实例化所有证券, 函数输出的结果会用于现金流分配

    Args:
        df_schedule:
        tranches:
        product:
        interest_start_date (datetime.date): 计息起始日
        trade_after_payments (dict): 证券是否有应付未付利息 ，key-证券代码，value-bool, True表示没有应付未付本息
        sec_start_periods (dict): 证券上一计息日所在行，key-证券代码，value-int,在日期中的行数
        coupon_rate_change (float): 利差变动，一般是0，在压力场景下可以设置大于0用于压力测试。
        trade_date (datetime.date): 估值日
        pred_start_period (int): 下一归集日所在行，输出结果中各证券实例都需要处于这一期。
        df_plan:
        day_count_method (str) : 利息天数计算方式

    Returns:
        object:

    Note:
         不考虑历史期次中有未足额偿付的情况


    **逻辑**

    1. 先计算利息率 `coupon_rate` ， 用于计算利息，对于次级证券相当于期间收益：

            * 如果是固定利息，则利息率为固定利率加上 `coupon_rate_change`
            * 如果是浮动利息，则利息率为浮动利率基准 + 浮动利差 + `coupon_rate_change`

    2. 计算应计利息:

        * 历史计息期间截止日为下一周期起点，计算与估值日之间的年化天数（如果是算头不算尾的则估值日和历史计息区间截止日都算，算尾不算头的则只考虑估值日），
        乘以 `coupon_rate` 和报告期末剩余本金额作为应计利息.
        * 次级的应计利息统一设置为0,即认为次级没有应计利息.

    3. 初始化证券实例，非次级证券用 ``Tranche`` , 次级证券用 ``SubTranche``， 需要输入的参数中:

                * 当前剩余本金额在最新披露的一期证券应付本息已经支付时，为期末本金余额；未付时为期初本金余额。
                * `prin_due` 表示应付本金，在最新披露的一期证券应付本息已经支付时为0，未付时为报告披露的当期应付本金
                * `int_due` 表示应付利息，在最新披露的一期证券应付本息已经支付时为0，未付时为报告披露的当期应付利息
                * `last_period_end_date` 为最新披露的一期证券的计息截止日，不管是否实际已支付。其用于运行现金流瀑布时的利息计算。
                * 初始化时，对应应付未付的本息，会进行支付，默认能够足额支付并且不会扣除和影响后续现金流归集。

    4. 如果证券上一支付日所在行早于 `pred_start_period` , 通过各个实例的 `next_period` ， 将所有证券的当前日期都推到 `pred_start_period` 所在行。

    """

    tranches_obj = []
    for tr in tranches:
        subord = tr.security_level
        subLevel = 'sub' in subord
        prin_dates = df_schedule[subord + "_prin_date"].tolist()
        int_dates = df_schedule[subord + "_int_date"].tolist()
        int_pay_dates = df_schedule[subord + "_int_pay_date"].tolist()
        trade_after_payment = trade_after_payments[tr.security_level]
        sec_start_period = sec_start_periods[tr.security_level]
        coupon_rate = (tr.fixed_rate + coupon_rate_change) if tr.interest_type == "fixed" else \
            tr.floating_rate_benchmark + tr.floating_rate_spread + coupon_rate_change

        if tr.if_progressive_rate:
            arr_coupon = np.full(len(int_dates), float('nan'))
            change_points = tr.progressive_rates
            dates_ = change_points.split(";")[0].split(",")
            rates_ = change_points.split(";")[1].split(",")
            rates_ = [float(x) for x in rates_]
            i = 0
            j = 0
            while j < len(int_dates) and i < len(dates_):
                ed_dt = to_date2(dates_[i])
                rate_ = rates_[i]
                if str(int_dates[j]) == 'nan':
                    j += 1
                elif int_dates[j] <= ed_dt:
                    arr_coupon[j] = rate_
                    j += 1
                else:
                    i += 1
            arr_coupon = pd.Series(arr_coupon).ffill().values
        else:
            arr_coupon = np.full(len(int_dates), coupon_rate)

        int_due = tr.current_interest_due if not trade_after_payment else 0
        prin_due = tr.current_principal_due if not trade_after_payment else 0  # 当前已宣告未支付的本息
        remaining_balance = tr.period_end_balance if trade_after_payment else tr.period_begin_balance
        int_pay_with_prin = tr.int_pay_with_prin

        if day_count_method == 'begin':
            ai_count_way = 'bilateral'
        elif day_count_method == 'end':
            ai_count_way = 'end'
        else:
            raise ValueError(f"wrong interest count method {day_count_method}")

        if remaining_balance > 0.01:  # 没有本金了就不计算应计利息
            period_begin_date = tr.period_end_date if trade_after_payment else tr.period_begin_date # 如果没有存续期，读取的时候会用计息起始日填
            if period_begin_date < interest_start_date:
                ai = 0
            else:
                ai = count_year(period_begin_date, trade_date, tr.daycount, ai_count_way) * coupon_rate * remaining_balance
        else:
            ai = 0

        security_amort_plan = None
        if tr.amort_type in ["fixed", "target-balance"]:
            security_amort_plan = df_plan[df_plan["security_level"] == subord]
        if not subLevel:  # 不是次级

            tr_obj = Tranche(security_id=tr.security_code, subordination=subord, notional=tr.initial_principal,
                             remaining_balance=remaining_balance, coupon_rates=arr_coupon,
                             daycount=tr.daycount, current_vol=tr.current_vol,
                             initial_interest_date=interest_start_date,
                             last_period_num=sec_start_period, begin_period_num=pred_start_period,
                             last_period_end_date=tr.period_end_date, last_payment_date=tr.payment_date,
                             mat_date=tr.legal_maturity_date, legal_maturity=product.legal_due_date,
                             prin_dates=prin_dates, int_dates=int_dates, int_pay_dates=int_pay_dates,
                             amort_type=tr.amort_type, amort_plan=security_amort_plan,
                             trade_after_payment=trade_after_payment,
                             int_due=int_due, prin_due=prin_due, compensate=compensate,
                             int_pay_with_prin=int_pay_with_prin)
            tr_obj.ai = ai

        else:  # 次级
            rev_periods = df_schedule['is_revolving_period']
            tr_obj = SubTranche(security_id=tr.security_code, subordination=subord, notional=tr.initial_principal,
                                remaining_balance=remaining_balance, coupon_rates=arr_coupon,
                                fcc=tr.sub_fcc, daycount=tr.daycount,  current_vol=tr.current_vol,
                                initial_interest_date=product.interest_start_date,
                                last_period_num=sec_start_period, begin_period_num=pred_start_period,
                                last_period_end_date=tr.period_end_date, last_payment_date=tr.payment_date,
                                mat_date=tr.legal_maturity_date,
                                legal_maturity=product.legal_due_date,
                                prin_dates=prin_dates,
                                int_dates=int_dates, int_pay_dates=int_pay_dates, amort_type=tr.amort_type,
                                amort_plan=security_amort_plan,
                                rev_status=rev_periods, trade_after_payment=trade_after_payment,
                                prin_due=prin_due, int_due=int_due)
            tr_obj.ai = 0  # 次级没有应计利息
        tranches_obj.append(tr_obj)

    obj = TrancheCollect(*tranches_obj)  # 实例集合
    return obj

