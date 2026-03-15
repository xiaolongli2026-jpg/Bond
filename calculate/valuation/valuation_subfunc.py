"""
估值、衍生指标计算相关函数
"""

import numpy as np
from datetime import date
from scipy.optimize import newton


def price_by_curve(total_accrual_interest, payment_times, future_payments, amount, benchmark_rates, spread=0):
    """
    返回估值全价和净价（曲线+利差折现）

    Args:
        total_accrual_interest (float): 应计利息
        payment_times (np.array): 未来支付日与估值日之间的年化天数
        future_payments (np.array): 未来的现金流支付
        amount (int): 发行张数
        benchmark_rates (pd.Series): 基准折现曲线
        spread (float): 利差

    Returns:
        tuple: tuple contains:
        全价
        净价
    """

    benchmark_rates = 0 if benchmark_rates is None else benchmark_rates
    dirty_value = calc_pv(payment_times, future_payments, benchmark_rates, spread)
    clean_value = dirty_value - total_accrual_interest
    return dirty_value / amount, clean_value / amount


def price_by_ytm(total_accrual_interest, payment_times, future_payments, amount, ytm=0):  # valuation parameters
    """
    返回估值全价和净价（到期收益率折现）

    Args:
        total_accrual_interest (float): 应计利息
        payment_times (np.array): 未来支付日与估值日之间的年化天数
        future_payments (np.array): 未来的现金流支付
        amount (int): 发行张数
        ytm (float): 到期收益率

    Returns:
        tuple: tuple contains:
        全价
        净价
    """
    benchmark_curve = None

    return price_by_curve(total_accrual_interest, payment_times, future_payments, amount, benchmark_curve, spread=ytm)


def spread_by_price(total_accrual_interest, payment_times, future_payments, benchmark_rates, amount,
                    price, price_type="dirty_price"):  # valuation parameters
    """
    根据全价或净价、证券端现金流、基准即期利率曲线等，计算利差

    Args:
        total_accrual_interest (float): 应计利息
        payment_times (np.array): 未来支付日与估值日之间的年化天数
        future_payments (np.array): 未来的现金流支付
        benchmark_rates (pd.Series): 基准折现曲线
        amount (int): 发行张数
        price (float): 价格
        price_type (str): 'dirty_price'-价格是全价， 'clean_price'-价格是净价

    Returns:
        tuple: tuple contains:
            到期收益率， 全价， 净价
    """

    if price_type == "dirty_price":
        dirty_value = price * amount
    elif price_type == "clean_price":
        dirty_value = price * amount + total_accrual_interest
    else:
        raise ValueError("Unsupported price_type: " + price_type)

    spread = solve_spread(payment_times, future_payments, benchmark_rates, dirty_value)
    clean_value = dirty_value - total_accrual_interest
    return spread, dirty_value / amount, clean_value / amount


def ytm_by_price(total_accrual_interest, payment_times, future_payments, amount, price, price_type="dirty_price"):
    """

    根据全价或净价、证券端现金流, 计算到期收益率

    Args:
        total_accrual_interest (float): 应计利息
        payment_times (np.array): 未来支付日与估值日之间的年化天数
        future_payments (np.array): 未来的现金流支付
        amount (int): 发行张数
        price (float): 价格
        price_type (str): 'dirty_price'-价格是全价， 'clean_price'-价格是净价

    Returns:
        tuple: tuple contains:
        到期收益率， 全价， 净价
    """

    benchmark_curve = None

    return spread_by_price(total_accrual_interest, payment_times, future_payments, benchmark_curve, amount,
                           price, price_type)


def calc_pv(payment_times, payments, benchmark_rates, spread):
    """
    折现
    Args:
        payment_times (np.array): 未来支付日与估值日之间的年化天数
        payments (np.array): 未来的现金流支付总和
        benchmark_rates(pd.Series): 基准折现曲线
        spread (float): 利差

    Returns:
        float: 全价 * 张数


    公式 ::

        >> payments_pv = payments / (1. + benchmark_rates + spread) ** payment_times
        >> pv = np.sum(payments_pv)

    """
    payments_pv = payments / (1. + benchmark_rates + spread) ** payment_times
    pv = np.sum(payments_pv)
    return pv


def calc_duration(payment_times, payments, benchmark_rates, spread):
    """
    计算久期(修正)

    Args:
        payment_times (np.array): 未来支付日与估值日之间的年化天数
        payments (np.array): 未来的现金流支付总和
        benchmark_rates(pd.Series): 基准折现曲线
        spread (float): 利差

    Returns:
        float: 久期


    公式 ::

        >> payments_pv = payments / (1. + benchmark_rates + spread) ** payment_times  # 各期现金流的折现值
        >> pv = np.sum(payments_pv)  # 全价 * 张数
        >> duration = np.sum(payments_pv * payment_times / (1. + benchmark_rates + spread)) / pv  # 修正久期

    TODO: 未考虑浮动利率证券的久期和凸度重置频率和时间
    """

    payments_pv = payments / (1. + benchmark_rates + spread) ** payment_times
    pv = np.sum(payments_pv)
    if pv == 0:
        return float('nan')
    else:
        duration = np.sum(payments_pv * payment_times / (1. + benchmark_rates + spread)) / pv  # modified duration
        return duration


def calc_dollar_duration(payment_times, payments, benchmark_rates, spread):
    """
    计算美元久期

    Args:
        payment_times (np.array): 未来支付日与估值日之间的年化天数
        payments (np.array): 未来的现金流支付总和
        benchmark_rates(pd.Series): 基准折现曲线
        spread (float): 利差

    Returns:
        float: 美元久期


    公式 ::

        >> payments_pv = payments / (1. + benchmark_rates + spread) ** payment_times  # 各期现金流的折现值
        >> duration = np.sum(payments_pv * payment_times / (1. + benchmark_rates + spread))   # 久期

    """
    payments_pv = payments / (1. + benchmark_rates + spread) ** payment_times

    dollar_duration = np.sum(payments_pv * payment_times / (1. + benchmark_rates + spread))  # duration
    return dollar_duration


def calc_convexity(payment_times, payments, benchmark_rates, spread):
    """
    计算凸度

    Args:
        payment_times (np.array): 未来支付日与估值日之间的年化天数
        payments (np.array): 未来的现金流支付总和
        benchmark_rates(pd.Series): 基准折现曲线
        spread (float): 利差

    Returns:
        float: 凸度


    公式 ::

        >> payments_pv = payments / (1. + benchmark_rates + spread) ** payment_times  # 各期现金流的折现值
        >> pv = np.sum(payments_pv)  # 全价 * 张数
        >> convexity = np.sum(payments_pv * payment_times * (payment_times + 1.) / (1. + benchmark_rates + spread) ** 2) / pv # 凸度


    TODO: 未考虑浮动利率证券的久期和凸度重置频率和时间
    """

    payments_pv = payments / (1. + benchmark_rates + spread) ** payment_times
    pv = np.sum(payments_pv)
    if pv == 0:
        return float('nan')
    else:
        convexity = np.sum(payments_pv * payment_times * (payment_times + 1.) / (1. + benchmark_rates + spread) ** 2) / pv
        return convexity


def solve_spread(payment_times, payments, benchmark_rates, pv):
    """
    计算利差。也可以用于计算到期收益率，此时将基准曲线 `benchmark_rates` 的值设为 0

    Args:
        payment_times (np.array): 未来支付日与估值日之间的年化天数
        payments (np.array): 未来的现金流支付总和
        benchmark_rates (pd.Series): 基准折现曲线
        pv (float): 估值全价 * 张数

    Returns:
        float: 利差

    """
    try:

        def _func(x):
            return calc_pv(payment_times, payments, benchmark_rates, x) - pv

        def _fprime(x):
            return - calc_dollar_duration(payment_times, payments, benchmark_rates, x)

        spread = newton(func=_func, x0=0, fprime=_fprime)

    except RuntimeError as e:
        print('价格与现金流相差过大，无法迭代出利差或到期收益率')
        spread = float('nan')

    return spread


def calc_wal(payment_times, payments):
    """计算加权平均剩余期限，为未来现金流与期限的乘积和除以未来总支付"""
    sum_payments = np.sum(payments)
    if abs(sum_payments) < 0.01:
        wal = 0
    else:
        wal = np.sum(payment_times * payments) / sum_payments
    return wal


def calculate_coverage_ratio(dates, check_dates, inflows, tax_exp_outflows, interest_outflows,
                             security_balances, security_maturity_dates, security_flags):
    """

    计算每个证券的覆盖倍数。毛流入 / 毛流出
    假设有 n 个支付日，k 只证券, m 个归集日

    Args:
        dates (np.array): 支付日, (n,)
        check_dates (np.array): 归集日， (m,)
        inflows (np.array): 现金流入 (m,)
        tax_exp_outflows (np.array): 税费流出 (m,)
        interest_outflows (np.array):  所有证券总的利息流出（不包含次级） (n,)
        security_balances (np.array):  证券当前剩余本金 (k,)
        security_maturity_dates (np.array): 证券实际到期日 (k,)
        security_flags (np.array): 证券是否计算  (k,)，不计算的都是次级

    Returns:
        tuple: tuple contains:
               * coverage_ratios (np.array): 证券覆盖倍数
               * whole_coverage_ratio (float): 整体覆盖倍数

    **逻辑**

    1. 从最优先级证券开始，对每一只证券，截取归集日在上一等级证券实际到期日之后，该证券实际到期日（现金流分配后得到的最后一个支付日）之前的归集日的 '本期应收本金', '本期应收利息', '回收金额' 总和作为 `in_total`。
    2. 获取同一段区间内的税费支出总和、利息支出总和（不包含次级），和该支证券的当前的剩余本金额，加总作为 `out_total`
    3. 证券的覆盖倍数 = `in_total` / `out_total`
    4. 如果流出超过了流出，则计算 `reserve = in_total - out_total` , 计算下一支证券的覆盖倍数时，在 `in_total` 中加入 `reserve`
    5. 总体覆盖倍数 = 应收本金、应收利息、回收金额总和 / (税费支出、利息支出、非次级证券剩余本金总和）

    """

    dates = np.array(dates)
    check_dates = np.array(check_dates)
    inflows = np.array(inflows)
    tax_exp_outflows = np.array(tax_exp_outflows)
    interest_outflows = np.array(interest_outflows)
    coverage_ratios = []
    reserve = 0
    last_sec_mat_dt = date(2000, 1, 1)  # 上一优先级证券的预期到期日

    for bal, mat_dt, flag in zip(security_balances, security_maturity_dates, security_flags):
        ratio = None
        if flag:
            attr_to_sec = np.logical_and(dates > last_sec_mat_dt, dates <= mat_dt)
            attr_to_pool = np.logical_and(check_dates > last_sec_mat_dt, check_dates <= mat_dt)
            # 归属于该证券的现金流入
            in_total = reserve + sum(inflows[attr_to_pool])

            # 归属于该证券的税费支出
            tax_exp_attr_to_sec = sum(tax_exp_outflows[attr_to_pool])
            # 归属于该证券的利息支出
            int_attr_to_sec = sum(interest_outflows[attr_to_sec])
            # 总支出
            out_total = tax_exp_attr_to_sec + int_attr_to_sec + bal

            reserve = max(in_total - out_total, 0)
            if out_total > 1e-2:
                ratio = in_total / out_total

        coverage_ratios.append(ratio)

        last_sec_mat_dt = mat_dt

    whole_outflow = (
            sum(tax_exp_outflows) +
            sum(interest_outflows) +
            sum([bal for bal, flag in zip(security_balances, security_flags) if flag]))

    whole_coverage_ratio = None
    if whole_outflow > 1e-2:
        whole_coverage_ratio = sum(inflows) / whole_outflow

    return coverage_ratios, whole_coverage_ratio

