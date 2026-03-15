# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from collections import namedtuple
from utils.miscutils import ifnull
from utils.timeutils import to_date2
from calculate.valuation.valuation_subfunc import (price_by_curve, price_by_ytm, ytm_by_price, spread_by_price,
                               calc_wal, solve_spread, calc_duration, calc_convexity, calculate_coverage_ratio)
from utils.timeutils import count_year


def security_valuation(trade_date, tranche_obj, input_type,
                       input_value, value_method, curve=None):
    """
    计算单个证券的估值和衍生指标

    Args:
        trade_date (datetime.date): 估值日
        tranche_obj (class): ``Tranch`` 形成的证券的对象 ( ``calculate_abs`` 的输出结果）
        input_type (str): 输入的是到期收益率、利差、全价还是净价, 决定了要计算估值还是反算收益率
        input_value (float): input_type对应的值
        value_method (str): 估值方式， curve-曲线折现, yield_-到期收益率折现
        curve (pd.series): 基准即期利率曲线, index-年, value-即期利率

    Returns:
         dict: 证券估值及衍生指标


    **逻辑**

    1. 折现:

        * 如果是非次级，则对本金加利息进行折现。选择其中计息日超过估值日的进行折现，因此如果收益分配报告披露滞后，则会与实际的剩余本金总和有差异
        * 如果是次级，对本金、利息、超额收益、固定资金成本总和进行折现。

    2. 根据 `input_type` 选择不同计算方式：

            * 'yield_' ：表示用到期收益率折现( ``price_by_ytm`` )
            * 'spread' ： 表示用收益率曲线 + 固定利差折现 （ ``price_by_curve`` )
            * 'dirty_price' : 表示输入的是全价，需要计算对应的利差(如果 `value_method` 为 'curve') 或者到期收益率 (如果 `value_method` 为 'yield_') （ 通过 ``spread_by_price`` ）

    3. 如果需要计算衍生指标，则用 ``derivatives`` 计算。

    TODO:
        由于缺少支付日数据，只有计息日，所以选择计息日超过估值日的现金流。实际应该用支付日。
    """
    tr = tranche_obj
    trade_date = to_date2(trade_date)

    payment_dates = np.array([ifnull(p, i) for p, i in zip(tr.prin_dates, tr.int_dates)])
    date_nonnull = ~pd.isna(payment_dates)
    payment_dates = payment_dates[date_nonnull]  # 付息利息

    if 'sub' in tr.subordination.lower():
        int_payments = np.array(tr.int_payments) + np.array(tr.exret_payments) + np.array(tr.fcc_payments)
    else:
        int_payments = np.array(tr.int_payments)
    int_payments = int_payments[date_nonnull]
    prin_payments = np.array(tr.prin_payments)[date_nonnull]

    # 如果证券端报告更新不及时，则现金流中会存在早于估值日的计息日，此时需要从中剔除
    payment_times = np.array([count_year(trade_date, dt, tr.daycount) for dt in payment_dates])
    future = (payment_times > 0)
    future_payment_times = payment_times[future]
    future_int_payments = int_payments[future]
    future_prin_payments = prin_payments[future]
    future_payments = future_int_payments + future_prin_payments

    derivatives_result = {}
    valuation_result = {}
    benchmark_rates = None
    spread = None
    ytm = None
    # 估值或反算收益率
    if input_type == 'yield_':
        # 用到期收益率估值
        ytm = input_value * 0.01  # 输入yield是%

        dirty_price, clean_price = price_by_ytm(total_accrual_interest=tr.ai,
                                                payment_times=future_payment_times,
                                                future_payments=future_payments,
                                                amount=tr.current_vol,
                                                ytm=ytm)
    elif input_type == 'spread':
        # todo 直接四舍五入取值
        if curve is not None:
            benchmark_rates = np.array([curve[round(dt, 2)] for dt in future_payment_times])
        else:
            benchmark_rates = 0.

        spread = input_value * 0.0001  # 输入spread是BP
        dirty_price, clean_price = price_by_curve(total_accrual_interest=tr.ai,
                                                   payment_times=future_payment_times,
                                                   future_payments=future_payments,
                                                   amount=tr.current_vol,
                                                   benchmark_rates=benchmark_rates, spread=spread)
    else:  # input_type is 'dirty_price' or 'clean_price'
        # dict_ = {"dirty_price": "dirty", "clean_price": "clean"}
        # price_type = dict_[input_type]  # dirty or clean
        price = input_value
        if value_method == 'yield_':
            ytm, dirty_price, clean_price = ytm_by_price(total_accrual_interest=tr.ai, payment_times=future_payment_times,
                                                         future_payments=future_payments,
                                                         amount=tr.current_vol,
                                                         price=price, price_type=input_type)

            derivatives_result['ytm'] = ytm * 100
        elif value_method == 'curve':
            if curve is not None:
                benchmark_rates = np.array([curve[round(dt, 2)] for dt in future_payment_times])
            else:
                benchmark_rates = 0.

            spread, dirty_price, clean_price = spread_by_price(total_accrual_interest=tr.ai,
                                                               payment_times=future_payment_times,
                                                               future_payments=future_payments,
                                                               benchmark_rates=benchmark_rates,
                                                               amount=tr.current_vol,
                                                               price=price, price_type=input_type)
            derivatives_result['spread'] = spread * 10000
        else:
            raise ValueError(f"input_type '{input_type}' and value_method '{value_method}' do not match")

    # 计算衍生指标

    derivatives_result = derivatives(tr.begin_balance, tr.ai, dirty_price, tr.current_vol,
             future_payment_times, future_payments, spread, ytm, benchmark_rates=benchmark_rates)
    derivatives_result = dict(derivatives_result._asdict())

    valuation_result['price'] = dirty_price
    valuation_result['clean_price'] = clean_price
    valuation_result['accrual_interest'] = derivatives_result['accrual_interest']
    valuation_result['security_code'] = tr.security_id

    derivatives_result['security_code'] = tr.security_id
    derivatives_result['final_balance'] = tr.balance
    derivatives_result['cal_expiry_date'] = (tr.actual_expiry_date).strftime('%Y%m%d')
    derivatives_result['date_shift'] = (tr.actual_expiry_date - tr.expect_expiry_date).days
    derivatives_result['security_level'] = tr.subordination
    derivatives_result['cash_per_sec'] = sum(future_payments) / tr.current_vol
    derivatives_result['ratio_cash_face'] = \
        derivatives_result['cash_per_sec'] / derivatives_result['remaining_face_value'] \
        if derivatives_result['remaining_face_value'] > 0 else float('nan')

    return valuation_result, derivatives_result


def project_valuation(trade_date, tranche_obj_list, pool_result, input_types,
                      input_values, value_methods, curves=None):
    """
    计算项目下所有证券的估值和衍生指标。对所有证券逐个使用 ``security_valuation`` 计算估值和衍生指标，同时用 ``cal_coverage`` 计算证券覆盖倍数和整体覆盖倍数

    Args:
        trade_date: datetime.date, 估值日
        tranche_obj_list: object -> list, tranche.py形成的各证券的对象
        pool_result: pd.DataFrame, 资产池现金流入
        input_types: dict, key-证券代码，value-意义同security_valuation
        input_values: dict, key-证券代码，value-意义同security_valuation
        value_methods: dict, key-证券代码，value-意义同security_valuation
        curves: dict, key-证券代码，value-意义同security_valuation

    Returns:
        pd.DataFrame :估值结果和衍生指标

    """

    coverage_ratios, whole_coverage_ratio = cal_coverage(pool_result, tranche_obj_list)

    derivatives_result_list = []
    valuation_result_list = []
    for level_ in tranche_obj_list.all_levels:
        tr = getattr(tranche_obj_list, level_)

        security_code = tr.security_id
        input_type = input_types[security_code]
        input_value = input_values[security_code]
        value_method = value_methods[security_code]
        curve = curves[security_code]
        valuation_result, derivatives_result = security_valuation(trade_date, tr, input_type, input_value,
                                                                  value_method, curve)

        derivatives_result['coverage_ratio'] = coverage_ratios[security_code]
        derivatives_result["whole_coverage"] = whole_coverage_ratio
        derivatives_result_list.append(derivatives_result)
        valuation_result_list.append(valuation_result)

    valuation_result = pd.DataFrame(valuation_result_list)
    derivatives_result = pd.DataFrame(derivatives_result_list)
    return valuation_result, derivatives_result


def derivatives(balance, total_accrual_interest, dirty_price, amount,
                future_payment_times, future_payments, spread, ytm, benchmark_rates=None,):
    """
    计算单个证券的衍生指标

    Args:
        balance (float): 当前剩余本金 （如果有应付未付本息，则为不扣除应付本金）
        total_accrual_interest (float): 应计利息
        dirty_price (float): 估值全价
        amount (int): 发行张数
        future_payment_times (np.array): 未来支付日与估值日之间的年化天数
        future_payments (np.array): 未来的现金流支付
        spread (float): 利差
        ytm (float): 到期收益率
        benchmark_rates (pd.Series): 基准折现曲线

    Returns:
        tuple: 衍生指标


    **公式**

    1. 剩余票面 = `balance` / `amount`
    2. 单位应计利息 = `total_accrual_interest` / `amount`
    3. 加权平均剩余期限 = np.sum(future_payment_times * future_payments) / np.sum(payments)
    4. 久期通过 ``calc_duration`` 计算
    5. 凸度通过 ``calc_convexity`` 计算
    6. 如果是通过曲线折现的方式，通过 ``solve_spread`` 计算到期收益率。输出结果中，到期收益率单位为 %， 利差单位为 BP
    """

    if benchmark_rates is None:
        benchmark_rates = 0

    remaining_face_value = balance / amount
    accrual_interest = total_accrual_interest / amount
    wal = calc_wal(future_payment_times, future_payments)
    if ytm is not None:
        benchmark_rates = 0
        spread = ytm

    duration = calc_duration(future_payment_times, future_payments, benchmark_rates, spread)
    convexity = calc_convexity(future_payment_times, future_payments, benchmark_rates, spread)
    pv_face = dirty_price / remaining_face_value if remaining_face_value > 0 else float('nan')

    if benchmark_rates is not None:
        ytm = solve_spread(future_payment_times, future_payments, 0, dirty_price*amount)
    else:
        ytm = spread
        spread = None
    ytm = ytm * 100 if ytm is not None else None
    spread = spread * 10000 if spread is not None else None
    PriceResult = namedtuple(
        "PriceResult",
        "remaining_face_value accrual_interest yield_ spread duration wal convexity pv_face")
    return PriceResult(remaining_face_value, accrual_interest, ytm, spread, duration, wal,
                       convexity, pv_face)


def cal_coverage(pool_result, tranches_obj):
    """
    计算证券覆盖倍数（次级除外）和整体覆盖倍数,  计算公式见 ``calculate_coverage_ratio``

    Args:
        pool_result (pd.DataFrame): 资产池现金流入
        tranche_obj (class): ``Tranche`` 形成的各证券的对象

    Returns:
        tuple: tuple contains:
            * coverage_ratios (np.array): 证券覆盖倍数
            * whole_coverage_ratio (float): 总覆盖倍数

    """


    tranche_obj_list = [getattr(tranches_obj, level_) for level_ in tranches_obj.all_levels]

    if len(pool_result) < 1:
        coverage_ratios = dict(zip([tr.security_id for tr in tranche_obj_list], [float('nan')] * len(tranche_obj_list)))
        whole_coverage_ratio = float('nan')
    else:

        security_codes = [tr.security_id for tr in tranche_obj_list]
        security_balances = [tr.begin_balance for tr in tranche_obj_list]
        security_maturity_dates = [tr.actual_expiry_date for tr in tranche_obj_list]
        cal_coverage = ['sub' not in tr.subordination.lower() for tr in tranche_obj_list]

        # 覆盖倍数
        coverage_cal_outflows_list = [tr.int_payments for tr in tranche_obj_list if not tr.is_sub] # 不考虑次级的利息
        coverage_sec_dates_list = [tr.int_dates for tr in tranche_obj_list] + [tr.prin_dates for tr in tranche_obj_list]
        coverage_sec_dates = coverage_sec_dates_list[0]
        for i in range(1, len(security_codes)):
            coverage_sec_dates = [ifnull(x, y) for (x, y) in zip(coverage_sec_dates, coverage_sec_dates_list[i])]

        coverage_sec_dates = pd.Series(coverage_sec_dates).ffill().bfill().values

        pool_copy = pool_result[['CHECK_DATE', 'CURRENT_PRINCIPAL_DUE', 'CURRENT_INTEREST_DUE', 'RECYCLE_AMOUNT', 'TAX_PAID', 'EXP_PAID']].copy()
        pool_copy.dropna(subset=['CHECK_DATE'], axis=0, inplace=True)
        coverage_pool_dates = [to_date2(x) for x in pool_copy['CHECK_DATE']]
        coverage_cal_inflows = pool_copy[['CURRENT_PRINCIPAL_DUE', 'CURRENT_INTEREST_DUE', 'RECYCLE_AMOUNT']].sum(axis=1)
        coverage_cal_taxexp = pool_copy[['TAX_PAID', 'EXP_PAID']].sum(axis=1)
        coverage_cal_outflows = list(pd.DataFrame(coverage_cal_outflows_list).fillna(0.).sum(axis=0).values)

        coverage_ratios, whole_coverage_ratio = calculate_coverage_ratio(
            dates=coverage_sec_dates,
            check_dates=coverage_pool_dates,
            inflows=coverage_cal_inflows,
            tax_exp_outflows=coverage_cal_taxexp,
            interest_outflows=coverage_cal_outflows,
            security_balances=security_balances,
            security_maturity_dates=security_maturity_dates,
            security_flags=cal_coverage)

        coverage_ratios = dict(zip([tr.security_id for tr in tranche_obj_list], coverage_ratios))

    return coverage_ratios, whole_coverage_ratio

