# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

from utils.timeutils import to_date2, age_, dates_compare, holiday_adjsut
from utils.miscutils import isnull


def any_balance_null(cashflow):
    """
    检查现金流表是否有任一期的初/期末本金为空

    Args:
        cashflow (pd.DataFrame): 现金流归集表

    Returns:
        bool: 是否存在初/期末本金为空为空
    """
    return pd.isna(cashflow[['begin_principal_balance', 'end_principal_balance']]).any().any()


def fill_balance(cashflow):
    """
    当期初期末本金为空而当期本金回收不为空时, 对已经按日期排列好的现金流表，填充期初期末本金。即通过本金回收累加的方式填充期初期末本金余额，原先即使有维护部分期初期末本金，也会舍弃。

    Args:
        cashflow (pd.DataFrame): 现金流归集表

    Returns:
        pd.DataFrame: 期初本金、期末本金、当期本金回收都完整的现金流归集表
    """
    cashflow_new = cashflow.copy()
    if not pd.isna(cashflow_new[['current_principal_due', 'begin_principal_balance']]).any().any():
        cashflow_new.loc[:, 'end_principal_balance'] = cashflow_new['begin_principal_balance'] - \
                                                       cashflow_new['current_principal_due']

    elif not pd.isna(cashflow_new[['current_principal_due', 'end_principal_balance']]).any().any():
        cashflow_new.loc[:, 'begin_principal_balance'] = cashflow_new['end_principal_balance'] + \
                                                         cashflow_new['current_principal_due']

    else:
        cashflow_new.fillna(0., inplace=True)
        prin_reverse = cashflow_new.loc[:, 'current_principal_due'].values[::-1]
        begin_prin_bal = np.cumsum(prin_reverse)[::-1]
        cashflow_new.loc[:, 'begin_principal_balance'] = begin_prin_bal
        cashflow_new.loc[:, 'end_principal_balance'] = np.append(begin_prin_bal[1:], 0.)
    return cashflow_new


def fill_current_due(cashflow):
    """当当期应收本金为空时，用期初本金减去期末本金填补当期应收本金

    """
    cashflow['begin_principal_balance'] = \
        cashflow['begin_principal_balance'].fillna(cashflow['end_principal_balance'].shift(1))

    cashflow['end_principal_balance'] = \
        cashflow['end_principal_balance'].fillna(cashflow['begin_principal_balance'].shift(-1))

    cashflow.loc[:, 'current_principal_due'] = \
        cashflow['begin_principal_balance'] - cashflow['end_principal_balance']

    return cashflow


def fill_rev_balance(cashflow, static_begin_balance):
    """令循环期的本金回收和利息回收均为0，并且循环期的期初期末本金余额等于摊还期的首期期初本金余额，由于循环期不参与分配，故该简化处理不会有影响"""
    cashflow.loc[:, ['current_principal_due', 'current_interest_due']] = 0
    cashflow.loc[:, ['begin_principal_balance', 'end_principal_balance']] = static_begin_balance
    return cashflow


def transform_cashflow(cashflow, initial_date, rev_end_date, is_rev, workday_schedule=None):
    """

    处理现金流归集表

    Args:
        cashflow (pd.DataFrame): 现金流表
        initial_date (datetime.date): 维护的初始起算日
        rev_end_date (datetime.date): 循环购买结束日
        is_rev (bool): 是否循环购买
        workday_schedule:  工作日日历

    **逻辑**
        1. 处理现金流归集表（除需要重新测算循环购买现金流归集表的情况），对期初本金、当期回收本金、期末本金进行互相补充
        2. 选择用支付日还是归集日：如果 `pay_date` 列不为空，则根据归集日排序后，将归集日向后填充，然后将支付日相同的加总，并用支付日列替换原来的归集日列。最终形成列 'date_'
        3.  首次归集日如晚于给出的初始起算日，则将前面几期的现金流加总

    """

    # 1. 处理支付日列
    cashflow = fill_cashflow(cashflow, rev_end_date, is_rev)
    use_pay_dates = not pd.isna(cashflow['pay_date']).all()
    if not use_pay_dates:
        cashflow.loc[:, 'pay_date'] = cashflow['pool_date']

    cashflow_1 = cashflow.copy()
    cashflow_1.loc[:, 'pay_date'] = cashflow_1['pay_date'].bfill().ffill()
    cashflow_1.loc[:, 'pay_date'] = cashflow_1['pay_date'].apply(to_date2)

    if workday_schedule is not None:
        before_ = np.array(list(set(cashflow_1['pay_date'])))
        before_.sort()
        after_ = holiday_adjsut(date_series=before_, schedule=workday_schedule, holiday_rule='forward')
        dict_ = dict(zip(before_, after_))
        cashflow_1.loc[:, 'pay_date'] = cashflow_1['pay_date'].apply(lambda x: dict_[x])

    cashflow_1.loc[:, 'date_'] = cashflow_1['pay_date']

    cashflow_1 = combine_cashflow2(cashflow_1)
    cashflow = cashflow_1.copy()
    cashflow.sort_values(by=['date_'], inplace=True)

    # 2. 数据格式转化
    cashflow = cashflow.applymap(lambda x: x if not isnull(x) else float('nan'))
    cashflow['date_'] = cashflow.loc[:, 'date_'].apply(to_date2)

    # # 3. 首次归集日/支付日如晚于给出的初始起算日，则将前面几期的现金流加总
    if len(cashflow) > 0:
        if cashflow['date_'].min() <= initial_date:
            rows_ = cashflow['date_'] <= initial_date
            begin_balance = cashflow.loc[rows_, 'begin_principal_balance'].max()
            first_check_date_index = cashflow.loc[~rows_, :].index[0]
            cashflow.loc[first_check_date_index, 'current_principal_due'] = \
                cashflow.loc[first_check_date_index, 'current_principal_due'] +\
                cashflow.loc[rows_, 'current_principal_due'].sum()
            cashflow.loc[first_check_date_index, 'current_interest_due'] = \
                cashflow.loc[first_check_date_index, 'current_interest_due'] + \
                cashflow.loc[rows_, 'current_interest_due'].sum()
            cashflow.loc[first_check_date_index, 'begin_principal_balance'] = begin_balance
            cashflow.drop(index=cashflow.loc[rows_, :].index, inplace=True)

    cashflow.index = range(len(cashflow))
    cashflow.loc[:, 'is_revolving_period'] = [1 if x <= rev_end_date else 0 for x in
                                                   cashflow['date_']] if is_rev else [0] * len(cashflow)  # 加入是否循环期的标志
    return cashflow


def combine_cashflow(cashflow, initial_date):
    """
    (用于参数预测模型)将支付频率高于月度的现金流归集表转化为月度归集，通过计算整数的账龄，然后将四舍五入的距离初始起始日的月数相同的行加总。

    Args:
        cashflow (pd.DataFrame): 现金流表
        initial_date (datetime.date): 维护的初始起算日

    Returns:
        月频的现金流归集表
    """

    cashflow.loc[:, 'age'] = cashflow['date_'].apply(lambda x: age_(initial_date, x))
    after = \
        cashflow.groupby('age')[['current_principal_due', 'current_interest_due']].sum()
    after.reset_index(drop=False, inplace=True)
    after['date_'] = cashflow.groupby('age')['date_'].max().values
    after['begin_principal_balance'] = cashflow.groupby('age')['begin_principal_balance'].max().values
    after['end_principal_balance'] = cashflow.groupby('age')['end_principal_balance'].min().values
    after.reset_index(drop=True, inplace=True)
    return after


def combine_cashflow2(cashflow):
    """
    将多行对应一个支付日的情况加总成一行

    Args:
        cashflow (pd.DataFrame): 现金流表

    """

    after = \
        cashflow.groupby('date_')[['current_principal_due', 'current_interest_due']].sum()
    after.reset_index(drop=False, inplace=True)
    after['date_'] = cashflow.groupby('date_')['date_'].max().values
    after['begin_principal_balance'] = cashflow.groupby('date_')['begin_principal_balance'].max().values
    after['end_principal_balance'] = cashflow.groupby('date_')['end_principal_balance'].min().values
    after.reset_index(drop=True, inplace=True)
    return after


def tranfer_prediction_to_initial_freq(initial_prediction, after_prediction):
    """
    (用于参数预测模型) 将 ``combine_cashflow`` 中压缩过的现金流归集日恢复成原来的个数。

    Args:
        initial_prediction (pd.DataFrame): 原始的现金流归集表
        after_prediction (pd.DataFrame): 用预测参数加压后的现金流归集表

    Returns:
        pd.DataFrame: 现金流归集表：归集日与 `initial_prediction` 一样，现金流与 `after_prediction` 的一样
    """
    if len(initial_prediction) == len(after_prediction):
        return after_prediction
    else:  # 简化处理

        final_prediction = after_prediction.merge(initial_prediction[['date_']], how='outer', on=['date_'])
        fillna_cols = ['current_principal_due', 'current_interest_due', 'prepay_amount',
                       'default_amount', 'recycle_amount']
        final_prediction.loc[:, fillna_cols] = final_prediction[fillna_cols].fillna(0.)
        final_prediction.ffill(inplace=True)
        return final_prediction


def fill_cashflow(cashflow, rev_end_date, if_rev):
    """
    对期初本金、当期回收本金、期末本金进行互相补充

    Args:
        cashflow:
        rev_end_date (datetime.date): 循环购买结束日
        if_rev (bool): 是否循环购买

    Returns:
        pd.DataFrame: 没有空值的现金流归集表

    将现金流归集表分成循环期和摊还期两部分:

            * 对于摊还期:

                - 如果本金回收和为 0 ，则用 ``fill_current_due`` ，根据初期期末本金余额对当期应收本金进行补充。如果期初期末本金存在空值，则用 ``fill_balance`` 以cumsum的方式重新计算所有期初和期末本金余额。
                - 另外，检查期初期末本金是否与当期回收本金匹配（也可以是本息和，为本息和模式时利息列为0，但是期初期末本金和本金回收列仍需要匹配），通过计算期初本金和期末本金的差的和，与当期本金回收和的差距判断（误差为10）。

            * 对于循环期:

                * 用 ``fill_rev_balance`` 填充循环期的现金流归集表，循环期的现金流入全部设置为0。注意：填补后，循环期的余额等于摊还期首期余额，因此当仍处于循环期时，
                在 ``update_prediction`` 中调整现金流余额时，会使得摊还期的本金流入总和等于当期（循环期）的本金余额。

    """

    cashflow.loc[:, ['current_principal_due', 'current_interest_due']] = \
        cashflow[['current_principal_due', 'current_interest_due']].fillna(0.)
    cashflow = cashflow.sort_values(by='pool_date', ascending=True)
    # 去掉无效行
    cashflow = cashflow.dropna(subset=['pool_date'], how='any')

    if if_rev:
        cashflow_rev = cashflow.loc[cashflow['pool_date'] <= rev_end_date, :]  # 因为循环期的特殊性，将循环期和摊还期分开处理
        cashflow_static = cashflow.loc[cashflow['pool_date'] > rev_end_date, :]
    else:
        cashflow_static = cashflow.copy()
        cashflow_rev = pd.DataFrame(columns=cashflow.columns)

    # 2 非循环购买/循环购买摊还期处理
    if len(cashflow_static) > 0:
        if (pd.isna(cashflow_static['current_principal_due']).all() or
                cashflow_static['current_principal_due'].sum() < 0.01) and \
                (not pd.isna(cashflow_static[['begin_principal_balance', 'end_principal_balance']]).all().all()): # 补充的前提是期初期末数据存在

            # 2.1 摊还期的当期应付本金为空时，用期初期末本金计算填充
            cashflow_static = fill_current_due(cashflow_static)

        elif pd.isna(cashflow_static[['begin_principal_balance', 'end_principal_balance']]).any().any():
            # 2.2 反之，摊还期的期初期末本金为空时，则用各期摊还进行
            cashflow_static = fill_balance(cashflow_static)

        # 3. 循环期数据的填充，对于不重新计算现金流的循环购买类ABS,直接分配摊还期，虽然也补充循环期数据，但是实际上不会用这里的本金、利息进行分配
        static_begin_prin = max(cashflow_static['begin_principal_balance'])
        if len(cashflow_rev) > 0:
            cashflow_rev = fill_rev_balance(cashflow_rev, static_begin_prin)

    else:
        if len(cashflow_rev) > 0:
            cashflow_rev = fill_rev_balance(cashflow_rev, 0)

    cashflow = cashflow_rev.append(cashflow_static).fillna(0.)
    cashflow = cashflow.sort_values(by='pool_date', ascending=True)
    cashflow.reset_index(drop=True, inplace=True)
    return cashflow