# -*- coding: utf-8 -*-


import numpy as np
from prepare.data_preprocess.prepcashflow_new import fill_balance
from .cashflowPressure import cashflow_pressure
from utils.timeutils import count_months


def cashflowPressure_npl(df_prediction, remain_principal, history_date, initial_date, initial_principal, collect_amount,
                         is_revolving_pool, RR=0, dpp=0, portion=0., recal_cashflow=True):
    """
    不良贷款类现金流加压，根据是否重新加压采用不同的计算方式

    Args:
        df_prediction (pd.DataFrame): 现金流归集表
        remain_principal (float): 当前资产池剩余本金额
        history_date (datetime.date): datetime.date, 上一归集日
        initial_date (datetime.date): 初始起算日
        initial_principal (float): 初始本金额
        is_revolving_pool (bool): 是否循环购买,
        RR (float): 回收率假设
        dpp (int): 回收时间延后几个月
        portion (float): 延后支付的比例
        recal_cashflow (bool): 是否重新用加压参数进行加压

    Returns:
        pd.DataFrame, df_prediction，不良贷款现金流归集表

    .. caution::
        目前没循环类的不良贷款, 所以计算中暂时没有考虑

    """

    if recal_cashflow:
        prediction = cashflowPressure_npl_recal(df_prediction, remain_principal, initial_principal, collect_amount,
                                                RR, dpp, portion)
    else:
        prediction = cashflowPressure_npl_notrecal(df_prediction, history_date, initial_date, initial_principal,
                                                   is_revolving_pool)

    return prediction


def cashflowPressure_npl_recal(df_prediction, remain_principal, initial_principal, collect_amount, RR,
                               dpp=0, portion=0.):
    """
    重新加压的模式下, 先将不良贷款的现金流恢复成100%再重新加压

    Args:
        df_prediction (pd.DataFrame): 原始现金流归集表
        remain_principal (float): 剩余本金额
        initial_principal (float): 入池本金额
        collect_amount (float): 当前回收的金额
        RR (float): 回收率
        dpp (int): 延迟回收月数
        portion (float): 延迟回收比例

    Returns:
        pd.DataFrame, df_prediction，不良贷款重新加压后的现金流归集表

    **逻辑**

    1. 重新加压现金流归集表

        * 资产池本金余额指的是资产池剩余的基础资产（不良贷款）金额，其中能够通过催收收回的就是现金流归集表的本金回收列，
        故本金回收列的总和实际上应小于剩余资产余额。 将本金回收列总和加总后除以当前资产池余额，可以视作现金流归集表中隐含的回收率假设。
        * 再将本金列除以这个回收率，得到的是模拟的、假设资产池的资产全部是正常贷款时，每期应有的本金回款。
        * 然后再根据假设的无条件回收率计算新的加压条件下的回收金额(用前面的到的无违约时回收本金额乘以 max(0, RR - collect_amount / initial_principal) * initial_principal / remain_principal )，
        得到的就是重新加压后的不良贷款的现金流归集表

    2. 如果存在延迟回收，则：

        * 将本金回收金额乘以延迟回收率的金额从本金回收列中扣除，并保存在 ``withhold`` 。
        * 计算各个收款期间的月数（采用四舍五入）
        * 根据延迟回收月数的假设，将 ``withhold`` 中的金额加到本金列对应的期次上，比如，从账龄是3的归集日抽出的延迟回收金额，在 ``dpp`` 为 3 时，需要加到账龄是6的归集日中

    Notes:
        * 如果延迟后超出了现金流归集表的期限，则这部分会被抛弃，比如最后一期现金流归集表在延迟后肯定超出了现金流归集表的回收期，此时这笔就不会再进行回收
        * 输入的 `RR` 是项目完整存续期间回收金额占入池金额的百分比，因此如果当前已经回收了一部分，则需要扣除后计算未来的预计金额

    """

    # 1 计算归集表和资产池剩余金额反应的回收率
    if remain_principal > 0:
        present_rr = df_prediction['current_principal_due'].sum() / remain_principal
        predict_rr = max((RR - collect_amount / initial_principal), 0) * initial_principal / remain_principal
        # 2 将当期应收本金还原成100%回收
        prediction = df_prediction.copy()
        prediction.loc[:, ['current_principal_due', 'current_interest_due']] = \
            prediction[['current_principal_due', 'current_interest_due']] / present_rr * predict_rr
        # 2.2 延迟回收
        if dpp > 0:
            withhold = prediction['current_principal_due'] * portion  # 不良贷款一般都没有利息，只有本金列，因此不用管利息的延迟回收
            prediction.loc[:, 'current_principal_due'] = \
                prediction['current_principal_due'] * (1 - portion)
            prediction.loc[:, 'age'] = count_months(prediction.loc[0, 'date_'], prediction['date_'])
            prediction.loc[:, 'age'] = np.round(prediction['age'])
            shift_to_ages = prediction['age'] + dpp
            n = len(prediction)
            i = 0
            j = 0
            while i < n and j < n:
                if shift_to_ages[i] > prediction.loc[j, 'age']:
                    j += 1
                else:
                    prediction.loc[j, 'current_principal_due'] += withhold[i]
                    i += 1
            prediction.drop(columns=['age'], inplace=True)
        # 3 重新根据当期回收计算期初期末本金余额
        prediction.loc[:, ['begin_principal_balance', 'end_principal_balance']] = float('nan')
        prediction = fill_balance(prediction)  # 此时返回的期初期末本金不包含回收不了的
        diff = remain_principal - prediction.loc[0, 'begin_principal_balance']
        prediction.loc[:, ['begin_principal_balance', 'end_principal_balance']] =\
            prediction[['begin_principal_balance', 'end_principal_balance']] + diff
        prediction.loc[:, ['prepay_amount', 'default_amount', 'recycle_amount',
                           'begin_default_balance', 'end_default_balance']] = 0  # 对于不良贷款来说，不再存在违约、违约回收、早偿的概念
        return prediction
    else:
        print('剩余本金为0，不对不良贷款现金流归集表加压')
        df_prediction.loc[:, ['prepay_amount', 'default_amount', 'recycle_amount',
                              'begin_default_balance', 'end_default_balance']] = 0
        return df_prediction


def cashflowPressure_npl_notrecal(df_prediction, history_date, initial_date, initial_principal, is_revolving_pool):
    """
    不良贷款ABS不重新计算现金流时, 用零参数使用加压模块, 此时仅是为了将现金流的格式标准化，实际上没有进行任何加压

    """
    prediction, assumptions = cashflow_pressure(df_prediction=df_prediction, history_date=history_date,
                                                initial_date=initial_date, initial_principal=initial_principal,
                                                is_revolving_pool=is_revolving_pool, CDR=0, CPR=0, RR=0, DPP=0,
                                                suoe=True, realized_CDR=0., current_default=0.,
                                                minus_CDR=False, begin_default_recover=False,
                                                split_default=False, cpr_type='type1')
    return prediction