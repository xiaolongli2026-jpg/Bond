# -*- coding: utf-8 -*-
"""
现金流加压, 适用于:

        * `static_normal1` 现金流计算器-非循环购买-非不良贷款-数值模式1
        * `static_normal2` 现金流计算器-非循环购买-非不良贷款-数值模式2
        * `series_normal1` 现金流计算器-非循环购买-非不良贷款-序列模式1
        * `series_normal2` 现金流计算器-非循环购买-非不良贷款-序列模式2

"""

import numpy as np
import pandas as pd

from .loanpool import LoanPool
from utils.timeutils import to_date2


def cashflow_pressure(df_prediction, history_date, initial_date, initial_principal, is_revolving_pool,
                      CDR, DP, CPR, RR, DPP, suoe, realized_CDR, current_default, minus_CDR,
                      begin_default_recover, split_default, cpr_type):
    """
    适用于:  `static_normal1` 现金流计算器-非循环购买-非不良贷款-数值模式1

    Args:
        df_prediction (pd.DataFrame): 未加压现金流归集表
        history_date (datetime.date): 上一归集日
        initial_date (datetime.date): 初始起算日
        initial_principal (float): 初始本金额
        is_revolving_pool (bool): 是否循环购买
        CDR (float): 违约率，无单位
        CPR (float): 早偿率。无单位
        RR (float): 违约回收率，无单位
        DPP (int): 违约回收期，单位：月
        suoe (bool): 是否用缩额法 15+2+3
        realized_CDR (float): 已发生的累计违约率
        current_default (float): 当期剩余违约本金
        minus_CDR (bool): 是否从输入的 ``CDR`` 中扣除当期已发生的累计违约率
        begin_default_recover (bool): 是否对当前剩余违约金额计算违约回收
        split_default (bool): 是否从最新剩余本金中扣除当前剩余违约金额
        cpr_type (str): 早偿金额的算法

    Returns:
        tuple: tuple contains:
                     prediction (pd.DataFrame): 加压后的现金流归集表 \n
                     assumptions (pd.DataFrame): 匹配到每一期的参数假设
    **逻辑**

    1. 非循环购买:

        .. code-block:: python
            :linenos:

            # 直接调用加压模块
            loanpool = LoanPool(pool_start_date=history_date, original=df_prediction,
                                initial_principal=initial_principal, initial_date=initial_date,
                                begin_default=current_default, pool_start_CDR=realized_CDR,
                                begin_default_recover=begin_default_recover, split_default=split_default)
            prediction, assumptions = loanpool.scenario_analysis(CPR, CDR, RR, DPP, well_distributed=False, suoe=suoe,
                                                                 minus_CDR=minus_CDR, CPR_type='type1')


    2. 循环购买:

        根据日历中对循环期、摊还期的判断，将现金流归集表中摊还期内的行提取出来，替代 ``df_prediction`` 输入到加压模块中。即仅对于摊还期加压，不对循环期加压


    .. caution:: well_distributed 设置为默认参数 False，即违约分布跟本金回收分布一样，而不是仅跟间隔期限有关

    """
    assumptions = pd.DataFrame()
    history_date = to_date2(history_date)
    prediction_static = df_prediction[df_prediction['is_revolving_period'] == 0].reset_index(drop=True)
    if len(prediction_static) > 0:
        loanpool = LoanPool(pool_start_date=history_date, original=prediction_static,
                            initial_principal=initial_principal, initial_date=initial_date,
                            begin_default=current_default, pool_start_CDR=realized_CDR,
                            begin_default_recover=begin_default_recover, split_default=split_default)

        prediction, assumptions = loanpool.scenario_analysis(CPR=CPR, CDR=CDR, DPs=DP, RR=RR, DPP=DPP,
                                                             well_distributed=False, suoe=suoe,
                                                             minus_CDR=minus_CDR, CPR_type=cpr_type)
        # todo 循环购买这样加压在累计上其实并不是很严谨
    else:

        prediction = pd.DataFrame(
            columns=['date_', 'begin_princifpal_balance', 'current_principal_due', 'current_interest_due',
                     'end_principal_balance', 'prepay_amount', 'default_amount', 'recycle_amount',
                     'begin_default_balance', 'end_default_balance'])

    if is_revolving_pool:  # 循环购买的
        prediction = prediction.append(df_prediction[df_prediction['is_revolving_period'] == 1]).sort_values(
            by='date_').reset_index(drop=True)

    return prediction, assumptions


def cashflow_pressure_dynamic(df_prediction, history_date, initial_date, initial_principal,
                              CDR, DP, SMMs, RRs, suoe, realized_CDR, current_default, minus_CDR,
                              begin_default_recover, split_default, cpr_type):
    """
    现金流加压模块，非循环非不良，序列模式1

    Args:
        df_prediction (pd.DataFrame): 未加压现金流归集表
        history_date (datetime.date): 上一归集日
        initial_date (datetime.date): 初始起算日
        initial_principal (float): 初始本金额
        CDR (float): 违约率，无单位
        DP: np.adarray, 违约分布序列, 长度需要与现金流归集表一致
        SMMs: np.ndarray, 各期早偿率序列，长度需要与现金流归集表一致
        RRs: pd.Series, 违约回收率序列
        suoe (bool): 是否用缩额法
        realized_CDR (float): 已发生的累计违约率
        current_default (float): 当期剩余违约本金
        minus_CDR (bool): 是否从输入的 ``CDR`` 中扣除当期已发生的累计违约率
        begin_default_recover (bool): 是否对当前剩余违约金额计算违约回收
        split_default (bool): 是否从最新剩余本金中扣除当前剩余违约金额
        cpr_type (str): 早偿金额的算法

    Returns:
        pd.DataFrame: prediction, 加压后的现金流归集表


    .. code-block:: python
        :linenos:

        loanpool.dynamic_scenario_analysis(SMMs=SMMs, CDR=CDR, DPs=DP, RRs=RR, DPPs=DPP, suoe=suoe,
                                                        minus_CDR=minus_CDR, CPR_type=cpr_type)



    .. caution::
        不适合循环购买

    """
    # 0. 基本要素

    RR = np.array(list(RRs.values))
    DPP = np.array(list(RRs.index))

    loanpool = LoanPool(pool_start_date=history_date, original=df_prediction,
                        initial_principal=initial_principal, initial_date=initial_date,
                        begin_default=current_default, pool_start_CDR=realized_CDR,
                        begin_default_recover=begin_default_recover, split_default=split_default)

    prediction = loanpool.dynamic_scenario_analysis(SMMs=SMMs, CDR=CDR, DPs=DP, RRs=RR, DPPs=DPP, suoe=suoe,
                                                    minus_CDR=minus_CDR, CPR_type=cpr_type)

    return prediction


def cashflow_pressure_ycdr(df_prediction, history_date, initial_date, initial_principal, is_revolving_pool,
                           YCDR, CPR, RR, DPP, suoe, current_default, begin_default_recover, split_default, cpr_type):
    """
    适用于非循环非不良，非模型测算参数模式2, 用的是年化违约率

    Args:

        df_prediction (pd.DataFrame): 未加压现金流归集表
        history_date (datetime.date): 上一归集日
        initial_date (datetime.date): 初始起算日
        initial_principal (float): 初始本金额
        is_revolving_pool (bool): 是否循环购买
        YCDR (float): 年化违约率，无单位
        CPR (float): 早偿率。无单位
        RR (float): 违约回收率，无单位
        DPP (int): 违约回收期，单位：月
        suoe (bool): 是否用缩额法
        current_default (float): 当期剩余违约本金
        begin_default_recover (bool): 是否对当前剩余违约金额计算违约回收
        split_default (bool): 是否从最新剩余本金中扣除当前剩余违约金额
        cpr_type (str): 早偿金额的算法

    Returns:
        tuple: tuple contains:
                     prediction (pd.DataFrame): 加压后的现金流归集表 \n
                     assumptions (pd.DataFrame): 匹配到每一期的参数假设


    .. code-block:: python
        :linenos:

         loanpool.scenario_analysis_ycdr(CPR=CPR, YCDR=YCDR, RR=RR, DPP=DPP, suoe=suoe, CPR_type=cpr_type)

    """
    current_default = current_default
    assumptions = pd.DataFrame()
    prediction_static = df_prediction[df_prediction['is_revolving_period'] == 0].reset_index(drop=True)
    if len(prediction_static) > 0:

        loanpool = LoanPool(pool_start_date=history_date, original=prediction_static,
                            initial_principal=initial_principal,
                            initial_date=initial_date, begin_default=current_default,
                            begin_default_recover=begin_default_recover, split_default=split_default)
        prediction, assumptions = loanpool.scenario_analysis_ycdr(CPR=CPR, YCDR=YCDR, RR=RR, DPP=DPP,
                                                                  suoe=suoe, CPR_type=cpr_type)
    else:

        prediction = pd.DataFrame(
            columns=['date_', 'begin_princifpal_balance', 'current_principal_due', 'current_interest_due',
                     'end_principal_balance', 'prepay_amount', 'default_amount', 'recycle_amount',
                     'begin_default_balance',
                     'end_default_balance'])

    if is_revolving_pool:
        prediction = prediction.append(df_prediction[df_prediction['is_revolving_period'] == 1]).sort_values(
            by='date_').reset_index(drop=True)

    return prediction, assumptions


def cashflow_pressure_dynamic_ycdr(df_prediction, history_date, initial_date, initial_principal,
                                   SMDRs, SMMs, RRs, suoe, current_default, begin_default_recover,
                                   split_default, cpr_type):
    """
    现金流加压模块，非循环非不良，序列模式2

    Args:

        df_prediction (pd.DataFrame): 未加压现金流归集表
        history_date (datetime.date): 上一归集日
        initial_date (datetime.date): 初始起算日
        initial_principal (float): 初始本金额
        SMDRs (np.ndarray): 各期违约率序列，长度需要与现金流归集表一致
        SMMs (np.ndarray): 各期早偿率序列，长度需要与现金流归集表一致
        RRs: pd.Series, 违约回收率序列
        suoe (bool): 是否用缩额法
        current_default (float): 当期剩余违约本金
        begin_default_recover (bool): 是否对当前剩余违约金额计算违约回收
        split_default (bool): 是否从最新剩余本金中扣除当前剩余违约金额
        cpr_type (str): 早偿金额的算法


    Returns:
        pd.DataFrame: prediction, 加压后的现金流归集表


       .. code-block:: python
        :linenos:

            loanpool.dynamic_scenario_analysis_ycdr(SMMs=SMMs, SMDRs=SMDRs, RRs=RR, DPPs=DPP, suoe=suoe,
                                                                 CPR_type=cpr_type)


    """
    # 0. 基本要素

    current_default =current_default

    RR = np.array(list(RRs.values))
    DPP = np.array(list(RRs.index))
    # 加压，这一步会把多余的列去掉
    loanpool = LoanPool(pool_start_date=history_date, original=df_prediction,
                        initial_principal=initial_principal, initial_date=initial_date,
                        begin_default=current_default, begin_default_recover=begin_default_recover,
                        split_default=split_default)

    prediction = loanpool.dynamic_scenario_analysis_ycdr(SMMs=SMMs, SMDRs=SMDRs, RRs=RR, DPPs=DPP, suoe=suoe,
                                                         CPR_type=cpr_type)

    return prediction
