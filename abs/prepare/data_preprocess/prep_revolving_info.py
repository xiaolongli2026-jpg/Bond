# -*- coding: utf-8 -*-
"""
处理循环购买相关的假设参数。由于数据批量较少，目前不使用。
"""

import numpy as np
import pandas as pd
from utils.timeutils import to_date2


def transform_revolving_info(df_static_amortization, df_dynamic_params, start_date, yield_calway='remaining_principal'):
    """
    
    Parameters
    ----------
    df_static_amortization : DataFrame, 模拟静态池
    df_dynamic_params : DataFrame, 循环池动态假设
    start_date: str 资产池初始起算日
    yield_calway: str remaining_principal以剩余本金为分母, current_principal_due以当期本金回收为分母
    Returns
    -------
    df_revolving_params : dict
        循环池计算需要用到的假设条件.

    """

    df_static_amortization.loc[:, 'begin_date'] = df_static_amortization.loc[:, 'begin_date'].apply(to_date2)

    # 0.静态池存在时，填充摊还和收益率假设；1.静态池不存在时，只有一个假设缺失 2.静态池不存在时，两个及以上假设缺失
    # 0.1 静态池填充摊还分布
    if not pd.isna(df_static_amortization).all().all():

        df_static_amortization.loc[:, 'duration_month'] = df_static_amortization['begin_date'].apply(
            lambda x: int(np.ceil((to_date2(x) - to_date2(start_date)).days / 30)))
        if df_static_amortization['static_principal_due'].isna().all() and \
                not df_static_amortization['begin_principal_balance'].isna().all():
            df_static_amortization.loc[:, 'static_principal_due'] = \
                df_static_amortization['begin_principal_balance'] - df_static_amortization[
                    'begin_principal_balance'].shift(-1)
        df_static_amortization = df_static_amortization.groupby('duration_month').sum()
        df_static_amortization.reset_index(drop=False, inplace=True)
        df_static_amortization.loc[:, 'static_principal_due'] = df_static_amortization['static_principal_due'].fillna(
            0.)

        if len(df_dynamic_params) < 1:
            df_dynamic_params.loc[:, 'duration_month'] = df_static_amortization['duration_month']

        if pd.isna(df_dynamic_params['payback_proportion']).all():
            df_dynamic_params.drop(columns='payback_proportion', inplace=True)
            sum_principal_due = df_static_amortization.loc[:, 'static_principal_due'].sum()  # TODO 未考虑静态池总额不等于初始本金余额的可能
            df_static_amortization.loc[:, 'payback_proportion'] = \
                df_static_amortization.loc[:, 'static_principal_due'] / sum_principal_due * 100
            df_dynamic_params = \
                df_dynamic_params.merge(df_static_amortization[['duration_month', 'payback_proportion']],
                                        on='duration_month', how='outer')
            df_dynamic_params.loc[:, 'payback_proportion'] = df_dynamic_params.loc[:, 'payback_proportion'].fillna(0.)

        # 0.1用静态池数据填充收益率 计算的是剩余本金的收益率，非当期回收的收益率
        # TODO 如果没有特别说明，默认是以本金余额为收益率分母，如果计算结果跟现金流归集表差太大可能还要try以当期本金回收作分母
        if df_dynamic_params['dynamic_yield'].isna().all() and \
                not df_static_amortization['static_interest_due'].isna().all().all():

            if yield_calway == 'remaining_principal':
                df_dynamic_params.loc[:, 'dynamic_yield'] = \
                    df_static_amortization['static_interest_due'] / \
                    df_static_amortization.loc[:-1, 'static_principal_due'].cumsum() * 100
            elif yield_calway == 'current_principal_due':
                df_dynamic_params.loc[:, 'dynamic_yield'] = \
                    df_static_amortization['static_interest_due'] / \
                    df_static_amortization['static_principal_due'] * 100

    df_dynamic_params.set_index('duration_month', inplace=True)
    df_dynamic_params.rename(columns={'dynamic_defaultrate': '违约率',
                                      'dynamic_yield': '收益率', 'dynamic_discountrate': '折价率',
                                      'payback_proportion': '摊还比例', 'dynamic_prepayrate': '早偿率'},
                             inplace=True)

    return df_dynamic_params


def transfer_static_param(df_staticparams):
    """
    循环池静态假设数据处理

    Args:
        df_staticparams: df, 静态假设表

    Returns:

    """
    if len(df_staticparams) > 0:
        indicator_type_dict = {'1': '金额', '2': '比例（%）'}
        assumption_type_dict = {'200': '证券收益率',
                                '310': '违约率',
                                '320': '早偿率',
                                '330': '违约回收率',
                                '400': '资产池利息率',
                                '500': '循环购买比例',
                                '100': '税费总额（元）',
                                '110': '增值税及附加',
                                '120': '费用总额（元）',
                                '121': '托管费',
                                '122': '管理费',
                                '123': '审计费',
                                '124': '服务费',
                                '125': '其他费用',
                                '126': '划付费',
                                '127': '评级费',
                                '128': '兑付兑息费',
                                '129': '登记费',
                                '130': '律师费',
                                '131': '承销费',
                                '350': '逾期率',
                                '510': '循环购买折价率',
                                '600': '资产池占比',
                                '700': '合格投资收益率',
                                '340': '提前退租率'}
        df_staticparams.loc[:, 'assumption_type'] = df_staticparams['assumption_type'].apply(
            lambda x: assumption_type_dict[x])
        df_staticparams.loc[:, 'indicator_type'] = df_staticparams["indicator_type"].apply(
            lambda x: indicator_type_dict[x])
    return df_staticparams
