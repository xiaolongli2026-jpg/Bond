# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from doc.global_var import global_var as glv


def main_spot_curve(date_,  curve_id='cc_ll_gz', cur=None):
    """
    读取单条曲线的到期收益率

    Args:
        date_ (str): 日期
        curve_id (str): 曲线名称，跟估值库保存的一样
        cur (cursor):

    Returns:
        pd.Series: 即期利率曲线

    """
    close_conn = False
    if cur is None:
        from utils.quick_connect import connect_mysql
        conn, is_mysql = connect_mysql()
        cur = conn.cursor()
        close_conn = True

    yield_curve = getCurveData(date_, curve_id, cur)
    full_yield_curve = curve_compute_values(yield_curve)
    spot_curve = ytm_to_spot(full_yield_curve)

    if close_conn:
        cur.close()
        conn.close()

    return spot_curve / 100


#获取单条到期曲线数据参数,默认国债曲线
def getCurveData(date_,  curve_id='cc_ll_gz', cur=None):
    """
    读取单条曲线的到期收益率

    Args:
        date_ (str): 日期
        curve_id (str): 曲线名称
        cur (cursor):

    Returns:
        pd.DataFrame: 单日某条曲线的到期收益和参数构成

    """
    success_ = False
    runtime = 0
    CurveResult = pd.DataFrame()
    while (success_ is False) and (runtime < 10):

        sql_curve = f"select * from {'' if glv().get('is_mysql') else 'CSI_BOND_BS.'}CSI_BOND_GZ_PARAM_YTM_HIS where IMPORT_TIME = '{date_+'153000'}'"
        cur.execute(sql_curve)
        value_ = cur.fetchall()
        columns_ = cur.description
        CurveResult = pd.DataFrame(value_, columns=[x[0].lower() for x in columns_])
        CurveResult = CurveResult.loc[CurveResult['curveid'] == curve_id, :]
        if len(CurveResult) > 0:

            CurveResult = CurveResult[["t", "z", "b", "c", "d"]].reset_index(drop=True)
            success_ = True
        else:
            date_ = datetime.strftime(datetime.strptime(date_, '%Y%m%d') + timedelta(days=-1), '%Y%m%d')

        runtime += 1

    if len(CurveResult) < 1:
        raise ValueError(f'找不到{date_}的曲线{curve_id}')
    else:
        CurveResult = CurveResult.astype(float)

    return CurveResult


def curve_compute_values(para_df):
    """根据getCurveData的返回值中的参数计算所有时点的到期收益率(直接用西琦的逻辑）

    Args:
        para_df: return of getCurveData

    Returns:
        pd.Series: 到期收益率曲线，index-步长0.0. value-到期收益率
    """

    t_m = para_df['t']
    min_point = min(t_m)
    max_point = max(t_m)
    xx = np.arange(min_point, max_point, 0.01)
    curve_values = np.full(len(xx), float('nan'))
    for i in range(len(xx)):
        for j in range(len(t_m) - 1):
            if (xx[i] > t_m[j]) & (xx[i] <= t_m[j+1]):
                #location = j
                curve_values[i] = para_df['z'][j] + para_df['b'][j] * (xx[i] - t_m[j]) + para_df['c'][j] * (xx[i] - t_m[j]) ** 2 / 2 + para_df['d'][j] * (xx[i] - t_m[j]) ** 3 / 6
    xx = np.around(xx, decimals=2)
    curve_ = pd.Series(curve_values, index=xx)
    curve_.fillna(0., inplace=True)

    return curve_


def ytm_to_spot(curve_para0):
    """ 将到期收益率转化为即期收益率(直接用西琦的逻辑）

    Args:
        curve_para0 (pd.Series): index为期限(步长0.01)， value-到期收益率
    """
    curve_para = curve_para0.copy()
    for i in curve_para.index:
        if i <= 1:  # ?
            curve_para[i] = curve_para0[i]
        elif i > 1:
            ytm = curve_para0[i]
            t = round(i % 1, 2)
            n = int(i // 1) - 1 if t == 0.0 else int(i // 1)
            dirty_price = (1 + ytm/100) ** (1 - (t if t!=0 else 1))
            time_series = np.array([j+t for j in range(0, n+1)]) + 1 if t == 0.0 else np.array([j+t for j in range(0, n+1)])
            cash_flow = np.array([ytm] * n + [100 + ytm]) / 100
            spot_list = [curve_para[round(j, 2)] for j in time_series[:-1]]
            coupon_sum = (cash_flow[:-1]/(1+np.array(spot_list)/100)**time_series[:-1]).sum()
            curve_para[i] = ((cash_flow[-1] / (dirty_price - coupon_sum)) ** (1/time_series[-1]) - 1) * 100
    return curve_para


def ytm_to_forward(curve_para0):
    """将到期收益率转为远期利率"""
    curve_para = curve_para0.copy()
    last_i = 0
    for i in curve_para.index:
        if i <= 0:
            curve_para[i] = curve_para0[i]

        else:
            curve_para[i] = \
                np.exp(np.log((1 + curve_para0[i]) ** i/(1 + curve_para0[last_i]) ** last_i)/(i - last_i)) - 1
        last_i = i
    return curve_para


