# -*- coding: utf-8 -*-
"""
资产池外推模型预测累计违约率和违约分布序列。需要注意模型中用到的期次、账龄全部指的是从初始起算日开始的月数（四舍五入）
"""

import pandas as pd
import numpy as np
from warnings import warn
from sklearn import linear_model
from utils.quick_connect import connect_mysql
from utils.timeutils import to_date2, age_
from prepare.data_load.model_data import (get_adjust_factor, comparable_pools_data)
from calculate.asset.s_param_match import match_cashflow_dp


def extrapolation_model(project_seq: str, predict_date: str, second_class: str, initial_date,
                        df_prediction, history_date, realized_cdr, same_type=False):
    """
    资产池外推模型主函数

    Args:
        project_seq (str): 项目内码
        predict_date (str): 估值日
        second_class (str): 二级分类枚举值, 只能是 1、2，分别代表RMBS和汽车贷
        initial_date (datetime.date): 初始起算日
        df_prediction (pd.DataFrame): 最新现金流归集表，即需要应用得到的数据进行加压的对应的现金流归集表，用于对模型结果进行进一步的匹配
        history_date (datetime.date): 上一归集日
        realized_cdr (float): 当前已经违约的本金总额 / 初始本金
        same_type (bool): 可比资产池查找范围

                           * `True` - 查找同二级分类的作为可比资产池，
                           * `False` (默认） - 查找同发行人的作为可比资产池

    Returns:
        tuple: tuple contains:
            dp (np.array): 违约分布序列 \n
            lifetime_cdr (float): 项目存续期间累计违约率 \n
            extrapolate_staticpools_cdr (pd.DataFrame): 静态池累计违约率数据，index-账龄，columns-项目内码


    **逻辑**

    1. 通过 ``model_data.comparable_pools_data`` 读取可比资产池
    2. 通过 ``model_data.get_adjust_data`` 获取调整因子值
    3. ``main_extrapolate_model`` 为核心计算模块，进行资产池外推和标的项目累计违约率、违约分布的预测

    """

    conn, is_mysql = connect_mysql()
    cur = conn.cursor()
    project_seq = project_seq.strip("'").strip("\"")
    # 1. 计算现金流归集表中归集日距离初始起算日的月数（四舍五入）
    pred = df_prediction.copy()
    pred.loc[:, 'age'] = pred['date_'].apply(lambda x: age_(to_date2(initial_date), to_date2(x)))

    # 2. 可比项目历史数据
    valid_projects, static_pools_cdrs, staticpools_ucprs, staticpools_cprs, staticpools_rrs = \
        comparable_pools_data(project_seq, predict_date, same_type, cur)

    # 4. 获取模型所需的调整因子值
    df_initial_factor = get_adjust_factor(valid_projects + [project_seq], second_class, cur)

    # 5. 资产池外推，获取预测的违约率和违约分布
    dp, lifetime_cdr, current_cdr_theory, extrapolate_staticpools_cdr, success, current_t = \
        main_extrapolate_model(project_seq, static_pools_cdrs, df_initial_factor, realized_cdr,
                               pred, initial_date, history_date)

    cur.close()
    conn.close()

    return dp, lifetime_cdr, extrapolate_staticpools_cdr


def main_extrapolate_model(project_seq, static_pools_cdrs, df_factor, current_cdr, df_prediction,
                           initial_date, history_date):
    """
    资产池外推模型

    Args:
        project_seq (str): 项目内码
        static_pools_cdrs (pd.DataFrame): 可比项目cdr，index-age， column-project_seq
        df_factor (pd.DataFrame): 累计违约率调整因子, 为入池特征因子
        current_cdr (float): 当前已实现的累计违约率
        df_prediction (pd.DataFrame): 最新的现金流归集表
        initial_date (datetime.date): 初始起算日
        history_date (datetime.date): 最近一次历史归集日

    Returns:
         tuple: tuple contains

             DP (pd.Series), 违约分布序列，与最新的现金流归集表相匹配
             lifetime_cdr (float): 预测的理论累计违约率，由于预测到的最后期限与初始现金流归集表一样，如果存续期现金流归集表期限更短，加压计算中会截取
             current_cdr_theory (float): 根据模型，理论上当前已发生违约率的值，可能会与实际值有出入
             extrapolate_staticpools_cdr (pd.DataFrame): 可比资产池外推过的违约率序列
             success (bool): 是否有调整基准违约


    **逻辑**


    1. 用 ``extrapolate_program`` 对静态池进行外推，得到外推后的静态池累计违约率数据 ( `extrapolate_staticpools_cdr`: 列为项目内码，行为期次/月 ) 和基准累计违约率序列 ( `benchmark_cdrs`: 列为基准cdr，行为期次/月 )
    2. 用调整因子调整 `cdr`, 具体方式为

                * 用待估资产池最大账龄(设为 `total_t` )那一期的所有静态池的累计违约率除以当期的基准累计违约率，作为 `y`
                * 以 `df_factor` 中的因子值作为 `x`
                * 线性回归后待入待估项目的因子值，得到待估项目的调整因子 （ 具体公式见研究报告）
                * 用调整因子乘以基准违约率 ( `benchmark_cdrs['total_t']` ），即得到预计的项目整个存续期的累计违约率
                * 但是如果待估资产池没有读到因子值，则直接用基准累计违约率 ( `benchmark_cdrs['total_t']` ）

    3. 为了剔除当期已经违约的部分的影响，令预测的 `CDR` 为::

                CDR = lifetime_cdr - current_cdr_theory + current_cdr



    其中 `lifetime_cdr` 为 2 中预测的累计违约率； `current_cdr` 为标的项目当期已发生的累计违约率；
    `current_cdr_theory` 为 `history_date` 对应的那一期的基准累计违约率（即跟 `current_cdr` 对应的那一期的基准累计违约率），
    通过计算 `history_date` 与初始起算日之间的月数（四舍五入）后取用 `benchmark_cdrs` 中对应期次的值得到

    4. 计算违约分布

        * 先从 `benchmark_cdrs` 中选取的 `total_t` 及以前的数据，计算每一期的边际违约率占累计违约率的比例::

            portion_ = pd.Series(benchmark_cdrs.loc[: total_t, 'benchmark'] / benchmark_cdrs.loc[total_t, 'benchmark'])
            DP = portion_ - portion_.shift(1)
            DP[0] = portion_[0]
            DP[DP < 0] = 0
            DP = DP / DP.sum()


        * 然后带入 ``match_cashflow_dp`` 匹配到现金流归集表对应的期次（因为前面得到的结果是从第1期到项目到期的完整周期的模拟） ::

            dp_ = match_cashflow_dp(df_prediction, initial_date=initial_date, last_pool_date=history_date, dp=DP, param_match_method='all')


    Notes:
          在 ``module_abs_new.do_calculation_extrapolate`` 中，会设置 ``minus_CDR=True`` , 因此实际用到的这里违约分布 `dp_` 中各期的比例关系而不是绝对值。
          另外，从 ``loanpool`` 中的算法介绍可以看到， ``minus_CDR=True`` 时， ``MDR = (CDR - current_cdr) * (dp_ / sum(dp_))`` 。
          故在后续加压计算中，实际用到的是第3步中计算得到的 `CDR` 中的  ``lifetime_cdr - current_cdr_theory`` 这部分，也即假设了对未来期次边际违约率的假设不受此前已违约部分的影响。

    """
    # 1. 资产池外推
    current_t = age_(initial_date, history_date)
    total_t = int(max(df_prediction['age']))  # 待估资产池最大账龄
    extrapolate_staticpools_cdr, benchmark_cdrs = extrapolate_program(static_pools_cdrs.copy(), total_t)

    # 2. 调整cdr
    train_x_y = extrapolate_staticpools_cdr.loc[total_t, :] / benchmark_cdrs.loc[total_t, 'benchmark']

    train_x_y.name = 'y'

    df_factor.set_index('project_seq', drop=True, inplace=True)
    train_x_y = pd.concat([train_x_y, df_factor], join='inner', axis=1)
    train_x_y.replace(np.inf, np.NaN, inplace=True)
    train_x_y.replace(-np.inf, np.NaN, inplace=True)
    train_x_y.dropna(axis=0, how="any", inplace=True)

    success = False

    # 用因子调整基准值
    current_cdr_theory = 0
    lifetime_cdr = 0
    if len(train_x_y) > 1:
        predict_x = np.array(df_factor.loc[project_seq, :])
        train_y = np.array(train_x_y['y'])
        train_x = np.array(train_x_y.drop(columns=['y']))

        if len(predict_x) > 0 and np.isnan(predict_x).sum() < 1:
            # 新公式
            lr = linear_model.LinearRegression(fit_intercept=True).fit(train_x, train_y)
            predict_y = lr.predict(predict_x.reshape(1, 1))
            lifetime_cdr = predict_y[0] * benchmark_cdrs.loc[total_t, 'benchmark']
            current_cdr_theory = predict_y[0] * benchmark_cdrs.loc[current_t, 'benchmark']
            success = True

    if not success:
        warn("没有足够的回归因子调整基准违约率，直接用同类资产池均值替代.")
        lifetime_cdr = benchmark_cdrs.loc[total_t, 'benchmark']
        current_cdr_theory = benchmark_cdrs.loc[current_t, 'benchmark']

    portion_ = pd.Series(benchmark_cdrs.loc[: total_t, 'benchmark'] / lifetime_cdr)
    DP = portion_ - portion_.shift(1)
    DP[0] = portion_[0]
    DP[DP < 0] = 0
    DP = DP / DP.sum()
    lifetime_cdr = max(lifetime_cdr, current_cdr) - current_cdr_theory + current_cdr # 结果不能小于当前已经发生的累计违约率, 假设后面几期不受前面几期的影响
    dp_ = match_cashflow_dp(df_prediction, initial_date=initial_date, last_pool_date=history_date, dp=DP, param_match_method='all')

    return dp_, lifetime_cdr, current_cdr_theory, extrapolate_staticpools_cdr, success, current_t


def extrapolate_program(df_staticpool, n):
    """
    对可比资产池外推进行外推。\n
    由于各项目当前已经存续的时间不同，需要全部推到跟待估资产池一样长

    Args:
        df_staticpool (pd.DataFrame): 读取到的静态池数据，其中每一列代表一个项目，行指的是各个项目披露的历史数据的核算日距离自己的初始起算日的月数。
        n (int): 需要推到的期次，即待估资产池最新现金流归集表的最后一个日期跟初始起算日的月数

    Returns:
        tuple: tuple contains:
            df_staticpool (pd.DataFrame): 外推后的静态池
            df_benchmark (pd.DataFrame): 基准累计违约率序列，即对可比资产池每一期取均值得到的


    **逻辑**

    1. 假设所有资产池中，期限最短的项目账龄是 `n_sp_min` , 期限最长的项目的账龄是 `n_sp_max`
    2. 如果 `n<n_sp_min` ，无需外推
    3. 如果 `n>n_sp_min` ，需要外推，先构造累计违约率增速序列 g

                        1. 构造 `n_sp_max` 内的累计违约率增速序列：

                                * 通过计算所有静态池的 `MDR` 取均值作为 `delta_x_mean`
                                * 累加 `delta_x_mean` , 作为 `sum_mean`
                                * 由 `sum_mean` 除以上期的 `sum_mean` , 即为单期的增速

                        2， 计算 `n_sp_max` 至 `n` 之间的累计违约率增速序列，此时由于并没有任何可比资产池存续期次达到了这么长，需要用统计的方法计算增速：

                                * 以 `sum_mean` 作为 `y` , ln(t) 作为 x ，进行回归。其中的 t 是sum_mean对应的账龄
                                * 生成 `n_sp_max` 至 `n` 的序列 `T` , ``intercept_ + coef_ * ln(T)`` 得到的就是在这段没有静态池累计违约率数据的期间的预测累计违约率均值,设为 `pred_cdr` 。
                                * 同样以当期的 `pred_cdr` 除以上期的 `pred_cdr` ，得到单期增速, 与前一步得到的增速拼接成长度为 n 的 g

    4. 对于需要外推的静态池样本进行外推。假设某个样本的数据只到第 `n_pool` 期， 后续每一期 `n_p` 的外推值，通过直接选择 `g` 中第 `n_pool + 1 ` 至第 `n_p` 期的数据累乘，然后乘以第 `n_pool` 期该样本的实际累计违约率得到。 ::

                        cdr[n_p] = cdr[n_pool] * cumprod(g[n_pool + 1: n_p])


    Notes:
        原本在 `n_sp_max` 内的期次是通过将 `MDR` 均值数据（ `delta_x_mean` ） 中对应期次上的值，累加到需要外推的静态池样本的最大累计违约率上，
        以将数据填补到第 `n_sp_max` 期。但是这样做外推得到的累计违约率曲线的形态不平滑，特别是对于一些历史违约情况较为陡峭，或者特别平缓的，
        会在某一段出现明显的转折。故在  `n_sp_max` 内也通过累乘增长率的方式外推


    """
    df_staticpool.index = df_staticpool.index.astype(int)
    #  1. 可比资产池的最大最小账龄
    n_sp_max = max(df_staticpool.count(axis=0)) - 1  # 包括一期账龄为0的，需要扣除
    n_sp_min = min(df_staticpool.count(axis=0)) - 1

    #  2. 如果可比资产池期次不足以覆盖待估资产池，则外推
    if n > n_sp_min:
        df_delta_x = df_staticpool.diff()
        df_delta_x['delta_x_mean'] = df_delta_x.mean(axis=1)
        df_delta_x['sum_mean'] = df_delta_x['delta_x_mean'].cumsum()
        base_ = df_delta_x['sum_mean'].shift(1)
        g_ = np.zeros([n_sp_max+1, ])
        valid_rows = base_ > 0
        g_[valid_rows] = df_delta_x.loc[valid_rows, 'sum_mean'] / base_[valid_rows]
        if n > n_sp_max:
            df_staticpool = df_staticpool.merge(pd.DataFrame(index=range(0, n + 1)),
                                                left_index=True, right_index=True, how='right')
            #  2.1  回归得到cdr和log(t)的关系

            y_train = np.array(df_delta_x.iloc[1:n_sp_max+1, :]['sum_mean'].fillna(method='ffill').values)
            x_train = np.log(range(1, n_sp_max + 1)).reshape(-1, 1)

            lm = linear_model.LinearRegression(fit_intercept=True)

            lm.fit(x_train, y_train)

            pred_cdr = pd.DataFrame(lm.predict(np.log(range(1, n + 1)).reshape(-1, 1)), index=range(1, n + 1),
                                     columns=['cdr'])

            pred_cdr.loc[pred_cdr['cdr'] < 0, :] = 0
            pred_cdr.loc[pred_cdr['cdr'] > 1, :] = 1
            g_2 = (pred_cdr.loc[n_sp_max + 1:, :]['cdr'].values / pred_cdr.loc[n_sp_max: n - 1, :]['cdr'].values)
            g_ = np.vstack((g_.reshape(len(g_), 1), g_2.reshape(len(g_2), 1)))

        g_[g_ < 1] = 1
        # 2.2 外推

        for stp in list(df_staticpool.columns):
            age_stp = df_staticpool[stp].count() - 1
            df_staticpool.loc[age_stp + 1: n, :][stp] = \
                np.cumprod(g_[age_stp + 1: n + 1]) * df_staticpool.loc[age_stp, :][stp]

    # 3. 处理极端值
    df_staticpool[df_staticpool > 1] = float('nan')
    df_staticpool.fillna(method='ffill', inplace=True)
    df_benchmark = pd.DataFrame(df_staticpool.mean(axis=1), columns=['benchmark'])

    return df_staticpool, df_benchmark
