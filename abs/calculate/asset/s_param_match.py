# -*- coding: utf-8 -*-
"""
将序列型的输入参数与现金流归集表对应的期次进行匹配

"""

import numpy as np
import pandas as pd
from datetime import date, datetime
from dateutil import relativedelta
from copy import deepcopy
from utils.timeutils import count_months, count_month


def match_cashflow_cond_params(df_prediction, initial_date, last_pool_date, param_series: pd.Series = None,
                               param_match_method='all') -> np.ndarray:
    """
    适用于非循环非不良的序列型入参时，对年化假设 `CPRs`, `YCDRs` （输入的序列参数的index为月）与现金流归集表匹配, 使得加压步骤中而无需在加压时再根据期间天数进行处理

    Args:
        df_prediction (pd.DataFrame): 现金流归集表, 须有 `date_` 列, 即归集日列
        last_pool_date (datetime.date): 上一归集日, 当 ``param_match_method = 'remain'`` 时与 ``base_date`` 相同
        param_series (pd.Series): index为月份，value为当月的假设值

    Returns:
        np.ndarray: spp, single period param


    **逻辑**


        示例::

            >> param_series = pd.Series([0.01, 0.02, 0.021, 0.04, 0.03, 0.025, 0.011], index=[1, 2, 3, 4, 5, 6, 7])
            >> df_prediction['date_'] = np.array([datetime.date(2020, 1, 31), datetime.date(2020, 2, 29), datetime.date(2020, 3, 31),
                                      datetime.date(2020, 4, 30), datetime.date(2020, 5, 31), datetime.date(2020, 6, 30),
                                      datetime.date(2020, 7, 31)])  # 现金流归集表的归集日
            >> base_date = datetime.date(2020, 1, 1)  # 假设这个是上一归集日， 即 ``param_match_method = 'remain'``

            # 于是可以 ``timeutil.count_months`` 计算与日期 ``base_date`` 之间的月数作为账龄
            >> df_prediction['age_'] = np.array([0.96774194 1.96774194 2.96774194 3.96774194 4.96774194 5.96774194, 6.96774194])
            # 对于第一期，账龄 0.96774194 < 1, 故使用假设条件中的第一个值计算与第一个归集日匹配的压力参数
            >> 1 - (1 - 0.01) ** 0.96774194 -> 0.009678985672080587
            # 对于第2期，则为
            >> 1 - ((1 - 0.01) ** (1 - 0.96774194)) * ((1 - 0.02) ** (1.96774194 - 1)) -> 0.019679001548373543
            # 以此类推，最后得到
            >> spp = [0.00967899 0.019679   0.02096776 0.03939289 0.0303242  0.02516169, 0.01145474]

            # 如果 ``param_match_method = 'remain'``
            >> base_date = datetime.date(2019, 9, 1)  # 假设的初始起算日
            >> df_prediction['age_'] = array([ 4.96666667,  5.96666667,  6.96666667,  7.96666667,  8.96666667, 9.96666667, 10.96666667])
            >> last_pool_date = datetime.date(2020, 1, 1)
            >> last_age = count_month(base_date, last_pool_date)  -> 3.998924731182796
            # 此时对于第一期，4 < 4.96666667 < 5
            >> 1 - ((1 - 0.04) ** ( 4 - 3.998924731182796)) * ((1 - 0.03) ** (4.96666667 - 4)) -> 0.02905727
            # 依此类推，如果账龄超出了假设范围，则用最后一个假设条件，比如最后一个归集日上
            >> 1 - (1 - 0.11) ** (10.96666667 - 9.96666667) -> 0.11
            # 最终
            >> array([0.02905727, 0.02516708, 0.01146989, 0.011     , 0.011     , 0.011     , 0.011     ])

    """
    base_date = last_pool_date if param_match_method == 'remain' else initial_date
    if not isinstance(df_prediction.date_[0], (datetime, date)):
        df_prediction['date_'] = df_prediction['date_'].to_datetime()

    # 1 计算现金流归集表账龄
    df_prediction.sort_values(by='date_', inplace=True)
    df_prediction['age_'] = count_months(base_date, df_prediction['date_'])

    # 2 拓展假设条件期限, 使得能够覆盖现金流归集表期限
    max_age = int(np.ceil(df_prediction['age_'].max()))
    params = pd.Series(index=range(0, int(max_age) + 1), name='param', dtype='float64')
    for (x, y) in param_series.iteritems():
        params[x] = y
    params = params.bfill().ffill()
    copy_ = deepcopy(df_prediction)
    copy_['ceil_age'] = np.ceil(copy_.age_)

    # 3 计算现金流归集表各期对应的假设参数, 各月假设覆盖比其小的非整数月
    last_age = count_month(base_date, last_pool_date) if base_date != last_pool_date else 0
    last_param_id = int(np.ceil(count_month(base_date, last_pool_date))) if base_date != last_pool_date else 0
    n = len(copy_)
    spp = np.zeros(n)
    for i in range(0, n):
        id_ = copy_.index[i]
        next_age = copy_.loc[id_, 'age_']
        next_param_id = int(copy_.loc[id_, 'ceil_age'])

        if next_param_id == last_param_id:
            spp[i] = 1 - (1 - params[next_param_id]) ** (next_age - last_age)
        else:
            slice_ = params[last_param_id: next_param_id + 1]
            month_diff = np.zeros(len(slice_))
            month_diff[0] = slice_.index[0] - last_age
            month_diff[1: -1] = slice_.index[1: -1] - slice_.index[0: -2]
            month_diff[-1] = next_age - slice_.index[-2]
            spp[i] = 1 - np.prod((1 - slice_.values) ** month_diff)

        last_age = next_age
        last_param_id = next_param_id

    # 4 将超过100%的进行限制, 此类针对期初本金额的参数不能超过100%
    spp[spp > 1] = 1

    return spp


def match_cashflow_dp(df_prediction, initial_date, last_pool_date, dp: pd.Series = None, param_match_method='all') -> np.ndarray:
    """
    适用于非循环非不良，非模型测算参数模式，用于将DP与现金流归集表匹配

    Args:
        df_prediction (pd.DataFrame): 现金流归集表, 须有 `date_` 列, 即归集日列
        initial_date (datetime.date): 初始起算日

        last_pool_date (datetime.date): 上一归集日, 当 ``param_match_method = 'remain'`` 时与 ``base_date`` 相同
            dp (pd.Series): 违约分布序列

    Returns:
        np.array: 与现金流归集表匹配的违约分布序列


    **逻辑**


    违约分布的处理上会复杂一些，分两种情况，示例如下:

    * 假设 ::

            >> df_prediction['date_'] = np.array([datetime.date(2020, 1, 31), datetime.date(2020, 2, 29), datetime.date(2020, 3, 31),
                                      datetime.date(2020, 4, 30), datetime.date(2020, 5, 31), datetime.date(2020, 6, 30),
                                      datetime.date(2020, 7, 31)])  # 现金流归集表的归集日
            >> last_pool_date = datetime.date(2019, 12, 31)  # 假设这个是上一归集日， 即 ``param_match_method = 'remain'``
            >> initial_date = datetime.date(2019, 10, 1)


    * 1. ``param_match_method = 'remain'`` ::

            >> base_date = last_pool_date
            # 此时，对违约分布的假设仅针对与现金流归集表未来期次，故理论上当期已发生违约时不能超过1。
            >> DP = pd.Series([0.2, 0.19, 0.15, 0.11, 0.09, 0.05, 0.01], index=[1, 2, 3, 4, 5, 6, 7])
            # 从 ``base_date`` 开始推 ``DP`` 的假设条件中月份对应的具体日期
            >> date_ = np.array([datetime.date(2020, 1, 31), datetime.date(2020, 2, 29),
                                   datetime.date(2020, 3, 31), datetime.date(2020, 4, 30),
                                   datetime.date(2020, 5, 31), datetime.date(2020, 6, 30),
                                   datetime.date(2020, 7, 31)], dtype=object)
            # 然后用 ``DP`` 除以每个假设期间实际的天数得到每日的违约分布值
            >> day_DP = array([0.00645161, 0.00655172, 0.00483871, 0.00366667, 0.00290323,
                               0.00166667, 0.00032258])
            # 根据 df_prediction中每个归集日所在的假设区间，根据 ``day_DP`` 得到每个归集日的违约分布值。比如说第一个归集日为 20200131恰好在DP假设中的第一个月的假设上，故第一个归集日的违约分布值为 20%， 但是如果第一个归集日在20200215，则为 0.00645161 * 31 + 0.00655172 * 15 = 0.29827571
            >> dp = array([0.2 , 0.19, 0.15, 0.11, 0.09, 0.05, 0.01]) # 最终得到的用于加压的违约分布，这里由于对归集日的假设刚好在每个月的最后一天，故最终得到的违约分布与输入的假设一样，如果归集日是半月频等，得到的就跟输入值不一样。


    * 2. ``param_match_method = 'all'`` ::

            >> base_date = initial_date
            >> DP = pd.Series([0.05, 0.05, 0.1, 0.2, 0.19, 0.15, 0.11, 0.09, 0.05, 0.01], index=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
            # 此时 ``DP`` 的假设条件中月份对应的具体日期从初始起算日开始推，并不是跟归集日期完全匹配
            >> date_ = np.array([datetime.date(2019, 11, 1), datetime.date(2019, 12, 1),
                                   datetime.date(2020, 1, 1), datetime.date(2020, 2, 1),
                                   datetime.date(2020, 3, 1), datetime.date(2020, 4, 1),
                                   datetime.date(2020, 5, 1), datetime.date(2020, 6, 1),
                                   datetime.date(2020, 7, 1), datetime.date(2020, 8, 1)], dtype=object)
            >> day_DP = np.array([0.00166667, 0.0016129 , 0.00333333, 0.00645161, 0.00612903,
                                  0.00517241, 0.00354839, 0.003     , 0.0016129 , 0.00033333])
            >> dp = array([0.19677419, 0.18989989, 0.15171301, 0.11117204, 0.09076344, 0.05123656, 0.01134409])  # 此时因为基准日期的不同，最终匹配的结果有些不同


    Notes:
        当 ``abs_calculator`` 的 ``dp_match_perfectly=True`` 时，说明输入的 ``DP`` 本身就是与现金流归集表匹配的，不需要用这个函数。


    """

    base_date = last_pool_date if param_match_method == 'remain' else initial_date
    """datetime.date, 用于计算现金流归集表未来账龄的依据：

                        * 如果假设条件是针对资产池整个生命周期，即主函数``abs_calculator`` 输入的 ``param_match_method = 'all'`` 时，设置为初始起算日，程序匹配对应月份的假设条件到资产池归集表；
                        * 如果假设条件是针对未来资产池的（当前默认是这种类型）, 即主函数``abs_calculator`` 输入的 ``param_match_method = 'remain'`` 时， 则base_date设置为最新一期现金流归集日

    """
    # 0 如果DP总和不等于100%, 则缩放在100%
    if param_match_method == 'all':
        dp = dp / dp.sum()

    if not isinstance(df_prediction.date_[0], (datetime, date)):
        df_prediction['date_'] = df_prediction['date_'].to_datetime()

    prediction = deepcopy(df_prediction)
    prediction['daycounts'] = prediction['date_'].apply(lambda x: (x-base_date).days)

    # 1 将DP拆成日的数据
    dp_copy = pd.DataFrame(columns=['day_DP', 'date_', 'daycounts'])
    dp_index = dp.index

    dp_copy['date_'] = [base_date + relativedelta.relativedelta(months=i) for i in dp_index]
    dp_copy['daycounts'] = dp_copy['date_'].apply(lambda x: (x-base_date).days)
    dp_copy['diff_'] = dp_copy['daycounts'] - dp_copy['daycounts'].shift(1)
    dp_copy.loc[0, 'diff_'] = dp_copy.loc[0, 'daycounts']
    dp_copy['day_DP'] = dp.values / dp_copy['diff_']

    day_series = pd.Series(index=range(1, max(dp_copy['daycounts'])), name='DP', dtype='float64')
    dp_copy = dp_copy.merge(day_series, left_on='daycounts', right_index=True, how='outer').reset_index(drop=True)
    dp_copy.sort_values(by=['daycounts'], ignore_index=True, inplace=True)
    dp_copy = dp_copy[['daycounts', 'day_DP']].bfill()

    # 2 根据现金流归集表的天数选择对应的DP部分加总
    prediction['DP'] = float('nan')
    last_day = (last_pool_date - base_date).days

    for ind in prediction.index:
        this_day = prediction.loc[ind, 'daycounts']
        prediction.loc[ind, 'DP'] = \
            dp_copy.loc[(dp_copy['daycounts'] > last_day) & (dp_copy['daycounts'] <= this_day), 'day_DP'].sum()
        last_day = this_day

    dp = prediction['DP']
    return dp


