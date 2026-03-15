"""
日期运算函数
"""

import re
import calendar
import numpy as np
from warnings import warn
import datetime
from typing import Union
import pandas as pd
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, DAILY, MO, TH, WE, TU, FR


def to_date(s):
    """
    Transform string into datetime.date object
        "2019-01-01" -> date(2019,1,1)
        "2019/1/1" -> date(2019,1,1)
        "20190101" -> date(2019,1,1)
        20190101 -> date(2019,1,1)
    """
    if isinstance(s, datetime.date):
        return s

    s = str(s)

    try:
        if "-" in s:
            ss = s[:10].split(sep="-")
            y, m, d = [int(x) for x in ss]
            return datetime.date(y, m, d)

        elif "/" in s:
            ss = s.split(sep="/")
            y, m, d = [int(x) for x in ss]
            return datetime.date(y, m, d)

        else:
            n = int(s[:8])
            y = n // 10000
            m = (n % 10000) // 100
            d = n % 100
            return datetime.date(y, m, d)

    except Exception as e:
        raise ValueError(f"Cannot parse date {s} because {e}")


def to_date2(s):
    """
    在to_date的基础上，兼容"八位数字+X个工作日"的格式
    有此格式的字段：首次偿付日、法定到期日、循环期届满日

    """

    s = str(s).strip("'")
    if re.fullmatch(r'\d{8}\+\d{1,2}个工作日', s):
        start_date = to_date(s[:8])
        business_days = int(s[9:-4])
        # 只考虑周末，暂未考虑其他假期
        end_date = rrule(freq=DAILY, dtstart=start_date,
                         interval=business_days, byweekday=(MO, TU, WE, TH, FR))[1].date()
        return end_date
    else:
        return to_date(s)


def try_to_date(x):
    """
     估值库有 20130229

    """
    try:
        return to_date(x)
    except:
        return float('nan')


def get_payment_dates(start_date, end_date, tenor, freq, add_one):
    """
    从起始日开始推算出日期序列

    Args:
        start_date (datetime.date): 起始日
        end_date (datetime.date) 结束日
        tenor: int, 频率
        freq: str, tenor的单位 Y-年,M-月,W-周,D-日
        add_one (bool): 如果end_date不是刚好与频率重合,是否多加一个日期

    Returns:
        np.array: 日期序列
    """

    total_days = (end_date - start_date).days
    if freq == 'D':
        number_ = total_days // tenor + 1 # 统一多加一期
        dates = np.array([start_date + relativedelta(days=i * tenor) for i in range(0, number_ + 1)])
    elif freq == 'W':
        number_ = (total_days // 7 + 1) // tenor + 1
        dates = np.array([start_date + relativedelta(weeks=i * tenor) for i in range(0, number_ + 1)])

    elif freq == 'M':
        number_ = (total_days // 30 + 1) // tenor + 1
        dates = np.array([start_date + relativedelta(months=i * tenor) for i in range(0, number_ + 1)])
    elif freq == 'Y':
        number_ = (total_days // 365 + 1) // tenor + 1
        dates = np.array([start_date + relativedelta(year=i * tenor) for i in range(0, number_ + 1)])
    else:
        raise ValueError("Cannot parse the frequency unit \"{}\".".format(freq))

    payment_dates = dates[dates <= end_date]
    if add_one and (end_date not in dates):
        extra_one = dates[dates > end_date].min()
        payment_dates = np.append(payment_dates, extra_one)

    return payment_dates


def holiday_adjsut(date_series: np.ndarray, schedule: pd.DataFrame, holiday_rule='forward', ignores: np.ndarray=None) -> np.ndarray:
    """
    调整日期序列的

    Args:
        date_series (np.array): 日期序列，可能为节假日。不能有重复值，即使有也会被忽略. 日期值为 datetime.date 格式
        schedule (pd.DataFrame): 日历，包括 date_(datetime.date), is_workday (bool值) 列, date列必须覆盖并超出整个 `date_series` 范围, all_date列无重复值
        holiday_rule (str): 日期调整方式，forward-遇节假日后推到最近一个工作日， backward-遇节假日后推到上一个工作日
        ignores (np.array): 日期中不需要调整的部分

    Returns:
        np.array: 调整后序列

    """
    if schedule is None:
        return date_series

    schedule.sort_values(by='date_', ignore_index=True, inplace=True)
    schedule.reset_index(drop=True, inplace=True) # 日历的index需要为0开始的连续值，date_列从小到大排序

    if ignores is not None:
        dates = np.setdiff1d(date_series, ignores, assume_unique=False)
    else:
        dates = date_series.copy()

    all_dates = np.array(schedule['date_'].values)
    bools = np.array(schedule['is_workday'].values)
    indexs = np.array(schedule.index)
    idx_limit = len(bools) - 1
    adjusted_dates = dates.copy()
    min_ = schedule.loc[0, 'date_']
    max_ = schedule['date_'].iloc[-1]

    for d_ in range(len(dates)):
        if dates[d_] < min_ or dates[d_] > max_:
            continue

        idx = np.where(all_dates == dates[d_])[0][0]
        adjust_ = not bools[idx]

        if adjust_:

            if holiday_rule == 'forward':
                extract_idxs = indexs[idx: min(idx+15, idx_limit)]

                if bools[extract_idxs].any():
                    adjust_to_idx = extract_idxs[bools[extract_idxs]][0]
                    adjusted_dates[d_] = all_dates[adjust_to_idx]
                else:
                    raise IndexError(f"工作日日历长度不足，无法调整日期 {dates[d_]}")

            elif holiday_rule == 'backward':
                extract_idxs = indexs[max(idx-15, 0): idx]

                if bools[extract_idxs].any():
                    adjust_to_idx = extract_idxs[bools[extract_idxs]][-1]
                    adjusted_dates[d_] = all_dates[adjust_to_idx]
                else:
                    raise IndexError(f"工作日日历长度不足，无法调整日期 {dates[d_]}")

            else:
                raise ValueError(f'unsupported holiday rule : {holiday_rule}')

    if ignores is not None:
        return np.sort(np.append(adjusted_dates, ignores))
    else:
        return np.sort(adjusted_dates)


def count_year(date1, date2, daycount="ACT/365", day_count_method='begin'):
    """
    计算年化天数 （算头不算尾），date2>date1

    Args:
        date1 (datetime.date): 起始日期
        date2 (datetime.date): 截止日期
        daycount (str): 计息基准 ，eg. ACT/365, ACT/ACT, ACT/360, 30/360
        day_count_method (str) :

                                * 'begin' - 算头不算尾
                                * 'end' - 算尾不算头
                                * 'bilateral' - 算头尾

    Returns:
        float: 年化天数
    """
    if day_count_method == 'bilateral':
        add_ = 1  # 需要多算一天
    else:
        add_ = 0

    daycount_upper = str(daycount).upper()

    if daycount_upper == "ACT/365":
        return ((date2 - date1).days + add_) / 365.0

    elif daycount_upper == "ACT/360":
        return ((date2 - date1).days + add_) / 360.0

    elif daycount_upper == "ACT/ACT":
        # 以计息起始日所在年份的总天数作为分母
        year = date1.year
        if (year % 400 == 0) or ((year % 4 == 0) and (year % 100 != 0)):
            return ((date2 - date1).days + add_) / 366.0
        else:
            return ((date2 - date1).days + add_) / 365.0

    elif daycount_upper == "30/360":
        # Reference: http://eclipsesoftware.biz/DayCountConventions.html#x3_02
        y1, m1, d1 = date1.year, date1.month, date1.day
        y2, m2, d2 = date2.year, date2.month, date2.day
        d1 = min(d1, 30)
        d2 = min(d2, 30)
        return y2 - y1 + (m2 - m1) / 12. + (d2 - d1 + add_) / 360.

    else:
        warn(f"Day-count convention '{daycount}' not supported, use ACT/365.")
        return ((date2 - date1).days + add_) / 365.0  # 暂时对其他情况(包括 None和 np.nan)采用默认 daycount 规则


def get_latest_month_end(d):
    """
    获取上一个月的月末

    Args:
        d (datetime.date): 日期

    Returns:
        datetime.date:  上一个月月末

    """
    if not (d + relativedelta(days=1)).day == 1:  # 当d不是月末时
        d = d - relativedelta(days=d.day)  # 把d改成上月月末
    return d


def get_this_month_end(d):
    """
    获取当月的月末

    Args:
        d (datetime.date): 日期

    Returns:
        datetime.date:  当月月末

    """
    d = datetime.date(d.year, d.month, 1) + relativedelta(months=1, days=-1)
    return d


def count_month(d1, d2):
    """
    计算两日之间准确月数,算尾不算头 。可以d1>d2, 此时返回负月数

    Args:
        d1 (datetime.date): 起始日期
        d2 (datetime.date): 截止日期

    Returns:
        float: 月数
    """

    if d1 < d2:
        m = c(d1, d2)
    elif d1 > d2:
        m = c(d2, d1) * (-1)
    else:
        m = 0

    return m


def c(d1, d2):
    """
    计算两日之间准确月数,算尾不算头，必须满足d1<d2

    Args:
        d1 (datetime.date): 起始日期
        d2 (datetime.date): 截止日期

    Returns:
        float: 月数

    """
    days1 = calendar.monthrange(d1.year, d1.month)[1]
    days2 = calendar.monthrange(d2.year, d2.month)[1]

    if d1.year == d2.year:
        if d1.month == d2.month:
            m = (d2 - d1).days / days1
        elif d2.month - d1.month == 1:
            m = (days1 - d1.day) / days1 + d2.day / days2
        elif d2.month - d1.month > 1:
            m = (days1 - d1.day) / days1 + d2.day / days2 + \
                (d2.month - d1.month - 1)
    elif d1.year < d2.year:
        m = (days1 - d1.day) / days1 + d2.day / days2 + \
            (12 - d1.month + d2.month - 1) + (d2.year - d1.year - 1) * 12
    return m


def count_months(base_date, dates_):
    """
    以 ``count_month`` 计算时间序列与基准日之间的准确月数, 算尾不算头， 如果序列中有日期早于基准日，则对应返回的月数为负值

    Args:
        base_date (datetime.date): 基准日
        dates_ (np.array): 日期序列,可以有空值 float('nan') ， 对应返回的月数也为空值

    Returns:
        np.array:
    """
    result = np.array([count_month(base_date, x) if str(x) != 'nan' else float('nan') for x in dates_])
    return result


def age_(d1, d2):
    """
    计算两个日期之间的整数月份,即 ``count_month`` 得到的月数进行四舍五入

    Args:
        d1 (datetime.date): 起始日期
        d2 (datetime.date): 截止日期

    Returns:
        int: 整数月数
    """
    try:
        d1 = to_date2(d1)
        d2 = to_date2(d2)
        m = count_month(d1, d2)
        m = int(np.round(m))

        return m
    except:
        return float('nan')


def dates_compare(date_series1, date_series2, orient='forward', number=3, base_date=None, end_date=None):
    """
    对比两个日期序列的部分日期是否一致

    Args:
        base_date (datetime.date): 基准日，对比这一日期前后的数据, 为 `None` 则直接从头或者尾开始对比
        date_series1 (np.array): 日期序列，日期均为 `datetime.date` 格式,不接受空值 float('nan'), 但是 `date_series1` 和 `date_series2` 可以长度不同
        date_series2 (np.array): 日期序列
        orient (str): 'forward' -对比基准日以后的日期, 'backward' -对比基准日以前的日期
        number (int): 比对的日期数量的数量，比如某个基准日期开始后的 `n` 个日期都一致，则输出 `True`
        end_date (datetime.date): 比对区间的另一端，如果有 则忽略number

    Returns:
        bool: 两个日期序列指定范围内的日期是否一致

    """
    date_series1 = np.unique(date_series1)
    date_series2 = np.unique(date_series2)
    date_series1 = np.sort(date_series1)
    date_series2 = np.sort(date_series2)

    len1 = len(date_series1)
    len2 = len(date_series2)
    if (len1 < 1 or len2 < 1):
        if len1 < 1 and len2 < 1:
            return True
        else:
            return False

    if base_date is None:
        if orient == 'backward':
            base_date = min(max(date_series1), max(date_series2))
        else:
            base_date = max(min(date_series1), min(date_series2))

    same_ = True


    if orient == 'forward':
        if end_date is None:
            future1 = date_series1[date_series1 >= base_date]
            future2 = date_series2[date_series2 >= base_date]
            future1_cmpr = future1[0: min(number, len(future1))]
            future2_cmpr = future2[0: min(number, len(future2))]
        else:
            future1_cmpr = date_series1[np.unique(np.intersect1d(np.where(date_series1 >= base_date), np.where(date_series1 <= end_date)))]
            future2_cmpr = date_series2[np.unique(np.intersect1d(np.where(date_series2 >= base_date), np.where(date_series2 <= end_date)))]
        if len(future1_cmpr) == len(future2_cmpr):
            same_ = same_ and (future1_cmpr == future2_cmpr).all()
        else: #  长度不一样，序列一定不一样
            same_ = False
    elif orient == 'backward':
        if end_date is None:
            past1 = date_series1[date_series1 <= base_date]
            past2 = date_series2[date_series2 <= base_date]
            past1_cmpr = past1[max(0, len(past1)-number): len(past1)]
            past2_cmpr = past2[max(0, len(past2)-number): len(past2)]
        else:
            past1_cmpr = date_series1[np.unique(np.intersect1d(np.where(date_series1 >= end_date), np.where(date_series1 <= base_date)))]
            past2_cmpr = date_series2[np.unique(np.intersect1d(np.where(date_series2 >= end_date), np.where(date_series2 <= base_date)))]

        if len(past1_cmpr) == len(past2_cmpr):
            same_ = same_ and (past1_cmpr == past2_cmpr).all()
        else:
            same_ = False
    return same_


def trans_date_class(x) -> Union[datetime.date]:
    """ 将序列中的日期格式统一为 datetime.date """
    if isinstance(x, pd.Timestamp) or isinstance(x, datetime.datetime):
        return x.date()
    elif isinstance(x, datetime.date):
        return x
    else:
        return np.nan
