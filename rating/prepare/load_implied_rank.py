# -*- coding: utf-8 -*-
"""

author: Double Q
"""
import pandas as pd
from rating.prepare.config import implied_rank_track


def load_implied_rank(date):
    """从估值结果监控中读取当日隐含评级，用于结果比对

    Args:
        date (str): YYYYMMDD

    Returns:

    """

    file_ = "%s\估值结果监控%s.txt" %(implied_rank_track, date)
    df = pd.read_csv(file_, header=0, usecols=['收益率曲线', '一级分类', '二级分类', '市场', '上海代码', '深圳代码', '银行间代码',
                                               '固定收益平台代码', '综合协议平台代码', '京市代码',
                                               '债券全称'], sep=',', index_col=False, dtype={'固定收益平台代码': 'Int64',
                                                                                            '综合协议平台代码': 'Int64',
                                                                                             })

    df = df[df['一级分类'] == '资产支持证券'].copy()
    df.loc[:, '代码'] = df['上海代码'].fillna(df['深圳代码']).fillna(df['银行间代码']).fillna(df['固定收益平台代码']).fillna(df['综合协议平台代码'])
    df.loc[:, '后缀'] = df['市场'].apply(lambda x: {"沪市": 'SH', "深市": "SZ", "银行间": "IB"}[x])
    df.loc[:, 'security_code'] = df["代码"].astype(str) + "." + df["后缀"]
    df.dropna(subset=['收益率曲线'], axis=0, inplace=True)
    df.loc[:, 'implied_rank'] = df['收益率曲线'].str.split("_").str[-1]
    df.loc[:, 'date'] = date
    df.loc[:, 'implied_rank'] = df['implied_rank'].str.upper()

    return df[['security_code', 'implied_rank', 'date']].reset_index(drop=True)
