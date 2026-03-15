# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from utils.timeutils import to_date2
from utils.miscutils import isnull
from abs.doc import enumerators


def transform_product(df_product, max_maturity_date):
    """
    处理项目基本数据

    Args:
        df_product (pd.DataFrame): 项目基本数据
        max_maturity_date (datetime.date): 所有证券预期到期日中最晚的一个

    Returns:
        pd.DataFrame: 经过处理的项目基本数据

    """
    warns_lst = []
    # 1.1 基本要素表信息检查
    date_cols = ['initial_date', 'interest_start_date', 'first_pay_date', 'legal_due_date']

    df_product.loc[:, date_cols] = df_product.loc[:, date_cols].applymap(
        lambda x: np.nan if pd.isna(x) else to_date2(x))

    # 1.2 循环购买起始日和结束日检查
    if not isnull(df_product.loc[0, 'revolving_expiry_date']):
        df_product.loc[0, 'revolving_expiry_date'] = to_date2(df_product.loc[0, 'revolving_expiry_date'])
        if (df_product.loc[0, 'revolving_expiry_date'] > df_product.loc[0, 'legal_due_date']) or \
                (df_product.loc[0, 'revolving_expiry_date'] > max_maturity_date):
            raise ValueError("循环购买的循环期结束日超过了次级预期到期日或者项目法定清算日，请假查日期数据是否正确")
    else:
        if df_product.loc[0, 'is_revolving_pool']:
            df_product.loc[0, 'revolving_expiry_date'] = pd.Timestamp(df_product.loc[0, 'start_revolving_purchase_date']) \
                                                         + pd.Timestamp(months=int(df_product.loc[0, 'revolving_period_in_month']))

    if not isnull(df_product.loc[0, 'start_revolving_purchase_date']):
        df_product.loc[0, 'start_revolving_purchase_date'] = to_date2(
            df_product.loc[0, 'start_revolving_purchase_date'])

    # 1.3 是否不良贷款
    df_product.loc[0, 'is_npls'] = (df_product.loc[0, 'secondary_classification'] == enumerators.SecondClass['NPLS'])

    return df_product, warns_lst