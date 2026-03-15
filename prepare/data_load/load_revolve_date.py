# -*- coding: utf-8 -*-
"""循环购买相关数据，用不上
"""
import numpy as pd
import pandas as pd
from utils.timeutils import to_date2
from prepare.data_load.load_basic_data import sql_read
from prepare.sqltemplate.sqltemplate_basic import *
from prepare.data_preprocess.prep_revolving_info import transform_revolving_info, transfer_static_param


def revolve_date(project_seq, trade_date, initial_date,cur, is_mysql):
    # 循环购买的静态池、动态假设、静态假设数据
    # TODO 可以删除 ， 用不上

    sql_static_amortization = static_amortization_template(project_seq)
    df_static_amortization = sql_read(sql_static_amortization, cur, is_mysql, single_return=False)

    sql_dynamic_params = dynamic_params_template(project_seq)
    df_dynamic_params = sql_read(sql_dynamic_params, cur, is_mysql, single_return=False)

    sql_static_params = basic_assumption(project_seq)
    df_static_params = sql_read(sql_static_params, cur, is_mysql, single_return=False)

    # 9.2 历史循环购买数据导入
    sql_repurchase = repurchase_history_template(project_seq, trade_date)
    df_repurchase = sql_read(sql_repurchase, cur, is_mysql, single_return=False)


    # 循环购买数据处理 ( 这块现在不更新维护了）
    df_revolving_params = pd.DataFrame()

    # 1 循环池动态假设
    try:
        df_revolving_params = transform_revolving_info(df_static_amortization, df_dynamic_params,
                                                       initial_date)
    except:
        pass  # 循环购买的数据质量较差，如果输出不了就不要了
    # 2 循环池静态假设
    try:
        df_static_params = transfer_static_param(df_static_params)
    except:
        pass
    # 3 历史循环购买数据 todo 简化

    repurchase_temp = df_repurchase.copy()
    repurchase_temp.drop_duplicates(inplace=True)
    repurchase_temp.sort_values(by='report_date', inplace=True)
    for i in range(0, len(repurchase_temp)):
        if repurchase_temp.loc[i, ['loop_date', 'start_date', 'end_date']].isna().all():
            # 全部循环购买日期都为空时，用报告日替代循环购买日
            repurchase_temp.loc[i, 'loop_date'] = repurchase_temp.loc[i, 'report_date']

    df_repurchase = pd.DataFrame(repurchase_temp['loop_date'], columns=['pool_date'])
    df_repurchase[['revolving_principal', 'revolving_principal_interest']] = np.nan

    repurchase_temp1 = repurchase_temp[~repurchase_temp['loop_date'].isnull()]
    repurchase_temp2 = repurchase_temp[repurchase_temp['loop_date'].isnull()].reset_index(drop=True)

    repurchase_temp1.loc[:, 'loop_date'] = repurchase_temp1['loop_date'].apply(to_date2)

    if not repurchase_temp2[['start_date', 'end_date']].isna().all().all():
        repurchase_temp2.loc[:, ('start_date', 'end_date')] = \
            repurchase_temp2[['start_date', 'end_date']].applymap(lambda x: to_date2(x)
            if not str(x) == 'nan' else float('nan'))
        repurchase_temp2.loc[:, 'interval_'] = repurchase_temp2[['start_date', 'end_date']].apply(
            lambda row: (row[1] - row[0]).days, axis=1)
        repurchase_temp2 = repurchase_temp2.applymap(lambda x: float('nan') if str(x) == 'None' else x)
        repurchase_new = pd.DataFrame(columns=repurchase_temp2.columns)

        for x in range(0, len(repurchase_temp2)):
            dates_ = pd.date_range(repurchase_temp2.loc[x, 'start_date'],
                                   repurchase_temp2.loc[x, 'end_date'], freq='D')
            dates_ = [to_date2(x) for x in dates_]
            # todo 这里有点问题
            repurchase_new_1 = pd.DataFrame({'loop_date': dates_})
            repurchase_new_1.loc[:, 'revolving_principal'] = repurchase_temp2.loc[x, 'revolving_principal'] / \
                                                             repurchase_temp2.loc[x, 'interval_']
            repurchase_new_1.loc[:, 'revolving_principal_interest'] = repurchase_temp2.loc[
                                                                          x, 'revolving_principal_interest'] / \
                                                                      repurchase_temp2.loc[x, 'interval_']
            repurchase_new_1.loc[:, 'revolving_cash_out'] = repurchase_temp2.loc[x, 'revolving_cash_out'] / \
                                                            repurchase_temp2.loc[x, 'interval_']

            repurchase_new = repurchase_new.append(repurchase_new_1)
        repurchase_temp = repurchase_temp1.append(repurchase_new)
    else:
        repurchase_temp2.loc[:, 'loop_date'] = repurchase_temp2['report_date'].apply(to_date2)
        repurchase_temp = repurchase_temp1.append(repurchase_temp2)

    n = len(df_repurchase)
    for i in range(0, n):
        if i == 0:
            cond = repurchase_temp['loop_date'] <= df_repurchase.loc[i, 'pool_date']
        else:
            cond = (repurchase_temp['loop_date'] <= df_repurchase.loc[i, 'pool_date']) & \
                   (repurchase_temp['loop_date'] > df_repurchase.loc[i - 1, 'pool_date'])
        df_repurchase.loc[i, ('revolving_principal', 'revolving_principal_interest', 'revolving_cash_out')] = \
            list(repurchase_temp.loc[
                cond, ('revolving_principal', 'revolving_principal_interest', 'revolving_cash_out')].sum(
                axis=0))
    df_repurchase = df_repurchase.loc[df_repurchase[['revolving_principal', 'revolving_cash_out']].sum(axis=1) > 1,
                    :].copy()

    contingent_params_dict = {'df_static_amortization': df_static_amortization,
                              'df_dynamic_params': df_dynamic_params,
                              'df_static_params': df_static_params,
                              'df_revolving_params': df_revolving_params,
                              'df_repurchase': df_repurchase}
    return contingent_params_dict