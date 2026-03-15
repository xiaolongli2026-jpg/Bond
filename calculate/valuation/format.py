# -*- coding: utf-8 -*-
"""将现金流分配过程得到数据处理成需要的输出格式，如上传的格式或者展示的格式

"""
import datetime

import pandas as pd
from main.abs_calculator.result_warning import result_check


def upload_cf_format(tranches_obj, df_prediction, df_schedule, security_seqs, project_seq, trade_date):
    # 整理出需要上传的现金流
    security_result = cf_table(tranches_obj, trade_date, security_seqs)
    pool_result = cf_pool(df_prediction, project_seq, df_schedule)
    return security_result, pool_result


def chinese_cf_format(tranches_obj, df_prediction, df_schedule, project_name):
    security_result = cf_table_chinese(tranches_obj, df_schedule, project_name)
    pool_result = cf_pool_chinese(df_prediction, project_name)

    return security_result, pool_result


def cf_table(tranches_obj, trade_date, security_seqs):
    """整理上传用的证券端现金流数据，现金流单位为元/张

    Args:
        tranches_obj (class):
        trade_date (str): 交易日期
        security_seqs (dict): 证券代码跟证券序号的关系

    Returns:
        pd.DataFrame: df_upload
    """

    df_upload = pd.DataFrame()
    levels = tranches_obj.all_levels
    for level_ in levels:
        obj_ = getattr(tranches_obj, level_)
        df_one = pd.DataFrame(
            {'CF_DATE': [x if str(x) != 'nan' else y for (x, y) in zip(obj_.int_pay_dates, obj_.prin_dates)],
             'ACCRUAL': obj_.int_payments if not obj_.is_sub else obj_.int_payments + obj_.fcc_payments + obj_.exret_payments,
             'PRINCIPAL': obj_.prin_payments,
             'CASH_FLOW': obj_.int_payments + obj_.prin_payments if not obj_.is_sub
             else obj_.int_payments + obj_.prin_payments + obj_.fcc_payments + obj_.exret_payments,
             "PRIN_REPLENISH": obj_.prin_complement,
             "INT_REPLENISH": obj_.int_complement,
             "EXRET_PAYMENT": obj_.exret_payments if obj_.is_sub else 0.,
             "FCC": obj_.fcc_payments if obj_.is_sub else 0.
             })
        df_one.loc[:, 'SECURITY_SEQ'] = security_seqs[obj_.security_id]
        df_one[['ACCRUAL', 'PRINCIPAL', 'CASH_FLOW', 'INT_REPLENISH', 'PRIN_REPLENISH', 'EXRET_PAYMENT', 'FCC']] = \
            df_one[['ACCRUAL', 'PRINCIPAL', 'CASH_FLOW', 'INT_REPLENISH', 'PRIN_REPLENISH',
                    'EXRET_PAYMENT', 'FCC']] / obj_.current_vol

        df_upload = df_upload.append(df_one)

    df_upload.loc[:, "TRADE_DATE"] = trade_date
    df_upload.dropna(subset=['CF_DATE'], axis=0, inplace=True)
    df_upload.loc[:, 'CF_DATE'] = [x.strftime("%Y%m%d") for x in df_upload['CF_DATE']]
    df_upload.reset_index(drop=True, inplace=True)
    del_row = df_upload[df_upload['CASH_FLOW'] == 0].index
    df_upload.drop(index=del_row, inplace=True)

    return df_upload


def cf_table_chinese(tranches_obj, df_schedule, project_name):
    """整理中文证券端现金流数据，现金流单位：元。相对于上传版本，会额外输出超额收益、固定资金成本列

    Args:
        tranches_obj (class):
        df_schedule (pd.DataFrame): 日历表
        project_name (str): 项目名称

    Returns:
        pd.DataFrame:
    """

    # 整理证券端现金流结果， 中文版，将证券端的利息更详细的分成利息、超额收益、固定资金成本
    data_full = pd.DataFrame()

    for level_ in tranches_obj.all_levels:
        data = pd.DataFrame({"偿付日期": df_schedule.loc[:, 'tax_exp_date']})  # 使用税费支付日期
        tr = getattr(tranches_obj, level_)
        subord = tr.subordination.capitalize()
        data.loc[:, '等级'] = subord
        data.loc[:, '证券代码'] = tr.security_id
        subLevel = tr.is_sub
        data.loc[:, "本金兑付"] = tr.prin_payments
        data.loc[:, "利息兑付"] = tr.int_payments
        data.loc[:, "本金补充兑付"] = tr.prin_complement
        data.loc[:, "利息补充兑付"] = tr.int_complement
        data.loc[:, "期末本金余额"] = tr.balance_records
        data.loc[:, "期末本金余额"] = data["期末本金余额"].ffill()
        data.loc[:, "期初本金余额"] = data["期末本金余额"] + data['本金兑付']
        data.loc[:, "单位本金兑付"] = tr.prin_payments / tr.current_vol
        data.loc[:, "单位利息兑付"] = tr.int_payments / tr.current_vol
        data.loc[:, "单位本金补充兑付"] = tr.prin_complement / tr.current_vol
        data.loc[:, "单位利息补充兑付"] = tr.int_complement / tr.current_vol
        data.loc[:, "单位期末本金余额"] = data["期末本金余额"] / tr.current_vol
        data.loc[:, "单位期初本金余额"] = data["期初本金余额"] / tr.current_vol
        if tr.amort_type not in ('pass_through', 'once'):
            data.loc[:, '目标余额'] = tr.target_balances
            data.loc[:, '目标本金偿付'] = tr.prin_due_payments

        if subLevel:
            data.loc[:, "超额收益"] = tr.exret_payments
            data.loc[:, "固定资金成本"] = tr.fcc_payments
            data.loc[:, "单位超额收益"] = tr.exret_payments / tr.current_vol
            data.loc[:, "单位固定资金成本"] = tr.fcc_payments / tr.current_vol

        data_full = data_full.append(data)

    cols = ['本金兑付', '利息兑付']
    if '超额收益' in data_full.columns:
        cols = cols + ['超额收益', '固定资金成本']

    data_full = data_full.loc[data_full[cols].fillna(0.).sum(axis=1) > 0.1, :].reset_index(drop=True)
    data_full['项目名称'] = project_name
    return data_full


def cf_pool(prediction, project_seq, df_schedule):
    """整理上传的资产池现金流

    Args:
        prediction (pd.DataFrame): 现金流归集表
        project_seq (str): 项目内码
        df_schedule (pd.DataFrame): 日历

    Returns:
        pd.DataFrame:
    """
    if len(prediction) > 0:
        df = prediction.copy()
        df.loc[:, 'is_rev_period'] = df_schedule['is_revolving_period']
        df.loc[:, 'project_seq'] = project_seq
        df.rename(columns={'date_': 'check_date'}, inplace=True)

        df.loc[:, ['check_date']] = \
            df[['check_date']].applymap(lambda x:
                                                         x.strftime("%Y%m%d") if str(x) !='nan' else float('nan'))

        cols = ['project_seq', 'check_date', 'begin_principal_balance', 'current_principal_due', 'current_interest_due',
                'recycle_amount', 'prepay_amount', 'default_amount', 'begin_default_balance',
                'end_default_balance', 'exp_paid', 'tax_paid', 'exp_paid_liability', 'tax_paid_liability',
                'incentive_fee', 'current_revolving_out',
                'end_principal_balance', 'event']
        df = df.loc[:, set(cols).intersection(set(df.columns))]  # 去掉多余列
        prediction_cols = df.columns
        df.columns = [x.upper() for x in prediction_cols]
        df = df.loc[~pd.isna(df[['CHECK_DATE']]).all(axis=1), :]
    else:
        df = pd.DataFrame()

    return df


def cf_pool_chinese(prediction, project_name):
    """整理中文的资产池现金流

    Args:
        prediction (pd.DataFrame): 现金流归集表
        project_name (str): 项目名称

    Returns:
        pd.DataFrame:
    """
    # 整理现金流归集表(去掉空行, 提取所需列)
    if len(prediction) > 0:
        df = prediction.copy()

        cols = ['DATE_', 'BEGIN_PRINCIPAL_BALANCE', 'CURRENT_PRINCIPAL_DUE', 'CURRENT_INTEREST_DUE',
                'RECYCLE_AMOUNT', 'PREPAY_AMOUNT', 'DEFAULT_AMOUNT', 'BEGIN_DEFAULT_BALANCE',
                'END_DEFAULT_BALANCE', 'EXP_PAID', 'TAX_PAID', 'EXP_PAID_LIABILITY', 'TAX_PAID_LIABILITY',
                'INCENTIVE_FEE', 'CURRENT_REVOLVING_OUT',
                'END_PRINCIPAL_BALANCE', 'EVENT']

        chinese_cols = ['归集日期', '期初本金余额', '本期应收本金', '本期应收利息', '回收金额',
                        '早偿金额', '违约金额', '期初违约本金余额', '期末违约本金余额', '实缴费', '实缴税', '应缴费', '应缴税',
                        '超额激励',  '循环购买支出', '期末本金余额', '发生重大事项枚举值']
        df.columns = [x.upper() for x in df.columns]
        df = df.loc[:, set(cols).intersection(set(df.columns))]  # 去掉多余列
        dict_ = dict(zip(cols, chinese_cols))
        df.columns = [dict_[x] for x in df.columns]
        pool_result = df.loc[df[['本期应收本金', '本期应收利息', '回收金额',
                       '早偿金额', '违约金额', '实缴费', '实缴税']].sum(axis=1) > 0.1, :]
        pool_result = pool_result.sort_values(by='归集日期', ignore_index=True)
        pool_result['项目名称'] = project_name
        return pool_result
    else:
        return pd.DataFrame()


def warns_table(project_seq, date_, *args):
    """整理数据问题信息为可上传版本

    Args:
        project_seq: 项目内码
        date_: 日期
        *args: 数据问题构成的list

    Returns:
        pd.DataFrame:
    """
    warn_lst = []
    for x in args:
        warn_lst = warn_lst + x

    df_warns = pd.DataFrame(warn_lst, columns=['WARNING_TYPE', 'WARNING_DETAIL'])
    if len(df_warns) > 0:
        df_warns.loc[:, 'PROJECT_SEQ'] = project_seq
        df_warns.loc[:, 'DATE'] = date_
    return df_warns


def markov_transfer_prob_format(df_transfer_prob):
    """整理马尔可夫模型的转移参数为上传版本

    """
    df = df_transfer_prob.copy()
    df.rename(columns={'prob_type': 'TRANSFER_TYPE', 'prob_num': 'VALUE'}, inplace=True)

    return df


def regression_coef_format(project_seq, cdr_coef, ucpr_coef):
    """将程序中得到的回归模型参数转化为上传格式

    Args:
        project_seq (str): 项目内码
        cdr_coef (pd.DataFrame): cdr回归系数
        ucpr_coef (pd.DataFrame): ucpr回归系数

    Returns:
        pd.DataFrame:
    """
    reflect_ = {'WAIR': 1, 'WAM': 2, 'WAM^2': 3, 'PREVIOUS': 4, 'SPREAD': 5, 'POPB': 6, 'LTV': 7, 'AGE': 8, 'AGE^2': 9,
                'RTL': 10, 'INTERCEPT': 11}

    # TODO 映射
    df = pd.DataFrame({'FACTOR_TYPE': [reflect_[x.upper()] for x in cdr_coef.columns], 'VALUE': cdr_coef.loc[0, :].values},
                      index=range(cdr_coef.shape[1]))
    df.loc[:, 'Y'] = '1'
    df1 = pd.DataFrame({'FACTOR_TYPE': [reflect_[x.upper()] for x in ucpr_coef.columns],
                        'VALUE': ucpr_coef.loc[0, :].values}, index=range(ucpr_coef.shape[1]))
    df1.loc[:, 'Y'] = '2'
    df = df.append(df1)
    df.loc[:, 'PROJECT_SEQ'] = project_seq
    return df


def predict_result_format(assumptions, project_seq):
    """整理模型预测得到的参数序列为上传版本，从无单位转为 %

    Args:
        assumptions (dict):
        project_seq (str):

    Returns:
    """
    result_dict = {}
    keys_ = [x.upper() for x in assumptions.keys()]
    suppose = dict(zip(keys_, assumptions.values()))
    suppose['DATE'] = suppose['DATE_'] if suppose.get('DATE_', None) is not None else suppose.get('DATE', None)
    suppose['DATE'] = [x.strftime("%Y%m%d") if isinstance(x, datetime.date) else x for x in suppose['DATE']]
    suppose['DP'] = suppose['DP'] if suppose.get('DP', None) is not None else suppose.get('DPS', None)
    if suppose['DATE'] is None:
        raise ValueError('没有假设参数日期列')

    coff_names = ['DP', 'UCPRS', 'CPRS', 'MDRS', 'SMDRS', 'SMMS', 'USMMS']  #在计算过程中都是无单位的，最后输出有单位的
    for x in coff_names:
        if suppose.get(x, None) is not None:
            str_ = ";".join([str(x) + "," + str(round(y, 4) * 100) for (x, y) in zip(suppose['DATE'], suppose[x])])
            result_dict[x] = str_

    if 'CDR' in suppose:
        result_dict['CDR'] = (suppose['CDR'][0] if type(suppose['CDR']) == list else suppose['CDR']) * 100

    df = pd.DataFrame(result_dict, index=[0])
    df.loc[:, 'PROJECT_SEQ'] = project_seq

    return df


def data_match_table(df_other, trade_date, project_seq):
    """日期匹配表

    """
    df_ = df_other[["has_prediction_duration",
    "has_pool_duration",
    "has_security_duration",
    "prediction_report_date",
    "pool_report_date",
    "history_report_date",
    "history_date",
    "history_count_date",
    "history_report_mismatch",
    "possible_match_count_date"]].copy()

    result = pd.DataFrame({"PROJECT_SEQ": project_seq,
                           "DATE": trade_date,
                           "REPORT_DATE_PREDICTION":
                               float('nan') if not df_.loc[0, 'has_prediction_duration']
                               else df_['prediction_report_date'],
                           "REPORT_DATE_SECURITY":
                               float('nan') if not df_.loc[0, 'has_security_duration']
                               else df_['history_report_date'],
                           "REPORT_DATE_ASSET":
                               float('nan') if not df_.loc[0, 'has_pool_duration']
                               else df_['pool_report_date'],
                           "PERIOD_END_DATE_SECURITY":
                               float('nan') if not df_.loc[0, 'has_security_duration']
                               else df_['history_count_date'],
                           "PERIOD_END_DATE_ASSET":
                               float('nan') if not df_.loc[0, 'has_pool_duration']
                               else df_['history_date'],
                           "POTENTIAL_MISMATCH": '1' if df_.loc[0, 'history_report_mismatch'] else '0',
                           "POSSIBLE_END_DATE_SECURITY": df_.loc[0, 'possible_match_count_date']
                           if df_.loc[0, 'history_report_mismatch'] else None
                           }, index=[0])

    return result


def valuation_checked(df_derivatives, df_other, distribution_info, df_tranches, tranches_obj, is_npls, security_seqs):
    """进行结果阈值检查并返回整理的表格

    """
    if df_derivatives is not None:
        df_ = df_derivatives[['security_code', 'ratio_cash_face', 'cash_per_sec', 'cal_expiry_date', 'date_shift']]
    else:
        df_ = pd.DataFrame()
    msg_, pass_info, msg_project = result_check(df_other, df_tranches, tranches_obj, distribution_info, is_npls)
    df_2 = pd.DataFrame.from_dict(msg_, orient='index')
    df_2 = df_2.merge(pd.DataFrame.from_dict(pass_info, orient='index'), left_index=True, right_index=True, how='outer')
    df_2.reset_index(drop=False, inplace=True)
    df_2.columns = ['security_code', 'aberration_reason', 'pass']

    if len(df_) > 0:
        df_ = df_.merge(df_2, how='outer', on='security_code')
    else:
        df_ = df_2

    # 是否有补充支付过
    cpl_ = []
    cpl_amount = []
    for level_ in df_tranches['security_level']:
        tr = getattr(tranches_obj, level_)
        cpl_amount.append(getattr(tr, 'complement_amount', 0))
        cpl_.append('1' if getattr(tr, 'complement_amount', 0) > 0 else '0')

    df_temp = pd.DataFrame({'security_code': df_tranches['security_code'], 'have_replenish': cpl_,
                            'replenish_amout': cpl_amount})
    df_ = df_.merge(df_temp, how='left', on='security_code')
    if len(df_) > 0:
        df_['security_seq'] = df_['security_code'].apply(lambda x: security_seqs[x])
        df_.drop(columns=['security_code'], inplace=True)
    df_.columns = [x.upper() for x in df_.columns]

    # 项目层面的检查指标
    df_project_warn = pd.DataFrame()

    df_project_warn[['adjust_factor', 'pool_divide_security']] = df_other[['adjust_factor', 'pool_divide_security']]
    df_project_warn['ratio_asset_sec'] = df_project_warn['pool_divide_security'] / df_project_warn['adjust_factor']
    df_project_warn['aberration_reason_project'] = ",".join(distribution_info[df_tranches.loc[0, 'project_seq']] +
                                                            msg_project)

    df_project_warn['gap'] = getattr(tranches_obj, 'excess_payment', 0)  # 资金缺口
    df_project_warn['project_seq'] = df_tranches.loc[0, 'project_seq']
    df_project_warn.columns = [x.upper() for x in df_project_warn.columns]

    return df_, df_project_warn


def valuation_result_standardize(valuation_results, derivative_results, security_seqs):
    """整理估值结果格式
    """
    val_ = valuation_results.copy()
    if valuation_results is not None:

        val_['security_seq'] = val_['security_code'].apply(lambda x: security_seqs[x])
        val_ = val_[['security_seq', 'accrual_interest', 'clean_price', 'price']]
        val_.columns = [x.upper() for x in val_.columns]

    deri_ = derivative_results.copy()
    if derivative_results is not None:

        deri_['security_seq'] = deri_['security_code'].apply(lambda x: security_seqs[x])
        deri_ = deri_[['security_seq', 'duration', 'convexity', 'wal', 'coverage_ratio', 'whole_coverage']]
        deri_.columns = [x.upper() for x in deri_.columns]

    return val_, deri_



