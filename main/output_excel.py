# -*- coding: utf-8 -*-
"""将指定日期指定模块的计算结果导出到excel

需要注意的是估值、评级模块会自动覆盖早期的测试结果，所以如果output的不是当天的测试结果，必须要重新弄运行批量测试测序才能导出对应日期的数据
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from conf.conf import local_db, local_db2, default_db, EXCEL_DIR
from utils.db_util import UploadLib
from conf.table_structure import table_info


# trade_date = datetime.now().strftime("%Y%m%d")
trade_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
# trade_date = '20240301'
test_module = 'valuation'  # 可选 valuation-估值、rating-评级、model-模型、user-输入模块测试
model_type = 'markov'  # test_module 为 model 时，可选 extrapolation_model-外推法、markov_model-马尔可夫、linear_model-线性回归、sigmoid_model-逻辑回归


def table_name_transfer(table_name, columns):
    tables = table_info()
    tuples = tables[table_name]
    dict_ = {row[0]: row[4] for row in tuples}
    dict_.update({"PROJECT_NAME": "项目名称", "SECURITY_CODE": "证券代码", 'IS_RECURRING_POOL':"是否循环购买"})
    return [dict_[x.upper()] for x in columns]

# trade_date = input("请输入提取数据日期：")
# test_type = input("测试模块(选项：估值、评级、数据检查、模型、输入模块测试): ")
# model_type = None
# if test_type == '估值':
#     test_module = 'valuation'
# elif test_type == '评级':
#     test_module = 'rating'
# elif test_type == '数据检查':
#     test_module = 'data_check'
# elif test_type == '输入模块测试':
#     test_module = 'user'
# elif test_type == '模型':
#     test_module = 'model'
#     model_ = input("请输入模型（选项:外推法、马尔可夫、线性回归、逻辑回归）")
#     if model_ == '外推法':
#         model_type = 'extrapolation_model'
#     elif model_ == '线性回归':
#         model_type = 'linear_model'
#     elif model_ == '逻辑回归':
#         model_type = 'sigmoid_model'
#     elif model_ == '马尔可夫':
#         model_type = 'markov_model'
#     else:
#         raise ValueError("错误输入")
#
# else:
#     raise ValueError("错误输入")


if test_module == 'valuation' or test_module == 'rating':
    db_local = UploadLib(local_db)
else:
    db_local = UploadLib(local_db2)

db_local.connect()
# 保存
db_cloud = UploadLib(default_db)
db_cloud.connect()
security_code_reflections = \
    db_cloud.select("""SELECT A.SECURITY_CODE, A.SECURITY_SEQ, A.PROJECT_SEQ, B.PROJECT_NAME, B.IS_RECURRING_POOL
                       FROM CSI_ABS_BASIC_INFO A 
                       LEFT JOIN (SELECT PROJECT_NAME, PROJECT_SEQ, IS_RECURRING_POOL
                       FROM CSI_ABS_PROJECT_BASIC_INFO) B 
                       ON A.PROJECT_SEQ = B.PROJECT_SEQ""")
df_params = db_cloud.select("SELECT * FROM CSI_BOND_ABS.CSI_ABS_VALUATION_INPUT")
db_cloud.close()

if test_module in ('valuation', 'model', 'user'):

    if test_module != 'model':
        filename_ = os.path.join(EXCEL_DIR, "批量估值结果_" + test_module + "_" + trade_date + '.xlsx')
    else:
        filename_ = os.path.join(EXCEL_DIR, "批量估值结果_" + model_type + "_" + trade_date + '.xlsx')

    write = pd.ExcelWriter(filename_, mode='a', engine='openpyxl')

    security_code_reflections = security_code_reflections.merge(df_params[['PROJECT_SEQ', 'INPUT_SEQ']].drop_duplicates(keep='last'), on='PROJECT_SEQ', how='left')

    if test_module == 'user' or 'valuation':
        df_warns_info = db_local.select('SELECT * FROM CSI_ABS_DATA_CHECK WHERE DATE = %s' % trade_date)
        df_warns_info = df_warns_info.merge(
            security_code_reflections[['PROJECT_SEQ', 'PROJECT_NAME', 'IS_RECURRING_POOL']].drop_duplicates(keep='last'), on='PROJECT_SEQ', how='left')
        df_warns_info.columns = table_name_transfer('CSI_ABS_DATA_CHECK', df_warns_info.columns)

        df_warns_info.to_excel(write, sheet_name='数据问题汇总', index=False)

        df_date_match = db_local.select('SELECT * FROM CSI_ABS_DATA_MATCH WHERE DATE = %s' % trade_date)
        df_date_match = df_date_match.merge(
            security_code_reflections[['PROJECT_SEQ', 'PROJECT_NAME']].drop_duplicates(keep='last'), on='PROJECT_SEQ',
            how='left')
        df_date_match.columns = table_name_transfer('CSI_ABS_DATA_MATCH', df_date_match.columns)
        df_date_match.drop_duplicates(keep='last', inplace=True)
        df_date_match.to_excel(write, sheet_name='资产证券端匹配关系', index=False)

    if test_module != 'user':
        df_assumptions = db_local.select('SELECT * FROM CSI_ABS_MODEL_PREDICT_PARAMS WHERE DATE = %s' % trade_date)
        df_assumptions = df_assumptions.merge(security_code_reflections[['PROJECT_SEQ', 'PROJECT_NAME']].drop_duplicates(keep='last'), on='PROJECT_SEQ', how='left')
        df_assumptions.columns = table_name_transfer('CSI_ABS_MODEL_PREDICT_PARAMS', df_assumptions.columns)
        df_assumptions.to_excel(write, sheet_name='模型预测参数', index=False)

        df_factor_regress = db_local.select('SELECT * FROM CSI_ABS_MODEL_COEFF_REGRESSION WHERE DATE = %s' % trade_date)
        df_factor_regress = df_factor_regress.merge(security_code_reflections[['PROJECT_SEQ', 'PROJECT_NAME']].drop_duplicates(keep='last'), on='PROJECT_SEQ', how='left')
        df_factor_regress.columns = table_name_transfer('CSI_ABS_MODEL_COEFF_REGRESSION', df_factor_regress.columns)
        df_factor_regress.to_excel(write, sheet_name='回归模型因子值', index=False)

        df_factor_markov = db_local.select('SELECT * FROM CSI_ABS_MODEL_COEFF_MARKOV WHERE DATE = %s' % trade_date)
        df_factor_markov = df_factor_markov.merge(security_code_reflections[['PROJECT_SEQ', 'PROJECT_NAME']].drop_duplicates(keep='last'), on='PROJECT_SEQ', how='left')
        df_factor_markov.columns = table_name_transfer('CSI_ABS_MODEL_COEFF_MARKOV', df_factor_markov.columns)
        df_factor_markov.to_excel(write, sheet_name='马尔可夫转移概率值', index=False)

    df_checked = db_local.select('SELECT * FROM CSI_ABS_VALUATION_CHECKED WHERE DATE = %s' % trade_date)
    df_checked = df_checked.merge(security_code_reflections[['SECURITY_SEQ', 'SECURITY_CODE', 'PROJECT_NAME', 'IS_RECURRING_POOL']].drop_duplicates(subset=['SECURITY_SEQ'], keep='last'), on='SECURITY_SEQ', how='left')
    df_checked.columns = table_name_transfer('CSI_ABS_VALUATION_CHECKED', df_checked.columns)
    df_checked.to_excel(write, sheet_name='估值结果核查表', index=False)

    df_checked_project = db_local.select('SELECT * FROM CSI_ABS_VALUATION_CHECKED_PROJECT WHERE DATE = %s' % trade_date)
    df_checked_project = df_checked_project.merge(security_code_reflections[['PROJECT_SEQ', 'SECURITY_CODE', 'PROJECT_NAME', 'IS_RECURRING_POOL']].drop_duplicates(subset=['PROJECT_NAME'], keep='last'), on='PROJECT_SEQ', how='left')
    df_checked_project.columns = table_name_transfer('CSI_ABS_VALUATION_CHECKED_PROJECT', df_checked_project.columns)
    df_checked_project.to_excel(write, sheet_name='项目指标核查表', index=False)

    valuation_results = db_local.select('SELECT * FROM CSI_ABS_GZ_RESULT WHERE DATE = %s' % trade_date)
    valuation_results = valuation_results.merge(security_code_reflections[['SECURITY_SEQ', 'SECURITY_CODE', 'PROJECT_NAME', 'IS_RECURRING_POOL']].drop_duplicates( keep='last'), on='SECURITY_SEQ', how='left')
    valuation_results.columns = table_name_transfer('CSI_ABS_GZ_RESULT', valuation_results.columns)
    valuation_results.to_excel(write, sheet_name='估值结果', index=False)

    derivative_results = db_local.select('SELECT * FROM CSI_ABS_VALUATION_INDICATOR WHERE DATE = %s' % trade_date)
    derivative_results = derivative_results.merge(security_code_reflections[['SECURITY_SEQ', 'SECURITY_CODE', 'PROJECT_NAME', 'IS_RECURRING_POOL']].drop_duplicates(keep='last'), on='SECURITY_SEQ', how='left')
    derivative_results.columns = table_name_transfer('CSI_ABS_VALUATION_INDICATOR', derivative_results.columns)
    derivative_results.to_excel(write, sheet_name='估值衍生指标', index=False)

    df_fail = db_local.select("SELECT * FROM CSI_ABS_FAIL WHERE DATE = %s AND MODULE= '%s'" %(trade_date, test_module if test_module != 'model' else model_type))
    df_fail = df_fail.merge(security_code_reflections[['INPUT_SEQ', 'PROJECT_NAME', 'SECURITY_CODE']].drop_duplicates(subset=['INPUT_SEQ'], keep='last'), left_on='SEQ', right_on='INPUT_SEQ', how='left')
    if 'INPUT_SEQ' in df_fail.columns:
        df_fail.drop(columns=['INPUT_SEQ'], inplace=True)
    df_fail.columns = table_name_transfer('CSI_ABS_FAIL', df_fail.columns)
    df_fail.to_excel(write, sheet_name='计算失败项目', index=False)

    patch_info = db_local.select("SELECT * FROM SUMMARY WHERE DATE = %s AND MODULE = '%s'" %(trade_date, test_module))
    patch_info.columns = table_name_transfer('SUMMARY', patch_info.columns)
    patch_info.to_excel(write, sheet_name='运行概况', index=False)

    write.save()
    db_local.close()

else:

    if test_module == 'rating':
        df_params = db_local.select("SELECT * FROM CSI_ABS_RATING_INPUT")
    else:
        df_params = pd.read_excel(os.path.join(EXCEL_DIR, "批量估值结果_" + test_module + "_" + trade_date + '.xlsx'))

    filename_ = os.path.join(EXCEL_DIR, "批量估值结果_" + test_module + "_" + trade_date + '.xlsx')
    write = pd.ExcelWriter(filename_, mode="a", engine='openpyxl')

    security_code_reflections = security_code_reflections.merge(df_params[['PROJECT_SEQ', 'RATING_INPUT_SEQ']].drop_duplicates(keep='last'), on='PROJECT_SEQ', how='left')

    df_fail = db_local.select("SELECT * FROM CSI_ABS_FAIL WHERE DATE = %s AND MODULE= '%s'" %(trade_date, test_module if test_module != 'model' else model_type))
    df_fail = df_fail.merge(security_code_reflections[['RATING_INPUT_SEQ', 'PROJECT_NAME', 'SECURITY_CODE']].drop_duplicates(keep='last'), left_on='SEQ', right_on='RATING_INPUT_SEQ', how='left')
    if 'RATING_INPUT_SEQ' in df_fail.columns:
        df_fail.drop(columns=['RATING_INPUT_SEQ'], inplace=True)
    df_fail.columns = table_name_transfer('CSI_ABS_FAIL', df_fail.columns)
    df_fail.to_excel(write, sheet_name='计算失败项目', index=False)

    patch_info = db_local.select("SELECT * FROM SUMMARY WHERE DATE = %s AND MODULE = '%s'" %(trade_date, test_module))
    patch_info.columns = table_name_transfer('SUMMARY', patch_info.columns)
    patch_info.to_excel(write, sheet_name='运行概况', index=False)

    df_model_params = db_local.select("SELECT * FROM CSI_ABS_RATING_NORM_PARAM WHERE DATE = %s" %(trade_date))
    df_model_params = df_model_params.merge(security_code_reflections[['PROJECT_SEQ', 'PROJECT_NAME']].drop_duplicates(keep='last'), on='PROJECT_SEQ', how='left')
    df_model_params.columns = table_name_transfer('CSI_ABS_RATING_NORM_PARAM', df_model_params.columns)
    df_model_params.to_excel(write, sheet_name='模型参数', index=False)

    write.save()

    db_local.close()
