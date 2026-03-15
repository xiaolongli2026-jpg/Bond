# * coding: utf8 *

# @Auther : Double Q
# @Email : hqq_1996@163.com
# @Createtime : MONTH: 05, DAY: 10, YEAR: 2022

import os
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from multiprocessing import Pool
from main.abs_calculator.module_abs_new import abs_calculator
from main.rating_module.rating_main import main_rating
from utils.quick_connect import connect_mysql
from utils.db_util import UploadLib
from conf.conf import local_db, local_db2, EXCEL_DIR
from rating.prepare.load_implied_rank import load_implied_rank


def main_batch_run(test_df, test_module='user', check_date=datetime.now().strftime('%Y%m%d'), df_params=None, model_type=None):
    """
    估值批量主函数

    Args:
        test_df (pd.DataFrame): 测试样本列表，至少包括 project_seq, security_code, project_name,
        test_module (str): 测试模块

                    * user-普通现金流测算模块,统一参数对多个加压场景进行测试
                    * model-统一参数对加压参数模型进行测试，需要对应输入 `model_type`
                    * valuation-估值,需要读取参数表

        check_date (str): 测试日期
        df_params (pd.DataFrame): 参数表
        model_type (str): 模型参数

    """
    time1 = time.time()

    # 保存数据
    if test_module == 'valuation':
        db_temp = UploadLib(local_db)
        db_temp.connect()
    else:
        db_temp = UploadLib(local_db2)
        db_temp.connect()

    test_df_full = test_df.copy()
    total_project_number = len(set(test_df['project_seq']))
    total_security_number = len(test_df['security_seq'])

    df_list = test_df[['project_seq', 'security_seq']]
    df_list.loc[:, 'DATE'] = check_date
    df_list.loc[:, 'UPDATE_DATA'] = test_df['update']

    # 区分是否更新数据，更新数据的用今日的新增现金流，没有更新数据的复制昨日的现金流
    static_securities = df_list.loc[df_list['UPDATE_DATA'] == 0, 'security_seq'].to_list()
    if len(static_securities) > 0:
        cf_his = db_temp.select("SELECT * FROM CSI_ABS_GZ_CF")
        cf_static = cf_his[cf_his['SECURITY_SEQ'].isin(static_securities)]
        cf_static.loc[:, 'TRADE_DATE'] = check_date
    else:
        cf_static = pd.DataFrame()

    test_df = test_df[test_df['update']==1].reset_index(drop=True)
    db_temp.delete('CSI_HISTORY_LIST', 'DATE = %s' % (check_date))  # 重复跑的话删掉一天
    db_temp.insert('CSI_HISTORY_LIST', df_list)

    # 1 分块
    test_blocks = test_list_split(test_df, box_num=100)

    # 2 计算

    cal_valuation = True
    results = []

    thread_pool = Pool(6)
    for single_block in test_blocks:
        copy_table = df_params.copy()
        results.append(thread_pool.apply_async(pscheck_user, args=(single_block, copy_table, check_date, cal_valuation)))
    thread_pool.close()
    thread_pool.join()

    data_sets = ['result_info', 'return_pool_result', 'return_security_result', 'return_pool_result_cn',
                 'return_security_result_cn', 'return_df_warns_info', 'return_df_assumptions',
                 'return_df_factor_regress', 'return_df_factor_markov', 'return_df_date_match',
                 'return_valuation_results', 'return_df_checked', 'return_derivative_results',
                 'return_df_checked_project']

    for name_ in data_sets:
        exec("%s = dict()" % name_, globals())

    for i in range(len(results)):
        result_ = results[i]
        result_info_1, return_pool_result_1, return_security_result_1, return_pool_result_cn_1,\
        return_security_result_cn_1,return_df_warns_info_1, \
        return_df_assumptions_1, return_df_factor_regress_1, \
        return_df_factor_markov_1, return_df_date_match_1, return_valuation_results_1, \
        return_df_checked_1, return_derivative_results_1, return_df_checked_project_1 = result_.get()


        for name_ in data_sets:
            exec("{}.update({})".format(name_, name_+"_1"))

    result = pd.DataFrame.from_dict(result_info, orient='index')
    result.rename_axis(index='input_seq', inplace=True)
    result.reset_index(drop=False, inplace=True)

    df_pool_result_cn = pd.DataFrame()
    for x in return_pool_result_cn:
        df_temp = return_pool_result_cn[x]
        df_pool_result_cn = df_pool_result_cn.append(df_temp)

    df_security_result_cn = pd.DataFrame()
    for x in return_security_result_cn:
        df_temp = return_security_result_cn[x]
        df_security_result_cn = df_security_result_cn.append(df_temp)

    if test_module != 'model':
        w = pd.ExcelWriter(os.path.join(EXCEL_DIR, "批量估值结果_" + test_module + "_" + check_date + '.xlsx'), mode="w", engine='openpyxl')
        module = test_module
    else:
        w = pd.ExcelWriter(os.path.join(EXCEL_DIR, "批量估值结果_" + model_type + "_" + check_date + '.xlsx'), mode="w", engine='openpyxl')
        module = model_type

    df_pool_result_cn.to_excel(w, sheet_name="资产池现金流", index=False)
    df_security_result_cn.to_excel(w, sheet_name='证券端现金流', index=False)

    df_params = df_params.merge(result, on='input_seq', how='left')
    df_fail = df_params[df_params['run_result']=='fail'][['input_seq', 'reason']]
    df_fail.columns = ['SEQ', 'REASON']
    try:
        success_number = df_params['run_result'].value_counts()['success']
    except:
        success_number = 0

    df_fail.loc[:, 'MODULE'] = test_module if test_module != 'model' else model_type
    df_fail.loc[:, 'DATE'] = check_date
    df_fail_project = \
        test_df.loc[test_df['project_seq'].isin(df_params.loc[df_params['run_result'] == 'fail', 'project_seq']), :]
    df_fail_project.loc[:, 'MODULE'] = test_module if test_module != 'model' else model_type
    df_fail_security = test_df.loc[test_df['project_seq'].isin(df_fail_project['project_seq']), :]

    # 未成功的数据单独保存，用于单独重新测算
    df_params.reset_index(drop=True, inplace=True)

    # 先清除当日数据然后提交数据到本地sql

    db_temp.delete('CSI_ABS_FAIL', "DATE = %s AND MODULE='%s'" %(check_date, module))
    db_temp.delete('CSI_ABS_VALUATION_POOL_RESULT', 'DATE = %s' %check_date)
    db_temp.delete('CSI_ABS_GZ_CF', None)
    db_temp.delete('CSI_ABS_GZ_CF_HIS', 'DATE = %s' % check_date)
    db_temp.delete('CSI_ABS_DATA_MATCH', 'DATE = %s' % check_date)
    db_temp.delete('CSI_ABS_GZ_CF_CUSTOM', 'TRADE_DATE = %s' % check_date)
    db_temp.delete('CSI_ABS_DATA_CHECK', 'DATE = %s' % check_date)
    db_temp.delete('CSI_ABS_MODEL_PREDICT_PARAMS', 'DATE = %s' % check_date)
    db_temp.delete('CSI_ABS_MODEL_COEFF_MARKOV', 'DATE = %s' % check_date)
    db_temp.delete('CSI_ABS_MODEL_COEFF_REGRESSION', 'DATE = %s' % check_date)
    db_temp.delete('CSI_ABS_GZ_RESULT', 'DATE = %s' % check_date)
    db_temp.delete('CSI_ABS_VALUATION_INDICATOR', 'DATE = %s' % check_date)
    db_temp.delete('CSI_ABS_VALUATION_CHECKED', 'DATE = %s' % check_date)
    db_temp.delete('SUMMARY', "DATE = %s AND MODULE='%s'" % (check_date, module))

    if len(df_fail) > 0:
        db_temp.insert('CSI_ABS_FAIL', df_fail)

    if len(cf_static) > 0:
        db_temp.insert('CSI_ABS_GZ_CF', cf_static)

    # 本地只保存最新值作为，如果更新数据则将历史的数据清除
    for i in range(len(df_params)):
        if df_params.loc[i, 'run_result'] == 'fail':
            continue

        input_seq = df_params.loc[i, 'input_seq']
        df_pool = return_pool_result[input_seq]
        df_security = return_security_result[input_seq]
        df_checked_project = return_df_checked_project[input_seq]

        if len(df_checked_project) > 0:
            db_temp.insert('CSI_ABS_VALUATION_CHECKED_PROJECT', df_checked_project)\

        if len(df_pool) > 0:
            db_temp.insert('CSI_ABS_VALUATION_POOL_RESULT', df_pool)

        if len(df_security) > 0:
            if test_module == 'valuation':
                db_temp.insert('CSI_ABS_GZ_CF', df_security)
            else:
                df_security['INPUT_SEQ'] = input_seq
                db_temp.insert('CSI_ABS_GZ_CF_CUSTOM', df_security) #自定义参数测试保存地址，测试参数组比较多所以需要额外一个INPUT_SEQ列，实际生产一天一个项目只有一组参数所以不需要

        if test_module == 'valuation' or test_module == 'user':
            df_match_info = return_df_date_match[input_seq]
            db_temp.insert("CSI_ABS_DATA_MATCH", df_match_info)
            df_warns = return_df_warns_info[input_seq]
            db_temp.insert('CSI_ABS_DATA_CHECK', df_warns)

        df_assumption = return_df_assumptions.get(input_seq, None)
        if df_assumption is not None:
            db_temp.insert("CSI_ABS_MODEL_PREDICT_PARAMS", df_assumption)

        df_factor = return_df_factor_markov.get(input_seq, None)
        if df_factor is not None:
            db_temp.insert("CSI_ABS_MODEL_COEFF_MARKOV", df_factor)

        df_factor_regress = return_df_factor_regress.get(input_seq, None)
        if df_factor_regress is not None:
            db_temp.insert("CSI_ABS_MODEL_COEFF_REGRESSION", df_factor_regress)

        df_valuation = return_valuation_results.get(input_seq, None)
        if df_valuation is not None:
            db_temp.insert("CSI_ABS_GZ_RESULT", df_valuation)

        df_derivative = return_derivative_results.get(input_seq, None)
        if df_derivative is not None:
            db_temp.insert("CSI_ABS_VALUATION_INDICATOR", df_derivative)

        df_checked = return_df_checked[input_seq]
        if len(df_checked) > 0:
            db_temp.insert("CSI_ABS_VALUATION_CHECKED", df_checked)

    df_params.rename(columns={'TRADE_DATE': 'DATE'}, inplace=True)
    df_params.to_excel(w, sheet_name='参数组', index=False)
    w.save()

    cf = db_temp.select('SELECT * FROM CSI_ABS_GZ_CF')# 这个表只会有当日的数据，不会有其他日期
    db_temp.insert('CSI_ABS_GZ_CF_HIS', cf)

    # 统计下有现金流的证券和没有现金流的证券,包括计算失败的和没有现金流的
    security_lack_cash = set(test_df_full['security_seq']).difference(set(cf['SECURITY_SEQ']))

    time2 = time.time()
    patch_info = pd.DataFrame(
        {'DATE': check_date[:8], 'CALCULATION_NUMBER': len(df_params), 'SUCCESS_NUMBER': success_number,
         'FAIL_NUMBER': len(df_params) - success_number,
         'CALCULATION_TIME': round((time2 - time1)/60, 2),
         'MODULE': test_module if test_module != 'model' else model_type,
         'CALCULATION_SECURITY_NUMBER': len(test_df),
         'SUCCESS_SECURITY_NUMBER': len(test_df) - len(df_fail_security),
         'FAIL_SECURITY_NUMBER': len(df_fail_security),
         'PROJECT_NUMBER': total_project_number,
         'SECURITY_NUMBER': total_security_number,
         'INVALID_SECURITIES': ";".join(list(security_lack_cash))}, index=[0], )

    db_temp.insert('SUMMARY', patch_info)

    db_temp.close()

    return patch_info, df_fail


def main_batch_rating(test_df, test_module='rating', check_date=datetime.now().strftime('%Y%m%d'), df_params=None):
    """
    评级测试主函数

    Args:
        test_df (pd.DataFrame): 测试样本列表，至少包括 project_seq, security_code, project_name,
        test_module (str): 测试模块

                    * rating_test-默认参数进行评级测试，检查是否能运行成功
                    * rating-评级

        check_date (str): 测试日期
        df_params (pd.DataFrame): 参数表

    """
    time1 = time.time()

    total_project_number = len(set(test_df['project_seq']))
    total_security_number = len(test_df['security_seq'])

    df_list = test_df[['project_seq', 'security_seq']]
    df_list.loc[:, 'DATE'] = check_date
    df_list.loc[:, 'UPDATE_DATA'] = test_df['update']

    if test_module == 'rating':
        db_temp = UploadLib(local_db)
        db_temp.connect()
    else:
        db_temp = UploadLib(local_db2)
        db_temp.connect()

    # 无更新的项目复制前日数据
    test_df = test_df[test_df['update'] == 1].reset_index(drop=True)  # 筛掉没有更新的项目，只评有更新的项目
    db_temp.delete('CSI_HISTORY_RATING_LIST', 'DATE = %s' % (check_date))  # 重复跑的话删掉一天
    db_temp.insert('CSI_HISTORY_RATING_LIST', df_list)

    test_blocks = test_list_split(test_df, box_num=100)

    db_temp.delete('CSI_ABS_FAIL', "DATE = %s AND MODULE='rating'" % check_date) # 重复跑同一天的时候，删除当日的
    db_temp.delete('SUMMARY', "DATE = %s AND MODULE='rating'" % check_date)
    db_temp.delete('CSI_ABS_RATING_RESULT', 'DATE = %s' % check_date)
    db_temp.delete('CSI_ABS_RATING_CRITICAL_VALUE', 'DATE = %s' % check_date)
    db_temp.delete('CSI_ABS_RATING_NORM_PARAM', 'DATE = %s' % check_date)

    # 2 计算

    results = []

    thread_pool = Pool(6)
    for single_block in test_blocks:
        results.append(thread_pool.apply_async(pscheck_rating, args=(single_block, check_date, df_params)))
    thread_pool.close()
    thread_pool.join()

    df_ranks_dict, df_critical_dict, df_ranks_cn_dict, df_critical_cn_dict, df_model_params_dict, result_info,\
        base_params = \
        {}, {}, {}, {}, {}, {}, {}

    for i in range(len(results)):
        result_ = results[i]
        df_ranks_dict_1, df_critical_dict_1, df_ranks_cn_dict_1, df_critical_cn_dict_1, df_model_params_dict_1, \
        result_info_1, base_params_1 = result_.get()
        df_ranks_dict.update(df_ranks_dict_1)
        df_ranks_cn_dict.update(df_ranks_cn_dict_1)
        df_critical_cn_dict.update(df_critical_cn_dict_1)
        df_critical_dict.update(df_critical_dict_1)
        df_model_params_dict.update(df_model_params_dict_1)
        result_info.update(result_info_1)
        base_params.update(base_params_1)

    result = pd.DataFrame.from_dict(result_info, orient='index')
    result.rename_axis(index='rating_input_seq', inplace=True)
    result.reset_index(drop=False, inplace=True)
    if len(result) > 0:
        df_params = df_params.merge(result, on='rating_input_seq', how='left')
        df_fail = df_params[df_params['run_result']=='fail'][['rating_input_seq', 'reason']]
        df_fail.columns = ['SEQ', 'REASON']
        try:
            success_number = df_params['run_result'].value_counts()['success']
        except:
            success_number = 0

        df_fail['MODULE'] = test_module
        df_fail['DATE'] = check_date
        df_fail_project = \
            test_df.loc[test_df['project_seq'].isin(df_params.loc[df_params['run_result'] == 'fail', 'project_seq']), :]
        df_fail_project['MODULE'] = test_module
        df_fail_project.columns = [x.upper() for x in df_fail_project.columns]
    else:
        df_fail = pd.DataFrame()
        df_fail_project = pd.DataFrame()
        success_number = len(df_params)
        df_params['run_result'] = 'success'

    time2 = time.time()
    patch_info = pd.DataFrame(
        {'DATE': check_date[:8], 'CALCULATION_NUMBER': len(df_params),
         'SUCCESS_NUMBER': success_number,
         'FAIL_NUMBER': len(df_params) - success_number,
         'CALCULATION_TIME': round((time2 - time1) / 60, 2),
         'MODULE': test_module,
         'CALCULATION_SECURITY_NUMBER': len(test_df),
         'SUCCESS_SECURITY_NUMBER': len(test_df) - len(df_fail_project),
         'FAIL_SECURITY_NUMBER': len(df_fail_project),
         'PROJECT_NUMBER': total_project_number,
         'SECURITY_NUMBER': total_security_number,
         }, index=[0], )

    db_temp.insert('SUMMARY', patch_info)

    if len(df_fail) > 0:
        db_temp.insert('CSI_ABS_FAIL', df_fail)

    for i in range(len(df_params)):
        if df_params.loc[i, 'run_result'] == 'fail':
            continue

        input_seq = df_params.loc[i, 'rating_input_seq']
        df_ranks = df_ranks_dict[input_seq]
        df_ranks['RATING_INPUT_SEQ'] = input_seq
        df_critical = df_critical_dict[input_seq]
        df_critical['RATING_INPUT_SEQ'] = input_seq
        df_model_params = df_model_params_dict.get(input_seq, None)
        db_temp.insert("CSI_ABS_RATING_RESULT", df_ranks)
        db_temp.insert("CSI_ABS_RATING_CRITICAL_VALUE", df_critical)

        if (df_model_params is not None) and len(df_model_params) > 0:
            df_model_params['RATING_INPUT_SEQ'] = input_seq
            df_model_params['DATE'] = check_date
            db_temp.insert("CSI_ABS_RATING_NORM_PARAM", df_model_params)

        base_ = base_params.get(input_seq, None)

        if (base_ is not None) and len(base_) > 0:
            base_['RATING_INPUT_SEQ'] = input_seq
            base_['DATE'] = check_date
            db_temp.insert("CSI_ABS_RATING_BASIC_PARAMS", base_)

    df_ranks_cn = pd.DataFrame()
    for x in df_ranks_cn_dict:
        df_temp = df_ranks_cn_dict[x]
        df_temp['RATING_INPUT_SEQ'] = x
        df_ranks_cn = df_ranks_cn.append(df_temp)

    df_critical_cn = pd.DataFrame()
    for x in df_critical_cn_dict:
        df_temp = df_critical_cn_dict[x]
        df_temp['RATING_INPUT_SEQ'] = x
        df_critical_cn = df_critical_cn.append(df_temp)

    w = pd.ExcelWriter(os.path.join(EXCEL_DIR, "批量估值结果_" + test_module + "_" + check_date + '.xlsx'), mode="w",
                           engine='openpyxl')

    df_params.rename(columns={'TRADE_DATE': 'DATE'}, inplace=True)
    df_params.to_excel(w, sheet_name='参数组', index=False)
    df_ranks_cn.to_excel(w, sheet_name='评级结果', index=False)
    df_critical_cn.to_excel(w, sheet_name='临界值', index=False)
    w.save()

    # 额外插入隐含评级数据，用于结果比对
    df_IR = load_implied_rank(check_date)
    df_IR = df_IR.merge(test_df[['security_code', 'security_seq']], on='security_code', how='left')
    df_IR = df_IR.merge(df_params[['security_code', 'rating_input_seq']], on='security_code', how='left')
    df_IR.columns = [x.upper() for x in df_IR.columns]

    db_temp.update(df_IR, 'CSI_ABS_RATING_RESULT', ['SECURITY_SEQ', 'DATE', 'RATING_INPUT_SEQ'], ['IMPLIED_RANK'])

    df_pass = db_temp.select("SELECT SECURITY_SEQ, RATING_INPUT_SEQ, DATE, RANK, IMPLIED_RANK FROM "
                   "CSI_ABS_RATING_RESULT WHERE DATE = %s" % check_date)
    from rating.prepare.config import threshold_rank, rating_dict
    df_pass.loc[:, 'PASS'] = df_pass[['RANK', 'IMPLIED_RANK']].apply(lambda row: '1' \
        if (((rating_dict[str(row[0])]-rating_dict[str(row[1])]) > threshold_rank[0] and \
            (rating_dict[str(row[0])]-rating_dict[str(row[1])]) < threshold_rank[1]) or (
        np.isnan(rating_dict[str(row[0])]) or np.isnan(rating_dict[str(row[1])]))) else '0', axis=1)
    db_temp.update(df_pass, 'CSI_ABS_RATING_RESULT', ['SECURITY_SEQ', 'DATE', 'RATING_INPUT_SEQ'], ['PASS'])

    db_temp.close()

    return df_ranks_dict, df_critical_dict, df_ranks_cn_dict, df_critical_cn_dict, df_model_params_dict, \
           patch_info, df_fail, df_params, df_fail_project


def pscheck_user(test_df, df_pool_params, check_date, cal_valuation=False):
    """
    计算器批量测试

    Args:
        test_df (pd.DataFrame): 测试项目列表
        df_pool_params (pd.DataFrame): 各项目分别设置证券端参数

    Returns:
        tuple: tuple contains:
            result_info (dict): 计算是否成功

    """
    end_df = test_df.copy()
    pool_params_copy = df_pool_params.copy()
    result_info = {}
    end_df.reset_index(drop=True, inplace=True)

    pool_result_upload_dict, security_result_upload_dict, df_warns_info_dict, df_assumptions_dict, \
    df_factor_regress_dict, df_factor_markov_dict, df_date_match_dict, valuation_results_dict, df_checked_dict,\
    security_result_cn_dict, pool_result_cn_dict, derivative_results_dict, df_checked_pro_dict = \
        {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}

    conn, is_mysql = connect_mysql()
    cur = conn.cursor()

    total_ = len(end_df)
    last_project = '0'

    for i in range(0, total_):

        if last_project == end_df.loc[i, "project_seq"]:  # 因为是按项目跑，如果下一行是同一个项目，就跳过
            last_project = end_df.loc[i, "project_seq"]
        else:

            last_project = end_df.loc[i, "project_seq"]
            project_seq = end_df.loc[i, 'project_seq']
            print("当前:", end_df.loc[i, 'project_name'])

            securitys_gz = end_df.loc[(end_df['project_seq'] == project_seq) & (end_df['is_gz']),  'security_seq']

            securitys_all = end_df.loc[end_df['project_seq'] == project_seq, 'security_seq']
            params_dict = pool_params_copy[pool_params_copy['project_seq']==project_seq].to_dict(orient='index') # 不reset_index

            for num_ in params_dict:
                params_ = params_dict[num_]
                params = params_.copy()
                input_seq = params['input_seq']

                if params['module_type'] == 'static_npl_recal' and (not end_df.loc[i, 'is_npls']):
                    result_info[input_seq] = {'run_result': 'fail', 'reason': '不能对非不良贷款类用不良贷款重新加压的计算模块'}

                elif ((params['module_type'] == 'static_rev_recal') or (params['module_type'] == 'series_rev_recal')) \
                    and (not end_df.loc[i, 'is_recurring_pool']):
                    result_info[input_seq] = {'run_result': 'fail', 'reason': '不能对非循环购买用循环购买重新加压的计算模块'}

                else:
                    try:

                        params['cur'] = cur
                        params['is_security'] = False
                        pool_result_upload, security_result_upload, pool_result_cn, security_result_cn, df_warns_info, \
                        df_assumptions, df_factor, df_date_match, valuation_results, df_checked, derivative_results, \
                        df_checked_project = \
                            abs_calculator(params, cal_valuation=cal_valuation)

                        if len(securitys_gz) < len(securitys_all):
                            security_result_upload = \
                                security_result_upload[security_result_upload['SECURITY_SEQ'].isin(securitys_gz)]

                        pool_result_cn['INPUT_SEQ'] = input_seq
                        pool_result_upload['INPUT_SEQ'] = input_seq
                        pool_result_upload['DATE'] = check_date
                        if df_assumptions is not None and len(df_assumptions) > 0:
                            df_assumptions['INPUT_SEQ'] = input_seq
                            df_assumptions['DATE'] = check_date

                        if valuation_results is not None and len(valuation_results) > 0:
                            valuation_results['INPUT_SEQ'] = input_seq
                            valuation_results['DATE'] = check_date

                        if derivative_results is not None and len(derivative_results) > 0:
                            derivative_results['INPUT_SEQ'] = input_seq
                            derivative_results['DATE'] = check_date

                        if len(df_checked) > 0:
                            df_checked['INPUT_SEQ'] = input_seq
                            df_checked['DATE'] = check_date

                        if len(df_checked_project) > 0:
                            df_checked_project['INPUT_SEQ'] = input_seq
                            df_checked_project['DATE'] = check_date

                        pool_result_upload_dict[input_seq], security_result_upload_dict[input_seq], \
                        df_warns_info_dict[input_seq], df_date_match_dict[input_seq], df_checked_dict[input_seq], \
                        security_result_cn_dict[input_seq], pool_result_cn_dict[input_seq], df_checked_pro_dict[input_seq]= \
                            pool_result_upload,  security_result_upload, df_warns_info, df_date_match, df_checked, \
                            security_result_cn, pool_result_cn, df_checked_project

                        if df_assumptions is not None:
                            df_assumptions_dict[input_seq] = df_assumptions
                        if valuation_results is not None:
                            if len(securitys_gz) < len(securitys_all):
                                valuation_results = \
                                    valuation_results[valuation_results['SECURITY_SEQ'].isin(securitys_gz)]

                            valuation_results_dict[input_seq] = valuation_results
                        if derivative_results is not None:
                            if len(securitys_gz) < len(securitys_all):
                                derivative_results = \
                                    derivative_results[derivative_results['SECURITY_SEQ'].isin(securitys_gz)]
                            derivative_results_dict[input_seq] = derivative_results

                        if df_factor is not None:
                            df_factor['INPUT_SEQ'] = input_seq
                            df_factor['DATE'] = check_date
                            if params['model_type'] == 'markov':
                                df_factor_markov_dict[input_seq] = df_factor
                            elif params['model_type'] == 'linear_model' or params['model_type'] == 'sigmoid_model':
                                df_factor_regress_dict[input_seq] = df_factor
                        print(end_df.loc[i, 'project_name'], "success")
                        result_info[input_seq] = {'run_result': 'success', 'reason': ''}
                    except (ValueError, IndexError, Exception) as e:
                        e = str(e)
                        result_info[input_seq] = {'run_result': 'fail', 'reason': e}
                        del e
                        print(end_df.loc[i, 'project_name'], 'fail')
                    finally:
                        del params

    cur.close()
    conn.close()

    return result_info, pool_result_upload_dict, security_result_upload_dict, pool_result_cn_dict, \
           security_result_cn_dict, df_warns_info_dict, df_assumptions_dict, df_factor_regress_dict, \
           df_factor_markov_dict, df_date_match_dict, valuation_results_dict, df_checked_dict, derivative_results_dict,\
           df_checked_pro_dict


def pscheck_rating(test_df, check_date, df_pool_params=None,):
    """
    量化评级批量测试

    Args:
        test_df (pd.DataFrame): 测试项目列表
        check_date (str): 日期
        df_pool_params (pd.DataFrame): 如果不用统一参数，为各项目分布设置资产加压参数

    Returns:
        tuple: tuple contains:

    """
    end_df = test_df.copy()
    pool_params_copy = df_pool_params.copy()
    result_info = {}
    end_df.reset_index(drop=True, inplace=True)

    df_ranks_dict, df_critical_dict, df_ranks_cn_dict, df_critical_cn_dict, model_params_dict, base_params_dict = \
        {}, {}, {}, {}, {}, {}

    conn, is_mysql = connect_mysql()
    cur = conn.cursor()

    total_ = len(end_df)
    last_project = '0'

    for i in range(0, total_):

        if last_project == end_df.loc[i, "project_seq"]:  # 因为是按项目跑，如果下一行是同一个项目，就跳过
            last_project = end_df.loc[i, "project_seq"]
        else:
            last_project = end_df.loc[i, "project_seq"]
            project_seq = end_df.loc[i, 'project_seq']
            security_code = end_df.loc[i, 'security_code']
            securitys_gz = end_df.loc[(end_df['project_seq'] == project_seq) &
                                      (end_df['is_suit_quantify_level']),  'security_seq']
            params_dict = pool_params_copy[pool_params_copy['project_seq'] == project_seq].to_dict(
                orient='index')  # 不reset_index

            for num_ in params_dict:
                params_ = params_dict[num_]
                params = params_.copy()
                input_seq = params['rating_input_seq']

                params = params_.copy()
                exist_exp_tax = params.get('exist_tax_exp', False)
                exp_rate = params.get('exp_rate', 0.)
                tax_rate = params.get('tax_rate', 0.)
                scenario_sets = None
                rating_method = params.get('rating_method', None)
                tdr_source = params.get('tdr_source', None)
                custom_tdr = None
                if tdr_source == 'customize':
                    custom_tdr = params.get('custom_tdr')

                lognorm_source = params.get('lognorm_source', None)
                multiplier_source = 'rating_report'
                default_prob_source = params.get('default_prob_source', 'rating_report')
                data_source = params.get('data_source', None)
                custom_cdr, custom_dp, custom_cprs, custom_cpr, custom_rr, custom_dpp, custom_rrs, custom_yr, \
                custom_yrs, custom_pp, custom_pps, custom_rp \
                    = None, None, None, None, None, None, None, None, None, None, None, None
                module_type = 'static'
                dp_match_perfectly = False
                param_match_method = 'all'
                if data_source == 'customize':
                    module_type = params.get('module_type', 'static')
                    param_match_method = params.get('param_match_method', 'all')
                    custom_rp = params.get('custom_rp', None)
                    dp_match_perfectly = params.get('dp_match_perfectly', False)
                    if module_type == 'static':
                        custom_cdr = params.get('custom_cdr', None)
                        custom_cpr = params.get('custom_cpr', None)
                        custom_rr = params.get('custom_rr', None)
                        custom_dpp = params.get('custom_dpp', None)
                        custom_yr = params.get('custom_yr', None)
                        custom_pp = params.get('custom_pp', None)

                    else:
                        custom_dp = params.get('custom_dp', None)
                        custom_cprs = params.get('custom_cprs', None)
                        custom_rrs = params.get('custom_rrs', None)
                        custom_yrs = params.get('custom_yrs', None)
                        custom_pps = params.get('custom_pps', None)

                same_type = params.get('same_type', True)

                try:
                    exist_exp_tax = {'false': False, 'true': True}[str(exist_exp_tax).lower()]
                    same_type = {'false': False, 'true': True, 'none': False}[str(same_type).lower()]
                    dp_match_perfectly = {'false': False, 'true': True, 'none': False}[str(dp_match_perfectly).lower()]

                    data_source = None if str(data_source) == 'None' else data_source
                    lognorm_source = None if str(lognorm_source) == 'None' else lognorm_source
                    tdr_source = None if str(tdr_source) == 'None' else tdr_source
                    rating_method = None if str(rating_method) == 'None' else rating_method

                    if custom_dpp is not None:
                        custom_dpp = int(custom_dpp)

                    df_ranks, df_critical, df_ranks_cn, df_critical_cn, warn_lst, df_model_params, base_params =\
                    main_rating(
                        security_code=security_code, rating_date=check_date,
                        exist_tax_exp=exist_exp_tax, exp_rate=exp_rate,
                        tax_rate=tax_rate,
                        customize_scenario=False,
                        scenario_sets=scenario_sets, rating_method=rating_method,
                        rating_range='project', tdr_source=tdr_source,
                        multiplier_source=multiplier_source,
                        custom_multiplier=None, custom_tdr=custom_tdr,
                        lognorm_source=lognorm_source, default_prob_source=default_prob_source,
                        custom_default_prob=None,
                        data_source=data_source, custom_cdr=custom_cdr, custom_dp=custom_dp,
                        custom_cprs=custom_cprs, custom_cpr=custom_cpr, custom_rr=custom_rr,
                        custom_dpp=custom_dpp, custom_rrs=custom_rrs, custom_yr=custom_yr,
                        custom_yrs=custom_yrs, custom_pp=custom_pp, custom_pps=custom_pps,
                        custom_rp=custom_rp, module_type=module_type,
                        dp_match_perfectly=dp_match_perfectly, param_match_method=param_match_method,
                        same_type=same_type)

                    # 去掉不在清单的
                    if len(set(df_ranks['SECURITY_SEQ']).difference(set(securitys_gz))) > 0:
                        df_ranks = df_ranks[df_ranks['SECURITY_SEQ'].isin(securitys_gz)]
                        df_critical = df_critical[df_critical['SECURITY_SEQ'].isin(securitys_gz)]

                    df_ranks_dict[input_seq] = df_ranks
                    df_critical_dict[input_seq] = df_critical
                    df_ranks_cn_dict[input_seq] = df_ranks_cn
                    df_critical_cn_dict[input_seq] = df_critical_cn
                    model_params_dict[input_seq] = df_model_params
                    base_params_dict[input_seq] = base_params

                    result_info[input_seq] = {'run_result': 'success', 'reason': ''}
                    print(end_df.loc[i, 'project_name'], 'success')
                except (ValueError, IndexError, Exception) as e:
                    e = str(e)
                    result_info[input_seq] = {'run_result': 'fail', 'reason': e}
                    print(end_df.loc[i, 'project_name'], 'fail')

                    del e

    cur.close()
    conn.close()

    return df_ranks_dict, df_critical_dict, df_ranks_cn_dict, df_critical_cn_dict, model_params_dict, result_info, \
           base_params_dict


def test_list_split(test_df, box_num=1000):
    """拆分样本集，用于多进程运行"""
    total_number = len(test_df) - 1
    split_n = total_number // box_num + 1
    test_blocks = []

    i = 1
    last_index = -1
    while i <= split_n:
        this_index = min(i * box_num, total_number)
        try:
            while test_df.loc[this_index, 'project_seq'] == test_df.loc[this_index + 1, 'project_seq']:
                this_index += 1

        except:
            pass
        test_df_part = test_df.loc[last_index + 1: this_index, :].copy()
        test_blocks.append(test_df_part)
        last_index = this_index
        i += 1
    return test_blocks