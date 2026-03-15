# -*- coding: utf-8 -*-
"""用于读取出需要估值或者评级的项目清单，及读取项目对应的估值参数和评级参数

"""
import os
import pandas as pd
from datetime import datetime
from utils.quick_connect import connect_mysql
from utils.db_util import UploadLib
from conf.conf import param_source_db, LOG_DIR, local_db
from conf.table_structure import data_enumerators


def new_project(check_date, return_module='list', aim='valuation'):
    """
    读取用于计算的项目列表(剔除当前已摘牌)

    Args:
        check_date (str): 截止日期
        return_module (str): 读取方式

                        * all-所有未到期项目
                        * list-根据清单表格中的是否估值、是否评级字段判断

        aim (str): 清单用途

                    * valuation-估值或者自定义参数测算
                    * rating-评级
                    * model-参数测算模型

    Returns:
        pd.DataFrame: 待计算项目的基本信息


    **逻辑** :

    1. 将 `CSI_ABS_BASIC_INFO` 的信息作为全量信息( `all_project` )，并从 `CSI_ABS_PROJECT_BASIC_INFO` 读取是否清算、二级分类、是否循环购买作为筛选项目标准。
    再从 `CSI_BOND_STOCK` 和 `CSI_UNLISTING_ABS_STOCK` 读取摘牌日作为筛选标准, 如果项目证券摘牌日早于 `check_date` 则不会进行测算

    """

    conn, is_mysql = connect_mysql()
    all_project = full_project_list(check_date, is_mysql, conn)

    if return_module == 'list':
        sql_list = "SELECT SECURITY_SEQ, IS_GZ, IS_SUIT_QUANTIFY_LEVEL, IS_SUIT_ASSETPOOL_CF " \
                   "FROM CSI_BOND_ABS.CSI_ABS_PROJECT_KEY_TEMP"

        df_list = pd.read_sql(sql=sql_list, con=conn)
        df_list[['IS_SUIT_QUANTIFY_LEVEL', 'IS_GZ']] = \
            df_list[['IS_SUIT_QUANTIFY_LEVEL', 'IS_GZ']].applymap(lambda x: False if str(x) == '2' else True)

        end_df = all_project.merge(df_list[['SECURITY_SEQ', 'IS_GZ', 'IS_SUIT_QUANTIFY_LEVEL']], how='inner', on='SECURITY_SEQ')
        if aim == 'valuation':
            end_df = end_df[end_df['IS_GZ']] # 不用管项目下的证券是否完整，每次都会得到项目下所有证券的现金流

        elif aim == 'rating':
            end_df['IS_SUIT_QUANTIFY_LEVEL'] = end_df['IS_SUIT_QUANTIFY_LEVEL'] & end_df['IS_GZ']  # TODO 按照估值标准筛掉的证券一般不能计算
            end_df = end_df[end_df['IS_SUIT_QUANTIFY_LEVEL']]

    else:
        end_df = all_project.copy()
        end_df['IS_GZ'] = True
        end_df['IS_SUIT_QUANTIFY_LEVEL'] = True

    if len(end_df) > 0:
        if aim == 'rating':
            end_df = end_df[end_df['PRIMARY_CLASSIFICATION']=='1']  # 评级只能用于银行间 并且限制品种
            end_df = end_df[end_df['SECONDARY_CLASSIFICATION'].isin(['1', '2', '3', '4', '5', '6', '7', '9'])]

        elif aim == 'model':
            end_df = end_df[(end_df['SECONDARY_CLASSIFICATION'] == '1') | (end_df['SECONDARY_CLASSIFICATION'] == '2')]

    conn.close()
    end_df = end_df.drop_duplicates(subset=['SECURITY_CODE'])
    end_df.sort_values(by=['PROJECT_SEQ', 'SECURITY_CODE'], ignore_index=True, inplace=True)
    end_df.columns = [x.lower() for x in end_df.columns]

    return end_df


def updated_project_list(check_date, last_date=None):
    """
    读取 `csi_abs_security_duration` , `csi_abs_duration_status` 中在起始日期截止日期之间有更新数据的项目、

    """
    conn, is_mysql = connect_mysql()

    if last_date is None:
        last_date = '19000101'

    begin_ = str(int(last_date) * 1000000)
    end_ = str(int(check_date) * 1000000)
    sql_sec_update = f"SELECT PROJECT_SEQ\
                       FROM CSI_ABS_SECURITY_DURATION \
                       WHERE MODIFY_TIME < {end_}  \
                       AND MODIFY_TIME>{begin_}"

    sec_update_project = pd.read_sql(sql=sql_sec_update, con=conn)
    sec_update_project = sec_update_project.drop_duplicates()

    sql_pool_update = f"SELECT PROJECT_SEQ \
                        FROM CSI_ABS_DURATION_STATUS \
                        WHERE MODIFY_TIME < {end_}  \
                        AND MODIFY_TIME>{begin_}"
    pool_update_project = pd.read_sql(sql=sql_pool_update, con=conn)
    pool_update_project = pool_update_project.drop_duplicates()
    conn.close()
    return set(pool_update_project['PROJECT_SEQ']).union(set(sec_update_project['PROJECT_SEQ'])
                                                  )


def full_project_list(check_date, is_mysql, conn):
    sql_all = "SELECT A.SECURITY_CODE, A.SECURITY_SEQ, A.PROJECT_SEQ, B.PROJECT_NAME, B.IS_RECURRING_POOL, " \
              "B.PRIMARY_CLASSIFICATION, B.SECONDARY_CLASSIFICATION, A.MODIFY_TIME " \
              "FROM CSI_ABS_BASIC_INFO A " \
              "LEFT JOIN (SELECT PROJECT_SEQ, PROJECT_NAME, IS_RECURRING_POOL, PRIMARY_CLASSIFICATION, " \
              "SECONDARY_CLASSIFICATION " \
              "FROM CSI_ABS_PROJECT_BASIC_INFO) B " \
              "ON A.PROJECT_SEQ = B.PROJECT_SEQ"

    sql_expiry_date1 = "SELECT A.DELIST_DATE, A.CB_SEQ, B.BOND_CODE, B.MARKET " \
                       "FROM {}.CSI_BOND_STOCK A " \
                       "LEFT JOIN (SELECT CB_SEQ, BOND_CODE, MARKET " \
                       "FROM CSI_BOND_STOCK_CODE) B " \
                       "ON A.CB_SEQ = B.CB_SEQ".format('BOND' if is_mysql else 'CSI_BOND_BS')

    sql_expiry_date2 = "SELECT SECURITY_SEQ, DELIST_DATE " \
                       "FROM CSI_BOND_ABS.CSI_UNLISTING_ABS_STOCK"

    market_suffix = {'1': 'SH', '2': 'SZ', '3': 'IB', '8': 'BJ', '9': 'BJ'}
    all_project = pd.read_sql(sql=sql_all, con=conn)  # 返回dataframe格式
    if len(all_project) < 1:
        raise Exception("项目基本数据表CSI_ABS_BASIC_INFO为空")

    all_project.drop_duplicates(keep='last', inplace=True, ignore_index=True)
    expiry_date1 = pd.read_sql(sql=sql_expiry_date1, con=conn)
    expiry_date1.dropna(subset=['BOND_CODE'], how='any', axis=0, inplace=True)
    expiry_date1['SECURITY_CODE'] = expiry_date1[['BOND_CODE', 'MARKET']].apply(
        lambda row: str(row[0]) + "." + market_suffix
        [row[1]], axis=1
        )
    all_project = all_project.merge(expiry_date1[['SECURITY_CODE', 'DELIST_DATE']], on='SECURITY_CODE', how='left')
    all_project.loc[:, 'IS_RECURRING_POOL'] = all_project['IS_RECURRING_POOL'] == '1'
    all_project.loc[:, 'IS_NPLS'] = all_project['SECONDARY_CLASSIFICATION'] == '6'

    try:
        expiry_date2 = pd.read_sql(sql_expiry_date2, con=conn)
        if len(expiry_date2) > 0:
            all_project = all_project.merge(expiry_date2, on='SECURITY_SEQ', how='left', suffixes=('', '_2'))
            all_project['DELIST_DATE'] = all_project['DELIST_DATE'].fillna(all_project['DELIST_DATE_2'])
            all_project.drop(columns=['DELIST_DATE_2'], inplace=True)
    except:
        print("未找到CSI_BOND_ABS.CSI_UNLISTING_ABS_STOCK，忽略可能存在的未上市券的摘牌日")
        pass

    # 去掉已摘牌
    all_project.loc[:, 'DELIST_DATE'] = all_project['DELIST_DATE'].apply(
        lambda x: int(x) if (str(x) != 'None') and str(x) != 'nan' else float('nan'))
    all_project.loc[:, 'MODIFY_TIME'] = all_project['MODIFY_TIME'].apply(
        lambda x: int(x) if (str(x) != 'None') and str(x) != 'nan' else float('nan'))
    all_project = all_project.loc[
                  (all_project['DELIST_DATE'] > int(check_date)) | (pd.isna(all_project['DELIST_DATE'])), :]  # 已到期的不管

    return all_project


def blob_to_series(blob_):
    """将 '1,10;2,20;' 格式的字符串转为pd.Series"""
    return pd.Series(blob_to_dict(blob_))


def blob_to_dict(blob_):
    """将 '1,10;2,20;' 格式的字符串转为dict"""
    return {x.split(",")[0]: x.split(",")[1] for x in blob_.split(";")}


def blob_to_double_dict(blob):
    """将 'cpr_rate,100;cdr_rate,90 cpr_rate,90;cdr_rate,90' 格式的字符串转为dict"""
    lst1 = blob.split(" ")
    dicts = [blob_to_dict(y) for y in lst1]
    keys_ = range(len(lst1))
    return dict(zip(keys_, dicts))


def read_input(test_df, trade_date):
    """读取列表中对应的估值参数

    Args:
        test_df (pd.DataFrame): 估值样本
        trade_date (str): 估值日期

    Returns:
        tuple, tuple contains:

                        test_params (pd.DataFrame): 参数列表
                        fail_projects (list): 读不到参数的项目

    """
    param_db = UploadLib(param_source_db)
    param_db.connect()

    project_seqs = list(set(test_df['project_seq']))
    test_params = param_db.select(read_param(trade_date))
    param_db.close()
    test_params = test_params.loc[test_params['PROJECT_SEQ'].isin(project_seqs)]

    if len(test_params) > 0:
        dict_ = data_enumerators()
        input_dict = dict_['CSI_ABS_VALUATION_INPUT']
        for x in input_dict:
            test_params[x] = test_params[x].apply(lambda y: input_dict[x].get(y, None))

        BLOB_COLS = ['DPS', 'CPRS', 'RRS', 'CDRS', 'PPS', 'YRS', 'DRS', 'YCDRS']

        test_params[BLOB_COLS] =\
            test_params[BLOB_COLS].applymap(lambda x: blob_to_series(x) if str(x) not in ('None', 'nan', '') else None)

        test_params = test_params.replace('True', True).replace('False', False).replace('None', None)
        test_params.columns = [x.lower() for x in test_params.columns]
        test_params['trade_date'] = trade_date
        test_params['is_security'] = False

    if len(test_params) > 0:
        test_params = test_params.merge(
            test_df[['project_seq', 'security_code']].drop_duplicates(subset=['project_seq'], keep='first'),
            on='project_seq', how='outer')

        test_params.reset_index(drop=True, inplace=True)
        # 设置折现参数，只是为了保证能看到简单的折现结果和衍生指标
        test_params['value_method'] = 'yield_'
        test_params['curve_name'] = float('nan')
        test_params['input_type'] = 'yield_'
        test_params['input_value'] = 1

        fail_projects = list(set(test_params[test_params['input_seq'].isna()].drop_duplicates(subset=['project_seq'], keep='first')['security_code'].tolist()))
    else:
        fail_projects = test_df['security_code']

    if len(fail_projects) > 0:
        print("以下证券对应项目缺乏估值参数： ", ",".join(fail_projects))
        with open(os.path.join(LOG_DIR, 'log_input_' + trade_date + '.txt'), "w") as f:
            f.write(datetime.now().strftime("%Y%m%d%H%M%S") + "|" + "以下证券对应项目缺乏估值参数： " + ",".join(fail_projects))
            f.close()
    if len(test_params) > 0:
        test_params.dropna(subset=['input_seq'], axis=0, inplace=True)

    test_params = test_params.drop_duplicates(subset=['project_seq'], keep='first')
    test_params.reset_index(drop=True, inplace=True)
    return test_params, fail_projects


def read_input_rating(test_df, trade_date):
    """读取列表中对应的评级参数.
    注意当数据来源为'2'，即采用对应的估值入参时，则将估值入参填入到自定义入参对应的字段中（因为评级主函数不支持读取估值的入参仅支持自定义输入或者直接读取评级报告、根据历史数据计算）

    Args:
        test_df (pd.DataFrame): 评级样本
        trade_date (str): 评级日期

    Returns:
        tuple, tuple contains:

                        test_params (pd.DataFrame): 参数列表
                        fail_projects (list): 读不到参数的项目

    """
    param_db = UploadLib(param_source_db)
    param_db.connect()

    project_seqs = list(set(test_df['project_seq']))
    test_params = param_db.select(read_param_rating(project_seqs, trade_date))

    if len(test_params) > 0:
        cols = ['DPS', 'CPRS', 'RRS', 'PPS', 'YRS', 'CDR', 'CPR', 'RR', 'DPP', 'PP', 'YR', 'RP',
                'DP_MATCH_PERFECTLY', 'PARAM_MATCH_METHOD', 'EXIST_TAX_EXP', 'EXP_RATE', 'TAX_RATE']
        # 依赖于估值入参的情况，读取估值入参
        df_correlate = test_params[test_params['DATA_SOURCE']=='2']
        df_correlate.rename_axis('index', inplace=True)
        df_correlate.reset_index(drop=False, inplace=True)
        if len(df_correlate) > 0:
            valuation_inputs = pd.DataFrame()
            part1 = tuple(list(df_correlate.loc[~df_correlate['VALUATION_INPUT_SEQ'].isna(), 'VALUATION_INPUT_SEQ']))
            if len(part1) > 0:
                valuation_inputs1 = param_db.select("SELECT * FROM CSI_ABS_VALUATION_INPUT WHERE INPUT_SEQ IN %s" % part1)
                valuation_inputs = valuation_inputs.append(valuation_inputs1)

            part2 = list(df_correlate.loc[df_correlate['VALUATION_INPUT_SEQ'].isna(), 'PROJECT_SEQ'])
            if len(part2) > 0:
                valuation_inputs2 = param_db.select(read_param(part2, trade_date))
                valuation_inputs = valuation_inputs.append(valuation_inputs2)

            df_correlate = df_correlate.merge(valuation_inputs[['PROJECT_SEQ'] + cols],
                                              on='PROJECT_SEQ', how='outer')

            df_correlate.loc[:, 'MODULE_TYPE'] = df_correlate['CPR'].apply(lambda x:
                                                                           'static' if str(x) not in ('None', 'nan', '')
                                                                           else 'dynamic')
            test_params[cols] = None
            test_params.loc[df_correlate['index'], cols + ['MODULE_TYPE']] = \
                df_correlate[cols + ['MODULE_TYPE']]

            # 转化以长字符串保存，在程序里以dict或者pd.Series格式进行处理的参数
            BLOB_COLS = ['DPS', 'CPRS', 'RRS', 'PPS', 'YRS']  # 需要转为series的
            test_params[BLOB_COLS] = test_params[BLOB_COLS].applymap(
                lambda x: blob_to_series(x) if str(x) not in ('None', 'nan', '') else None)

        dict_ = data_enumerators()
        # 枚举值映射
        input_dict = dict_['CSI_ABS_RATING_INPUT']
        for x in input_dict:
            test_params[x] = test_params[x].apply(lambda y: input_dict[x].get(y, None))

        BLOC_COLS2 = ['CUSTOM_TDR']  # 需要转为dict的
        test_params[BLOC_COLS2] = test_params[BLOC_COLS2].applymap(
            lambda x: blob_to_dict(x) if str(x) not in ('None', 'nan', '') else None)

        test_params.columns = [x.lower() for x in test_params.columns]
        test_params['trade_date'] = trade_date
        test_params['rating_range'] = 'project'

        test_params = test_params.merge(
            test_df[['project_seq', 'security_code']].drop_duplicates(subset=['project_seq'], keep='first'),
            on='project_seq', how='outer')

        test_params.reset_index(drop=True, inplace=True)
        fail_projects = list(set(test_params[test_params['rating_input_seq'].isna()].drop_duplicates(subset=['project_seq'], keep='first')['security_code'].tolist()))
        test_params.dropna(subset=['rating_input_seq'], axis=0, inplace=True)

        test_params.rename(columns={'dps': 'custom_dp', 'cprs': 'custom_cprs', 'rrs': 'custom_rrs',
                                    'pps': 'custom_pps', 'yrs': 'custom_yrs',
                                    'cdr': 'custom_cdr', 'cpr': 'custom_cpr',
                                    'rr': 'custom_rr', 'dpp': 'custom_dpp', 'yr': 'custom_yr',
                                    'rp': 'custom_rp'}, inplace=True)

        test_params = test_params.replace('True', True).replace('False', False).replace('None', None)

    else:# 读不到参数的情况
        fail_projects = test_df['security_code']

        if len(fail_projects) > 0:
            print("以下证券对应项目缺乏评级参数： ", ",".join(fail_projects))
            with open(os.path.join(LOG_DIR, 'log_input_' + trade_date + '.txt'), "w") as f:
                f.write(datetime.now().strftime("%Y%m%d%H%M%S") + "|" + "以下证券对应项目缺乏评级参数： " + ",".join(fail_projects))
                f.close()

    param_db.close()
    return test_params, fail_projects


def read_param(trade_date):
    """读取项目估值参数的SQL"""
    s = """SELECT * FROM CSI_BOND_ABS.CSI_ABS_VALUATION_INPUT 
        WHERE TRADE_DATE = (SELECT MAX(TRADE_DATE) 
        FROM CSI_BOND_ABS.CSI_ABS_VALUATION_INPUT 
        WHERE TRADE_DATE <= %s )"""%(trade_date)
    return s


def read_param_rating(project_seqs, trade_date):
    """读取项目评级参数的SQL"""
    s = "SELECT MAX(A.DATE) as UPDATE_DATE, B.* FROM CSI_ABS_RATING_INPUT A " \
        "LEFT JOIN CSI_ABS_RATING_INPUT B " \
        "ON A.PROJECT_SEQ = B.PROJECT_SEQ " \
        "AND A.DATE = B.DATE " \
        "WHERE A.PROJECT_SEQ IN %s " \
        "AND A.DATE <= %s " \
        "GROUP BY A.PROJECT_SEQ "% (tuple(project_seqs), trade_date)
    return s


def get_last_rundate(trade_date):
    param_db = UploadLib(local_db)
    param_db.connect()
    df = param_db.select("select max(date) as last_date, invalid_securities as fails from SUMMARY "
                    "where module='valuation' "
                    f"and date < {trade_date}")
    last_date = df.loc[0, 'last_date']
    invalid_securities = df.loc[0, 'fails'].split(";")
    param_db.close()
    return last_date, invalid_securities


def get_last_date_list(last_date):
    param_db = UploadLib(local_db)
    param_db.connect()
    df = param_db.select("select * from CSI_HISTORY_LIST "
                         f"where date = {last_date}")
    param_db.close()
    return df
