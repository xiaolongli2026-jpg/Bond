# -*- coding: utf-8 -*-

"""评级所需数据读取，包括基础数据和配置数据

"""


import numpy as np
from rating.prepare.config import rating_indicator_dict, rating_method_dict, multiplier_dict, default_prob_dict
from abs.prepare.data_load.load_basic_data import load_all_basic
from abs.prepare.data_preprocess.dataFullandAdjust import data_full
from conf.conf import default_params
from conf.table_structure import data_enumerators
from utils.sql_util import sql_read
from abs.doc.global_var import global_var as glv
from operator import itemgetter
from rating.prepare.config import scenario_combination, scenario_nums_dict, scenario_dict


def data_load(security_code, rating_date, cur):
    """获取评级所需数据，包括用于现金流计算器的数据，及评级表的数据"""
    # 一、数据匹配、提取
    # 1.1 读取ABS项目相关基本信息（非评级信息）

    # 预计在ABS基础信息维护系统新增加三张表的维护功能，每个表字段预计不超过十个，字段不涉及复杂逻辑处理（由人工导入维护）；
    # 原数据表预计新增不超过10个字段，每个字段也不涉及复杂逻辑
    # 封装估值部提供的评分模型，支持数据层调用，跑出模型结果，并将ABS评分模型结果落表
    df_product, df_tranches, df_prediction, df_sequence, df_plan, df_other, df_trigger, events, df_date_rule, \
    contingent_param_dict, security_seq, project_seq, warns_lst, df_date_rule2, df_work_schedule \
        = load_all_basic(security_code, rating_date, cur=cur)
    is_revolving_pool = df_product.loc[0, 'is_revolving_pool']  # 只有消费贷循环购买产品才适用循环购买加压
    df_product, df_tranches, df_prediction, df_sequence, df_plan, df_other, df_trigger, df_schedule, \
    warns_lst2 \
        = data_full(df_product=df_product, df_tranches=df_tranches, df_prediction=df_prediction,
                    df_sequence=df_sequence, df_plan=df_plan, df_other=df_other, df_trigger=df_trigger,
                    events=events,
                    df_date=df_date_rule, if_recal=is_revolving_pool, split_default=default_params['split_default'],
                    df_daterule=df_date_rule2, df_calendar=df_work_schedule)  # 数据处理

    # 1.2 提取评级报告数据
    secondary_classification = df_product.loc[0, 'secondary_classification']
    rating_report_info = rating_report_data(secondary_classification, rating_date, cur)

    # 1.3 评级数据里可以没有待估的这一单
    if project_seq.strip("'") not in rating_report_info.columns:
        rating_report_info.loc[:, project_seq.strip("'")] = rating_report_info.mean(axis=1)

    return df_product, df_tranches, df_prediction, df_sequence, df_plan, df_other, df_trigger, events, \
           contingent_param_dict, df_schedule, \
           rating_report_info, project_seq, security_seq, is_revolving_pool, secondary_classification


def rating_report_data(secondary_classification: str, rating_date, cur):
    """
    收入ABS二级分类，获取分类下对应的评级报告数据

    Args:
        secondary_classification (str): ABS二级分类
        rating_date (str): 评级日期

    Returns: info, DataFrame, 评级报告中采集的数据

    """

    # 一、数据读取
    sql_ = """SELECT A.PROJECT_SEQ, A.RATE_SOURCE, A.INDICATOR_TYPE AS INDICATOR, A.INDICATOR_VALUE 
              FROM CSI_ABS_ASSETPOOL_LEVEL A 
              RIGHT JOIN (SELECT PROJECT_SEQ, MAX(REPORT_DATE) AS LAST_REPORT FROM CSI_ABS_ASSETPOOL_LEVEL 
              WHERE SECONDARY_CLASSIFICATION=%s AND REPORT_DATE <= %s GROUP BY PROJECT_SEQ) B 
              ON A.PROJECT_SEQ=B.PROJECT_SEQ AND A.REPORT_DATE = B.LAST_REPORT """ %(secondary_classification, rating_date)

    is_mysql = glv().get('is_mysql')
    info = sql_read(sql_, cur, is_mysql, False)

    # 2. 重设index column
    if len(info) < 1:
        raise ValueError(f"该二级分类 ( {secondary_classification} ) 下无评级报告数据，为不支持的分类，不予评级")
    info = info.groupby(by=['indicator', 'project_seq']).mean() # 不同数据来源（评级机构）的数据不一样，取均值，如果其中有NULL，不会用在计算中
    info.reset_index(drop=False, inplace=True)
    info = info.pivot(index='indicator', columns='project_seq', values='indicator_value')

    # 3 违约率（非不良）、回收率（不良）数据填充
    if secondary_classification != '6':
        # 非不良贷款填充违约率，采用优先顺序为：CDR，MU（对数正态分布），核销率
        info = info.apply(lambda s: s.fillna(s.mean()), axis=1) # 为空的数据用同类均值填充
        if (rating_indicator_dict['MU'] in info.index) and (rating_indicator_dict['CDR'] not in info.index):
            info.loc[rating_indicator_dict['CDR'], :] = np.exp(info.loc[rating_indicator_dict['MU'], :]\
                                       + info.loc[rating_indicator_dict['SIGMA'], :] ** 2 / 2) # 计算参数对应的累计违约率（假设了累计违约率服从对数正态分布）

        hxl_exist = rating_indicator_dict['HXL'] in info.index
        cdr_exist = rating_indicator_dict['CDR'] in info.index
        if cdr_exist and hxl_exist:
            info.loc[rating_indicator_dict['CDR'], :] = info.loc[[rating_indicator_dict['CDR'], rating_indicator_dict['HXL']], :].mean(0)
        elif cdr_exist or hxl_exist:
            info.loc[rating_indicator_dict['CDR'], :] = info.loc[rating_indicator_dict['CDR'], :] \
                if cdr_exist else info.loc[rating_indicator_dict['HXL'], :]
        else:
            raise KeyError(f'二级分类{secondary_classification}下缺少违约率、核销率、mu数据,无法获取有效的累计违约率数据')

    else:
        # 不良贷款
        info.loc[rating_indicator_dict['CS_FEE'], :] = info.loc[rating_indicator_dict['CS_FEE'], :].fillna(
            info.loc[rating_indicator_dict['CS_FEE'], :].mean()) # 催收费用
        info.loc[rating_indicator_dict['RcR_MAO'], :] = info.loc[rating_indicator_dict['RR_MAO'], :].fillna(
            info.loc[rating_indicator_dict['RR_NET'], :] / (1 - info.loc[rating_indicator_dict['CS_FEE'], :])) # 毛回收率=净回收率/（1-催收费用）
        info = info.apply(lambda s: s.fillna(s.mean()), axis=1)

    return info


def auto_rating_method(second_class, rating_date, cur):
    """
    自动选择评级方法，当前根据config中的配置，每一个二级分类一种

    Args:
        second_class (str): 二级分类
        rating_date (str): 日期
        cur (cursor): UploadLib实例

    Returns:
        tuple: tuple contains:
                data_source: 基准数据来源,
                rating_method: 评级方式,
                lognorm_source: 如果是对数正态分布的，参数来源,
                tdr_source: 目标违约率计算方式

    """
    try:
        sql_ = """SELECT RATING_METHOD, LOGNORM_SOURCE, TDR_SOURCE, DATA_SOURCE FROM CSI_ABS_RATING_CONFIG 
                  WHERE SECOND_CLASS = %s 
                  AND DATE = (SELECT  MAX(DATE) FROM CSI_ABS_RATING_CONFIG 
                  WHERE SECOND_CLASS = %s and DATE <= %s 
                  GROUP BY SECOND_CLASS)""" %(second_class, second_class, rating_date)

        conf = cur.select(sql_)
        input_dict = data_enumerators()
        dict_ = input_dict['CSI_ABS_RATING_CONFIG']

        data_source, rating_method, lognorm_source, tdr_source = \
            dict_['DATA_SOURCE'].get(conf.loc[0, 'DATA_SOURCE'], None), \
            dict_['RATING_METHOD'].get(conf.loc[0, 'RATING_METHOD'], None), \
            dict_['LOGNORM_SOURCE'].get(conf.loc[0, 'LOGNORM_SOURCE'], None), \
            dict_['TDR_SOURCE'].get(conf.loc[0, 'TDR_SOURCE'], None)

    except:  #当前还没有表
        print("采用配置文件中的评级方法设置")
        if second_class in rating_method_dict:
            data_source, rating_method, lognorm_source, tdr_source = rating_method_dict[second_class]
        else:
            raise ValueError(f"该二级分类下的项目暂不支持评级: {second_class}")
    return data_source, rating_method, lognorm_source, tdr_source


def get_multiplier(second_class, rating_date, cur):
    """
    获取对应的压力乘数dict

    Args:
        second_class (str): 二级分类
        rating_date (str): 日期
        cur (cursor): UploadLib实例

    Returns: dict

    """

    try:
        sql_ = """SELECT RANK, MULTIPLIER FROM CSI_ABS_RATING_MULTIPLIER 
                    WHERE SECOND_CLASS = %s
                    AND DATE = (SELECT 
                    MAX(DATE) FROM CSI_ABS_RATING_MULTIPLIER 
                    WHERE SECOND_CLASS = %s and DATE <= %s 
                    GROUP BY SECOND_CLASS)""" % (second_class, second_class, rating_date)

        conf = cur.select(sql_)
        multipliers = dict(zip(conf['RANK'], conf['MULTIPLIER']))
    except:  # 当前还没有表
        print("压力乘数表无数据，采用配置文件中的压力乘数设置")
        if second_class in multiplier_dict:
            multipliers = multiplier_dict[second_class]
        else:
            raise ValueError(f"该二级分类下的项目暂不支持评级: {second_class}")
    return multipliers


def get_default_prob(second_class, rating_date, cur):
    """
    获取违约概率

    Args:
        second_class (str): 二级分类
        rating_date (str): 日期
        cur (cursor): UploadLib实例

    Returns: dict

    """
    try:
        sql_ = """SELECT RANK, DEFAULT_RATE FROM CSI_ABS_RATING_TARGET_DEFAULT_RATE 
                    WHERE SECOND_CLASS = %s
                    AND DATE = (SELECT 
                    MAX(DATE) FROM CSI_ABS_RATING_TARGET_DEFAULT_RATE 
                    WHERE SECOND_CLASS = %s and DATE <= %s)""" % (second_class, second_class, rating_date)

        conf = cur.select(sql_)
        default_prob = dict(zip(conf['RANK'], conf['DEFAULT_RATE']))
    except:  # 当前还没有表
        print("压力乘数表无数据，采用配置文件中的违约概率设置")
        if second_class in default_prob_dict:
            default_prob = default_prob_dict[second_class]
        else:
            raise ValueError(f"该二级分类下的项目暂不支持评级: {second_class}")
    return default_prob


def get_scenarios(second_class, rating_date, is_rev, cur):
    """获取压力情景（同二级分类使用同样的一种情况）

    Args:
        second_class (str): 二级分类
        rating_date (str): 日期
        is_rev (bool): 是否循环购买
        cur (cursor): UploadLib实例

    Returns:

    """
    try:
        sql_ = "SELECT SCENARIO_SEQS FROM CSI_ABS_RATING_SCENARIO_COMBINATION " \
               "WHERE SECOND_CLASS = '%s' " \
               "AND IS_REV = '%s' " \
               "AND DATE = (SELECT MAX(DATE) " \
               "FROM CSI_ABS_RATING_SCENARIO_COMBINATION " \
               "WHERE SECOND_CLASS = '%s' AND DATE <= %s AND IS_REV = '%s' ) " %(second_class,
                                                                                  '1' if is_rev else '0',
                                                                                  second_class,
                                                                                  rating_date,
                                                                                  '1' if is_rev else '0')

        conf = cur.select(sql_)
        scenarios = tuple(conf.iloc[-1]['SCENARIO_SEQS'].split(","))

        sql2 = f"""SELECT SCENARIO_SEQ, SCENARIO, SCENARIO_DETAIL FROM CSI_ABS_RATING_SCENARIO 
        WHERE SCENARIO_SEQ IN {scenarios}"""
        df = cur.select(sql2)
        labels = list(df['SCENARIO_SEQ'])
        sets_ = {}
        i = -1
        for x in df['SCENARIO']:
            i += 1
            lst = []
            x_lst = x.split(" ")
            for y in x_lst:
                dict_ = {}
                y_lst = y.split(";")
                for z in y_lst:
                    z_lst = z.split(",")
                    try:
                        z_lst[1] = float(z_lst[1])
                    except:
                        pass
                    dict_.update({z_lst[0]: z_lst[1]})
                lst.append(dict_)
            sets_.update({labels[i]: lst})
        scenario_desc = df['SCENARIO_DETAIL'].to_list()

    except:

        labels = scenario_combination['non-revolve'][second_class]
        sets_ = scenario_nums_dict.copy()
        scenario_desc = itemgetter(*labels)(scenario_dict)

    return labels, sets_, scenario_desc