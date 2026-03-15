# -*- coding: utf-8 -*-

"""批量测试函数，只用于检查模型是否正常跑通，不用于生产

"""

import numpy as np
import pandas as pd
from itertools import product
from datetime import datetime
from main.production.pscheck_add import main_batch_run, main_batch_rating
from main.production.filter import new_project


def get_virtual_params(test_module, check_date, model_type):
    if test_module == 'user':
        parameters = param_set_test(check_date)
        # parameters = params_set_user(check_date)
    elif test_module == 'rating_test':
        parameters = params_set_rating(check_date)
    elif test_module == 'model':
        parameters = param_set_model(check_date, model_type)
    else:
        raise ValueError('必须输入参数组列表')

    df_params = pd.DataFrame({'project_seq': test_df['project_seq'], 'security_code': test_df['security_code'],
                              'is_recurring_pool': test_df['is_recurring_pool'], 'is_npls': test_df['is_npls']}).drop_duplicates(subset=['project_seq'], keep='first')
    project_number = len(df_params)
    df_params = pd.DataFrame(np.repeat(df_params.values, len(parameters), axis=0), columns=['project_seq',
                                                                                            'security_code',
                                                                                            'is_recurring_pool',
                                                                                            'is_npls'])

    df_ = pd.DataFrame(parameters)
    df_['num'] = range(len(df_))
    df_['num'] = df_['num'].astype(str)
    df_ = pd.DataFrame(list(df_.values.tolist()) * project_number, columns=df_.columns)
    df_params.loc[:, df_.columns] = df_

    prefix = datetime.now().strftime("%Y%m%d")
    if test_module == 'model':
        suffix = model_type[0].upper()
    elif test_module == 'rating_test':
        suffix = 'RT'
    elif test_module == 'user':
        suffix = 'U'
    else:
        suffix = 'GZ'

    if test_module == 'rating_test':
        df_params['rating_input_seq'] = prefix + df_params['security_code'].str.replace(".", "") + df_params['num'] + suffix
    else:
        df_params['input_seq'] = prefix + df_params['security_code'].str.replace(".", "") + df_params['num'] + suffix
    df_params.drop(columns='num', inplace=True)
    if test_module != 'rating_test':
        df_params.drop(index=df_params[(~df_params['is_recurring_pool']) & ((df_params['module_type'] == 'static_rev_recal') | (df_params['module_type'] == 'series_rev_recal'))].index, inplace=True)
        df_params.drop(index=df_params[(~df_params['is_npls']) & (df_params['module_type']=='static_npl_recal')].index, inplace=True)
    df_params.reset_index(drop=True, inplace=True)
    return df_params


def params_set_rating(check_date):
    """
    用于测算是不是能够跑通程序的单一参数组

    Args:
        check_date:

    Returns:

    """
    params = [{"rating_date": str(check_date[:8].strip("'")),  # 评级日期
               'customize_scenario': False, 'scenario_sets': None, 'exist_exp_tax': False,
               'exp_rate': 1, 'tax_rate': 3.26, 'suoe': False,  # 只能缩期
               'data_source': None,  # None 自动选择
               'rating_method': None,  # 自动选
               'multiplier_source': 'rating_report',
               'custom_multiplier': None, 'custom_tdr': None,
               'lognorm_source': None, 'tdr_source': None}]
    return params


def params_set_user(check_date, params_dict=None):
    """
    设置统一入参 `common_params` , 如果没有输入任何参数，则直接设置一些默认参数作为测试参数,最终由 ``get_params_list`` 判断最后使用的参数（根据是否是对单个项目使用不同的参数）

    Args:
        check_date （str): 当前日期
        params_dic (dict): [{'module_type': 'static_normal1', 'CDR': 1, 'RR': 20, 'DPP': 1, 'CPR': 10}]

    Returns:
        list: 参数，由几个不同的参数组合(dict) 构成的列表，如果只有一种加压情景则只有一个dict


    """
    param_dict = []
    params = {"param_name": '缩额法',
     "trade_date": str(check_date[:8].strip("'")),  # 交易日期
     # 计算参数
     "exist_tax_exp": True,
     "exp_rate": 0,
     "tax_rate": 3.26,
     'coupon_rate_change': 0,

     # 计算依据
     "scenario_type": 'user',
     "model_type": None,
     "suoe": True,
     "upload": False,  # 暂且不上传模型结果
     "is_security": False,
     "cal_derivatives": True
     }

    if params_dict is not None:
        basic_params = params_dict
        for module_type in basic_params.keys():

            p_ = params.copy()
            p_.update((basic_params[module_type]))
            param_dict.append(p_)

    else:
        CDR = 0
        CPR = 0
        RR = 0
        DPP = 1
        YCDR = 0

        DR = 100
        RP = 100
        YR = 0
        PP = 20


        # value_method = ['yield_', 'curve']
        value_method = ['yield_'] # todo 数据库曲线不够
        module_types = ['static_normal1',
                        'static_normal2',
                        'static_npl_recal',
                       'static_rev_recal']
        curve_name = 'cc_ll_gz'

        for (vm, module_type) in product(value_method, module_types):
            if vm == 'yield_':
                input_value = 3
                input_type = 'yield_'
            else:
                input_value = 0.3
                input_type = 'spread'
            p_ = params.copy()
            p_.update({'input_type': input_type, "input_value": input_value, 'module_type': module_type, 'curve_name': curve_name,
                       'value_method': vm})
            if module_type == 'static_normal1':
                pressure_param = {'CDR': CDR, 'CPR': CPR, 'RR': RR, 'DPP': DPP, 'recal_cashflow': False}
            elif module_type == 'static_normal2':
                pressure_param = {'YCDR': YCDR, 'CPR': CPR, 'RR': RR, 'DPP': DPP, 'recal_cashflow': False}
            elif module_type == 'static_npl_recal':
                pressure_param = {'RR': RR, 'recal_cashflow': True}
            elif module_type == 'static_rev_recal':
                pressure_param = {'CDR': CDR, 'CPR': CPR, 'RR': RR, 'DPP': DPP, 'PP': PP, 'RP': RP, 'recal_cashflow': True}

            p_.update(pressure_param)
            #
            if module_type == 'static_rev_recal':
                p1 = p_.copy()
                p1.update({'YR': YR})
                param_dict.append(p1)
                p2 = p_.copy()
                p2.update({'DR': DR})
                param_dict.append(p2)
            else:
                param_dict.append(p_)

    return param_dict


def param_set_model(check_date, model_type):
    """
    为加压参数模型设置统一入参 `common_params` , 如果没有输入任何参数，则直接设置一些默认参数作为测试参数, 最终由 ``get_params_list`` 判断最后使用的参数（根据是否是对单个项目使用不同的参数）

    Args:
        check_date （str): 当前日期
        model_type (str): 模型类型

    Returns:
        list: 参数，由几个不同的参数组合(dict) 构成的列表，如果只有一种加压情景则只有一个dict

    """
    CPRs = pd.Series([10, 8, 5], index=[1, 2, 3])
    RRs = pd.Series([30, 20, 10], index=[1, 3, 5])
    param_dict = []
    params = {"param_name": '缩额法',
     "trade_date": str(check_date[:8].strip("'")),  # 交易日期
     # 计算参数
     "exist_tax_exp": True,
     "exp_rate": 0,
     "tax_rate": 3.26,
     'coupon_rate_change': 0,

     # 计算依据
     "scenario_type": 'model',
     "model_type": model_type,
     "suoe": True,
     "upload": False,  # 暂且不上传模型结果
     "is_security": False,
     "cal_derivatives": True,
     "module_type": 'series_normal1',
     "value_method": 'yield_',
     "input_value": 3,
     "input_type": 'yield_'}

    p_ = params.copy()
    if model_type == 'linear_model' or model_type == 'sigmoid_model':
        p_.update({'RRs': RRs})
    elif model_type == 'extrapolate_model':
        p_.update({'RRs': RRs, 'CPRs': CPRs})

    if model_type == 'markov_model':
        p1 = p_.copy()
        p1['n_markov'] = True
        param_dict.append(p1)
        p2 = p_.copy()
        p2['n_markov'] = False
        param_dict.append(p2)

    elif model_type == 'linear_model' or model_type == 'sigmoid_model':
        p1 = p_.copy()
        p1['bayers'] = True
        param_dict.append(p1)
        p2 = p_.copy()
        p2['bayers'] = False
        param_dict.append(p2)

    else:
        param_dict.append(p_)

    return param_dict

def param_set_test(check_date):
    param_dict = []
    CDR = 0
    CPR = 0
    RR = 0
    DPP = 1
    input_value = 0
    input_type = 'yield_'
    params = {"param_name": '缩额法',
              "trade_date": str(check_date[:8].strip("'")),  # 交易日期
              # 计算参数
              "exist_tax_exp": False,
              "exp_rate": 0,
              "tax_rate": 0,
              'coupon_rate_change': 0,

              # 计算依据
              "scenario_type": 'user',
              "model_type": None,
              "suoe": True,
              "upload": False,  # 暂且不上传模型结果
              "is_security": False,
              "cal_derivatives": True,
              'CDR': CDR, 'CPR': CPR, 'RR': RR, 'DPP': DPP, 'recal_cashflow': False,
              'input_type': input_type, "input_value": input_value,
              'module_type': 'static_normal1',
               'curve_name': None,
               'value_method': 'yield_'
              }
    param_dict.append(params)
    return param_dict


if __name__ == '__main__':

    test_range = pd.date_range(start='20240101', end='20240131', freq='D')
    # trade_date = datetime.now().strftime("%Y%m%d")

    test_module = 'user'
    model_type = 'extrapolate_model'

    for trade_date in test_range:
        try:
            trade_date = trade_date.strftime("%Y%m%d")
            if test_module in ('user', 'model'):
                test_df = new_project(check_date=trade_date, return_module='all', aim=test_module)
                test_df['update'] = 1
                if len(test_df) < 1:
                    raise ValueError("无项目")

                df_params = get_virtual_params(test_module, trade_date, model_type)

                patch_info, df_fail = \
                    main_batch_run(test_df, test_module=test_module, check_date=trade_date, df_params=df_params, model_type=model_type)

            elif test_module == 'rating_test':

                test_df = new_project(check_date=trade_date, return_module='all', aim='rating')
                test_df['update'] = 1
                test_df = test_df.loc[0:10, :]
                df_params = get_virtual_params(test_module, trade_date, model_type=None)

                if len(test_df) < 1 or len(df_params) < 1:
                    raise ValueError("缺少项目信息")

                df_ranks_dict, df_critical_dict, df_ranks_cn_dict, df_critical_cn_dict, df_model_params_dict, \
                patch_info, df_fail, df_params, df_fail_project = \
                    main_batch_rating(test_df, test_module='rating_test', check_date=trade_date, df_params=df_params)
        except:
            pass