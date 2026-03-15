# -*- coding: utf-8 -*-
"""
module_abs_new测试

修改文件中security_code, scenario_type, model_type, 使用默认参数就行测试。也可以修改其他参数进行测试。
"""
import os
import sys
from pathlib import Path
ROOT_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(ROOT_PATH)

import time
import pandas as pd
from main.abs_calculator.module_abs_new import abs_calculator

log_path = Path(ROOT_PATH)  / 'log'
log_path.mkdir(parents=True, exist_ok=True)
# logging.basicConfig(filename=log_path / 'debug.log', level=logging.DEBUG)  # 解除注释就会记录详细的现金流日志

#pd.set_option('mode.chained_assignment', None)  # 不显示warning

time1 = time.time()

# =============================================================================
# 说明：
# 1、选择一个scenario_type, model or user;
# 2、scenario_type为model， 需要选择一个model_type, 每个模型所特有的参数如下；
# 3、scenario_type为user表示由用户输入参数， 将下方2.1模块前面的"#"去掉用就可以。
# 4.test_params需输入其他参数，也可以不修改。

# =============================================================================
# case 1 使用预测模型

# scenario_type = 'model'
# revo_cashflow_method = 'user' #直接用披露的现金流归集表加压计算
# revo_cashflow_method = 'recal' #循环购买的现金流归集表重新测算，需要配各种参数

# =============================================================================
# case 1.1

# case 1.1.1
# model_type = 'linear_model'
# or
# model_type = 'sigmoid_model'

bayers = False  # (项目中的bayes全都写成了bayers)

# =============================================================================
# case 1.2
# model_type = 'markov_model'   # 需要注意Markov用新的口径调整了
n_markov = False

# =======00======================================================================
# case 1.3
# model_type = 'extrapolate_model'
CPR = 1.

# =============================================================================
# case 2.1 不使用预测模型，并假设违约为均匀分布，每期条件早偿率为常数
scenario_type = 'user'
model_type = None
# scenario_type='model'
# model_type = 'linear_model'
# =============================================================================
# case 2.2 不使用预测模型，在scenario_df里指定每一期的违约分布和条件早偿率
#          scenario_df 有三列：date_: 八位数字格式日期，具体有哪些日期可以从不加压的现金流归集表得到；
#                            DP: 违约分布，理论上其和应为1，但程序不会检查。注意不是违约回收延迟月数；
#                            CPR: 年化条件早偿率，单位不是%而是1
# scenario_type = 'user_dynamic'
# scenario_df = pd.DataFrame({'date_': ['20211231', '20220131'],
#                             'DP': [0.05, 0.1],
#                             'CPR': [0.11, 0.12]})  # 需要自己创建DataFrame
# model_type = None

# =============================================================================
# =============================================================================


# 非循环购买、循环购买不重新测算现金流的参数
#test_params = {
#
#    "security_code": '2189237.IB',
#    "trade_date": '20240301',  # 交易日期
#    "CDR":0,
#    "DPs": pd.Series([30, 10, 10, 10, 10], index=[1,2,3,4,5]), # 不能出现index=0
#    "CPR": 3,  # 年化条件早偿率，单位是 %
#    "CPRs": pd.Series([30, 20, 10], index=[1,2,3]),
#    "RRs": pd.Series([20,10,5], index=[1,2,3]),
#    "YCDRs": pd.Series([10,4,6], index=[1,5,8]),
#    "YCDR": 3,
#    "RR": 22,  # 违约回收率，单位是 %
#    "DPP": 12,  # 违约回收延迟月数。用'DP'也可以

#    "module_type": 'static_normal1',
#    "exist_tax_exp": True,  # 是否有税费
#    "exp_rate": 0,  # 费率，单位是 %
#    "tax_rate": 3.26,
#    'coupon_rate_change': 0,  # 票面利率变化多少个 bps

#    #  循环购买所需参数
#    'recal_cashflow': False,  # 循环购买类是否自定义重新测算现金流，一期写死为False

#    # 计算依据
#    "scenario_type": scenario_type,
#    "value_method": "curve",
#    # "value_method": 'yield_',
#    "input_type": "spread",
#    # "input_type":'yield_',
#    'curve_name': 'cc_ll_gz',
#    # "input_type": "dirty_price",Z
#    # "input_type": "clean_price",

#    "input_value": 3,  # 单位是 %
#    # "input_value": 90,

#    "model_type": model_type,
#    "suoe": True,
#    "upload": False, # 暂且不上传模型结果
#    "is_security": False,
#    "cal_derivatives": True,

#}
# # 输入的收益率、摊还比例可以为单个数值，或者dataframe,其index表示地几个月

# =============================================================================

# 参数二 循环购买重新测算现金流的参数

# 输入方式 1：
RP = 100 # 循环购买率
YR = 2 # 收益率
PP = 15 # 每期摊还比例
CDR = 1 # 每期违约率
RR = 20 # 回收率
DP = 3 # 回收延迟月数
CPR = 10 # 早偿率
DR = 90
#
# #输入方式 2：以序列的方式输入（注：可以数值和序列参数结合）
# RP = 100
# DP = None
RRs = pd.Series([20, 20, 20], index=[2,4,6]) # 回收率，index为延迟月份，数值为回收率
CDRs = pd.Series([0.1, 0.2, 0.3, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5], index=[1,2,3,4,5,6,7,8,9])
PPs = pd.Series([20,10,10,10,10,10,10,10,10], index=[1,2,3,4,5,6,7,8,9])
CPRs = pd.Series([20,10,10,10,10,10,10,10,10], index=[1,2,3,4,5,6,7,8,9])
YRs = pd.Series([5,5,5,5,5,10,10,10,10], index=[1,2,3,4,5,6,7,8,9])
DRs = pd.Series([90,90,90,90,90,99,99,99,99], index=[1,2,3,4,5,6,7,8,9])
# discount_or_yield = 'yield' #yield-YR乘以当期回收本金作为利息，discount-YR
# # #
test_params = {
     "security_code": '143678.SZ',
     "trade_date": '20240118',  # 交易日期

     # 计算参数
     "CDR": CDR,  # 单位是 %\
     "CDRs": CDRs,
     "CPR": CPR,  # 单位是 %
     "CPRs": CPRs,
     "RRs": RRs,
     "PPs": PPs,
     # "DRs": DRs,
     # "DR": DR,
     "RR": RR,  # 单位是 %
     "DPP": DP, # todo 回收序列处理
     "exist_tax_exp": True,
     "exp_rate": 0.5,  # 单位是 %
     "tax_rate": 3.26,  # 单位是 %
     'coupon_rate_change': 0,  # 单位是 bps。注意：琴琴 20220513 版本中这里单位是 %

     # # 循环购买所需参数
     'recal_cashflow': True,  # 循环购买类是否自定义重新测算现金流， 一期写死为False
     "RP" : RP, # 循环购买率
     "YR" : YR, # 收益率
     "PP" : PP, # 每期固定摊还比例
     "interest_method": 'yield_',

     # 计算依据
     "scenario_type": scenario_type,
     "scenario_df": None,
     "value_method": "yield_",
     "input_type": "yield_",
     # "input_type": "dirty_price",
     # "input_type": "clean_price",

     "input_value": 3,  # 单位是 %
     # "input_value": 90,

     "model_type": model_type,
     "suoe": True,
     "upload": False,  # 暂且不上传模型结果
     "module_type": 'static_rev_recal',
     "is_security": True,
     "cal_derivatives": True,
}

if scenario_type == 'model':
    if model_type == 'linear_model' or model_type == 'sigmoid_model':
        test_params["bayers"] = bayers
    elif model_type == 'markov_model':

        test_params['n_markov'] = n_markov
    elif model_type == 'extrapolate_model':
        test_params['CPR'] = CPR
    pool_result_upload, security_result_upload, pool_result_cn, security_result_cn, df_warns_info, \
    df_assumptions, df_factor, df_date_match, valuation_results_upload, df_checked, derivative_results_upload, \
    df_checked_project = abs_calculator(test_params, cal_valuation=True)

elif scenario_type == 'user':
    # 用无加压的数据
    pool_result_upload, security_result_upload, pool_result_cn, security_result_cn, df_warns_info, \
    df_assumptions, df_factor, df_date_match, valuation_results_upload, df_checked, derivative_results_upload, \
    df_checked_project = abs_calculator(test_params, cal_valuation=True)

# elif scenario_type == 'user_dynamic':
#     # 用无加压的数据
#     scenario_df = pd.DataFrame({'date_': ['20210430', '20210531', '20210630', '20210731'],
#                                 'DP': [0, 0, 0 , 0],
#                                 'CPR': [0, 0, 0, 0]})
#     test_params['scenario_df'] = scenario_df
#     out_dict, df_pool, df_security, project_abbr = abs_calculator(test_params)

time2 = time.time()
print("total time:", time2 - time1, "seconds")
# from utils.db_util import UploadLib
# ul = UploadLib("db_test")
# ul.connect()
# # ul.insert('CSI_ABS_GZ_CF', cf_upload)
# # ul.transfer('CSI_ABS_GZ_CF', 'CSI_ABS_GZ_CF_HIS', delete_=True, condition="TRADE_DATE=20230707")
#
# ul.insert('CSI_BOND_ABS.CSI_ABS_GZ_CF', cf_upload)
# # ul.transfer('CSI_ABS_GZ_CF', 'CSI_ABS_GZ_CF_HIS', delete_=False, condition="TRADE_DATE=20230726")
# ul.close()