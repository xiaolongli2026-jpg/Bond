# -*- coding: utf-8 -*-

"""
单个证券评级测试
"""
import time

from main.rating_module.rating_main import main_rating

# 0. 通用参数
security_code = '2389158.IB'  # 证券代码
rating_date = '20240115'  # 若选 tracking_rating 需要设置评级日期
customize_scenario = False
scenario_sets = None
exist_exp_tax = False
exp_rate = 1
tax_rate = 3.26
rating_range = 'project'
suoe = False  # 只能缩期
recal_cashflow = False

data_source = None  # None 自动选择

rating_method = None
multiplier_source = 'rating_report'
custom_multiplier = None
custom_tdr = None

if rating_method == 'lognorm_distribution':
    # 1
    # lognorm_source = 'customize'
    # custom_mu = 0
    # custom_sigma = 1
    # # 2
    # lognorm_source = None

    # 1
    default_prob_source = 'rating_report'

    # default_prob_source = 'customize'
    # custom_default_prob = None



output_format = 'table'
# 1. 不良贷款

if customize_scenario:
    # 如果是自定义加压，则输入加压场景
    scenario_sets = {'自定义场景 1': {'bp_change': 25,
                         'dpp': 3,
                         'portion': 0.2,
                         },
                     '自定义场景 2': {'bp_change': 50,
                         'dpp': 6,
                         'portion': 0.3,
                         },
                     '自定义场景 3': {'bp_change': 25,
                         'dpp': 12,
                         'portion': 0.5,
                         }}


# 2. 非不良贷款

# 2.1 非循环购买

# if basic_params_source == 'scenario':
# 评级方法、数据来源等，如为None，则根据设置自动选择
lognorm_source = None
tdr_source = None

if customize_scenario:
    scenario_sets = {'自定义场景 1': {'rr_rate': 100.,
                                 'cpr_rate': 100.,
                                 'bp_change': 0.,
                                 'dp_rate': 0.,
                                 'dp_direction': 'stay_put',
                                 },
                     '自定义场景 2': {'rr_rate': 80,
                                 'cpr_rate': 100,
                                 'bp_change': 0.,
                                 'dp_rate': 20,
                                 'dp_direction': 'front',
                                 },
                     '自定义场景 3': {'rr_rate': 80,
                                 'cpr_rate': 12,
                                 'bp_change': 25,
                                 'dp_rate':10,
                                 'dp_direction': 'back',
                                 },
                     }


# 2.2 循环购买

if customize_scenario:
    scenario_sets = {'自定义场景 1': {'rr_rate': 100.,
                                 'cpr_rate': 100.,
                                 'bp_change': 0.,
                                 'rp_rate': 100.,
                                 'yr_rate': 100.,
                                 'pp_rate': 120,
                                 },
                     '自定义场景 2': {'rr_rate': 100.,
                                 'cpr_rate': 100.,
                                 'bp_change': 0.,
                                 'rp_rate': 100.,
                                 'yr_rate': 80,
                                 'pp_rate': 120,
                                 },
                     '自定义场景 3': {'rr_rate': 100.,
                                 'cpr_rate': 100.,
                                 'bp_change': 25,
                                 'rp_rate': 80,
                                 'yr_rate': 80,
                                 'pp_rate': 120 ,
                                 },
                     }


if not customize_scenario:
    scenario_sets = None

time1 = time.time()
df_ranks, df_critical, df_ranks_cn, df_critical_cn, warn_lst, model_params, base_params\
    = main_rating(security_code=security_code, rating_date=rating_date,
                exist_tax_exp=exist_exp_tax, exp_rate=exp_rate, tax_rate=tax_rate,
                customize_scenario=customize_scenario, scenario_sets=scenario_sets,
                rating_method=rating_method, rating_range=rating_range,
                tdr_source=tdr_source, multiplier_source=multiplier_source,
                lognorm_source=lognorm_source, data_source=data_source, )
                # default_prob_source=default_prob_source, custom_mu=custom_mu, custom_sigma=custom_sigma,
                # custom_default_prob=custom_default_prob,
                # custom_cdr=custom_cdr, custom_dp=custom_dp,
                # custom_cprs=custom_cprs, custom_cpr=custom_cpr,
                # custom_rr=custom_rr, custom_dpp=custom_dpp, custom_rrs=custom_rrs,
                # custom_yr=custom_yr, custom_yrs=custom_yrs,
                # custom_pp=custom_pp, custom_pps=custom_pps, custom_rp=custom_rp,
                # module_type=module_type, dp_match=dp_match, param_match_method=param_match_method)

time2 = time.time()
print('耗时' + str(time2-time1))
