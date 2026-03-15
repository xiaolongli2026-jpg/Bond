# -*- coding: utf-8 -*-
"""评级主函数

"""

import numpy as np
import pandas as pd
from random import choice
from collections import defaultdict

from rating.prepare.rating_data import data_load, get_multiplier, get_default_prob, auto_rating_method
from rating.prepare.config import threshold, threshold_npls, threshold_rev
from rating.pressure_multiplier import pressure_multiplier
from rating.crr import main_crr
from rating.ccdr import main_ccdr
from rating.parameters_estimation import params_estimate
from rating.lognorm_method import lognorm_distribution
from abs.doc.enumerators import SecondClass
from utils.quick_connect import connect_mysql
from utils.timeutils import to_date2
from utils.db_util import UploadLib
from conf.conf import config_source_db



def main_rating(security_code, rating_date,
                exist_tax_exp, exp_rate, tax_rate,
                customize_scenario, data_source, rating_method, scenario_sets=None, rating_range='project',
                tdr_source=None, multiplier_source='rating_report', custom_multiplier: dict=None, custom_tdr: dict=None,
                lognorm_source=None, default_prob_source='rating_report',
                custom_mu=None, custom_sigma=None, custom_default_prob=None,
                custom_cdr: (float, int)=None, custom_dp: pd.Series=None,
                custom_cprs: pd.Series=None, custom_cpr: (float, int)=None,
                custom_rr: (float, int)=None, custom_dpp: int=None, custom_rrs: pd.Series=None,
                custom_yr: (float, int)=None, custom_yrs: pd.Series=None,
                custom_pp: (float, int)=None, custom_pps: pd.Series=None, custom_rp: (float, int)=None,
                module_type='static', dp_match_perfectly=False, param_match_method='remain', same_type=False):

    """
    ABS量化评级
    1. 如果选择自定义加压，需要输入对应的自定义加压场景, 具体可输入的参数见输入参数表,
    2. 如果是对项目自定义参数并保存在了参数表，则对于这些参数读取后作为custom_xxx输入，否则会读取一些配置好的参数或者根据数据库保存到历史数据进行估算

    Args:
        security_code (str): 证券代码, `必须带市场后缀`
        rating_date (str): 评级日期，格式YYYYMMDD
        exist_tax_exp (bool): 是否考虑税费
        exp_rate (float): 费率
        tax_rate (float): 税率
        customize_scenario (bool): 是否自定义加压场景（即计算临界违约率/回收率时,其他的指标如早偿率的变化率）,

        data_source (str): 基础参数来源，rating_report-直接读取评级报告, calculation-根据历史数据计算，customize-自定义,
                                        bayesian_estimation-根据历史数据估算同时基准违约率用贝叶斯估计进行调整,
                                        None-选择配置的默认方式. 另外数据库里也可以选择用估值入参，但是输入评级程序的时候选择其中的 'customize'
        rating_method (str): 评级方式, pressure_multiplier-压力乘数法, lognorm_distribution-对数正态分布法, None-根据二级分类选择默认的评级方法
        scenario_sets (dict): custimize_scenario=True,
                       for example, {'自定义场景 1': {'bp_change': 25,'dpp': 3, 'portion': 0.2,},
                         '自定义场景 2': {'bp_change': 50, 'dpp': 6, 'portion': 0.3,}}
        rating_range (str): security-只对于输入的security_code进行评级, project-对同项目下所有证券(除次级)进行评级
        tdr_source (str): 目标违约率的计算方法, rating_report-直接取评级报告维护的,
                                           calculation-压力乘数法下根据基准违约率乘以各等级下对应的压力乘数获得，正态分布法通过计算得到
                                           customize-自定义输入, 此时需要输入custom_tdr,
                                           both-评级报告数据和计算值的均值
                                           None-根据二级分类选择默认的配置好的方法
        multiplier_source (str): 当用的是压力乘数法时，压力乘数来源, rating_report-直接根据二级分类提取评级报告的数据(在config.py), customize-自定义输入
        custom_multiplier (dict): 当用的是压力乘数法 and multiplier_source=customize时，输入自定义压力乘数，for example, {'AAA+': 1, 'AAA', 2,...}
        custom_tdr (dict): 当用的是压力乘数法 and tdr_source=customize时，输入自定义目标违约率/早偿率，eg. {'AAA+': 0.5, 'AAA', 0.4,...}
        lognorm_source (str): 当用对数正态分布法时, mu和sigma的来源, rating_report-直接从评级报告读取,
                                                               calculation-根据同类可比项目的数据估算,
                                                               customize-自定义输入,
                                                               None-根据二级分类选择默认的配置好的方法
        default_prob_source (str): 目标违约概率来源, rating_report-直接根据二级分类提取评级报告的数据（在config.py), customize-自定义
        custom_mu (float): 当用对数正态分布法且lognorm_source=customize时，需要输入的对数正态分布均值参数
        custom_sigma (float): 当用对数正态分布法且lognorm_source=customize时，需要输入的对数正态分布波动率参数
        custom_default_prob (dict): 当用对数正态分布法且default_prob_source=customize时, 需要输入自定义的目标违约概率
        custom_cdr (float): （当data_source=customize时）自定义累计违约率，以下参数具体输入哪个见输入参数.xlsx
        custom_dp (float): （当data_source=customize时，）自定义违约分布
        custom_cprs (pd.Series):（当data_source=customize时，）自定义早偿率序列
        custom_cpr (float): （当data_source=customize时，）自定义早偿率
        custom_rr (float): （当data_source=customize时，）自定义回收率
        custom_dpp (int): （当data_source=customize时，）自定义回收延迟月数
        custom_rrs (pd.Series):当data_source=customize时，）自定义回收率序列
        custom_yr (float): （当data_source=customize时，）自定义资产池收益率
        custom_yrs (pd.Series):（当data_source=customize时，）自定义资产池收益率序列
        custom_pp (float): （当data_source=customize时，）自定义月摊还比例
        custom_pps (pd.Series):（当data_source=customize时，）自定义月摊还比例序列
        custom_rp (float): （当data_source=customize时，）自定义循环购买率
        module_type (float):  输入的自定义参数是序列型（dynamic)还是数值型(static)
        dp_match_perfectly (bool): 输入的custom_dp是否刚好与现金流归集表各期一一匹配，如果不是的话需要匹配到每一期
        param_match_method (str): 参数匹配方式，如果 custom_dp 不是刚好与现金流归集表匹配，是从初始起算日开始匹配（custom_dp总和应为1）还是从最近一次历史归集日（总和小于等于1）开始匹配
        same_type (bool): 选择可比项目时用同二级分类(True) 还是同发起人（False)

    Returns:
        tuple: tuple contains:
            df_ranks (pd.DataFrame): 评级、最大临界违约率 / 最小临界回收率 ， 对应等级的目标违约率/回收率
            df_crrs / df_cdrs (pd.DataFrame): 临界回收率 / 临界违约率
            trrs / tdrs (pd.DataFrame): 目标回收率 / 目标违约率
            base_params (dict): 基准参数 ( ``params_estimate`` 估计的结果）
            model_params (dict): 评级模型用到的参数，评级方法、数据来源、mu、sigma等。


    **逻辑**

    1. ``data_load`` 读取数据
    2. 确定使用的模型、数据来源。如果在入参有规定，则用输入的模型类型，如果没有，则用 ``auto_rating_method `` 选择默认的模型类型、数据来源。目前同二级分类下都一样。
    3. ``params_estimate`` 估计基准回收率、基准早偿率等参数。如果是自定义基准参数的类型，会用自定义的参数，注意自定义基准参数的单位都是%（除了dpp）
    4. 如果是不良贷款，用 ``main_crr`` 计算临界回收率，并用压力乘数法评级。
    5. 如果是非不良贷款，``main_ccdr`` 计算临界违约率，如果是压力乘数法评级，则使用 ``pressure_multiplier`` 进行评级；如果是对数正态分布法，则用 ``lognorm_distribution`` 评级。


    Notes:
        * 评级方式 （ `rating_method` ） 虽然是可选项，但是如果选择了评级方式，没有输入对应的相关自定义参数，且评级报告也没有该项目的数据，则无法计算。比如选择对数正常分布，但是缺少 mu或者sigma
        * 如果某证券完成了还款，则在结果中不会返回其评级

    TODO
        完成配置参数建表后，修改配置文件中的 `config_source_db` 为估值库的名称

    """

    conn, is_mysql = connect_mysql()
    cur = conn.cursor()
    db_config = UploadLib(config_source_db)
    db_config.connect()
    suoe = False # 只能是缩期法
    model_params = {}  # 保存模型中用到的参数，因为很多是根据一些历史数据推算的，保存起来便于检查
    df_model_params = None
    rating_info = defaultdict(list)
    # 1. 数据读取
    # 1.1
    df_product, df_tranches, df_prediction, df_sequence, df_plan, df_other, df_trigger, events, contingent_param_dict,\
    df_schedule, rating_report_info, project_seq, security_seq, is_revolving, secondary_classification = \
        data_load(security_code, rating_date, cur)
    recal_cashflow = True if (is_revolving or secondary_classification == '6') else True

    try:

        data_source_auto, rating_method_auto, lognorm_source_auto, tdr_source_auto = \
            auto_rating_method(secondary_classification, rating_date, db_config)  # 各个二级分类所需要采取的评级方法和参数计算方式
        # 1.2 如果没有输入评级方法，就按照二级分类选择评级方法
        rating_method = rating_method_auto if rating_method is None else rating_method
        data_source = data_source_auto if data_source is None else data_source
        lognorm_source = lognorm_source_auto if lognorm_source is None else lognorm_source
        tdr_source = tdr_source_auto if tdr_source is None else tdr_source

        # 1.3 检查输入是否正确
        check_input(secondary_classification, is_revolving,
                    exist_tax_exp,
                    suoe, recal_cashflow,
                    customize_scenario, scenario_sets,
                    rating_method, rating_range,
                    tdr_source, multiplier_source, custom_multiplier, custom_tdr,
                    lognorm_source, default_prob_source, custom_mu, custom_sigma, custom_default_prob,
                    data_source, custom_cdr, custom_dp,
                    custom_cprs, custom_cpr, custom_rr, custom_dpp, custom_rrs,
                    custom_yr, custom_yrs, custom_pp, custom_pps, custom_rp, module_type)

        if exist_tax_exp:
            tax_rate /= 100
            exp_rate /= 100
        else:
            tax_rate, exp_rate = 0, 0
    except (ValueError) as e:
        raise ValueError(e)

    initial_date = df_product.loc[0, 'initial_date']
    initial_principal = df_product.loc[0, 'initial_principal']
    current_cdr = df_other.loc[0, 'CDR']

    # 2 基础参数
    base_params = {}
    # 2.1 基础参数估计,包括早偿率、回收率等
    collected_amount = df_other.loc[0, 'collect_amount']
    pool_dates = df_schedule['pool_date'].dropna()
    period_num = len(pool_dates[pool_dates > to_date2(df_other.loc[0, 'history_date'])])
    param_e = params_estimate(project_seq, rating_date, period_num, initial_date, df_prediction, initial_principal,
                              collected_amount, secondary_classification, is_revolving, recal_cashflow,
                              to_date2(df_other.loc[0, 'history_date']), rating_report_info, current_cdr, same_type, data_source, cur)

    if param_e.data_source == 'customize':
        # 2.2 自定义输入基础参数，则以输入的为准，不取用维护的数据；如果是保存人工拍定的参数，也需要自定义输入参数
        param_e.set_custom_params(custom_cdr, custom_dp, custom_cprs, custom_cpr, custom_rr, custom_dpp, custom_rrs,
                                  custom_yr, custom_yrs, custom_pp, custom_pps, custom_rp, module_type,
                                  dp_match_perfectly, param_match_method)

    # 3 评级
    levels = dict(zip(df_tranches['security_code'], df_tranches['security_level']))
    end_prins = dict(zip(df_tranches['security_code'], df_tranches['period_end_balance']))
    security_seqs = dict(zip(df_tranches['security_code'], df_tranches['security_seq']))
    if secondary_classification == SecondClass['NPLS']: # 3.1 不良贷款类
        realized_rr = param_e.realized_rr_npl
        unrealized_rr = param_e.unrealized_rr_npl
        base_params['rr'] = realized_rr + unrealized_rr
        base_params['current_rr'] = realized_rr
        base_params['future_rr'] = unrealized_rr
        # 3.1.1 计算各个加压场景下的临界违约率
        crrs, scenario_list, result_info, must_default_secs = main_crr(security_code, project_seq, rating_date, exist_tax_exp,
                                                    exp_rate, tax_rate, suoe, df_product, df_tranches, df_prediction,
                                                    df_sequence, df_plan, df_other, df_trigger, events, df_schedule,
                                                    contingent_param_dict,
                                                    rating_range=rating_range, max_time=100,
                                                    customize_scenario=customize_scenario,
                                                    scenario_sets=scenario_sets)
        df_crrs = pd.DataFrame(crrs)

        # 3.1.2 计算目标违约率，不良贷款只有压力乘数法
        pm = pressure_multiplier(project_seq, secondary_classification,
                                 rating_report_info, tdr_source)
        if custom_multiplier is None:
            dict_ = get_multiplier(df_product.loc[0, 'secondary_classification'], rating_date, db_config)  # 获取压力乘数
        else:
            dict_ = custom_multiplier
        pm.multiplier_dict(dict_)  # 设置压力乘数
        pm.cal_trrs(realized_rr, unrealized_rr)

        ranks = pm.npl_rating(df_crrs)
        trrs = pm.return_trrs()
        rating_info = pm.rating_info

        # 4 结合一些指标，对结果为NR的非次级进行原因分析
        rating_info = \
            result_analysis(basic_params=base_params, is_npls=secondary_classification == SecondClass['NPLS'],
                            is_rev=is_revolving, save_dict=rating_info)

        # 4.2 整理输出数据

        df_critical, df_critical_cn = critical_result_transfer(crrs, scenario_list, result_info, must_default_secs,
                                                               security_seqs, end_prins, project_seq, rating_date)
        df_ranks, df_ranks_cn = result_transfer_npls(crrs, trrs, ranks, security_seqs, levels, end_prins, project_seq,
                                                     rating_date, rating_info)
        cur.close()
        conn.close()
        db_config.close()
        warn_lst = param_e.warn_lst
        df_base_param = pd.DataFrame(base_params, index=[0])
        df_base_param.loc[0, 'project_seq'] = project_seq
        return df_ranks, df_critical, df_ranks_cn, df_critical_cn, warn_lst, df_model_params, df_base_param

    else:  # 3.2 非不良贷款

        cprs = param_e.cprs
        rrs = param_e.rrs
        realized_cdr = param_e.realized_cdr
        unrealized_cdr = param_e.unrealized_cdr

        base_params['cdr'] = realized_cdr + unrealized_cdr
        base_params['current_cdr'] = realized_cdr
        base_params['future_cdr'] = unrealized_cdr
        df_base_params = pd.DataFrame(base_params, index=[0])
        df_base_params.loc[0, 'project_seq'] = project_seq
        base_params['rrs'] = [rrs]
        df_base_params.loc[0, 'rrs'] = ";".join([str(x) + "," + str(y) for x, y in zip(rrs.index, rrs.values)])
        base_params['cprs'] = [cprs]
        df_base_params.loc[0, 'cprs'] = ";".join([str(x) + "," + str(y) for x, y in zip(cprs.index, cprs.values)])
        # 3.2.1 计算临界违约率
        if is_revolving:  # 3.2.1.1 循环购买类
            yrs = param_e.yrs  # 为非循环购买时，这两个数据无效
            pps = param_e.pps
            rp = param_e.rp
            base_params['yrs'] = [yrs]
            df_base_params.loc[0, 'yrs'] = ";".join([str(x) + "," + str(y) for x, y in zip(yrs.index, yrs.values)])
            base_params['pps'] = [pps]
            df_base_params.loc[0, 'pps'] = ";".join([str(x) + "," + str(y) for x, y in zip(pps.index, pps.values)])
            base_params['rp'] = rp
            c_cdr, scenario_desc, result_info, must_default_secs = main_ccdr(project_seq=project_seq, security_code=security_code,
                                                          trade_date=rating_date, exist_tax_exp=exist_tax_exp,
                                                          exp_rate=exp_rate, tax_rate=tax_rate, suoe=suoe,
                                                          df_product=df_product, df_tranches=df_tranches,
                                                          df_prediction=df_prediction, df_trigger=df_trigger,
                                                          df_sequence=df_sequence, df_plan=df_plan, df_other=df_other,
                                                          df_schedule=df_schedule, events=events,
                                                          contingent_param_dict=contingent_param_dict,
                                                          base_rrs=rrs, base_cprs=cprs, base_yrs=yrs, base_pps=pps,
                                                          base_rp=rp, customize_scenario=customize_scenario,
                                                          scenario_sets=scenario_sets, rating_range=rating_range,
                                                          max_time=100)
        else:  # 3.2.1.2 非循环购买类
            dp = param_e.dp  # 只包含未来的, 与df_prediction一一对应
            base_params['dp'] = [dp]
            df_base_params.loc[0, 'dp'] = ";".join([str(x) + "," + str(y) for x, y in zip(dp.index, dp.values)])

            c_cdr, scenario_desc, result_info, must_default_secs = main_ccdr(project_seq=project_seq, security_code=security_code,
                                                          trade_date=rating_date, exist_tax_exp=exist_tax_exp,
                                                          exp_rate=exp_rate, tax_rate=tax_rate, suoe=suoe,
                                                          df_product=df_product, df_tranches=df_tranches,
                                                          df_prediction=df_prediction, df_trigger=df_trigger,
                                                          df_sequence=df_sequence, df_plan=df_plan, df_other=df_other,
                                                          df_schedule=df_schedule, events=events,
                                                          contingent_param_dict=contingent_param_dict,
                                                          base_rrs=rrs, base_cprs=cprs, base_dp=dp,
                                                          customize_scenario=customize_scenario,
                                                          scenario_sets=scenario_sets, rating_range=rating_range)
        df_ccdr = pd.DataFrame(c_cdr)
        # 3.2.2 计算目标违约率
        if rating_method == 'pressure_multiplier':  # 3.2.2.1 压力乘数法
            pm = pressure_multiplier(project_seq, secondary_classification,
                                     rating_report_info, tdr_source
                                     )
            if custom_multiplier is None:
                dict_ = get_multiplier(df_product.loc[0, 'secondary_classification'], rating_date, db_config)
            else:
                dict_ = custom_multiplier
            pm.multiplier_dict(dict_)
            pm.cal_tdrs(realized_cdr, unrealized_cdr)
            ranks = pm.rating(df_ccdr)
            tdrs = pm.return_tdrs()
            rating_info.update(pm.rating_info)

        elif rating_method == 'lognorm_distribution':  # 3.2.2.2 对数正态分布法

            comparable_project_cdrs = param_e.staticpools_cdr
            ld = lognorm_distribution(project_seq, secondary_classification, rating_report_info, realized_cdr,
                                      lognorm_source, comparable_project_cdrs, scenario_mu=custom_mu,
                                      scenario_sigma=custom_sigma)
            if custom_multiplier is None:
                dict_ = get_default_prob(df_product.loc[0, 'secondary_classification'], rating_date, db_config)
            else:
                dict_ = custom_default_prob

            ld.default_prob_dict(dict_)
            ranks = ld.rating(df_ccdr)
            tdrs = ld.return_tdrs()
            model_params['MU'] = ld.mu
            model_params['SIGMA'] = ld.sigma
            model_params['PROJECT_SEQ'] = project_seq
            df_model_params = pd.DataFrame(model_params, index=[0])
            rating_info.update(ld.rating_info)
        else:
            raise ValueError(f'wrong rating_method input: {rating_method}')

        df_ccdr['scenario'] = scenario_desc
        df_ccdr['success'] = result_info

        # 4 结合一些指标，对结果为NR的非次级进行原因分析
        rating_info = \
            result_analysis(basic_params=base_params, is_npls=secondary_classification == SecondClass['NPLS'],
                            is_rev=is_revolving, save_dict=rating_info)

        # 4.2 整理输出结果
        df_critical, df_critical_cn = critical_result_transfer(c_cdr, scenario_desc, result_info, must_default_secs,
                                                               security_seqs, end_prins, project_seq, rating_date)
        df_ranks, df_ranks_cn = result_transfer(c_cdr, tdrs, ranks, security_seqs, levels, end_prins,
                                                     project_seq, rating_date, rating_info)


        cur.close()
        conn.close()
        db_config.close()
        warn_lst = param_e.warn_lst
        return df_ranks, df_critical, df_ranks_cn, df_critical_cn, warn_lst, df_model_params, df_base_params


def check_input(secondary_classification, is_revolving,
                exist_tax_exp,
                suoe, recal_cashflow, customize_scenario, scenario_sets, rating_method, rating_range,
                tdr_source, multiplier_source, custom_multiplier, custom_tdr,
                lognorm_source, default_prob_source, custom_mu, custom_sigma, custom_default_prob,
                data_source, custom_cdr, custom_dp,custom_cprs, custom_cpr, custom_rr, custom_dpp, custom_rrs,
                custom_yr, custom_yrs, custom_pp, custom_pps, custom_rp, module_type):
    """检查输入参数是否符合要求，按不同的输入参数模式进行检查"""

    # 1 检查通用参数枚举值是否准确
    assert suoe in (True, False), f"错误的缩额法输入{suoe}"
    assert exist_tax_exp in (True, False), f"exist_tax_exp输入错误：{exist_tax_exp}"

    assert rating_range in ('security', 'project'), f"评级范围输入错误：{rating_range}"
    assert rating_method in ('pressure_multiplier', 'lognorm_distribution'), \
        f"评级方法输入错误：{rating_method}"

    # 2 按评级方式确认相关入参是否正确
    if secondary_classification == SecondClass['NPLS']:
        assert data_source in ('calculation', 'rating_report', 'customize'), \
            f"错误的基准参数计算方式：{data_source}"
    else:
        assert data_source in ('bayesian_estimation', 'calculation', 'rating_report', 'customize'), \
            f"错误的基准参数计算方式：{data_source}"

    if rating_method == 'pressure_multiplier':

        assert tdr_source in ('calculation', 'rating_report', 'both', 'customize'), f"错误的目标回收率计算方式：{tdr_source}"
        assert multiplier_source in ('rating_report', 'customize'), f"错误的压力乘数计算方式：{multiplier_source}"

        if (tdr_source == 'customize') and ((custom_tdr is None) or (not isinstance(custom_tdr, dict))):
            raise ValueError(f"自定义目标违约率/回收率模式下，自定义值不合规：{custom_tdr}")

        elif (tdr_source == 'calculation') and (multiplier_source == 'customize') and \
            ((custom_multiplier is None) or (not isinstance(custom_multiplier, dict))):
            raise ValueError(f"自定义压力乘数模式下，自定义值不合规：{custom_multiplier}")

    elif rating_method == 'lognorm_distribution':

        if secondary_classification == SecondClass['NPLS']:
            raise ValueError('不良贷款不支持对数正态分布法')

        assert lognorm_source in ('rating_report', 'calculation', 'customize'), \
            f"对数正态分布评级中未正确输入参数计算方式：{lognorm_source}"
        assert default_prob_source in ('rating_report', 'customize'), \
            f"对数正态分布评级中未正确输入目标违约概率来源：{lognorm_source}"

        if (lognorm_source == 'customize') and ((custom_mu is None) or (custom_sigma is None)):
            raise ValueError(f"自定义正态分布参数模式下，未输入自定义值：{custom_mu, custom_sigma}")

        if (default_prob_source == 'customize') and \
            ((custom_default_prob is None) or (not isinstance(custom_default_prob, dict))):
            raise ValueError(f"自定义目标违约概率模式下，自定义值不合规：{custom_default_prob}")

    # 3 检验自定义加压场景设置
    if (customize_scenario):

        if scenario_sets is None:
            raise ValueError("自定义加压模式下，未输入自定义加压场景")

        # 随机检查一个key是否正确
        victim = choice(range(0, len(scenario_sets)))
        scene = scenario_sets[list(scenario_sets.keys())[victim]]
        keys = list(scene.keys())

        if secondary_classification == SecondClass['NPLS']:
            limits = ['bp_change', 'dp_rate', 'dp_direction']
        else:
            if not is_revolving:
                limits = ['rr_rate', 'cpr_rate', 'bp_change', 'dp_rate', 'dp_direction']
            else:
                limits = ['rr_rate', 'cpr_rate', 'bp_change', 'rp_rate', 'yr_rate', 'pp_rate']

        diff = set(keys).difference(set(limits))
        if len(diff) > 0:
            raise ValueError(f'自定义加压情景中，存在不适合该类ABS的加压参数{diff}')

        # 检查值是否符合要求
        if 'dp_direction' in keys:
            if scene['dp_direction'] not in ('front', 'back') :
                raise ValueError(f"前置/后置枚举值错误{scene['dp_direction']}")
            else:
                if scene.get('dp_rate', 0) > 100:
                    raise ValueError(f'前置后置比例不能超过100%')
                del scene['dp_direction']

        if not (np.array(list(scene.values())) > 0).all():
            raise ValueError(f"加压参数必须大于0")

    # 4 检查自定义输入基准违约率时的自定义值
    if data_source == 'customize':
        if secondary_classification == SecondClass['NPLS']:
            assert isinstance(custom_rr, (float, int)), "不良贷款下，自定义基准参数时，需要正确输入数值型回收率"
        else:
            if ( not is_revolving) or (is_revolving and recal_cashflow):
                if module_type == 'dynamic':

                    is_wrong = ~(isinstance(custom_cdr, (float, int)) and isinstance(custom_dp, pd.Series)
                                 and isinstance(custom_rrs, pd.Series)
                                 and isinstance(custom_cprs, pd.Series))

                else:

                    is_wrong = ~(isinstance(custom_cdr, (float, int)) and isinstance(custom_cpr, (float, int))
                                 and isinstance(custom_rr, (float, int))
                                 and isinstance(custom_dpp, int))

            else:
                if module_type == 'dynamic':
                    is_wrong = ~(isinstance(custom_cdr, (float, int))
                                 and isinstance(custom_pps, pd.Series)
                                 and isinstance(custom_cprs, pd.Series)
                                 and isinstance(custom_rrs, pd.Series)
                                 and isinstance(custom_rp, (float, int))
                                 and isinstance(custom_yrs, pd.Series))
                else:
                    is_wrong = ~(isinstance(custom_cdr, (float, int))
                                 and isinstance(custom_pp, (float, int))
                                 and isinstance(custom_cpr, (float, int))
                                 and isinstance(custom_rr, (float, int))
                                 and isinstance(custom_dpp, int)
                                 and isinstance(custom_rp, (float, int))
                                 and isinstance(custom_yr, (float, int)))

            if is_wrong:
                raise ValueError("自定义基准参数时，入参不符合标准")


def critical_result_transfer(crrs, scenario_list, result_info, must_default_secs, security_seqs,
                         end_prins, project_seq, rating_date):

    """标准化临界值计算的输出结果"""
    # 临界值
    df_ = pd.DataFrame(crrs)
    df_critical = pd.DataFrame()
    df_critical['CRITICAL_VALUE'] = np.array(df_.values).flatten('F')
    df_critical['SECURITY_CODE'] = np.array([[code_] * df_.shape[0] for code_ in df_.columns]).reshape(df_.shape[0] * df_.shape[1], )
    df_critical['SCENARIO_DETAIL'] = np.array([scenario_list * df_.shape[1]]).reshape(df_.shape[0] * df_.shape[1], )
    df_critical['RESULT_INFO'] = np.array([result_info * df_.shape[1]]).reshape(df_.shape[0] * df_.shape[1], )
    df_critical['DATE'] = rating_date

    for x in must_default_secs:
        if end_prins[x] < 0.1:
            df_critical.drop(index=df_critical[df_critical['SECURITY_CODE'] == x].index, inplace=True)  # 已完成还款的不评级
        else:
            df_critical.loc[df_critical['SECURITY_CODE'] == x, 'RESULT_INFO'] = f"证券：{x}， " \
                                                                                f"在0%违约率/100%回收率条件下仍违约吗，请通过一般现金流分配过程判断问题来源"

    df_critical['SECURITY_SEQ'] = df_critical['SECURITY_CODE'].apply(lambda x: security_seqs[x])
    df_critical['PROJECT_SEQ'] = project_seq

    df_critical_cn = df_critical.copy()
    df_critical_cn.drop(columns=['PROJECT_SEQ', 'SECURITY_SEQ'], inplace=True)
    df_critical_cn.rename(columns={'SECURITY_CODE': '证券代码', 'CRITICAL_VALUE': '临界回收率/违约率',
                                   'SCENARIO_DETAIL': '加压场景描述', 'RESULT_INFO': '临界违约率/回收率迭代信息',
                                   'DATE': '日期'}, inplace=True)

    df_critical.drop(columns=['SECURITY_CODE'], inplace=True)
    return df_critical, df_critical_cn


def result_transfer_npls(crrs, trrs, ranks, security_seqs, levels, end_prins, project_seq, rating_date, result_info):
    """标准化评级的输出结果（不良贷款）"""
    df_crrs = pd.DataFrame(crrs)
    df_ranks = pd.DataFrame()
    df_ranks['SECURITY_CODE'] = ranks.keys()
    df_ranks['RANK'] = ranks.values()
    df_ranks['DATE'] = rating_date

    drop_row = []
    for i in range(0, len(df_ranks)):

        code_ = df_ranks.loc[i, 'SECURITY_CODE']
        if end_prins[code_] < 0.1:
            drop_row.append(i)
            continue

        df_ranks.loc[i, 'EXTREMUM_CRITICAL_VALUE'] = df_crrs[code_].max()
        df_ranks.loc[i, 'RESULT_INFO'] = "".join(result_info[code_])  # 结果说明信息

        rank = ranks[code_]
        if rank != 'NR':
            trr = trrs[rank]
            df_ranks.loc[i, 'TARGET_VALUE'] = trr
            df_ranks.loc[i, 'DISTANCE'] = trr - df_ranks.loc[i, 'EXTREMUM_CRITICAL_VALUE']
            df_ranks.loc[i, 'TARGET_VALUE_SERIES'] = ";".join([str(x) + ',' + str(trrs[x]) for x in trrs])


    df_ranks['SECURITY_SEQ'] = df_ranks['SECURITY_CODE'].apply(lambda x: security_seqs[x])
    df_ranks['PROJECT_SEQ'] = project_seq
    df_ranks.drop(index=drop_row, inplace=True)

    df_ranks_cn = df_ranks.copy()
    df_ranks_cn.drop(columns=['PROJECT_SEQ', 'SECURITY_SEQ'], inplace=True)
    df_ranks_cn.rename(columns={'SECURITY_CODE': '证券代码', 'RANK': '量化评级', 'TARGET_VALUE': '目标违约率/回收率',
                                'EXTREMUM_CRITICAL_VALUE': '临界违约率最小值/临界回收率最大值',
                                'DISTANCE': '信用保护距离', 'TARGET_VALUE_SERIES': '目标值序列',
                                'DATE': '日期', 'RESULT_INFO': '结果说明'}, inplace=True)
    df_ranks_cn['证券优先级'] = [levels[code_] for code_ in df_ranks_cn['证券代码']]

    df_ranks.drop(columns=['SECURITY_CODE'], inplace=True)
    return df_ranks, df_ranks_cn


def result_transfer(ccdrs,tdrs, ranks, security_seqs, levels,
                         end_prins, project_seq, rating_date, result_info):
    """标准化评级的输出结果"""
    df_ccdrs = pd.DataFrame(ccdrs)
    df_ranks = pd.DataFrame()
    df_ranks['SECURITY_CODE'] = ranks.keys()
    df_ranks['RANK'] = ranks.values()
    df_ranks['DATE'] = rating_date

    drop_row = []
    for i in range(0, len(df_ranks)):

        code_ = df_ranks.loc[i, 'SECURITY_CODE']
        if end_prins[code_] < 0.1:
            drop_row.append(i)
            continue

        df_ranks.loc[i, 'EXTREMUM_CRITICAL_VALUE'] = df_ccdrs[code_].min()
        df_ranks.loc[i, 'RESULT_INFO'] = "".join(result_info[code_])

        rank = ranks[code_]
        if rank != 'NR':
            tdr = tdrs[rank]
            df_ranks.loc[i, 'TARGET_VALUE'] = tdr
            df_ranks.loc[i, 'DISTANCE'] = df_ranks.loc[i, 'EXTREMUM_CRITICAL_VALUE'] - tdr
            df_ranks.loc[i, 'TARGET_VALUE_SERIES'] = ";".join([str(x) + ',' + str(tdrs[x]) for x in tdrs])

    df_ranks['SECURITY_SEQ'] = df_ranks['SECURITY_CODE'].apply(lambda x: security_seqs[x])
    df_ranks['PROJECT_SEQ'] = project_seq
    df_ranks.drop(index=drop_row, inplace=True)

    df_ranks_cn = df_ranks.copy()
    df_ranks_cn.drop(columns=['PROJECT_SEQ', 'SECURITY_SEQ'], inplace=True)
    df_ranks_cn.rename(columns={'SECURITY_CODE': '证券代码', 'RANK': '量化评级', 'TARGET_VALUE': '目标违约率/回收率',
                                'EXTREMUM_CRITICAL_VALUE': '临界违约率最小值/临界回收率最大值',
                                'DISTANCE': '信用保护距离', 'TARGET_VALUE_SERIES': '目标值序列', 'DATE': '日期',
                                'RESULT_INFO': '结果说明'}, inplace=True)
    df_ranks_cn['证券优先级'] = [levels[code_] for code_ in df_ranks_cn['证券代码']]

    df_ranks.drop(columns=['SECURITY_CODE'], inplace=True)
    return df_ranks, df_ranks_cn


def result_analysis(basic_params: dict, is_npls: bool, is_rev: bool, save_dict: dict):
    """注意对于序列型的参数用均值判断是否超过阈值

    Args:
        basic_params: 基准参数
        is_npls:
        is_rev:
        save_dict: 用于按证券保存结果预警信息的dict

    Returns:

    """
    project_warning = []
    if is_npls:
        if basic_params['future_rr'] < threshold_npls['rr'][0] or \
            basic_params['future_rr'] > threshold_npls['rr'][1]:
            project_warning.append("不良贷款基准参数未来回收率 %f 超阈值 %s " % (basic_params['future_rr'], threshold_npls))

    else:
        mean_cprs = basic_params['cprs'][0].mean()

        if is_rev:
            mean_yrs = basic_params['yrs'][0].mean()
            mean_pps = basic_params['pps'][0].mean()
            rp = basic_params['rp']
            if mean_yrs < threshold_rev['yr'][0] or mean_yrs > threshold_rev['yr'][1]:
                project_warning.append('基准收益率均值 %f 超阈值 %s '%(mean_yrs, threshold_rev['yr']))

            if rp < threshold_rev['rp'][0] or rp > threshold_rev['rp'][1]:
                project_warning.append('基准循环购买率 %f 超阈值 %s' % (rp, threshold_rev['rp']))

            if mean_pps < threshold_rev['pp'][0] or mean_pps > threshold_rev['pp'][1]:
                project_warning.append('基准月还款率均值 %f 超阈值 %s' % (mean_pps, threshold_rev['pp']))

            if mean_cprs < threshold_rev['cpr'][0] or mean_cprs > threshold_rev['cpr'][1]:
                project_warning.append('循环购买基准早偿率均值 %f 超阈值 %s' % (mean_cprs, threshold_rev['cpr']))

            mean_cdr = basic_params['cdr']
            if mean_cdr < threshold_rev['cdr'][0] or mean_cdr > threshold_rev['cdr'][1]:
                project_warning.append('循环购买累计违约率 %f 超阈值 %s' % (mean_cdr, threshold_rev['cdr']))

        else:
            if mean_cprs < threshold['cpr'][0] or mean_cprs > threshold['cpr'][1]:
                project_warning.append('基准早偿率均值 %f 超阈值 %s' % (mean_cprs, threshold_rev['cpr']))

            mean_cdr = basic_params['future_cdr']
            if mean_cdr < threshold['cdr'][0] or mean_cdr > threshold['cdr'][1]:
                project_warning.append('基准违约率(未实现部分) %f 超阈值 %s' % (mean_cdr, threshold_rev['cdr']))

    str_ = ";".join(project_warning)
    for k_ in save_dict.keys():
        save_dict[k_].append(str_)  # 证券层面的预警信息，每个都额外加入项目层的信息

    return save_dict