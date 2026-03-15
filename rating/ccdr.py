# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

from main.abs_calculator.module_abs_new import main_calculation
from conf.conf import default_params
from utils.db_util import UploadLib
from conf.conf import config_source_db
from rating.prepare.rating_data import get_scenarios


def main_ccdr(project_seq, security_code, trade_date, exist_tax_exp, exp_rate, tax_rate, suoe,
              df_product, df_tranches, df_prediction, df_sequence, df_plan,
              df_other, df_trigger, events, df_schedule, contingent_param_dict,
              base_cprs, base_rrs, base_dp=None, base_pps=None, base_yrs=None, base_rp=1.,
              customize_scenario=False, scenario_sets=None, rating_range='security',
              max_time=100):
    """
    计算不同加压场景下非不良贷款ABS的临界违约率和估计违约率，包括循环购买和非循环购买

    Args:
        base_cprs (pd.Series): 基准早偿率
        base_rrs (pd.Series): 基准违约回收率
        base_dp (pd.Series): 基准违约分布序列（如果是非循环购买）
        base_pps (pd.Series): 基准摊还比例序列（如果是循环购买）
        base_yrs (pd.Series): 基准收益率序列（如果是循环购买）
        base_rp (float) : 循环购买率（如果是循环购买）
        rating_range (str): security-security_code对应的单个证券的临界回收率，project-项目下所有证券同时计算临界回收率
        max_time (int): 最大跌打次数，超过则跳出
        customize_scenario: bool, 是否自定义加压
        scenario_sets: dict, 自定义加压场景下输入


    Returns:
        tuple: tuple contains:
            c_cdr (list): 记录各场景下的临界违约率，元素个数为场景数量。
            scenario_desc (list): 对各个加压场景的描述，元素个数为场景数量。
            result_info (list) : 记录各情景是否正常计算出临界违约率，元素个数为场景数量。

    ------
    逻辑
    ------
    1. 首先计算在0违约、其他参数均等于输入的基准值时，是否有证券违约，如果是，则记录为需要跳过的证券，在后续计算临界违约率时不会计算。
    2. 如果不是自定义场景，则用 ``get_scenarios`` 根据二级分类，获取加压场景
    3. 如果是循环购买，分别将各加压场景带入 ``scenario_analysis_rev`` 得到各个场景的临界违约率，如果是非循环，则代入  ``scenario_analysis_nonrev``

    Notes:
        注意最终得到的临界违约率，只针对未来存续期，未来违约总额占入池金额的比例，而不是项目存续周期总违约占入池金额的比例。

    """

    is_revolving_pool = df_product.loc[0, 'is_revolving_pool']
    secondary_classification = df_product.loc[0, 'secondary_classification']

    # 1. 先算下在0违约的情况下是否无法正常偿还，如果是的话对应的证券直接设置0%为临界违约率
    remain_balances, have_defaulted, actual_default_rate = \
        judge_default(cdr=0, project_seq=project_seq, security_code=security_code, trade_date=trade_date,
                      initial_principal=df_product.loc[0, 'initial_principal'],
                      exist_tax_exp=exist_tax_exp, exp_rate=exp_rate, tax_rate=tax_rate, suoe=suoe,
                      df_product=df_product, df_tranches=df_tranches, df_prediction=df_prediction,
                      df_sequence=df_sequence, df_plan=df_plan, df_other=df_other, df_trigger=df_trigger,
                      events=events, df_schedule=df_schedule, contingent_param_dict=contingent_param_dict,
                      rrs=base_rrs, cprs=pd.Series([0], index=[1]), bp_change=0, rp=base_rp, pps=base_pps,
                      dp=base_dp, yrs=base_yrs, module_type='series_normal1' if not is_revolving_pool else 'series_rev_recal',
                      is_rev=is_revolving_pool)

    must_default = [x for x in df_tranches['security_code'] if have_defaulted.get(x, False)]
    # 2. 压力场景
    if customize_scenario:
        labels = scenario_sets.keys
        sets_ = scenario_sets.copy()
        scenario_desc = {}
        for i in sets_:
            sets_[i] = [sets_[i]]
            scenario_desc[i] = f"早偿率变动{scenario_sets[i].get('cpr_rate', 0) - 100}%"\
                               f"利差缩减{scenario_sets[i].get('bp_change', 0)}BP" \
                               f"回收率变动{scenario_sets[i].get('rr_rate', 0) - 100}%" \
                               f"早偿率变动{scenario_sets[i].get('cpr_rate', 0) - 100}%"

            orient = scenario_sets[i].get('direction', 'stay_put')
            if orient == 'front':
                scenario_desc[i] = scenario_desc[i] + f"违约分布前置{scenario_sets[i].get('dp_rate', 0)}%"
            elif orient == 'back':
                scenario_desc[i] = scenario_desc[i] + f"违约分布后置{scenario_sets[i].get('dp_rate', 0)}%"

            if is_revolving_pool:
                scenario_desc[i] = scenario_desc[i] + f"收益率变动{scenario_sets[i].get('yr_rate', 0)-100}%" + \
                                   f"月还款率变动{scenario_sets[i].get('pp_rate', 0)- 100}%"
    else:

        db_config = UploadLib(config_source_db)
        db_config.connect()
        labels, sets_, scenario_desc = get_scenarios(secondary_classification, trade_date, is_revolving_pool, db_config)
        db_config.close()

    c_cdr = []
    result_info = []
    # 3. 计算临界违约率
    for k in labels:
        try:
            if is_revolving_pool:
                min_critical_cdr = scenario_analysis_rev(project_seq=project_seq, security_code=security_code,
                                                        trade_date=trade_date, exist_tax_exp=exist_tax_exp,
                                                        exp_rate=exp_rate, tax_rate=tax_rate, suoe=suoe,
                                                        df_product=df_product, df_tranches=df_tranches,
                                                        df_prediction=df_prediction, df_sequence=df_sequence,
                                                        df_plan=df_plan, df_other=df_other, df_trigger=df_trigger,
                                                        events=events, df_schedule=df_schedule,
                                                        contingent_param_dict=contingent_param_dict,
                                                        scenario_sets=sets_[k], base_rrs=base_rrs,
                                                        base_cprs=base_cprs, base_rp=base_rp, base_pps=base_pps,
                                                        base_yrs=base_yrs,
                                                        cal_base = 50, cal_time=max_time,
                                                        rating_range=rating_range, skip=have_defaulted)
            else:

                min_critical_cdr = scenario_analysis_nonrev(project_seq=project_seq, security_code=security_code,
                                                            trade_date=trade_date, exist_tax_exp=exist_tax_exp,
                                                            exp_rate=exp_rate, tax_rate=tax_rate, suoe=suoe,
                                                            df_product=df_product, df_tranches=df_tranches,
                                                            df_prediction=df_prediction, df_sequence=df_sequence,
                                                            df_plan=df_plan, df_other=df_other, df_trigger=df_trigger,
                                                            events=events, df_schedule=df_schedule,
                                                            contingent_param_dict=contingent_param_dict,
                                                            scenario_sets=sets_[k], base_rrs=base_rrs,
                                                            base_cprs=base_cprs, base_dp=base_dp,
                                                            cal_base=50, cal_time=max_time,
                                                            rating_range=rating_range,
                                                            skip=have_defaulted)

            result_info.append('success')
            c_cdr.append(min_critical_cdr)
        except (ValueError) as e:
            result_info.append(e)
            c_cdr.append({})

    return c_cdr, scenario_desc, result_info, must_default


# 压力测试（情景分析）
def scenario_analysis_nonrev(project_seq, security_code, trade_date,
                             exist_tax_exp, exp_rate, tax_rate, suoe,
                             df_product, df_tranches, df_prediction, df_sequence, df_plan,
                             df_other, df_trigger, events, df_schedule, contingent_param_dict,
                             scenario_sets,
                             base_rrs, base_cprs, base_dp, cal_base=50, cal_time=100,
                             rating_range='security', skip={}):
    """
    非循环购买计算临界违约率

    Args:
        base_cprs (pd.Series): 基准早偿率
        base_rrs (pd.Series): 基准违约回收率
        base_dp (pd.Series): 基准违约分布序列
        rating_range (str): security-security_code对应的单个证券的临界回收率，project-项目下所有证券同时计算临界回收率
        cal_base (float): 首个迭代值
        cal_time (int): 最大跌打次数，超过则跳出
        scenario_sets (list): 与 ``main_ccdr`` 中所指的场景不同，这里在list中如果存在多种场景，指的是类似于 `回收率上升/下降20%` 这种，将其拆成上升20%和下降20% ，最终对每个证券得到一个临界回收率（为list中场景计算的临界违约率中的最小值）
        skip (dict): key-证券代码，value-(bool)，表示是否跳过计算

    Returns:
        dict: 各证券的临界违约率


    **逻辑**

    1. 先用加压场景中对各参数的加压条件和 ``pressure_params_notrev`` , 获取加压场景下的违约回收率、早偿率和违约分布
    2. 代入 ``ccdr_calculator`` 计算临界违约率

    """
    cdr_final = {}
    for scenario in scenario_sets:

        rr_rate = scenario.get('rr_rate', 100)/100
        cpr_rate = scenario.get('cpr_rate', 100)/100
        bp_change = scenario.get('bp_change', 0) / 10000
        dp_rate = scenario.get('dp_rate', 0)/100
        dp_direction = scenario.get('dp_direction', 'stay_put')  # 没有的时候使用默认值，实际上不会有影响

        rrs, cprs, dp = pressure_params_notrev(base_rrs, base_cprs, base_dp, rr_rate,
                                               cpr_rate, dp_rate, dp_direction)
        cdrs = ccdr_calculator(project_seq=project_seq, security_code=security_code, trade_date=trade_date,
                               exist_tax_exp=exist_tax_exp, exp_rate=exp_rate, tax_rate=tax_rate, suoe=suoe,
                               df_product=df_product, df_tranches=df_tranches, df_prediction=df_prediction,
                               df_sequence=df_sequence, df_plan=df_plan,
                               df_other=df_other, df_trigger=df_trigger, events=events, df_schedule=df_schedule,
                               contingent_param_dict=contingent_param_dict,
                               rrs=rrs, cprs=cprs, bp_change=bp_change, dp=dp,  cal_base=cal_base, cal_time=cal_time,
                               module_type='series_normal1', rating_range=rating_range,
                               is_rev=False, skip=skip)

        if rating_range == 'security':
            cal_base = cdrs[security_code]
            cdr_final[security_code] = min(cdr_final.get(security_code, 100), cdrs[security_code])
        else:
            cal_base = min(cdrs.values())
            for x in cdrs:
                cdr_final[x] = min(cdr_final.get(x, 100), cdrs[x])
    return cdr_final


def scenario_analysis_rev(project_seq, security_code, trade_date,
                          exist_tax_exp, exp_rate, tax_rate, suoe,
                          df_product, df_tranches, df_prediction, df_sequence, df_plan,
                          df_other, df_trigger, events, df_schedule, contingent_param_dict,
                          base_rrs, base_cprs, scenario_sets, base_rp=None, base_pps=None, base_yrs=None,
                          cal_base=50, cal_time=100,
                          rating_range='security', skip={}):
    """
    循环购买计算临界违约率

    Args:
        base_cprs (pd.Series): 基准早偿率
        base_rrs (pd.Series): 基准违约回收率
        base_pps (pd.Series): 基准摊还比例序列（如果是循环购买）
        base_yrs (pd.Series): 基准收益率序列（如果是循环购买）
        base_rp (float) : 循环购买率（如果是循环购买）
        rating_range (str): security-security_code对应的单个证券的临界回收率，project-项目下所有证券同时计算临界回收率
        cal_base (float): 首个迭代值
        cal_time (int): 最大跌打次数，超过则跳出
        scenario_sets (list): 与 ``main_ccdr`` 中所指的场景不同，这里在list中如果存在多种场景，指的是类似于 `回收率上升/下降20%` 这种，将其拆成上升20%和下降20% ，最终对每个证券得到一个临界回收率（为list中场景计算的临界违约率中的最小值）
        skip (dict): key-证券代码，value-(bool)，表示是否跳过计算

    Returns:
        dict: 各证券的临界违约率


    **逻辑**

    1. 先用加压场景中对各参数的加压条件和 ``pressure_params_rev`` , 获取加压场景下的违约回收率、早偿率和违约分布
    2. 代入 ``ccdr_calculator`` 计算临界违约率

    """
    cdr_final = {}
    for scenario in scenario_sets:
        rr_rate = scenario.get('rr_rate', 100)/100
        pp_rate = scenario.get('pp_rate', 100)/100
        bp_change = scenario.get('bp_change', 0) / 10000
        yr_rate = scenario.get('yr_rate', 100)/100
        rp_rate = scenario.get('rp_rate', 100)/100  # 没有的时候使用默认值，实际上不会有影响
        rrs, pps, yrs, rp = pressure_params_rev(base_rrs, base_pps, base_yrs, base_rp, rr_rate, pp_rate, yr_rate, rp_rate)
        cdrs = ccdr_calculator(project_seq=project_seq, security_code=security_code, trade_date=trade_date,
                            exist_tax_exp=exist_tax_exp, exp_rate=exp_rate, tax_rate=tax_rate, suoe=suoe,
                            df_product=df_product, df_tranches=df_tranches, df_prediction=df_prediction,
                            df_sequence=df_sequence, df_plan=df_plan,
                            df_other=df_other, df_trigger=df_trigger, events=events, df_schedule=df_schedule,
                            contingent_param_dict=contingent_param_dict,
                            rrs=rrs, cprs=base_cprs, rp=rp, pps=pps, bp_change=bp_change, yrs=yrs, cal_base=cal_base,
                            cal_time=cal_time,
                            module_type='series_rev_recal', rating_range=rating_range, is_rev=True, skip=skip)
        if rating_range == 'security':
            cal_base = cdrs[security_code]
            cdr_final[security_code] = min(cdr_final.get(security_code, 100), cdrs[security_code])
        else:
            cal_base = min(cdrs.values())
            for x in cdrs:
                cdr_final[x] = min(cdr_final.get(x, 100), cdrs[x])

    return cdr_final


def ccdr_calculator(project_seq, security_code, trade_date,
                    exist_tax_exp, exp_rate, tax_rate, suoe,
                    df_product, df_tranches, df_prediction, df_sequence, df_plan,
                    df_other, df_trigger, events, df_schedule, contingent_param_dict,
                    rrs, cprs, bp_change, rp=None, pps=None, dp=None, yrs=None, cal_base=50, cal_time=100,
                    module_type='series_normal1',
                    rating_range='security', is_rev=False, skip={}):

    """
    计算临界违约率

    Args:
        rrs (pd.Series): 违约回收率
        cprs (pd.Series): 早偿率序列
        dp (pd.Series): 违约分布序列
        bp_change (float): 利差变动
        cal_base (float): 迭代起始cdr
        cal_time (int): 最大计算次数
        module_type (str): 计算模块
        rating_range (str):

                           * 'security'-仅单个证券，
                           * 'project'-项目下所有证券（except次级）

    Returns:
        dict: 各证券的临界违约率，key-证券代码，value-临界违约率，单位 %


    **逻辑**

        1. 如果是计算单个证券的临界回收率，若 `skip` 显示该证券跳过，则返回证券临界违约率为 0%。如果证券最新剩余金额已经是0，则返回证券临界违约率为100%，即已经完成还款，即使后续无回款该证券也不会违约。否则进行的迭代 :

            * 将 `cal_base` 作为首个用于迭代累计违约率 `cdr` 输入 ``judge_default`` ，同时在计算中会设置 `minus_CDR` 参数为是。：

                        * 如果证券未违约，表示 `cdr` 偏小，则令迭代下限 `floor_` 等于 `cdr` , 然后计算上下限的中间点，作为新的 `cdr`
                        * 如果证券违约，则表示 `cdr` 偏大，令迭代上限 `ceil_` 等于 `cdr` ,  然后计算上下限的中间点，作为新的 `cdr`

            * 如果上下限的差异大于 0.0001 % , 则用新的 `cdr` 继续迭代，直到上下限的差异小于0.0001%，或者达到迭代次数上限。
            * 迭代停止后，将最新迭代结果的现金流归集表中的违约金额加总除以入池本金金额，得到临界违约率（%）。因为 ``LoanPool`` 中提到的期初剩余本金对当期本金回收、违约、早偿总和的限制，所以现金流归集表实际的累计违约率不一定等于模型输入值，故作此调整。
            * 另外，当累计违约率过大时，由于违约、早偿的相互关系，当存在早偿时，无法达到100%违约，迭代不出有效的临界违约率。因此，过程中，如果迭代的累计违约率超过95%，并且最新迭代中，证券仍未违约，则：

                    * 如果是缩额法，就改成缩期， 因为缩额法下，即使当期违约很大且超出了原来规定的当期正常应收，仍会通过比例计算一个正常回收金额（详细原理见 ``LoanPool`` )，因此不可能达到100%违约的状态。
                    * 逐步缩小早偿率，cprs_ = cprs_ * 0.9 。 原因在于，早偿和违约存在矛盾，由于存在违约分布，因此不可能在第一期就出现全部的资产都违约，未违约的部分就可以正常计算早偿。显然，此时也不可能达到100%违约的状态。故通过逐步减少早偿的金额，可以使得违约率逐步逼近100%
                    * 如果早偿已经很小了，但是还是无法迭代出来，则减小违约回收率 rrs_ = rrs_ * 0.9 。 因为当违约率很大时，违约回收金额的结果同样很可观，当证券本金余额并不大时，很难出现违约
                    * 最后，如果违约回收率也很小了，则将违约分布重新计算为现金流归集表中各期本金回收占总本金回收的比例。

    2. 如果是计算项目下所有证券，则:

            * 从等级最低的证券开始，根据 1 中的步骤算出临界违约率, 同时记录下迭代过程中用到的所有 `cdr` 和对应的各证券的最终剩余本金额
            * 进行更高等级证券的迭代，首先，如果该证券显示需要跳过，则给出临界违约率为0%，否则，从前述的记录中，找到能令当前迭代证券的最终剩余本金额大于0的最小的累计违约率，作为本次迭代的违约回收率上限，然后根据 1 中的步骤计算得到该支证券的临界违约率。依此类推得到所有证券的临界违约率

    Notes:
        * 次级证券不予计算
        * 设置 minus_CDR 为是主要是为了排除已发生违约金额的影响，

    """
    # 临界cdr算得是未来存续期的无条件cdr,根据现金流计算器的规则,先把已违约部分加进去,然后在加压中会扣除已经违约的
    if not is_rev:
        dp_ = dp / dp.sum()
    else:
        dp_ = None
    cprs_ = cprs.copy()
    rrs_ = rrs.copy()
    initial_principal = df_product.loc[0, 'initial_principal']

    floor_ = 0
    ceil_ = 100
    tcd = cal_base  # 理论上的累计违约率
    acd = 0
    if rating_range == 'security':
        if skip.get(security_code, False):
            return {security_code: 0.}

        # 20230331 违约率极高时早偿降为0，违约回收也逐渐下降，否则迭代不出结果，有些证券可能永远不违约
        last_remain = 0
        run_time = 0
        period_end_principal = df_tranches.loc[df_tranches['security_code'] == security_code, 'period_end_balance']

        while abs(ceil_ - floor_) > 0.0001 and run_time < cal_time and period_end_principal > 0:
            run_time += 1
            cdr = tcd / 100
            remain_balances, have_defaulted, acd = judge_default(cdr=cdr, project_seq=project_seq,
                                                                 security_code=security_code, trade_date=trade_date,
                                                                 initial_principal=initial_principal,
                                                                 exist_tax_exp=exist_tax_exp, exp_rate=exp_rate,
                                                                 tax_rate=tax_rate, suoe=suoe, df_product=df_product,
                                                                 df_tranches=df_tranches, df_prediction=df_prediction,
                                                                 df_sequence=df_sequence, df_plan=df_plan,
                                                                 df_other=df_other, df_trigger=df_trigger,
                                                                 events=events, df_schedule=df_schedule,
                                                                 contingent_param_dict=contingent_param_dict,
                                                                 rrs=rrs_, cprs=cprs_, bp_change=bp_change,
                                                                 rp=rp, pps=pps, dp=dp_, yrs=yrs,
                                                                 module_type=module_type, is_rev=is_rev)

            remain_value = remain_balances[security_code]
            if_default = have_defaulted[security_code]

            if tcd > 95 and remain_value == 0 and last_remain == 0: # 如果说cprs 减小了一次后，剩余本金显著变化了则继续修改cdr，直至再次进入不再变化的状态

                if suoe:
                    suoe = ~suoe  # 缩额到不了100%违约

                if sum(cprs_) > 0.0001:
                    cprs_ = cprs_ * 0.9

                else:
                    if sum(rrs_) > 0.0001:
                        rrs_ = rrs_ * 0.9
                    else:
                        dp_ = df_prediction['current_principal_due'] / df_prediction['current_principal_due'].sum()

                # 如果输入的累计违约率在持续增大，而证券端剩余本金额变动很小，说明因为早偿、回收或者违约分布的关系，导致违约的可能性极小，无法迭代出临界值
                # 此时 1 逐步缩小早偿率，理论上早偿不应该与极高的违约概率同时存在。 2 早偿至0时，逐步缩小违约回收率

            if if_default:  # 违约率偏大
                ceil_ = tcd
                tcd = (floor_ + ceil_) / 2
            else:  # 违约率偏小
                floor_ = tcd
                tcd = (floor_ + ceil_) / 2

            last_remain = remain_value

        if period_end_principal > 0:
            return {security_code: acd}
        else:
            return {security_code: 100.}
    else:
        acds = {}

        df_tranches.sort_values(by='security_level', inplace=True, ignore_index=True)
        i = len(df_tranches) - 1
        level_ = df_tranches.loc[i, 'security_level'].capitalize()

        while 'Sub' in level_:
            i -= 1
            level_ = df_tranches.loc[i, 'security_level'].capitalize()
        securitys = list(df_tranches.loc[0: i, 'security_code'])
        cal_record = pd.DataFrame(columns=securitys + ['cdr'])

        while i >= 0:

            last_remain = 0
            run_time = 0
            code_ = df_tranches.loc[i, 'security_code']
            if skip.get(code_, False):
                acds[code_] = 0.
                i -= 1
            else:
                period_end_principal = df_tranches.loc[i, 'period_end_balance']
                while abs(ceil_ - floor_) > 0.0001 and run_time < cal_time and period_end_principal > 0:

                    run_time += 1
                    cdr = tcd / 100

                    remain_values, if_defaults, acd = judge_default(cdr=cdr, project_seq=project_seq,
                                                                     security_code=security_code, trade_date=trade_date,
                                                                     initial_principal=initial_principal,
                                                                     exist_tax_exp=exist_tax_exp, exp_rate=exp_rate,
                                                                     tax_rate=tax_rate, suoe=suoe, df_product=df_product,
                                                                     df_tranches=df_tranches, df_prediction=df_prediction,
                                                                     df_sequence=df_sequence, df_plan=df_plan,
                                                                     df_other=df_other, df_trigger=df_trigger,
                                                                     events=events, df_schedule=df_schedule,
                                                                     contingent_param_dict=contingent_param_dict,
                                                                     rrs=rrs_, cprs=cprs_, bp_change=bp_change,
                                                                     rp=rp, pps=pps, dp=dp_, yrs=yrs,
                                                                     module_type=module_type, is_rev=is_rev)

                    cal_record.loc[run_time, 'cdr'] = tcd
                    for one_code in securitys:
                        cal_record.loc[run_time, one_code] = remain_values[one_code]

                    if tcd > 95 and remain_values[code_] == 0 and last_remain == 0:  # 如果说cprs 减小了一次后，剩余本金显著变化了则继续修改cdr，直至再次进入不再变化的状态
                        if suoe:
                            suoe = ~suoe  # 缩额到不了100%违约

                        if sum(cprs_) > 0:
                            cprs_ = cprs_ * 0.9

                        else:
                            if sum(rrs_) > 0:
                                rrs_ = rrs_ * 0.9
                            else:
                                dp_ = df_prediction['current_principal_due'] / df_prediction['current_principal_due'].sum()

                        # 如果输入的累计违约率在持续增大，而证券端剩余本金额变动很小，说明因为早偿、回收或者违约分布的关系，导致违约的可能性极小，无法迭代出临界值
                        # 此时 1 逐步缩小早偿率，理论上早偿不应该与极高的违约概率同时存在。 2 早偿至0时，逐步缩小违约回收率

                    if if_defaults[code_]> 0.01:  # 违约率偏大
                        ceil_ = tcd
                        tcd = (floor_ + ceil_) / 2
                    else:  # 违约率偏小
                        floor_ = tcd
                        tcd = (floor_ + ceil_) / 2

                    last_remain = remain_values[code_]

                if run_time >= cal_time:
                    print(f"{code_}临界回收率计算因超过迭代次数限制退出")

                if period_end_principal > 0:
                    acds[code_] = acd
                else:
                    acds[code_] = 100.

                i -= 1
                while i >= 0:
                    code_ = df_tranches.loc[i, 'security_code']
                    if skip.get(code_, False):
                        acds[code_] = 0.
                        i -= 1
                    else:
                        cal_record.reset_index(drop=True, inplace=True)
                        default_record = cal_record.loc[cal_record[code_] > 0, [code_, 'cdr']]  #  从记录的结果里找到最合适的迭代上限
                        if len(default_record) > 0:
                            default_record.sort_values(by='cdr', ascending=True, ignore_index=True, inplace=True)
                            ceil_ = default_record.loc[0, 'cdr']
                        else:
                            ceil_ = 100  # 逐渐滚动到高等级，故重置违约率上限
                        break

        return acds


def pressure_params_rev(base_rrs, base_pps, base_yrs, base_rp, rr_rate=1., pp_rate=1., yr_rate=1., rp_rate=1.,):
    """
     将每个基础参数调整为加压情况下的值

     Args:
        base_rrs (pd.Series): 基准违约回收率
        base_pps (pd.Series): 基准摊还比例序列（如果是循环购买）
        base_yrs (pd.Series): 基准收益率序列（如果是循环购买）
        base_rp (float) : 循环购买率（如果是循环购买）
        rr_rate (float) : 回收率为基准参数的 rr_rate%
        pp_rate (float) : 月回款率为基准参数的 pp_rate% (仅循环购买适用)
        yr_rate (float) : 收益率为基准参数的 yr_rate% (仅循环购买适用)
        rp_rate (float) : 循环购买率为基准参数的 rp_rate% (仅循环购买适用)

     Returns:
        tuple: tuple contains:
            * rrs (pd.Series): rr_rate * base_rrs，如果违约回收率总和超过1，则 rrs=rrs/rrs.sum()
            * pps (pd.Series): pp_rate * base_pps，pps=pps/pps.sum(), 摊还比例的总和应该等于 1
            * yrs (pd.Series): yr_rate * base_yrs
            * rp (float): min(rp_rate * base_rp, 1)，不能超过1
     """

    rp = min(rp_rate * base_rp, 1)
    pps = pp_rate * base_pps
    pps = pps / pps.sum()
    yrs = yr_rate * base_yrs
    rrs = rr_rate * base_rrs
    if rrs.sum() > 1:
        rrs = rrs / rrs.sum()

    return rrs, pps, yrs, rp


def pressure_params_notrev(base_rrs, base_cprs, base_dp, rr_rate=1., cpr_rate=1.,
                           dp_rate=0., dp_direction='stay_put'):
    """
     将每个基础参数调整为加压情况下的值

     Args:
         base_cprs (pd.Series): 基准早偿率
         base_rrs (pd.Series): 基准违约回收率
         base_dp (pd.Series): 基准违约分布序列
         rr_rate: float, 回收率为原始参数的 rr_rate%
         cpr_rate: float/array, 提前偿还率为原始参数的 cpr_rate%
         dp_rate: float, 违约分布前置或后置 dp_rate %
         dp_direction: str, 违约分布变动方向，front-前置，back-后置，stay_put-不变

     Returns:
         tuple: tuple contains:
             * rrs (pd.Series): rr_rate * base_rrs, 如果违约回收率总和超过1，则 rrs=rrs/rrs.sum()
             * cprs (pd.Series): cpr_rate * base_cprs, 序列中超过 1 的则改为 1
             * dp (pd.Series): 将 `base_dp` 中所有值都乘以 (1-dp_rate), 假设总共扣减了 sum(base_dp) * dp_rate。如果 `dp_direction` 是前置，则将扣减的金额根据比例分配到 `dp` 序列的前半段。 如果是后置，则将扣减的金额根据比例分配到 `dp` 序列的后半段。
     """

    cprs = cpr_rate * base_cprs
    cprs[cprs > 1] = 1
    len_dp_valid = len(base_dp.dropna())
    sum_dp = sum(base_dp)
    dp = base_dp * (1 - dp_rate)

    if dp_direction == 'front':
        if sum(dp[:int(np.ceil(0.5 * len_dp_valid))]) > 0:
            dp[:int(np.ceil(0.5 * len_dp_valid))] = \
                (1 + sum_dp * dp_rate / sum(dp[:int(np.ceil(0.5 * len_dp_valid))])) \
                * dp[:int(np.ceil(0.5 * len_dp_valid))]
        else:  # 如果某些假设，比如评级报告的假设期次不足以覆盖最新现金流归集表的期次，可能会遇到，此时用基准dp
            dp = base_dp.copy()
    elif dp_direction == 'back':
        if sum(dp[int(np.ceil(0.5 * len_dp_valid)):]) > 0:
            dp[int(np.ceil(0.5 * len_dp_valid)):] = \
                (1 + sum_dp * dp_rate / sum(dp[int(np.ceil(0.5 * len_dp_valid)):])) \
                * dp[int(np.ceil(0.5 * len_dp_valid)):]
        else:
            dp = base_dp.copy()

    rrs = rr_rate * base_rrs
    if rrs.sum() > 1:
        rrs = rrs / rrs.sum()  # 回收不能超过100%

    return rrs, cprs, dp


def judge_default(cdr, project_seq, security_code, trade_date, initial_principal,
                    exist_tax_exp, exp_rate, tax_rate, suoe,
                    df_product, df_tranches, df_prediction, df_sequence, df_plan,
                    df_other, df_trigger, events, df_schedule, contingent_param_dict,
                    rrs, cprs, bp_change, rp=None, pps=None, dp=None, yrs=None,
                    module_type='series_normal1',
                    is_rev=False):
    """
    根据输入的参数，获取指定压力参数下，证券的最终剩余本金额和是否违约

    Args:
       cdr (float): 迭代的累计违约率
       rrs, cprs, bp_change, rp, pps, dp, yrs: 根据压力情况处理后的、除累计违约率外的加压参数。

    Returns:
        tuple: tuple contains:
            * remain_balances (dict): key-证券代码，value-（float）在给定的 `cdr` 下，证券最终的剩余本金余额
            * have_defaulted (dict): key-证券代码，value-（bool）在给定的 `cdr` 下，是否有发生违约
            * acd (float): 加压后现金流归集表的未来违约本金总和占初始入池本金额的比例。

    Notes:
        * 为非循环时，将现金流计算器运算的 `dp_match_perfectly` 设为 True, 因为在参数估计阶段已经将违约分布处理成跟现金流归集表完全匹配, 同时 `minus_CDR` 为 True.
        * 循环购买的只能用收益率模式，不能用折现率模式
        * `param_match_method` 设为 'remain' ，因为在参数估计中处理过
        * 一些参数可以设置为默认值，配置同计算器的一致 （ 见 `abs.doc.conf` )
    """

    if is_rev:
        cdrs = pd.Series(cdr, index=range(1, len(pps) + 1))
        security_result_upload, pool_result_upload, security_result, pool_result, tranches_obj, \
        df_assumptions, df_factor, distribution_info \
            = main_calculation(
            df_product=df_product, df_tranches=df_tranches, df_prediction=df_prediction,
            df_sequence=df_sequence, df_plan=df_plan, df_other=df_other, df_trigger=df_trigger,
            df_schedule=df_schedule,
            events=events,
            project_seq=project_seq, trade_date=trade_date,
            scenario_type='user', model_type=None, coupon_rate_change=bp_change,
            recal_cashflow=True, CDRs=cdrs, CPRs=cprs, RRs=rrs, YRs=yrs,
            interest_method='yield_',
            RP=rp, PPs=pps, exist_tax_exp=exist_tax_exp, exp_rate=exp_rate, tax_rate=tax_rate,
            suoe=suoe, module_type=module_type, minus_CDR=True,
            begin_default_recover=default_params['begin_default_recover'],
            add_remain_account=default_params['add_remain_account'], cpr_type=default_params['cpr_type'],
            split_default=default_params['split_default'],
            day_count_method=default_params['day_count_method'],
            param_match_method='remain', compensate=default_params['compensate_rating'])
    else:
        security_result_upload, pool_result_upload, security_result, pool_result, tranches_obj, df_assumptions, \
        df_factor, distribution_info\
            = main_calculation(
            df_product=df_product, df_tranches=df_tranches, df_prediction=df_prediction,
            df_sequence=df_sequence, df_plan=df_plan, df_other=df_other, df_trigger=df_trigger,
            df_schedule=df_schedule,
            events=events,
            project_seq=project_seq, trade_date=trade_date,
            scenario_type='user', model_type=None, coupon_rate_change=bp_change,
            recal_cashflow=False, CDR=cdr, DPs=dp, CPRs=cprs, RRs=rrs,
            exist_tax_exp=exist_tax_exp, exp_rate=exp_rate, tax_rate=tax_rate,
            suoe=suoe, module_type=module_type, minus_CDR=True,
            dp_match_perfectly=True,
            begin_default_recover=default_params['begin_default_recover'],
            add_remain_account=default_params['add_remain_account'], cpr_type=default_params['cpr_type'],
            split_default=default_params['split_default'],
            day_count_method=default_params['day_count_method'],
            param_match_method='remain', compensate=default_params['compensate_rating'])

    have_defaulted = {}
    remain_balances = {}
    for i in range(0, len(df_tranches)):
        level_ = df_tranches.loc[i, 'security_level'].capitalize()
        if 'Sub' not in level_:
            code_ = df_tranches.loc[i, 'security_code']
            sec_result = security_result[security_result['证券代码'] == code_]
            if len(sec_result) > 0:
                remain_ = sec_result["期末本金余额"].iloc[-1]
                if_default = remain_ > 0.1
                have_defaulted.update({code_: if_default})
                remain_balances.update({code_: remain_})
            else:
                if df_tranches.loc[i, 'period_end_balance'] < 0.01:
                    have_defaulted.update({code_: False})
                    remain_balances.update({code_: 0.})
                else:
                    have_defaulted.update({code_: True})
                    remain_balances.update({code_: df_tranches.loc[i, 'period_end_balance']})

    acd = pool_result['违约金额'].sum() / initial_principal * 100  # 返回的是未来的违约率，不是总违约率
    return remain_balances, have_defaulted, acd