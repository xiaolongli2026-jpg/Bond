# -*- coding: utf-8 -*-


import pandas as pd

from main.abs_calculator.module_abs_new import main_calculation
from conf.conf import default_params
from utils.db_util import UploadLib
from conf.conf import config_source_db
from rating.prepare.rating_data import get_scenarios


def main_crr(security_code, project_seq, trade_date, exist_tax_exp, exp_rate, tax_rate, suoe,
             df_product, df_tranches, df_prediction, df_sequence, df_plan, df_other,
             df_trigger, events, df_schedule, contingent_param_dict,
             rating_range='security', max_time=100, customize_scenario=False, scenario_sets=None):
    """
    计算临界回收率

    Args:
        rating_range (str): security-security_code对应的单个证券的临界回收率，project-项目下所有证券同时计算临界回收率
        max_time (int): 最大跌打次数，超过则跳出
        customize_scenario (bool): 是否是自定义场景
        scenario_sets (dict): 如果是自定义场景，一定要输入


    Returns:
        tuple: tuple contains:
            * c_rr (list): 记录各场景下的临界回收率，元素个数为场景数量。
            * scenario_desc (list): 对各个加压场景的描述，元素个数为场景数量。
            * result_info (list) : 记录各情景是否正常计算出临界回收率，元素个数为场景数量。
            * must_default (list): 必然违约证券


    **逻辑**

    1. 首先计算在100%回收率、无延迟回收、无利差变化下是否有证券违约，如果是，则记录为需要跳过的证券，在后续计算临界回收率时不会计算。
    2. 如果不是自定义场景，则从 ``config.scenario_nums_dict`` 中根据二级分类，获取加压场景
    3. 分别将各加压场景带入 ``scenario_analysis_npl`` 得到各个场景的临界回收率

    Notes:
        注意最终得到的临界回收率只针对未来存续期，即未来回收总额占当前资产池本金余额的比例，而不是项目存续周期总回收占入池金额的比例。

    """

    c_rr = []  # 记录临界回收率
    result_info = [] # 记录各情景是否正常计算出临界违约率
    # 1. 先判断下在回收率为100%时是否有证券违约，
    remain_balances, have_defaulted, a = judge_npls_default(100, project_seq=project_seq,
                                                            trade_date=trade_date, exist_tax_exp=exist_tax_exp,
                                                            exp_rate=exp_rate, tax_rate=tax_rate, suoe=suoe,
                                                            df_product=df_product, df_tranches=df_tranches,
                                                            df_prediction=df_prediction, df_sequence=df_sequence,
                                                            df_plan=df_plan, df_other=df_other, df_trigger=df_trigger,
                                                            events=events, df_schedule=df_schedule,
                                                            coupon_rate_change=0, dpp=0, portion=0)

    must_default = [x for x in df_tranches['security_code'] if have_defaulted.get(x, False)]

    if customize_scenario:
        labels = scenario_sets.keys
        sets_ = scenario_sets.copy()
        scenario_desc = {}
        for i in sets_:
            sets_[i] = [sets_[i]]
            scenario_desc[i] = f"利差缩减{scenario_sets[i].get('bp_change', 0)}BP" \
                               f"回收金额的{scenario_sets[i].get('portion', 0)*100}%" \
                               f"向后延长{scenario_sets[i].get('dpp', 0)}个月"
    else:
        secondary_classification = df_product.loc[0, 'secondary_classification']
        is_rev = df_product.loc[0, 'is_revolving_pool']
        db_config = UploadLib(config_source_db)
        db_config.connect()
        labels, sets_, scenario_desc = get_scenarios(secondary_classification, trade_date, is_rev, db_config)
        db_config.close()

    for k in labels:
        try:
            max_crr = scenario_analysis_npl(security_code=security_code, project_seq=project_seq,
                                            trade_date=trade_date, exist_tax_exp=exist_tax_exp, exp_rate=exp_rate,
                                            tax_rate=tax_rate, suoe=suoe,
                                            df_product=df_product, df_tranches=df_tranches, df_prediction=df_prediction,
                                            df_sequence=df_sequence, df_plan=df_plan, df_other=df_other,
                                            df_trigger=df_trigger,
                                            events=events, df_schedule=df_schedule,
                                            contingent_param_dict=contingent_param_dict,
                                            rating_range=rating_range,
                                            scenario_=sets_[k],
                                            max_time=max_time,
                                            basic_rr_=50, skip=have_defaulted)
        except (ValueError) as e:
            result_info.append(e)
            c_rr.append({})
        else:
            result_info.append('success')
            c_rr.append(max_crr)

    return c_rr, scenario_desc, result_info, must_default


def scenario_analysis_npl(security_code, project_seq, trade_date, exist_tax_exp, exp_rate, tax_rate, suoe,
                          df_product, df_tranches, df_prediction, df_sequence, df_plan, df_other, df_trigger,
                          events, df_schedule, contingent_param_dict,
                          rating_range='security', max_time=100, scenario_=None, basic_rr_=50, skip={}):
    """
    不良贷款类的情景分析

    Args:
        rating_range (str): security-security_code对应的单个证券的临界回收率，project-项目下所有证券同时计算临界回收率
        max_time (int): 最大跌打次数，超过则跳出
        skip (dict): key-证券代码，value-(bool)，表示是否跳过计算
        scenario_ (list): 与 ``main_crr`` 中所指的场景不同，这里在list中如果存在多种场景，指的是类似于 `回收率上升/下降20%` 这种，将其拆成上升20%和下降20% ，最终对每个证券得到一个临界回收率（为list中场景计算的临界回收率中的最大值）
        basic_rr_ (float): 首个迭代违约回收率

    Returns:
        dict: crr_final: 临界回收率, key为security_code, value为临界违约率，不包含次级。当计算范围为单个证券时，其中只有一个证券

    """
    crr_final = {}
    for scenario in scenario_:
        bp_change = scenario.get('bp_change', 0) / 10000
        dpp = scenario.get('dpp', 0)
        portion = scenario.get('portion', 0) / 100
        crr_ = crr_calculator(security_code, project_seq, trade_date, exist_tax_exp, exp_rate, tax_rate, suoe,
                              df_product, df_tranches, df_prediction, df_sequence, df_plan, df_other, df_trigger,
                              events, df_schedule, contingent_param_dict, coupon_rate_change=bp_change,
                              dpp=dpp, portion=portion, cal_base=basic_rr_, upper_limit=100, lower_limit=0,
                              rating_range=rating_range, max_time=max_time, skip=skip)

        if rating_range == 'security':
            basic_rr_ = crr_[security_code]
            crr_final[security_code] = max(crr_final.get(security_code, 0), crr_[security_code])
        else:
            basic_rr_ = max(crr_.values())
            for x in crr_:
                crr_final[x] = max(crr_final.get(x, 0), crr_[x])

    return crr_final


# 临界回收率计算器
def crr_calculator(security_code, project_seq, trade_date, exist_tax_exp, exp_rate, tax_rate, suoe, df_product,
                   df_tranches, df_prediction, df_sequence, df_plan, df_other, df_trigger, events, df_schedule,
                   contingent_param_dict, coupon_rate_change=0, dpp=0, portion=0, cal_base=50, upper_limit=100, lower_limit=0,
                   rating_range='security', max_time=100, skip={}):
    """

    找到临界回收率，规则为迭代，直到该等级证券的本金无法足额偿付

    Args:
        security_code:
        coupon_rate_change (float): 利差变动
        dpp (int): 延迟回收月份
        portion (float): 延迟回收比例
        cal_base (float): 迭代起始值，即进行迭代的首个违约回收率
        upper_limit (float): 违约回收率上限
        lower_limit (float): 违约回收率下限
        rating_range (str): security-security_code对应的单个证券的临界回收率，project-项目下所有证券同时计算临界回收率
        max_time (int): 最大跌打次数，超过则跳出
        skip (dict): key-证券代码，value-(bool)，表示是否跳过计算

    Returns:
        dict: 各证券的临界回收率，key-证券代码，value-临界回收率，单位 %

    **逻辑**

    1. 如果是计算单个证券的临界回收率，若 `skip` 显示该证券跳过，则返回证券临界回收率为 100%。如果证券最新剩余金额已经是0，则返回证券临界回收率为0，即已经完成还款，即使后续无回款该证券也不会违约。否则进行的迭代 :

            * 将 `cal_base` 作为首个违约回收率 `RR` 代入 ``judge_npls_default`` ：

                        * 如果证券未违约，表示 `RR` 大于等于临界回收率，则令 `upper_limt` 等于 `RR` , 然后计算上下限的中间点，作为新的 `RR`
                        * 如果证券违约，则表示 `RR` 小于等于临界违约率，令 `lower_limit` 等于 `RR` ,  然后计算上下限的中间点，作为新的 `RR`

            * 如果上下限的差异大于 0.0001 % , 则用新的 `RR` 继续迭代，直到上下限的差异小于0.0001%，或者达到迭代次数上限。

    2. 如果是计算项目下所有证券，则:

            * 从等级最低的证券开始，根据 1 中的步骤算出临界回收率, 同时记录下迭代过程中用到的所有违约回收率和对应的各证券的最终剩余本金额
            * 进行更高等级证券的迭代，首先，如果该证券显示需要跳过，则给出临界回收率为100%，否则，从前述的记录中，找到能令当前迭代证券的最终剩余本金额大于0的最大的违约回收率，作为本次迭代的违约回收率下限，然后根据 1 中的步骤计算得到该支证券的临界回收率。依此类推得到所有证券的临界回收率

    Notes:
        次级证券不予计算
    """

    lower_ = lower_limit
    upper_ = upper_limit
    mid_ = cal_base

    if rating_range == 'security':
        if skip.get(security_code, False):
            return {security_code: 100}
        period_end_principal = df_tranches.loc[df_tranches['security_code']==security_code, 'period_end_balance']
        run_time = 0
        arr = 0
        while abs(upper_ - lower_) > 0.0001 and run_time < max_time and period_end_principal > 0:
            run_time += 1
            RR = mid_ * 0.01  # do_calculation使用
            remain_balances, have_defaulted, arr = \
                judge_npls_default(rr=RR, df_product=df_product, df_tranches=df_tranches, df_prediction=df_prediction,
                                   df_sequence=df_sequence, df_plan=df_plan, df_other=df_other, df_trigger=df_trigger,
                                   events=events, df_schedule=df_schedule,
                                   project_seq=project_seq, trade_date=trade_date,
                                   exist_tax_exp=exist_tax_exp, exp_rate=exp_rate, tax_rate=tax_rate,
                                   coupon_rate_change=coupon_rate_change,
                                   suoe=suoe, dpp=dpp, portion=portion, )

            if have_defaulted[security_code]:  #不够 增加回收率
                lower_ = mid_
                mid_ = (lower_ + upper_) / 2
            else:
                upper_ = mid_
                mid_ = (lower_ + upper_) / 2

        if period_end_principal > 0:
            crr = arr
        else:
            crr = 0

        if run_time >= max_time:
            print("临界回收率计算因超过迭代次数限制退出")

        return {security_code: crr}

    else:
        crrs = {}
        df_tranches.sort_values(by='security_level', inplace=True, ignore_index=True)
        i = len(df_tranches) - 1
        level_ = df_tranches.loc[i, 'security_level'].capitalize()
        while 'Sub' in level_:
            i -= 1
            level_ = df_tranches.loc[i, 'security_level'].capitalize()
        securitys = list(df_tranches.loc[0: i, 'security_code'])
        cal_record = pd.DataFrame(columns=securitys + ['rr'])
        while i >= 0:
            run_time = 0
            code_ = df_tranches.loc[i, 'security_code']
            if skip.get(code_, False):
                crrs[code_] = 100
                i -= 1
            else:
                period_end_principal = df_tranches.loc[i, 'period_end_balance']
                arr = 0
                while abs(upper_ - lower_) > 0.0001 and run_time < max_time and period_end_principal > 0:
                    max_time += 1
                    RR = mid_ * 0.01  # do_calculation使用
                    remain_balances, have_defaulted, arr = \
                        judge_npls_default(rr=RR, df_product=df_product, df_tranches=df_tranches,
                                           df_prediction=df_prediction, df_sequence=df_sequence, df_plan=df_plan,
                                           df_other=df_other, df_trigger=df_trigger, events=events,
                                           df_schedule=df_schedule,
                                           project_seq=project_seq, trade_date=trade_date,
                                           exist_tax_exp=exist_tax_exp, exp_rate=exp_rate, tax_rate=tax_rate,
                                           coupon_rate_change=coupon_rate_change,
                                           suoe=suoe, dpp=dpp, portion=portion, )

                    cal_record.loc[run_time, 'rr'] = RR * 100
                    for one_code in securitys:
                        cal_record.loc[run_time, one_code] = remain_balances[one_code]
                    if have_defaulted[code_]:  # 不够 增加回收率
                        lower_ = mid_
                        mid_ = (lower_ + upper_) / 2
                    else:
                        upper_ = mid_
                        mid_ = (lower_ + upper_) / 2

                if run_time >= max_time:
                    print(f"{code_}临界回收率计算因超过迭代次数限制退出")

                if period_end_principal > 0:
                    crrs[code_] = arr
                else:
                    crrs[code_] = 0

                i -= 1
                while i >= 0:
                    code_ = df_tranches.loc[i, 'security_code']
                    if skip.get(code_, False):
                        crrs[code_] = 100
                        i -= 1
                    else:
                        # 从record里找该等级下会违约的rr作为下限
                        cal_record.reset_index(drop=True, inplace=True)
                        default_record = cal_record.loc[cal_record[code_] > 0, [code_, 'rr']]
                        if len(default_record) > 0:
                            default_record.sort_values(by='rr', ascending=False, ignore_index=True, inplace=True)
                            lower_ = default_record.loc[0, 'rr']
                        else:
                            lower_ = 0  # 重置迭代下限
                        break
        return crrs


def judge_npls_default(rr, project_seq, trade_date, exist_tax_exp, exp_rate, tax_rate, suoe, df_product,
                       df_tranches, df_prediction, df_sequence, df_plan, df_other, df_trigger, events, df_schedule,
                       coupon_rate_change=0, dpp=0, portion=0):
    """

    Args:
        rr (float): 当前迭代的违约回收率
        coupon_rate_change (float): 利差变动
        dpp (int): 延迟回收月份
        portion (float): 延迟回收比例

    Returns:
        tuple: tuple contains:

        * remain_balances (dict): key-证券代码，value-（float）在给定的 `rr` 下，证券最终的剩余本金余额
        * have_defaulted (dict): key-证券代码，value-（bool）在给定的 `rr` 下，是否有发生违约
        * arr (float): 未来本金回收总额占入池本金额的比例，单位 %


    **逻辑**

        * 将所有参数待入到 ``main_calculation`` 中进行计算，得到所有证券的还本付息、本金余额情况。其中，固定计算模块 `module_type` 为重新计算不良贷款现金流的模式 （ static_npl_recal ）
        * 计算未来的实际回收率，为本金流入与入池本金总额之比。
        * 一些参数可以设置为默认值，配置同计算器的一致 （ 见 `abs.doc.conf` ) ， 总体来说不良贷款下参数较少
    """

    security_result_upload, pool_result_upload, security_result, pool_result, tranches_obj, \
    df_assumptions, df_factor, distribution_info = \
        main_calculation(df_product=df_product, df_tranches=df_tranches, df_prediction=df_prediction,
                         df_sequence=df_sequence, df_plan=df_plan, df_other=df_other, df_trigger=df_trigger,
                         events=events, df_schedule=df_schedule,
                         project_seq=project_seq, trade_date=trade_date,
                         exist_tax_exp=exist_tax_exp, exp_rate=exp_rate, tax_rate=tax_rate,
                         scenario_type='user', coupon_rate_change=coupon_rate_change,
                         recal_cashflow=True, suoe=suoe, module_type='static_npl_recal',
                         model_type=None, RR=rr, DPP=dpp,
                         portion=portion, add_remain_account=default_params['add_remain_account'],
                         day_count_method=default_params['day_count_method'],
                         compensate=default_params['compensate_rating'])

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

    initial_principal = df_product.loc[0, 'initial_principal']
    arr = pool_result['本期应收本金'].sum() / initial_principal * 100 if len(pool_result) > 0 else 0  # 返回的是未来的回收率，不是总回收率
    return remain_balances, have_defaulted, arr