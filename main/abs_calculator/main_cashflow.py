# -*- coding: utf-8 -*-
"""资产池现金流加压及证券端现金流分配

"""

import pandas as pd
from typing import Optional, Literal, Union

from abs.doc.enumerators import SecondClass
from abs.calculate.valuation.cash_distribution import calculate_abs
from abs.calculate.assetmodel.markov_model_predict import markov_model_predict
from abs.calculate.assetmodel.extrapolation_model import extrapolation_model
from abs.calculate.assetmodel.regression_model_predict import regression_model_predict
from abs.calculate.asset.cashflowPressure import (cashflow_pressure, cashflow_pressure_dynamic,
                                                  cashflow_pressure_ycdr, cashflow_pressure_dynamic_ycdr)
from abs.calculate.asset.cashflowPressure_npl import cashflowPressure_npl
from abs.calculate.asset.s_param_match import match_cashflow_dp, match_cashflow_cond_params
from abs.calculate.valuation.format import *
from utils.timeutils import to_date2


def main_calculation(df_product: pd.DataFrame, df_tranches: pd.DataFrame, df_prediction: pd.DataFrame,
                     df_sequence: pd.DataFrame, df_plan: pd.DataFrame, df_other: pd.DataFrame,
                     df_trigger: pd.DataFrame, events: dict, df_schedule,
                     project_seq: str, trade_date: str,
                     module_type: Literal['static_normal1', 'static_normal2', 'series_normal1',
                                          'series_normal2', 'static_npl_recal', 'series_rev_recal', 'static_rev_recal'],
                     exist_tax_exp: bool, exp_rate: Union[float, int], tax_rate: Union[float, int],
                     scenario_type: Literal['user', 'model'],
                     recal_cashflow: bool, suoe: bool,
                     day_count_method: Literal['begin', 'end'], compensate: bool,
                     CDR: Optional[Union[float, int]]=None, CPR: Optional[Union[float, int]]=None,
                     RR: Optional[Union[float, int]]=None,
                     DPP: Optional[Union[float, int]]=None, RP: Optional[Union[float, int]]=None,
                     DPs: Optional[pd.Series]=None, CPRs: Optional[pd.Series]=None, RRs: Optional[pd.Series]=None,
                     CDRs: Optional[pd.Series]=None, PPs: Optional[pd.Series]=None, YRs: Optional[pd.Series]=None,
                     DRs: Optional[pd.Series]=None,
                     YCDR: Optional[Union[float, int]]=None, YCDRs: Optional[pd.Series]=None,
                     portion: Optional[float] = 0.,
                     model_type: Optional[Literal['markove_model', 'linear_model', 'sigmoid_model', 'extrapolate_model']]=None,
                     bayes: Optional[bool]=False, n_markov: Optional[bool]=False,
                     coupon_rate_change: Union[float, int]=0,
                     param_match_method: Literal['all', 'remain']='remain',
                     interest_method: Literal['yield_', 'discount']='yield_',
                     minus_CDR: bool=True, begin_default_recover: bool=False, add_remain_account: bool=False,
                     dp_match_perfectly: bool=False, cur=None, split_default: bool=False, cpr_type='type1',
                     same_type=False, ):

    """
    根据所选的计算模式、参数，选择选择对应的计算方式，计算现金流

    Args:
        df_product(pd.DataFrame): 项目基本信息，包括项目起始日、首次付息日、入池本金额等信息
        df_tranches(pd.DataFrame): 证券基本信息，包括证券发行金额、票息、利息类型、最新一期的还本付息情况等信息
        df_prediction(pd.DataFrame): 现金流归集表
        df_sequence(pd.DataFrame): 支付顺序表
        df_plan(pd.DataFrame): 摊还计划表
        df_other(pd.DataFrame): 一些额外的信息, 包括资产池、证券端的最新报告日、最新资产池归集日、最新证券支付日等
        df_trigger(pd.DataFrame): 触发事件表
        events(pd.DataFrame): 重大事件发生情况， key-事件名称，value-bool
        df_schedule(pd.DataFrame): 日期信息，包括归集日、证券支付日、是否循环购买期等
        其他定义见 ``abs_calculator`` , 名称一致的参数在不同子函数内定义一致

    Returns:
        tuple: tuple contains:
                  pool_result_upload(pd.DataFrame): 加压后现金流归集表 \n
                  pool_result_cn(pd.DataFrame): 加压后现金流归集表(中文) \n
                  security_result_upload(pd.DataFrame): 证券端所有证券的预测现金流,用于直接上传 \n
                  security_result_cn(pd.DataFrame): 证券端所有证券的预测现金流,区分更细，保存在excel备查 \n
                  tranches_obj (list[object]): 保存了的现金流分配后证券端的所有信息  \n
                  df_assumptions(pd.DataFrame): 假设参数，为经过处理后，跟现金流归集表完全匹配的参数 \n
                  df_factor(pd.DataFrame): 模型预测参数，不用模型测算的话没有 \n
                  distribution_info(list): 现金流分配过程的提示信息

    **逻辑**

        1. 如果`scenario_type='user'`调用自定义加压模块`do_calculation_user`
        2. 如果`scenario_type='model'`:

                    * ``model_type=extrapolate_model`` 调用资产池外推模型 ``do_calculation_extrapolate``
                    * ``model_type=linear_model`` 或者 ``model_type=sigmoid_model`` 调用回归模型对应加压模块 ``do_calculation_regression``
                    * ``model_type=markov_model`` 则调用马尔可夫模型对应加压模块 ``do_calculation_markov``

    """
    df_assumptions = None
    df_factor = None
    if scenario_type == 'user':
        # 输入参数 非循环购买和循环购买，如果都是在普通自定义下，处理方法一样

        tranches_obj, prediction, distribution_info = \
            do_calculation_user(df_product=df_product, df_tranches=df_tranches, df_prediction=df_prediction,
                                df_sequence=df_sequence, df_plan=df_plan, df_other=df_other, df_trigger=df_trigger,
                                events=events, df_schedule=df_schedule,
                                trade_date=trade_date, module_type=module_type, coupon_rate_change=coupon_rate_change,
                                recal_cashflow=recal_cashflow, day_count_method=day_count_method,
                                exist_tax_exp=exist_tax_exp, exp_rate=exp_rate, tax_rate=tax_rate,
                                suoe=suoe, CDR=CDR, CPR=CPR, RR=RR, DPP=DPP, RP=RP, DPs=DPs, CPRs=CPRs,
                                RRs=RRs, CDRs=CDRs, PPs=PPs, YRs=YRs, DRs=DRs, YCDR=YCDR, YCDRs=YCDRs, portion=portion,
                                param_match_method=param_match_method, interest_method=interest_method,
                                minus_CDR=minus_CDR,
                                begin_default_recover=begin_default_recover, add_remain_account=add_remain_account,
                                dp_match_perfectly=dp_match_perfectly, cpr_type=cpr_type, compensate=compensate)

    elif scenario_type == 'model':

        if model_type == 'extrapolate_model':
            tranches_obj, prediction, distribution_info, assumptions = \
            do_calculation_extrapolate(project_seq=project_seq, df_product=df_product, df_tranches=df_tranches,
                                       df_prediction=df_prediction, df_sequence=df_sequence, df_plan=df_plan,
                                       df_other=df_other, df_trigger=df_trigger, events=events, df_schedule=df_schedule,
                                       trade_date=trade_date,
                                       coupon_rate_change=coupon_rate_change, recal_cashflow=recal_cashflow,
                                       day_count_method=day_count_method, exist_tax_exp=exist_tax_exp, exp_rate=exp_rate,
                                       tax_rate=tax_rate, suoe=suoe, CPR=CPR, RR=RR, DPP=DPP, CPRs=CPRs,
                                       RRs=RRs, param_match_method=param_match_method,
                                       begin_default_recover=begin_default_recover,
                                       add_remain_account=add_remain_account, same_type=same_type,
                                       compensate=compensate, split_default=split_default)

        elif model_type == 'markov_model':
            tranches_obj, prediction, distribution_info, assumptions, df_transfer_prob = \
                do_calculation_markov(project_seq=project_seq, df_product=df_product, df_tranches=df_tranches,
                                      df_prediction=df_prediction, df_sequence=df_sequence, df_plan=df_plan,
                                      df_other=df_other, df_trigger=df_trigger, events=events, df_schedule=df_schedule,
                                      trade_date=trade_date, coupon_rate_change=coupon_rate_change,
                                      day_count_method=day_count_method, exist_tax_exp=exist_tax_exp, exp_rate=exp_rate,
                                      tax_rate=tax_rate, suoe=suoe, n_markov=n_markov, cur=cur,
                                      add_remain_account=add_remain_account, split_default=split_default, same_type=same_type,
                                      begin_default_recover=begin_default_recover, compensate=compensate)
            df_factor = markov_transfer_prob_format(df_transfer_prob)

        elif model_type == 'linear_model' or model_type == 'sigmoid_model':
            tranches_obj, prediction, assumptions, distribution_info, cdr_coef, ucpr_coef = \
                do_calculation_regression(project_seq=project_seq, df_product=df_product,
                                          df_tranches=df_tranches, df_prediction=df_prediction,
                                          df_sequence=df_sequence, df_plan=df_plan,
                                          df_other=df_other, df_trigger=df_trigger,
                                          events=events, df_schedule=df_schedule, trade_date=trade_date,
                                          coupon_rate_change=coupon_rate_change, day_count_method=day_count_method,
                                          exist_tax_exp=exist_tax_exp, exp_rate=exp_rate, tax_rate=tax_rate, suoe=suoe,
                                          RR=RR, DPP=DPP, RRs=RRs, model_type=model_type, bayes=bayes,
                                          begin_default_recover=begin_default_recover,
                                          add_remain_account=add_remain_account, cur=cur,
                                          compensate=compensate, split_default=split_default)

            df_factor = regression_coef_format(project_seq, cdr_coef, ucpr_coef)

        else:
            raise ValueError(f"unexplainable model type {model_type}")

        df_assumptions = predict_result_format(assumptions, project_seq)
    else:
        raise ValueError(f"加压模式{scenario_type}不合规")

    security_seqs = dict(zip(df_tranches['security_code'], df_tranches['security_seq']))
    security_result_upload, pool_result_upload = \
        upload_cf_format(tranches_obj, prediction, df_schedule, security_seqs, project_seq, trade_date)

    security_result_cn, pool_result_cn = chinese_cf_format(tranches_obj, prediction, df_schedule, df_product.loc[0, 'project_abbr'])


    return security_result_upload, pool_result_upload, security_result_cn, pool_result_cn, tranches_obj, \
           df_assumptions, df_factor, distribution_info


def do_calculation_user(df_product, df_tranches, df_prediction, df_sequence, df_plan, df_other, df_trigger, events,
                        df_schedule, trade_date, module_type, recal_cashflow, day_count_method,
                        exist_tax_exp, exp_rate: (float, int), tax_rate: (float, int), suoe, add_remain_account,
                        compensate, coupon_rate_change=0,
                        CDR: (float, int)=None, CPR: (float, int)=None, RR: (float, int)=None,
                        DPP: (float, int)=None, RP: (float, int)=None,
                        DPs: pd.Series= None, CPRs: pd.Series= None, RRs: Optional[pd.Series]=None,
                        CDRs: pd.Series= None, PPs: pd.Series= None, YRs: pd.Series= None, DRs: pd.Series= None,
                        YCDR: (float, int)=None, YCDRs: pd.Series= None, portion: float=0., param_match_method='remain',
                        interest_method='yield_', minus_CDR=True,
                        begin_default_recover=False, dp_match_perfectly=False,
                        split_default=False, cpr_type='type1',):
    """scenario_type为user,即用户输入参数时的计算模块

    参数和返回值见 ``main_calculation`` , 名称一致的参数在不同子函数内定义一致。
    """

    project_secondclass = str(df_product.loc[0, 'secondary_classification'])
    revolve = df_product.loc[0, 'is_revolving_pool']  # 循环池
    remain_principal = df_other.loc[0, 'remaining_principal']
    history_date = to_date2(df_other.loc[0, 'virtual_history_date'])
    initial_date = df_product.loc[0, 'initial_date']
    initial_principal = df_product.loc[0, 'initial_principal']
    realized_CDR = df_other.loc[0, 'CDR']
    current_default = df_other.loc[0, 'default_principal']
    collect_amount = df_other.loc[0, 'collect_amount']
    assumptions = None
    if not (module_type == 'series_rev_recal'):
        if project_secondclass == str(SecondClass["NPLS"]):  # 不良贷款
            result_prediction = cashflowPressure_npl(df_prediction=df_prediction, remain_principal=remain_principal,
                                                     history_date=history_date, initial_date=initial_date,
                                                     initial_principal=initial_principal, collect_amount=collect_amount,
                                                     is_revolving_pool=revolve, RR=RR,
                                                     dpp=DPP, portion=portion,
                                                     recal_cashflow=recal_cashflow)
        else:
            if len(df_prediction) > 0:
                if module_type == 'static_normal1':  # 累计违约率

                    if dp_match_perfectly:
                        if len(DPs) != len(df_prediction):
                            raise ValueError(f"输入的DPs与现金流归集表不匹配,期次分别是{len(DPs), len(df_prediction)}")
                        else:
                            DP = DPs
                    else:
                        DP = match_cashflow_dp(df_prediction, initial_date=initial_date, last_pool_date=history_date,
                                               dp=DPs, param_match_method=param_match_method)

                    result_prediction, assumptions = cashflow_pressure(df_prediction, history_date, initial_date,
                                                                       initial_principal, revolve, CDR, DP, CPR, RR, DPP, suoe,
                                                                       realized_CDR, current_default, minus_CDR=minus_CDR,
                                                                       begin_default_recover=begin_default_recover,
                                                                       split_default=split_default, cpr_type=cpr_type)
                elif module_type == 'static_normal2':
                    result_prediction, assumptions = cashflow_pressure_ycdr(df_prediction, history_date, initial_date,
                                                                            initial_principal, revolve, YCDR, CPR, RR,
                                                                            DPP, suoe, current_default=current_default,
                                                                            begin_default_recover=begin_default_recover,
                                                                            split_default=split_default,
                                                                            cpr_type=cpr_type)
                elif module_type == 'series_normal1':
                    # 参数处理

                    if dp_match_perfectly:
                        if len(DPs) != len(df_prediction):
                            raise ValueError(f"输入的DPs与现金流归集表不匹配,期次分别是{len(DPs), len(df_prediction)}")
                        else:
                            DP = DPs
                    else:
                        DP = match_cashflow_dp(df_prediction, initial_date=initial_date, last_pool_date=history_date,
                                               dp=DPs, param_match_method=param_match_method)

                    SMMs = match_cashflow_cond_params(df_prediction, initial_date=initial_date, last_pool_date=history_date,
                                                      param_series=CPRs, param_match_method=param_match_method)
                    result_prediction = cashflow_pressure_dynamic(df_prediction, history_date, initial_date,
                                                                  initial_principal, CDR, DP, SMMs, RRs, suoe,
                                                                  realized_CDR=realized_CDR,
                                                                  current_default=current_default,
                                                                  minus_CDR=minus_CDR,
                                                                  begin_default_recover=begin_default_recover,
                                                                  split_default=split_default,
                                                                  cpr_type=cpr_type)
                    assumptions = pd.DataFrame({'DATE': df_prediction['date_'], 'DP': DP, 'SMMs': SMMs})
                    assumptions.dropna(how='any', inplace=True, axis=0)

                elif module_type == 'series_normal2':
                    SMDRs = match_cashflow_cond_params(df_prediction, initial_date=initial_date, last_pool_date=history_date,
                                                      param_series=YCDRs, param_match_method=param_match_method)
                    SMMs = match_cashflow_cond_params(df_prediction, initial_date=initial_date, last_pool_date=history_date,
                                                      param_series=CPRs, param_match_method=param_match_method)
                    result_prediction = cashflow_pressure_dynamic_ycdr(df_prediction, history_date, initial_date,
                                                                       initial_principal, SMDRs, SMMs, RRs, suoe,
                                                                       current_default=current_default,
                                                                       begin_default_recover=begin_default_recover,
                                                                       split_default=split_default)
                    assumptions = pd.DataFrame({'DATE': df_prediction['date_'], 'SMDRs': SMDRs, 'SMMs': SMMs})
                    assumptions.dropna(how='any', inplace=True, axis=0)

                else:
                    raise ValueError("入参模式不适用")
            else:
                result_prediction = pd.DataFrame()  # 已经是空的归集表 不加压

        tranches_obj, prediction, distribution_info = \
            calculate_abs(df_product=df_product, df_tranches=df_tranches, df_prediction=result_prediction,
                          df_sequence=df_sequence, df_plan=df_plan, df_other=df_other, df_trigger=df_trigger,
                          events=events, df_schedule=df_schedule, trade_date=trade_date,
                          exist_tax_exp=exist_tax_exp, exp_rate=exp_rate, tax_rate=tax_rate,
                          coupon_rate_change=coupon_rate_change, recal_cashflow=False,
                          day_count_method=day_count_method,
                          add_remain_account=add_remain_account, begin_default_recover=begin_default_recover,
                          split_default=split_default, compensate=compensate)

        if isinstance(assumptions, pd.DataFrame):
            assumption = {}
            for x in assumptions.columns:
                assumption[x] = assumptions[x].tolist()
            assumptions = assumption

    else:  # 即 revolve and recal_cashflow

        tranches_obj, prediction, distribution_info =\
            calculate_abs(df_product=df_product, df_tranches=df_tranches, df_prediction=df_prediction,
                          df_sequence=df_sequence, df_plan=df_plan, df_other=df_other, df_trigger=df_trigger,
                          events=events, df_schedule=df_schedule, trade_date=trade_date,
                          exist_tax_exp=exist_tax_exp, exp_rate=exp_rate, tax_rate=tax_rate,
                          coupon_rate_change=coupon_rate_change, recal_cashflow=recal_cashflow,
                          day_count_method=day_count_method,
                          CDRs=CDRs, PPs=PPs, RP=RP, CPRs=CPRs, RRs=RRs, YCDRs=YCDRs, YRs=YRs, DRs=DRs, suoe=suoe,
                          type_=interest_method, add_remain_account=add_remain_account,
                          begin_default_recover=begin_default_recover, split_default=split_default, compensate=compensate)

    return tranches_obj, prediction, distribution_info


def do_calculation_extrapolate(project_seq, df_product, df_tranches, df_prediction, df_sequence, df_plan,
                               df_other, df_trigger, events, df_schedule, trade_date,
                               coupon_rate_change, recal_cashflow, day_count_method,
                               exist_tax_exp, exp_rate, tax_rate, suoe, begin_default_recover,
                               add_remain_account, split_default, same_type, compensate,
                               RR: (float, int) = None, DPP: (float, int)=None, CPR=None, CPRs: pd.Series = None,
                               RRs: pd.Series = None, param_match_method='remain'):
    """资产池外推模型对现金流的加压和分配

    参数和返回值见 ``main_calculation`` , 名称一致的参数在不同子函数内定义一致。

    """

    n = len(df_prediction)
    history_date = to_date2(df_other.loc[0, 'virtual_history_date'])
    initial_date = df_product.loc[0, 'initial_date']
    second_class = df_product.loc[0, 'secondary_classification']
    realized_cdr = df_other.loc[0, 'CDR']

    if RRs is None:
        RRs = pd.Series(RR, index=[DPP])
    if CPRs is None:
        CPRs = pd.Series(CPR, index=range(1, n))

    dp, cdr, extrapolate_staticpools_cdr = \
        extrapolation_model(project_seq=project_seq, predict_date=trade_date, second_class=second_class,
                            initial_date=initial_date, df_prediction=df_prediction, history_date=history_date,
                            realized_cdr=realized_cdr, same_type=same_type)
    tranches_obj, prediction, distribution_info = \
        do_calculation_user(df_product=df_product, df_tranches=df_tranches, df_prediction=df_prediction,
                            df_sequence=df_sequence, df_plan=df_plan, df_other=df_other, df_trigger=df_trigger,
                            events=events, df_schedule=df_schedule,
                            trade_date=trade_date, module_type='series_normal1',
                            coupon_rate_change=coupon_rate_change, recal_cashflow=recal_cashflow,
                            day_count_method=day_count_method,
                            exist_tax_exp=exist_tax_exp, exp_rate=exp_rate, tax_rate=tax_rate, suoe=suoe, CDR=cdr,
                            dp_match_perfectly=True,
                            DPs=dp, CPRs=CPRs, RRs=RRs, param_match_method=param_match_method,
                            begin_default_recover=begin_default_recover, add_remain_account=add_remain_account,
                            minus_CDR=True, split_default=split_default, compensate=compensate)
    assumption = {'CDR': cdr, 'DPS': dp, 'DATE': prediction['date_']}

    return tranches_obj, prediction, distribution_info, assumption


def do_calculation_regression(project_seq: pd.DataFrame, df_product: pd.DataFrame, df_tranches: pd.DataFrame,
                              df_prediction: pd.DataFrame, df_sequence: pd.DataFrame, df_plan: pd.DataFrame,
                              df_other: pd.DataFrame, df_trigger: pd.DataFrame,
                              events: dict, df_schedule: pd.DataFrame, trade_date: str,
                              coupon_rate_change, day_count_method, exist_tax_exp,
                              exp_rate, tax_rate, model_type, bayes,
                              begin_default_recover, add_remain_account, cur, split_default,
                              compensate, suoe, RR: (float, int) = None, DPP: (float, int)=None,
                              RRs: pd.Series = None, ):

    """
    回归模型加压、现金流分配

    参数见 ``main_calculation`` , 名称一致的参数在不同子函数内定义一致。

    Returns:
        tuple: tuple contains:

                * cdr_coef (dict): 累计违约率回归中的模型系数 \n
                * ucpr_coef (dict): 无条件早偿率回归中的模型系数 \n
                * 其他参数参考 ``do_calculation_user`` 的输出


    """

    if RRs is None:
        RRs = pd.Series(RR, index=[DPP])

    after_cashflow, assumptions, cdr, cdr_coef, ucpr_coef = \
        regression_model_predict(project_seq, trade_date, df_product, df_prediction, df_other, RRs,
                                 model_type=model_type, suoe=suoe, bayes=bayes,
                                 begin_default_recover=begin_default_recover, cur=cur, split_default=split_default)

    tranches_obj, prediction, distribution_info = \
        calculate_abs(df_product=df_product, df_tranches=df_tranches, df_prediction=after_cashflow,
                      df_sequence=df_sequence, df_plan=df_plan, df_other=df_other, df_trigger=df_trigger,
                      events=events, df_schedule=df_schedule, trade_date=trade_date,
                      exist_tax_exp=exist_tax_exp, exp_rate=exp_rate, tax_rate=tax_rate,
                      coupon_rate_change=coupon_rate_change, recal_cashflow=False,
                      day_count_method=day_count_method,
                      add_remain_account=add_remain_account, split_default=split_default, compensate=compensate)

    return tranches_obj, prediction, assumptions, distribution_info, cdr_coef, ucpr_coef


def do_calculation_markov(project_seq, df_product, df_tranches, df_prediction, df_sequence, df_plan, df_other,
                          df_trigger, events, df_schedule, trade_date, coupon_rate_change, day_count_method,
                          exist_tax_exp, exp_rate, tax_rate, suoe, n_markov, same_type,
                          add_remain_account, begin_default_recover, split_default, cur=None, compensate=True):

    """
    Markov模型对现金流进行加压、现金流分配

    参数和返回值见 ``main_calculation`` , 名称一致的参数在不同子函数内定义一致。

    """
    # 模型加压, 只适用于非循环、非不良贷款类 只对违约率和早偿率建模

    last_pool_date = to_date2(df_other.loc[0, 'virtual_history_date'])
    after_cashflow, after_status, df_transfer_prob = markov_model_predict(
        project_seq=project_seq, last_pool_date=last_pool_date, cashflow=df_prediction,
        initial_default= df_other.loc[0, 'default_principal'],
        initial_date=df_product.loc[0,  'initial_date'], same_type=same_type,
        n_markov=n_markov, suoe=suoe, cur=cur, begin_default_recover=begin_default_recover, split_default=split_default)

    assumptions = {'SMMs': after_cashflow['prepay_amount'] / after_cashflow['begin_principal_balance'],
                   'SMDRs': after_cashflow['default_amount'] / after_cashflow['begin_principal_balance'],
                   'DATE': after_cashflow['date_']}

    tranches_obj, prediction, distribution_info = \
        calculate_abs(df_product=df_product, df_tranches=df_tranches, df_prediction=after_cashflow,
                      df_sequence=df_sequence, df_plan=df_plan, df_other=df_other, df_trigger=df_trigger,
                      events=events, df_schedule=df_schedule, trade_date=trade_date,
                      exist_tax_exp=exist_tax_exp, exp_rate=exp_rate, tax_rate=tax_rate,
                      coupon_rate_change=coupon_rate_change, recal_cashflow=False,
                      day_count_method=day_count_method,
                      add_remain_account=add_remain_account, split_default=split_default, compensate=compensate)

    return tranches_obj, prediction, distribution_info, assumptions, df_transfer_prob

