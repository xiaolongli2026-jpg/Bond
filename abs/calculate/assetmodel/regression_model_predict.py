# -*- coding: utf-8 -*-


import pandas as pd
import numpy as np
import math

import scipy.integrate as integrate
from abs.calculate.asset.loanpool import LoanPool
from abs.prepare.data_load.model_data import get_comparable_project, get_predict_factors, get_train_data

from abs.calculate.assetmodel.regression_model_train import regression_model_without_bayers
from abs.prepare.data_preprocess.prepcashflow_new import combine_cashflow, tranfer_prediction_to_initial_freq
from utils.timeutils import age_, to_date2

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def regression_model_predict(project_seq: str, trade_date: str, df_product, df_prediction, df_other, RRs,
                             model_type='linear_model', suoe=True, bayes=False, begin_default_recover=False,
                             cur=None, split_default=False):
    """

    Args:
        project_seq (str): 项目内码
        trade_date (str): 估值日
        df_product (pd.DataFrame): 项目基本要素
        df_prediction (pd.DataFrame): 项目最新现金流归集表
        df_other (pd.DataFrame): 其他信息
        RRs (pd.Series): 违约回收率序列
        model_type (str): 选择模型类别, 'linear_model' - 线性回归， 'sigmoid_model' - 逻辑回归
        suoe (bool): 缩额法（True)，缩期法（False）
        bayes (bool): 是否使用贝叶斯估计更新参数
        begin_default_recover (bool): 是否考虑当前已违约金额的违约回收
        cur (cursor):
        split_default (bool): 是否将已违约金额从现金流归集表首期期初金额中扣除

    Returns:
        tuple: tuple contains:
                * final_prediction (pd.DataFrame): 加压后现金流归集表
                * param_ (pd.DataFrame): 保存预测得到的加压参数，包括 'CPRs', 'UCPRs', 'MDRs', 'CDRs', 'DP', 'USMMs', 'SMMs' （算法见 LoanPool.scenario_analysis_regression)
                * cdr (float): 项目存续期的总累计违约率，为 `param_` 中 'CDRs' 列的最大值
                * cdr_coef (pd.DataFrame): 累计违约率回归模型的系数
                * ucpr_coef (pd.DataFrame): 无条件年化早偿率回归模型的系数


    **逻辑**

    1. 获取回归模型系数 （通过 ``get_regress_coef`` )
    2. 如果项目自身的历史数据披露超过5期，并且选择 ``bayes=True`` , 则进行贝叶斯更新。
    3. 将标的项目的最新现金流归集表调整为频率最高为月频的（因为回归模型以月为频率，如现金流归集表频率较高，比如半月频，则可能无法正确计算）
    4. 如果标的项目有几期的因子缺失，则直接采用向前向后的方式将因子进行填充。如果有因子完全缺失，则不能计算。
    5. 对现金流归集表进行加压 （通过 ``LoanPool.scenario_analysis_regression`` ) , 并最后将现金流归集表的频率恢复成跟原始的一致

    """
    # 1 读取项目相关数据
    trade_date = trade_date.strip("'")
    history_date = df_other.loc[0, 'history_date']
    initial_principal = df_product.loc[0, 'initial_principal']
    initial_date = df_product.loc[0, 'initial_date']
    begin_default = df_other.loc[0, 'cum_loss']
    begin_cdr = df_other.loc[0, 'CDR']
    second_class = df_product.loc[0, 'secondary_classification']
    if str(initial_principal) in ('None', 'nan'):
        raise ValueError('该单资产没有初始本金,不能进行回归预测')

    # 2 读取项目历史参数和回归因子
    df_cdr_factor_for_predict, df_ucpr_factor_for_predict, \
        initial_prediction = get_predict_factors(project_seq, second_class, history_date, cur)

    df_cdr_factor_for_train, df_ucpr_factor_for_train = get_train_data(trade_date, second_class,
                                                                       [project_seq.strip("'")], cur)

    # 3. 读取回归系数

    cdr_coef, ucpr_coef, std_cdr, std_ucpr = get_regress_coef(project_seq, second_class, trade_date, model_type, cur)
    
    if bayes:

        #3.1 历史数据
        if len(df_cdr_factor_for_train) > 5:
            #仅对超过五期的进行贝叶斯更新

            cdr_coef = bayesian_improvement(cdr_coef, df_cdr_factor_for_train.drop(columns='cdr'),
                                            df_cdr_factor_for_train, std_cdr, model_type)
        if len(df_ucpr_factor_for_train) > 5:
            ucpr_coef = bayesian_improvement(ucpr_coef, df_ucpr_factor_for_train.drop(columns='ucpr'),
                                             df_ucpr_factor_for_train, std_ucpr, model_type)

    df_prediction_monthly = combine_cashflow(df_prediction, initial_date)
    max_age = df_prediction_monthly['age'].max()
    min_age = df_prediction_monthly['age'].min()
    df_age = pd.DataFrame(range(min_age, max_age+1), columns=['age'])
    df_ucpr_factor_for_predict = df_ucpr_factor_for_predict.merge(df_age, on='age', how='right').ffill().bfill()
    df_cdr_factor_for_predict = df_cdr_factor_for_predict.merge(df_age, on='age', how='right').ffill().bfill()
    # 将现金流归集表和predict_x处理成一样长
    lp = LoanPool(pool_start_date=to_date2(history_date), original=df_prediction_monthly,
                  initial_principal=initial_principal, initial_date=initial_date, begin_default=begin_default,
                  pool_start_CDR=begin_cdr,
                  begin_default_recover=begin_default_recover, split_default=split_default)
    after_prediction = lp.scenario_analysis_regression(cdr_coef, ucpr_coef, df_cdr_factor_for_predict,
                                                   df_ucpr_factor_for_predict, model_type=model_type,
                                                   RRs=RRs, suoe=suoe)

    param_ = lp.params.copy()
    assumption = {}
    for x in param_.columns:
        assumption[x] = param_[x].tolist()

    # # 重新与原来的df_prediction匹配
    cdr = param_['CDRs'].max()
    assumption['CDR'] = cdr

    df_prediction.loc[:, 'age'] = df_prediction['date_'].apply(lambda x: age_(initial_date, x))
    final_prediction = tranfer_prediction_to_initial_freq(df_prediction, after_prediction)

    return final_prediction, assumption, cdr, cdr_coef, ucpr_coef


def bayesian_improvement(initial_coef, history_factor, history_params, std, model_type):
    """
    对回归模型系数进行贝叶斯更新

    Args:
        initial_coef (pd.DataFrame): 根据可比项目拟合处理的模型系数，此时并未考虑项目自身的因素
        history_factor (pd.DataFrame): 标的项目历史因子值
        history_params (pd.DataFrame): 标的项目历史参数值（CDR or UCPR)
        std (pd.DataFrame): 可比项目拟合过程中的标准差
        model_type (str): 模型类型

    Returns:
        pd.DataFrame: coef_new: 更新后的模型参数

    """
    # 对每一期进行滚动更新
    coef_new = initial_coef.copy()
    for t in range(1, len(history_factor)):
        # 对每一个参数进行更新
        for cf in list(initial_coef.columns):
            coef_single = coef_new.loc[0, cf]
            factor_t = history_factor.loc[t, :]
            sigma = 2 * abs(coef_single)
            
            def obs_pdct(theta):
                coef_new.loc[0, cf] = theta
                params1 = calculate(coef_new, factor_t, model_type)
                if 'cdr' in list(history_params.columns):
                    real_value = history_params.loc[t, 'cdr'] - history_params.loc[t-1, 'cdr']
                    params0 = calculate(coef_new, history_factor.loc[t-1, :], model_type)
                    predict_value = params1 - params0

                elif 'ucpr' in list(history_params.columns):
                    real_value = history_params.loc[t, 'ucpr']
                    predict_value = params1
                else:
                    raise ValueError("贝叶斯更新需要历史参数值，输入表格中没有历史参数值列")

                return real_value, predict_value
            #  p(θ)
            def probtheta(theta):
                prob = 1 / (sigma * pow(2 * math.pi, 0.5)) * np.exp(-((theta - coef_single) ** 2) / (2 * sigma ** 2))
                return prob
            
            # 假设早偿和违约服从对数正态分布
            # p(x|θ)
            def probx_theta(theta):
                ob, pred = obs_pdct(theta)
                px_theta_pro =1.
                px_theta = 1 / (std * pow(2 * math.pi, 0.5)) * np.exp(-((ob - pred) ** 2) / (2 * std ** 2))
                px_theta_pro = px_theta_pro * px_theta
                return px_theta_pro
            #  p(x)
            def probxx(theta):
                return probx_theta(theta) * probtheta(theta)
            
            def bounds_1(*args):
                return [coef_single - 5 * abs(coef_single), coef_single + 5 * abs(coef_single)]

            def probtheta_x(theta):
                return probx_theta(theta) * probtheta(theta) / probx[0]
            
            def theta_exp(theta):
                return theta * probtheta_x(theta)      
            
            probx = integrate.nquad(probxx, [bounds_1])
            
            coef_single_new = integrate.nquad(theta_exp, [bounds_1])[0]
            coef_new = coef_new.copy()
            coef_new.loc[0, cf] = coef_single_new                    

    return coef_new


def get_regress_coef(project_seq, second_class, trade_date, model_type, cur=None):
    """
    获取回归模型系数

    Args:
        project_seq (str): 项目内码
        second_class (str): 二级分类
        trade_date (str): 估值日期
        model_type (str): 选择模型类别, 'linear_model' - 线性回归， 'sigmoid_model' - 逻辑回归
        cur (cursor):

    Returns:
        tuple: tuple contains:
                * cdr_coef (pd.DataFrame): 累计违约率回归模型系数
                * ucpr_coef (pd.DataFrame): 无条件年化早偿率回归模型系数
                * std_cdr (float): 累计违约率回归模型标准差
                * std_ucpr (float): 无条件年化早偿率回归模型标准差

    """

    # 1. 获取可比项目
    project_seqs, if_read_success = get_comparable_project(project_seq, same_type=True, cur=cur)
    coef_dict, std = regression_model_without_bayers(trade_date, second_class, project_seqs, cur)

    std_cdr = std[model_type + '_cdr']
    std_ucpr = std[model_type + '_ucpr']
    cdr_coef = pd.DataFrame(coef_dict[model_type + '_cdr'], index=[0])
    ucpr_coef = pd.DataFrame(coef_dict[model_type + '_ucpr'], index=[0])

    return cdr_coef, ucpr_coef, std_cdr, std_ucpr


def calculate(coef, factor, model_type):
    """

    Args:
        coef (pd.DataFrame): 回归模型系数值
        factor (pd.Series): 因子值
        model_type (str): 模型类型

    Returns:
        float: 预测值

    """

    result = 0
    for i in coef.columns:
        result += float(factor[i])*coef.loc[0, i]

    if model_type =='sigmoid':
        result = 1/(1 + np.exp(-result))

    return result