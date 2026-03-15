# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

from sklearn import linear_model
from scipy.optimize import minimize
from abs.prepare.data_load.model_data import get_train_data


def regression_model_without_bayers(fit_date: str, second_class, project_seqs: list, cur=None):
    """
    拟合回归模型系数

    Args:
        fit_date (str): 拟合日期
        second_class (str): 二级分类
        project_seqs (list): 拟合样本项目内码
        cur (cursor):

    Returns:
        tuple: tuple contains:
            * coef_dict (dict): 各个回归模型的系数，key-回归模型名称，value-系数值(dict, key-因子名称, value-因子对应的系数值)
            * std (dict): 各个回归模型的标准差，key-回归模型名称，value-标准差（预测y与实际y的标准差）

    Notes:
        * 如果存在因子值缺失或者对应期的累计违约率数据的缺失，则直接剔除掉行。因为累计违约率和无条件年化早偿率的回归中都有 'age' 这一项，即期次因素已经在回归因子中体现，故即使缺失几期，也不会影响回归。
        * 但是如果出现某个因子完全缺失，则无法继续拟合
    """

    # 1.训练集样本的因子、历史y数据提取
    train_cdr, train_ucpr = get_train_data(fit_date, second_class, project_seqs, cur)
    #  如果有个因子全是空， 无法算下去
    cdr_factor_check = pd.isna(train_cdr).all(axis=0)
    ucpr_factor_check = pd.isna(train_ucpr).all(axis=0)
    if cdr_factor_check.any() or ucpr_factor_check.any():
        raise ValueError(f"因子值缺失{list(cdr_factor_check[cdr_factor_check].index)},{list(ucpr_factor_check[ucpr_factor_check].index)}")

    train_cdr.dropna(how='any', axis=0, inplace=True)
    train_ucpr.dropna(how='any', axis=0, inplace=True)
    #2.回归
    #2.1 线性回归
    y_cdr = train_cdr['cdr'].values
    x_cdr = train_cdr.drop(columns='cdr')

    y_ucpr = train_ucpr['ucpr'].values
    x_ucpr = train_ucpr.drop(columns='ucpr')

    default_linear = linear_model.LinearRegression(fit_intercept=True)
    default_linear.fit(x_cdr, y_cdr)
    default_linear_intercept, default_linear_coef = default_linear.intercept_, default_linear.coef_
    default_linear_coef_dict =\
        dict(zip(['intercept'] + list(x_cdr.columns), [default_linear_intercept] + list(default_linear_coef)))

    prepay_linear = linear_model.LinearRegression(fit_intercept=True)
    prepay_linear.fit(x_ucpr, y_ucpr)
    prepay_linear_intercept, prepay_linear_coef = prepay_linear.intercept_, prepay_linear.coef_
    prepay_linear_coef_dict = \
        dict(zip(['intercept'] + list(x_ucpr.columns), [prepay_linear_intercept] + list(prepay_linear_coef)))

    #2.2 sigmoid回归
    default_coef_start = np.array(list(default_linear_coef_dict.values()))
    default_sigmoid = minimize(sigmoid_loss(x_cdr, y_cdr), default_coef_start, method='Nelder-Mead')
    default_sigmoid_coef = default_sigmoid.x
    default_sigmoid_coef_dict = dict(zip(['intercept']+list(x_cdr.columns), default_sigmoid_coef))

    prepay_coef_start = np.array(list(prepay_linear_coef_dict.values()))
    prepay_sigmoid = minimize(sigmoid_loss(x_ucpr, y_ucpr), prepay_coef_start, method='Nelder-Mead')
    prepay_sigmoid_coef = prepay_sigmoid.x
    prepay_sigmoid_coef_dict = dict(zip(['intercept']+list(x_ucpr.columns), prepay_sigmoid_coef))

    #3.计算方差，用于后面贝叶斯的计算
    y_cdr_linear_predict = default_linear.predict(x_cdr)
    y_ucpr_linear_predict = prepay_linear.predict(x_ucpr)
    y_cdr_sigmoid_predict = sigmoid_predict(default_sigmoid_coef, x_cdr)
    y_ucpr_sigmoid_predict = sigmoid_predict(prepay_sigmoid_coef, x_ucpr)

    std = {}
    std['linear_model_cdr'] = (y_cdr_linear_predict - y_cdr).std()
    std['linear_model_ucpr'] = (y_ucpr_linear_predict - y_ucpr).std()
    std['sigmoid_model_cdr'] = (y_cdr_sigmoid_predict - y_cdr).std()
    std['sigmoid_model_ucpr'] = (y_ucpr_sigmoid_predict - y_ucpr).std()

    coef_dict = {'linear_model_cdr': default_linear_coef_dict, 'linear_model_ucpr': prepay_linear_coef_dict,
                 'sigmoid_model_cdr': default_sigmoid_coef_dict, 'sigmoid_model_ucpr': prepay_sigmoid_coef_dict}
    return coef_dict, std


def sigmoid_predict(beta, x_s):
    """根据因子和系数计算sigmoid模型预测的y"""
    x = beta[0] + np.dot(x_s, beta[1:])
    if x[0] >= 0:
        y = 1 / (1 + np.exp(-x))
    else:
        y = np.exp(x) / (1+np.exp(x))
    return y


def sigmoid_loss(x_train, y_train):
    """sigmoid模型损失函数，采用平方项作为损失函数"""
    fun = lambda beta: ((y_train - sigmoid_predict(beta, x_train))**2).sum()
    return fun

