# -*- coding: utf-8 -*-
"""
用马尔可夫转移矩阵进行现金流加压
"""

from abs.calculate.assetmodel.markov_model_train import markov_model_train
from abs.calculate.assetmodel.loanpool_markov import LoanPoolMarkov
from abs.prepare.data_preprocess.prepcashflow_new import combine_cashflow, tranfer_prediction_to_initial_freq
from abs.prepare.data_load.model_data import get_comparable_project, get_markov_data
from utils.quick_connect import connect_mysql


def markov_model_predict(project_seq, last_pool_date, cashflow, initial_default, initial_date, same_type, suoe,
                         begin_default_recover, split_default,
                         n_markov, upload=False, cur=None, ):
    """
    马尔可夫转移模型的主函数

    Args:
        project_seq (str):  项目内码
        last_pool_date (datetime.date): 最新历史归集日
        cashflow (pd.DataFrame): 最新现金流归集表
        initial_default (float): 当前违约本金余额
        initial_date (datetime.date): 初始起算日
        same_type (bool): 用于计算转移概率的可比项目的范围，True - 同一二级项目， False - 同一发起人
        suoe (bool): 缩额还是缩期
        n_markov (bool): 对每一期训练一次转移概率（True)还是所有期的转移概率都一样（False）
        upload (bool): 写入数据到数据库( TODO 现在数据库里没有表, 不能写入）
        cur (cursor):
        begin_default_recover (bool): 是否考虑当前违约本金余额的回收
        split_default (bool): 是否将违约本金从本金余额中剔除，并单独考虑

    Returns:
        tuple: tuple contains:
                * after_cashflow (pd.DataFrame): 加压后的现金流归集表
                * after_status (pd.DataFrame): 各个状态在每期末的金额


    **逻辑**

        1. 读取标的项目的状态数据
        2. 获取状态转移概率 (调取  ``get_tranfer_prob`` ）
        3. 将频率较高现金流归集表调整为月频（因为转移状态都是基于月频计算的，调用 ``prepcashflow_new.combine_cashflow`` ）; 频率低的没关系，计算的时候可以多乘几次转移矩阵
        4. 调整读取的资产池状态，使得正常状态、逾期1-30天、逾期31-60天和逾期61-90天的资金总额，与现金流归集表首期期初本金余额、 `df_other` 中的剩余本金完全一致
        5. 将月度数据转为跟原来一样的频率，避免跟日历不匹配


    TODO:
        保存加压后的现金流归集表
    """

    str_history_date = last_pool_date.strftime("%Y%m%d")
    project_seq = str(project_seq).strip("'")

    # 1. 标的项目的状态数据
    df_status = get_markov_data(projects=project_seq, fit_date=str_history_date, cur=cur)

    if len(df_status) < 1:
        raise ValueError('缺少资产池状态数据（包括初始本金），无法用Markov Model')   # 连初始本金余额都没有

    # 2. 获取状态转移概率
    df_transfer_prob = get_tranfer_prob(project_seq=project_seq, predict_date=str_history_date, df_status=df_status,
                                        same_type=same_type, fit_n_markov=n_markov, upload=upload,
                                        cur=cur)
    # 3. 将频率较高现金流归集表调整为月频（因为转移状态都是基于月频计算的），频率低的没关系，计算的时候可以多乘几次转移矩阵
    cashflow_monthly = combine_cashflow(cashflow=cashflow, initial_date=initial_date)

    # 4. 加压计算现金流结果
    # 4.1 调整读取的资产池状态，使得与现金流归集表、df_other中的剩余本金完全一致
    last_check_age = max(df_status['age'])
    last_status = df_status.loc[df_status['age'] == last_check_age, :].reset_index(drop=True).loc[0, :]

    last_status[['t_n', 't_o_1_30', 't_o_31_60', 't_o_61_90']] = \
        last_status[['t_n', 't_o_1_30', 't_o_31_60', 't_o_61_90']] * cashflow_monthly.loc[0, 'begin_principal_balance'] / sum(
        last_status[['t_n', 't_o_1_30', 't_o_31_60', 't_o_61_90']])

    lpm = LoanPoolMarkov(project_seq=project_seq, status=last_status, transfer_prob=df_transfer_prob,
                         cashflow=cashflow_monthly, initial_default=initial_default, last_pool_date=last_pool_date,
                         initial_date=initial_date, n_markov=n_markov, suoe=suoe, split_default=split_default,
                         begin_default_recover=begin_default_recover)

    after_cashflow, after_status = lpm.predict_allperiod()

    # 5.将月度数据转为跟原来一样的频率，避免跟日历不匹配
    after_cashflow = tranfer_prediction_to_initial_freq(cashflow, after_cashflow)

    return after_cashflow, after_status, df_transfer_prob


def get_tranfer_prob(project_seq, predict_date, df_status, same_type, fit_n_markov, upload=False, cur=None):
    """
    获取转移概率

    Args:
        project_seq (str): 项目内码
        predict_date (str): 估值日期
        df_status (pd.DataFrame): 标的项目历史状态数据
        fit_n_markov (bool): 对每一期训练一次转移概率（True)还是所有期的转移概率都一样（False）
        upload (bool): 写入数据到数据库( TODO 现在数据库里没有表, 不能写入）
        cur (cursor):

    Returns:
        pd.DataFrame: df_prob: 转移概率，包括列


    **逻辑**


    1. 如果项目本身的历史状态数据足够多（大于10期），即存续的时间比较长，则用其历史数据训练出单个转移概率组
    2. 如果项目本身的历史状态数据不够多，则读取可比项目，用可比项目的历史数据拟合出转移概率组。其中当 ``fit_n_markov=True`` 时，一期一训练；否则训练出单个转移概率组


    TODO:
        保存转移概率
    """
    if cur is None:
        conn, is_mysql = connect_mysql()
        cur = conn.cursor()
        close_ = True
    else:
        close_ = False

    if len(df_status) > 10:
        df_prob = markov_model_train(project_seq, df_status, fit_n_markov, upload)
    else:
        comparable_projects, success_ = get_comparable_project(project_seq=project_seq, same_type=same_type, cur=cur)
        df_status_comparable = get_markov_data(comparable_projects, predict_date, cur)
        df_prob = markov_model_train(comparable_projects, df_status_comparable, fit_n_markov, upload)

    if len(df_prob) < 6:
        raise ValueError("拟合错误,转移概率参数数量不足")

    if close_:
        cur.close()
        conn.close()

    return df_prob
