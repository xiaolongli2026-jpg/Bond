# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import scipy.optimize as op
from itertools import product


def markov_model_train(projects, df_status, fit_n_matrix=True, upload=False):
    """
    训练转移概率

    Args:
        projects (list): 项目内码列表，可以是待估项目本身的内码，也可以可比项目的内码。对应的资产状态表需要是待估项目本身的或者是多个可比项目的
        df_status (pd.DataFrame): 资产状态表，转移概率的训练就是基于状态表中的数据
        fit_n_matrix (bool): 用于多项目拟合时方法的选择
        upload (bool): 是否将数据上传到数据库内

    Returns:
        pd.DataFrame: df_p: 转移概率，包含列 prob_type-转移概率, prob_num-转移概率值, age-存续期次（月）


    **逻辑**

    分以下情况：

    1. 如是用标的项目自身的历史数据拟合，则：

            * 将历史数据的序列转为连续月份的，比如有项目披露了第1、3、5期的状态数据，则加入第2、4期，但是这两期的数据是空值。这样做的目的是避免前后两期不连续时，却被用来计算 `cost` , 比如第1期乘上转移矩阵后得到的是第2期，由于没有第二期，在计算 'cost' 时减去了第3期的值。此时会影响拟合结果。
            * 拟合得到能令 ``nperiod_cost_func`` 最小的值

    2. 如果是用可比项目的历史数据拟合，对于只拟合一组转移概率的，与 `1` 类似，但是每个项目的预测值需要与自身的值一致，因此将历史数据重置为三维的数据，任何拟合令 ``nperiod_nproject_cost_func`` 最小的一组转移概率
    3. 如果是用可比项目的历史数据拟合，且拟合多组概率的，则每期都选择这一期（月）和下一期（月）的状态数据（前后期有缺失的进行剔除），然后拟合令 ``oneperiod_cost_func`` 最小的一组转移概率值作为这一期的转移概率。如果因为数据缺失拟合不出来，则跳过，在取用时会用上一期的值。

    """

    # 1. 数据
    if len(df_status) < 1:
        raise ValueError("没有足够数据用于拟合转移矩阵")
    # 1)如果期数够，或者list里面的project_seq直接用这个项目的历史数据拟合一个转移概率矩阵，2)如果不够，用其他项目的历史数据拟合多个转移概率矩阵，作为未来每一期的转移情况
    if isinstance(projects, str):
        projects = [projects.strip("'")]

    cols_ = ['t_n', 't_o_1_30', 't_o_31_60', 't_o_61_90', 't_d', 'a_p', 'a_o', 'a_d']
    # 2. 拟合
    # =============================================================================
    #         对于多个项目的情况，1、采用逐期计算的形式算出多个转移概率矩阵，预测时不同账龄期选择对应的矩阵 2、为所有产品计算一个统一的转移概率矩阵
    #         两种方式优缺点：1优点，能够考虑到违约金额增速衰减的过程，缺点，对于期限过长的产品，某一期后将没有可用的转移概率矩阵，将会跟2变成一样的方法； 2优点，简单，
    #         共同缺陷，无法考虑待预测产品特性，因转移概率之间的相关性，擅自修改某一概率会导致其他概率偏离，形成局部最优的同时牺牲整体最优
    # =============================================================================
    df_p = pd.DataFrame(columns=['prob_type', 'prob_num', 'age'])
    if len(projects) > 1:
        p_initial = df_p.copy()
        if fit_n_matrix: # 2.1 每期拟合一次
            s_train = df_status.loc[:, cols_ + ['age', 'project_seq']].copy()  # 数据既是X 也是Y
            age_max = int(max(df_status['age']))

            first_period = True
            for the_age in range(0, age_max + 1):
                s_train_next = s_train.loc[s_train['age'] == the_age, :].copy()
                if len(s_train_next) < 1:
                    continue
                else:  # 找到存在上一期数据的
                    if first_period:
                        first_period = False
                    else:
                        interval_ = the_age - last_age
                        # 2.1.1 只拟合前后两期都有数据的项目
                        common = list(set(s_train_last['project_seq']) & set(s_train_next['project_seq']))  #
                        x_train_last = s_train_last.loc[s_train_last['project_seq'].isin(common), cols_].values
                        x_train_next = s_train_next.loc[s_train_next['project_seq'].isin(common), cols_].values

                        if len(x_train_last) < 1 or len(x_train_last) < 1:
                            pass
                        else:
                            # 如果出不来结果就用上一次的结果替代
                            p0 = np.array([0.05, 0.02, 0.5, 0.8, 0.8, 0.8])
                            bs = ((0, 1), (0, 1), (0, 1), (0, 1), (0, 1), (0, 1))
                            try:
                                res = op.minimize(oneperiod_cost_func(x_train_last, x_train_next, interval_), p0, bounds=bs)
                                p_ = res.x
                                p_temp = p_initial.copy()
                                p_temp['prob_type'] = range(0, 6)  # 给每个转移概率的枚举值
                                p_temp['prob_num'] = p_
                                p_temp['age'] = the_age
                                df_p = df_p.append(p_temp)
                            except Exception:
                                pass  # 用上一个账龄的结果

                    s_train_last = s_train_next.copy()
                    last_age = the_age

        else:  # 2.2 只拟合一次
            # 如果是只用输出一个，即所有产品共用一个转移概率矩阵 （PS 此时样本集应尽可能包含完整的生命周期, 理论上应都使用完整周期静态池）

            age_max = int(max(df_status['age']))
            ages = list(range(0, age_max+1))
            projects = list(set(df_status['project_seq']))
            df_full = pd.DataFrame(list(product(projects, ages)), columns=['project_seq', 'age'])
            df_s_train = pd.merge(df_status.loc[:, cols_ + ['project_seq', 'age']], df_full, how='outer', on=['project_seq', 'age'])
            df_s_train.sort_values(by=['project_seq', 'age'], inplace=True, ignore_index=True)
            df_s_train.set_index(['project_seq', 'age'], inplace=True)
            depth = len(projects)
            length = age_max + 1
            width = len(cols_)
            x_train_2D = df_s_train.values
            x_train_3D = x_train_2D.reshape((depth, length, width))
            p0 = np.array([0.05, 0.02, 0.5, 0.8, 0.8, 0.8])
            bs = ((0, 1), (0, 1), (0, 1), (0, 1), (0, 1), (0, 1))

            res = op.minimize(nperiod_nproject_cost_func(x_train_3D[:, :-1, :], x_train_3D[:, 1:, :]), p0, bounds=bs)
            p_ = res.x

            df_p['prob_type'] = range(0, 6)
            df_p['prob_num'] = p_

    else:
        df_s_train = df_status.loc[:, cols_ + ['age']]  # 数据既是X 也是Y
        age_max = df_s_train['age'].max()
        df_s_train = df_s_train.merge(pd.DataFrame({'age': range(0, age_max+1)}, index=range(0, age_max+1)), how='outer', on='age')
        df_s_train.sort_values(by=['age'], inplace=True)
        x_train = df_s_train[cols_].values
        p0 = np.array([0.05, 0.02, 0.5, 0.8, 0.8, 0.8])
        bs = ((0, 1), (0, 1), (0, 1), (0, 1), (0, 1), (0, 1))
        res = op.minimize(nperiod_cost_func(x_train[:-1, :], x_train[1:, :]), p0, bounds=bs)
        p_ = res.x
        df_p['prob_type'] = range(0, 6)
        df_p['prob_num'] = p_
    return df_p


# model 1 ：以某一单的历史数据计算出单个转移概率矩阵，将输入的转移概率转换成矩阵形式后计算
def nperiod_cost_func(x_train, y_train):
    """
    用于以标的项目自身历史状态数据为基础，拟合单个转移概率矩阵

    Args:
        x_train: 项目历史状态值，要求月份是连续的.即使读到的不是连续的也拓展成连续的，某些期次会是空值，但是对应的期次不会用在 `cost` 的计算中
        y_train: 实际上相当于比 `x_train` 滞后一期的状态值

    Returns:
        function: `cost = np.nansum(np.nansum((y_predict - y_train) ** 2))` , 即忽略状态中的空值，对非空值进行平方加总后得到 cost，cost最小化就是拟合转移概率的基础
    """

    def v(p):

        promatrix = [[1 - p[0] - p[1], p[0], 0, 0, 0, p[1], 0, 0],
                     [0, 0, p[2], 0, 0, 0, 1 - p[2], 0],
                     [0, 0, 0, p[3], 0, 0, 1 - p[3], 0],
                     [0, 0, 0, 0, p[4], 0, 1 - p[4], 0],
                     [0, 0, 0, 0, p[5], 0, 0, 1 - p[5]],
                     [0, 0, 0, 0, 0, 1, 0, 0],
                     [0, 0, 0, 0, 0, 0, 1, 0],
                     [0, 0, 0, 0, 0, 0, 0, 1]
                     ]
        trans_matrix = np.array(promatrix)
        y_predict = x_train @ trans_matrix
        cost = np.nansum(np.nansum((y_predict - y_train) ** 2))
        return cost

    return v


def oneperiod_cost_func(s_last, s_next, lag_interval):
    """
    用于以可比项目历史状态数据为基础，拟合一期拟合一组转移概率

    Args:
        s_last (np.array): 某一期的状态
        s_next (np.array): 对应的下一期的状态

    Returns:
        function: `cost = sum((s_next - s_predict)**2)` , 由于一期一算的在拟合之前就对前后期数据缺失的情况进行了剔除，故不需要在计算 `cost` 的时候提取非空值
    """

    def v(p):
        promatrix = [[1 - p[0] - p[1], p[0], 0, 0, 0, p[1], 0, 0],
                     [0, 0, p[2], 0, 0, 0, 1 - p[2], 0],
                     [0, 0, 0, p[3], 0, 0, 1 - p[3], 0],
                     [0, 0, 0, 0, p[4], 0, 1 - p[4], 0],
                     [0, 0, 0, 0, p[5], 0, 0, 1 - p[5]],
                     [0, 0, 0, 0, 0, 1, 0, 0],
                     [0, 0, 0, 0, 0, 0, 1, 0],
                     [0, 0, 0, 0, 0, 0, 0, 1]]
        trans_matrix = np.array(promatrix)
        if lag_interval > 1:
            s_predict = s_last @ np.linalg.matrix_power(trans_matrix, lag_interval)
        else:
            s_predict = s_last @ trans_matrix

        error = s_next - s_predict
        cost = sum(sum(error ** 2))  # 用均方误差

        return cost

    return v


def nperiod_nproject_cost_func(x_train, y_train):

    """
    用于以可比项目历史状态数据为基础，拟合单个转移概率矩阵

    Args:
        x_train (np.array): 项目历史状态值，要求月份是连续的，即使读到的不是连续的也拓展成连续的，某些期次会是空值，但是对应的期次不会用在 `cost` 的计算中
        y_train (np.array): 实际上相当于比 `x_train` 滞后一期的状态值

    Returns:
        function: `cost = np.nansum(np.nansum((y_predict - y_train) ** 2))` , 即忽略状态中的空值，对非空值进行平方加总后得到 `cost` ， `cost` 最小化就是拟合转移概率的基础
    """

    def v(p):
        promatrix = [[1 - p[0] - p[1], p[0], 0, 0, 0, p[1], 0, 0],
                     [0, 0, p[2], 0, 0, 0, 1 - p[2], 0],
                     [0, 0, 0, p[3], 0, 0, 1 - p[3], 0],
                     [0, 0, 0, 0, p[4], 0, 1 - p[4], 0],
                     [0, 0, 0, 0, p[5], 0, 0, 1 - p[5]],
                     [0, 0, 0, 0, 0, 1, 0, 0],
                     [0, 0, 0, 0, 0, 0, 1, 0],
                     [0, 0, 0, 0, 0, 0, 0, 1]
                     ]
        trans_prob = np.array(promatrix)

        y_predict = x_train.dot(trans_prob)
        cost = np.nansum(np.nansum((y_train - y_predict) ** 2))

        return cost

    return v

