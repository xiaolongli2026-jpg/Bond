# -*- coding: utf-8 -*-

import re
import pandas as pd
import numpy as np
from abs.calculate.assetmodel.regression_factor_collect import factor_choose
from abs.prepare.sqltemplate.sqltemplate_model import *
from utils.timeutils import age_, to_date2
from abs.prepare.data_load.load_yield_curve import getCurveData, curve_compute_values, ytm_to_forward
from utils.sql_util import sql_read
from abs.doc.global_var import global_var as glv


class RegressionFactorBuild(object):

    def __init__(self, project_seqs, second_class, fit_date: str, cur):
        """
        用于构建回归模型所需的因子值

        Args:
            project_seqs (list): 需要构建因子的项目内吗
            second_class (str): 二级分类
            fit_date (str): 估值日期
            cur (cursor):

        Notes:
            得到的结果不仅包含实际历史数据，也包含预测未来值。在拟合和预测的过程中分别截取历史值和未来预测值。

        """
        self.is_mysql = glv().get('is_mysql')

        self.project_seqs = project_seqs
        self.second_class = second_class
        self.fit_date = fit_date
        self.ucpr_model_factors, self.cdr_model_factors, self.duration_feature_dict, self.initial_feature_dict \
            = factor_choose(second_class)

        self.duration_feature_in_db = list(self.duration_feature_dict.keys())
        self.initial_feature_in_db = list(self.initial_feature_dict.keys())

        self.model_factors = set(self.ucpr_model_factors + self.cdr_model_factors + ['age'])
        self.cur = cur

        self.cdr_regression_factor = pd.DataFrame(columns=set(['project_seq', 'date_', 'age'] + self.cdr_model_factors))
        self.ucpr_regression_factor = pd.DataFrame(columns=set(['project_seq', 'date_', 'age'] + self.ucpr_model_factors))

    def all_factors(self):
        """

        Returns:
            tuple: tuple contains:
                    cdr_regression_factor (pd.DataFrame): 累计违约率回归模型因子值 （必须有project_seq, date_, age三列）
                    ucpr_regression_factor (pd.DataFrame): 无条件年化早偿率回归模型因子值 （必须有project_seq, date_, age三列）


        **逻辑**

        1. 分别读取需要的资产池存续特征、初始现金流归集表，并各自计算一列 'age' （即日期与初始起算日之间的月数，四舍五入）。如果有 'age' 重复，对存续特征保留最后一个值，对现金流归集则将当期回收加总
        2. 将资产池存续特征和初始现金流归集表根据项目内码和 'age' 进行合并，但是存续期特征需要延后一期，因为特征参数都是用上一期的期末值作为下一期的因子预测值，而现金流归集表本身就是当期预测值不需要延后一期。
        3. 对于入池特征，同样合并在一起，以形成完整的面板数据。因为入池特征只在发行期披露一次，因此将入池特征以项目内码跟资产池存续特征、初始现金流归集表合并在一起，即每个 'age' 上同一个项目的入池特征都是不变的。
        4. 最后，计算各项交叉项后，根据 `cdr_model_factors` 和 `ucpr_model_factors` 规定的所需因子，分别构建表格 `cdr_regression_factor` 和 `ucpr_model_factors` , 各自保存项目内码、日期、 'age' 和所需因子值。 每一行就是一组可以带入回归模型的值。
        5. 对于期限上的因子缺失，则直接向前向后填充。

        """
        # 1. 数据读取
        df_factor_duration = self.read_duration_feature()
        df_factor_initial = self.read_initial_feature()
        df_predictions = self.read_cashflow()
        self.initial_prediction = df_predictions

        sql_initial_date = get_initial_date(self.project_seqs)  # 初始起算日
        self.cur.execute(sql_initial_date)
        initial_date_dict = dict(self.cur.fetchall())
        if len(df_predictions) < 1:
            raise ValueError("缺少初始现金流归集表，无法使用线性回归模型")

        df_factor = df_factor_duration.merge(df_factor_initial, on=['project_seq'], how='left')

        if not df_factor.empty:
            df_factor.loc[:, 'age'] = df_factor[['project_seq', 'date_']].apply(
                lambda x: age_(initial_date_dict[x['project_seq']], x['date_']), axis=1)
        else:
            df_factor.loc[:, 'age'] = float('nan')

        df_factor = df_factor.dropna(subset=['date_'], how='any')
        df_factor.drop_duplicates(['project_seq', 'age'], keep='last', inplace=True)
        df_predictions.loc[:, 'age'] = df_predictions[['project_seq', 'date_']].apply(lambda x:
                                                    age_(initial_date_dict[x['project_seq']], x['date_']), axis=1)
        # 现金里归集表处理成月度及以上的， 避免账龄重复
        df_predictions_new = \
            df_predictions.groupby(['project_seq', 'age'])[['current_principal_due', 'current_interest_due']].sum()
        df_predictions_new.loc[:, 'begin_principal_balance'] = \
            df_predictions.groupby(['project_seq', 'age'])[['begin_principal_balance']].max()
        df_predictions_new.loc[:, 'end_principal_balance'] = \
            df_predictions.groupby(['project_seq', 'age'])[['end_principal_balance']].min()
        df_predictions_new.loc[:, 'date_'] = df_predictions.groupby(['project_seq', 'age'])[['date_']].max()
        df_factor.drop(columns=['date_'], inplace=True)
        df_factor = df_predictions_new.merge(df_factor, how='outer', on=['project_seq', 'age'])
        df_factor.sort_values(by='age', ignore_index=True, inplace=True)
        cols_ = list(self.duration_feature_dict.values()) + list(self.initial_feature_dict.values())
        df_factor.loc[:, cols_] = df_factor.groupby('project_seq')[cols_].shift(1)  #  因为特征参数都是用历史值预测下一期，所以往后延一期，方便匹配因子和预测期次
        df_factor.loc[:, cols_] = df_factor.groupby('project_seq')[cols_].ffill().bfill()
        df_factor = df_factor.dropna(subset=['date_'], how='any')
        df_factor.drop(index=df_factor.loc[df_factor['age'] < 0, :].index, inplace=True) # 出现了账龄小于0，可能是初始起算日维护的是错误的

        self.factors = {}
        for name in list(df_factor.columns):
            self.factors[name] = df_factor.loc[:, name].values

        # 计算不涉及因子交叉计算的因子
        defer_factor = []
        for factor_name in self.model_factors:
            if factor_name not in self.factors.keys():
                ft_lst = re.split(r"[*^/]", factor_name)  # 交叉项或者平方项, 不能是+-/, 在回归里面没有意义
                if len(ft_lst) > 1:
                    defer_factor += [factor_name]
                else:
                    self.factors[factor_name] = getattr(self, factor_name, None)

        # 交叉项的计算
        for name_ in defer_factor:
            factor_value = self.compound_factor(name_)
            self.factors[name_] = factor_value

        # 保存所有因子结果

        cdr_column = self.cdr_regression_factor.columns
        for factor_name in cdr_column:
            self.cdr_regression_factor.loc[:, factor_name] = self.factors[factor_name]

        ucpr_column = self.ucpr_regression_factor.columns
        for factor_name in ucpr_column:
            self.ucpr_regression_factor.loc[:, factor_name] = self.factors[factor_name]

        self.cdr_regression_factor.loc[:, self.cdr_model_factors] = \
            self.cdr_regression_factor.groupby('project_seq')[self.cdr_model_factors].ffill().bfill()

        self.ucpr_regression_factor.loc[:, self.ucpr_model_factors] = \
            self.ucpr_regression_factor.groupby('project_seq')[self.ucpr_model_factors].ffill().bfill()

        return self.cdr_regression_factor, self.ucpr_regression_factor

    def read_duration_feature(self):
        """从资产池特征_存续表格读取历史数据


        **逻辑**

        1. 先根据项目内码、估值日期和所需数据的枚举值读取存续期资产池特征数据::

            get_duration_feature_template(self.project_seqs, self.duration_feature_in_db, self.fit_date)


        2. 映射枚举值为因子名称后，展开为以项目内码和日期为行标签，因子名称为列表签的数据。如果没有数据，则在这一步会报错。
           因为实际在读取的时候会尝试从入池特征中也读取所需的指标，故这里的数据缺失指的是发行期和存续期都没有有效的值，
           无法通过向前填充得到的能用于回归的因子，因此会报错。如果仅是存续期未披露，则会通过前值填充，即用于回归的因子都是初始入池特征，
           并不会导致程序报错。

        """
        sql_ = get_duration_feature_template(self.project_seqs, self.duration_feature_in_db, self.fit_date)
        df_ = sql_read(sql_, self.cur, self.is_mysql, single_return=False)
        df_.loc[:, 'indicator_name'] = df_['indicator_name'].apply(lambda x: self.duration_feature_dict[x])
        if len(df_) > 0:
            df_ = pd.pivot_table(df_, index=['project_seq', 'date_'], columns=['indicator_name'], values=['indicator_value'])
        else:
            raise IndexError("无法得到任何有效的因子值")
        reset_columns = [x[1] for x in df_.columns]
        df_.columns = reset_columns
        df_factor_duration = df_.reset_index(drop=False)
        # 也可能有因子缺数据，需要补上对应的列（数据是空)
        lack_cols = [x for x in self.duration_feature_dict.values() if x not in df_factor_duration.columns]
        df_factor_duration.loc[:, lack_cols] = float('nan')
        df_factor_duration.loc[:, 'date_'] = df_factor_duration['date_'].apply(to_date2)
        return df_factor_duration

    def read_initial_feature(self):
        """
        从资产池特征_入池表格读取发行期特征因子数据


        **逻辑**

        1. 先根据项目内码、估值日期和所需数据的枚举值读取存续期资产池特征数据::

            get_initial_feature_template(self.project_seqs, self.initial_feature_in_db)

        2. 映射枚举值为因子名称后，展开为以项目内码为行标签，因子名称为列表签的数据。如果没有数据，则在这一步会报错。因为入池特征只在发行期披露一次，如果没有维护，没有其他途径补充数据，故会直接报错。

        """
        sql_ = get_initial_feature_template(self.project_seqs, self.initial_feature_in_db)
        df_ = sql_read(sql_, self.cur, self.is_mysql, single_return=False)
        df_.loc[:, 'indicator_name'] = df_['indicator_name'].apply(lambda x: self.initial_feature_dict[x])
        if len(df_) > 0:
            df_ = pd.pivot_table(df_, index=['project_seq'], columns=['indicator_name'], values=['indicator_value'])
        else:
            raise IndexError("缺少入池资产池特征因子")
        reset_columns = [x[1] for x in df_.columns]
        df_.columns = reset_columns
        df_factor_initial = df_.reset_index(drop=False)
        # 也可能有因子缺数据，需要补上对应的列（数据是空)
        lack_cols = [x for x in self.initial_feature_dict.values() if x not in df_factor_initial.columns]
        df_factor_initial.loc[:, lack_cols] = float('nan')
        return df_factor_initial

    def read_cashflow(self):
        """
        读取项目内码列表中所有项目对应的发行期现金流归集表，用于后续因子的计算

        Returns:
            pd.DataFrame: prediction_of_projects: 包括列 project_seq, date_, current_principal_due, current_interest_due, begin_principal_balance, end_principal_balance

        """
        sql_prediction = batch_prediction_issue_template(self.project_seqs)
        prediction_of_projects = sql_read(sql_prediction, self.cur, self.is_mysql, single_return=False)
        prediction_of_projects.loc[:, ['begin_principal_balance', 'end_principal_balance']] = \
            prediction_of_projects[['begin_principal_balance', 'end_principal_balance']].fillna(method='ffill')
        prediction_of_projects.loc[:, ['current_principal_due', 'current_interest_due']] = \
            prediction_of_projects[['current_principal_due', 'current_interest_due']].fillna(0.)
        prediction_of_projects['date_'] = prediction_of_projects['date_'].apply(to_date2)
        return prediction_of_projects

    def evaluate_attr(self, attr_):
        """
        用于提取某个因子名称对应的因子值序列, 或者将数字型字符串转为 `float` 。服务于交叉项、平方项因子的计算。

        Args:
            attr_ (str, float):

        Returns:
            (np.array, float): 如果是提取因子值则返回 `np.array`, 如果是将数字型的字符串转为 `float`，则会返回 `float`
        """
        if isinstance(attr_, (int, float)):
            return float(attr_)
        else:
            if attr_.isdigit():
                return float(attr_)
            else:
                try:
                    result = self.factors.get(attr_)
                except:
                    raise AttributeError(f"缺少因子{attr_}")
                else:
                    return result

    def compound_factor(self, name_):
        """
        计算一些类似于 'age^2' , 'factor1*factor2' 的交叉项，前提是对应的子因子已经保存在了 `self.factors` 里面。 不适用于 +,-, 因为在回归模型中由加减符号构成的因子实质上等同于单个因子。

        Args:
            name_ (str): 因子名称 / 因子表达式

        Returns:
            np.array: 复合因子值
        """
        priority = {'*': 1, '/': 1, '^': 2}

        n = len(name_)
        attr_ = ''
        attr_stack = []
        sign_stack = []
        for i in range(0, n):
            c = name_[i]
            if i == n-1:
                attr_ = attr_ + c
                attr_stack.append(self.evaluate_attr(attr_))
                break

            if c in priority:
                attr_stack.append(self.evaluate_attr(attr_))
                attr_ = ''
                if len(sign_stack) > 0:
                    if priority[c] <= sign_stack[-1]: # 计算
                        attr_stack, sign_stack = self.calc(attr_stack, sign_stack)
                sign_stack.append(c)
            else:
                attr_ = attr_ + c

        while len(sign_stack) > 0:
            attr_stack, sign_stack = self.calc(attr_stack, sign_stack)

        return attr_stack[0]

    def calc(self, number_stack, sign_stack):
        """
        服务于 ``compound_factor``

        """
        attr1 = number_stack[-2]
        attr2 = number_stack[-1]
        math_sign = sign_stack[-1]
        if math_sign == '^':
            new_attr = attr1 ** attr2 / 100  # 避免量级过大
        elif math_sign == '*':
            new_attr = attr1 * attr2
        elif math_sign == '/':
            new_attr = attr1 / attr2
        else:
            raise ValueError(f'因子名称里{sign_stack[-1]}不能识别')
        number_stack.pop()
        sign_stack.pop()
        number_stack[-1] = new_attr
        return number_stack, sign_stack


    @property
    def popb(self):
        """期初未偿本金比例 `popb` =存续期维护的各期期初本金余额/入池本金额

        Notes:
            由于该指标在预测过程中会根据现金流归集表一期一算，因此在因子构建中将历史数据算准就行，预测期可以保留为空值
        """
        try:
            period_begin_real_balance = self.factors['period_begin_real_balance']
            initial_principal = self.factors['initial_principal']
        except:
            raise ValueError('缺少期初本金和历史资产池存续无法计算因子popb')
        else:
            popb = period_begin_real_balance / initial_principal
        return popb


    @property
    def previous(self):
        """
        前期早偿情况 `PPC` , 为期初资产池剩余本金与发行说明书中不考虑早偿与违约的原始现金流归集表中的期初资产池剩余本金的比值。 同样的，该指标在预测过程中会根据现金流归集表一期一算。

        """
        try:
            period_begin_real_balance = self.factors.get('period_begin_real_balance')
            begin_principal_balance = self.factors.get('begin_principal_balance')
            previous = np.full((len(period_begin_real_balance), ), float('nan'))
            valid_ones = begin_principal_balance > 0.01
            previous[valid_ones] = period_begin_real_balance[valid_ones] / begin_principal_balance[valid_ones]
            return previous
        except:
            raise ValueError("缺少历史资产池存续和现金流归集表数据，无法计算因子previous")

    @property
    def spread(self):
        """
        合约利率与再融资利率之间的差额 `Spread` = 该期期初资产池的加权平均利率 `WAIR` 与归集日的一年期国债利率（为远期值，计算方法见 ``load_yield_curve.ytm_to_forward`` ）的差值

        """
        try:
            curve_params = getCurveData(self.fit_date, 'cc_ll_gz', self.cur)
            ytm_curve = curve_compute_values(curve_params)
            FR_curve = ytm_to_forward(ytm_curve)
            FR_curve.index = np.around(FR_curve.index, decimals=2)
            max_year = max(FR_curve.index)
            last_yield = FR_curve[FR_curve.index[-1]]
            ages_ = self.factors.get('age')
            ages_ = np.around(ages_, decimals=2)
            yields = np.array([FR_curve[age] if age < max_year else last_yield for age in ages_])
            wair = self.factors['wair']
            spread = wair - yields
            return spread
        except:
            raise ValueError("无法计算因子spread")
