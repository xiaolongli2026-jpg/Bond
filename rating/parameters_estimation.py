# -*- coding: utf-8 -*-


import pandas as pd
import numpy as np
from rating.prepare.config import rating_indicator_dict
from abs.calculate.assetmodel.extrapolation_model import (main_extrapolate_model)
from abs.prepare.data_load.model_data import (get_adjust_factor, comparable_pools_data)
from abs.prepare.data_load.model_data import get_checkdate_info
from abs.doc.enumerators import SecondClass
from abs.calculate.asset.s_param_match import match_cashflow_dp
from utils.timeutils import age_, to_date


class params_estimate:

    def __init__(self, project_seq, rating_date, period_num, initial_date, df_prediction, initial_principal,
                 colleced_amount, second_class, is_revolving, recal_cashflow, history_date,
                 rating_report_info, current_cdr, same_type, data_source='rating_report', cur=None):
        """
        用于计算评级所需的所有基准参数，最终输出值全部不带单位。

        Args:
            project_seq (str): 项目内码
            rating_date (str): 评级日期
            period_num (int): 未来归集日总次数
            initial_date (datetime.date): 初始起算日
            df_prediction (pd.DataFrame): 最新现金流归集表
            initial_principal (float): 入池本金
            colleced_amount (float): 资产池已回收的本金额/本息和金额（仅包含合计项）（具体看 ``load_data`` 的读取情况）
            second_class (str): 二级分类
            is_revolving (bool): 是否循环购买
            recal_cashflow (bool): 是否重新测算现金流
            history_date (datetime.date): 上一历史归集日
            rating_report_info (pd.DataFrame): 评级报告数据，index为指标名称，columns为项目内码
            current_cdr (float): 当前已实现累计违约率
            data_source (str): 基准参数计算方式
            cur (cursor):


        -------
        返回结果
        -------
        根据参数名称返回结果，实例化后直接根据如下名称（类属性）得到数据，其他的方法用于计算以下属性值:

            * realized_rr_npl
            * unrealized_rr_npl
            * realized_cdr
            * unrealized_cdr
            * dp
            * cdr
            * rrs
            * cprs
            * yrs
            * pps
            * rp
            * staticpools_cdr

        """
        project_seq = project_seq.strip("'").strip("\"")
        self.period_num = period_num
        self.project_seq = project_seq
        self.rating_date = rating_date
        self.initial_date = initial_date
        self.df_prediction = df_prediction
        self.initial_principal = initial_principal
        self.collected_amount = colleced_amount
        self.second_class = second_class
        self.is_revolving = is_revolving
        self.is_npls = self.second_class == SecondClass['NPLS']
        self.recal_cashflow = recal_cashflow
        self.rating_report_info = rating_report_info
        self.history_date = history_date
        self.data_source = data_source
        self._realized_cdr = current_cdr
        self.cur = cur
        self.same_type = same_type
        self.warn_lst = []

        if data_source in ('estimation', 'bayesian_estimation', 'both'):
            self.__prepare_history_params()

    def set_custom_params(self, custom_cdr, custom_dp,
                          custom_cprs, custom_cpr, custom_rr, custom_dpp, custom_rrs,
                          custom_yr, custom_yrs, custom_pp, custom_pps, custom_rp, module_type, dp_match=False,
                          param_match_method='remain'):
        """
        1. 输入自定义的基础参数， 后面的计算中会默认用这个数据，不会用计算或者评级报告得到的。
        2. 非不良的都处理成序列参数，除了累计违约率
        3. 所有数据都处理成无单位，即带百分比的数据变为小数
        """

        self._param_match_method = param_match_method
        if (not self.is_revolving) and self.is_npls:

            self.custom_rr = custom_rr / 100

        elif self.is_revolving and self.recal_cashflow:

            if module_type == 'dynamic':
                self.custom_cdr = custom_cdr / 100
                self.custom_rrs = custom_rrs / 100
                self.custom_cprs = custom_cprs / 100
                self.custom_yrs = custom_yrs / 100
                self.custom_pps = custom_pps / 100
                self.custom_rp = custom_rp / 100
            else:
                self.custom_rrs = pd.Series(custom_rr/100, index=[custom_dpp])
                self.custom_cdr = custom_cdr / 100
                self.custom_cprs = pd.Series(custom_cpr / 100, index=range(1, self.period_num)) # 默认向后补充
                self.custom_yrs = pd.Series(custom_yr / 100, index=range(1, self.period_num))
                n = int(np.ceil(100 / custom_pp))
                pps = np.full([n, ], custom_pp)
                pps[-1] = 100 - custom_pp * (n - 1)
                self.custom_pps = pd.Series(pps / 100, index=range(1, n+1))
                self.custom_rp = custom_rp / 100
        else:
            if module_type == 'dynamic':
                self.custom_cdr = custom_cdr / 100
                self.custom_dp = custom_dp / 100
                if not dp_match:
                    self.custom_dp = match_cashflow_dp(self.df_prediction, initial_date=self.initial_date,
                                                       dp=self.custom_dp, last_pool_date=self.history_date, param_match_method=param_match_method)
                self.custom_rrs = custom_rrs / 100
                self.custom_cprs = custom_cprs / 100
            else:
                self.custom_cdr = custom_cdr / 100
                self.custom_dp = self.df_prediction['current_principal_due'] / sum(self.df_prediction['current_principal_due'])
                self.custom_rrs = pd.Series(custom_rr/100, index=[custom_dpp])
                self.custom_cprs = pd.Series(custom_cpr / 100, index=range(1, self.period_num)) # 默认向后补充

    def __prepare_history_params(self):
        """
        获取可比资产池及其历史参数（CDR, CPR，RR）
        """
        valid_projects, self.static_pools_cdrs, self.staticpools_ucprs, self.staticpools_cprs, \
        self.staticpools_rrs = \
            comparable_pools_data(self.project_seq, self.rating_date, same_type=self.same_type, cur=self.cur)

    def set_cdr_rev(self):
        """
        计算循环购买的基准累计违约率:

        1. 如果 `data_source` 是 'rating_report' , 提取评级报告数据，如果没有则报错
        2. 如果 `data_source` 是 'customize' ，则为输入的自定义值
        3. 循环购买类的基准累计违约率不支持计算

        """
        if self.is_revolving and (self.data_source not in ('rating_report', 'custom')):
            raise ValueError('循环购买类的CDR只支持从报告获取或者自定义')

        if self.data_source == 'rating_report':
            try:
                self._cdr_rev = self.rating_report_info.loc[rating_indicator_dict['CDR'], self.project_seq]
            except (KeyError, IndexError) as e:
                raise KeyError("该循环购买项目的评级报告未维护累计违约率的假设")
        elif self.data_source == 'customize':
            self._cdr_rev = self.custom_cdr

    def ser_cdr_dp_nonrev(self):
        """
        获取非循环购买项目的基准违约分布和基准累计违约率:

        1. 如果 `data_source` 为 'estimation' ，则调用资产池外推法 ``main_extrapolate_model`` , 返回累计违约率和违约分布（此时返回的违约分布是与未来现金流归集表一一匹配的）
        2. 如果 `data_source` 为 'rating_report' , 表示通过评级报告的数据得到，违约分布为 ``__cal_dp`` 得到的值，累计违约率为评级报告中的 '平均违约比率/累计违约率'
        3. 如果 `data_source` 为 'both' , 则将评级报告读取的和资产池外推估算的 `dp` 和 `cdr` 取均值
        4. 如果 `data_source` 为 'bayesian_estimation' ， 此时需要使用贝叶斯估计:

                    * 先将日期设置在初始起算日，重新调用一次资产池外推，
                    * 调用 ``bayesian_justification`` 得到更新后的预计累计违约率数据。
                    * 最后，由于从初始起算日资产池外推得到的违约分布是对整个项目存续期的，因此需要调用一次 ``s_param_match.match_cashflow_dp`` 使之与未来的现金流归集表匹配，作为输出的违约分布。
                    * 如果失败，则用 `data_source` 为 'estimation' 的结果

        3. 如果 `data_source` 是 'customize' ，则为输入的自定义值, 违约分布同样处理成与现金流归集表匹配的类型

        """
        _dp_report, _dp_model, _lifetime_cdr_report, _lifetime_cdr_model = None, None, None, None
        if self.data_source in ('estimation', 'both', 'bayesian_estimation'):
            pred = self.df_prediction.copy()
            pred.loc[:, 'age'] = pred['date_'].apply(lambda x: age_(to_date(self.initial_date), to_date(x)))
            # 2. 标的项目历史数据
            valid_projects, self.staticpools_cdrs, staticpools_ucprs, self.staticpools_cprs, staticpools_rrs = \
                comparable_pools_data(self.project_seq, self.rating_date, same_type=self.same_type)

            df_factor = get_adjust_factor(valid_projects + [self.project_seq], self.second_class, self.cur)
            _dp_model, _lifetime_cdr_model, current_cdr_theory, self.extrapolate_staticpools_cdr, success_adjust,\
                current_t = main_extrapolate_model(self.project_seq, self.staticpools_cdrs, df_factor,
                                       self.realized_cdr, pred, self.initial_date, self.history_date)

        if self.data_source in ('rating_report', 'bayesian_estimation', 'both'):
            # 评级表的CDR值，
            _lifetime_cdr_report = self.rating_report_info.loc[rating_indicator_dict['CDR'], self.project_seq]
            _dp_report = self.__cal_dp()

        if self.data_source == 'rating_report':
            self._dp = _dp_report
            self._lifetime_cdr = _lifetime_cdr_report

        elif self.data_source == 'estimation':
            self._dp = _dp_model
            self._lifetime_cdr = _lifetime_cdr_model

        elif self.data_source == 'both':
            self._dp = pd.concat([_dp_report, _dp_model], axis=1).mean()
            self._lifetime_cdr = (_lifetime_cdr_model + _lifetime_cdr_report)/2

        elif self.data_source == 'bayesian_estimation':
            # 贝叶斯模式下，dp仅在初始的时候算一次
            schedule, second_class = get_checkdate_info(self.project_seq, self.cur)  # 获得核算日信息
            from datetime import datetime
            initial_date_str = datetime.strftime(self.initial_date, format='%Y%m%d')
            valid_projects, self.staticpools_cdrs_initial, staticpools_ucprs_initial, self.staticpools_cprs_initial, \
            staticpools_rrs_initial = \
                comparable_pools_data(self.project_seq, initial_date_str, same_type=self.same_type, cur=self.cur)
            df_factor_initial = get_adjust_factor(valid_projects + [self.project_seq], self.second_class, self.cur)





            _dp_model_initial, _lifetime_cdr_model_initial, ignore_1, self.extrapolate_staticpools_cdr_initial,\
            success_adjust, ignore_ = \
                main_extrapolate_model(self.project_seq, self.staticpools_cdrs_initial, df_factor_initial,
                                       current_cdr=0, df_prediction=schedule, initial_date=self.initial_date,
                                       history_date=self.initial_date) # 在起算日进行估计

            self._lifetime_cdr, baye_success, reason = self.bayesian_justification(predict_value=_lifetime_cdr_report,
                                                             lifetime_cdr_model=_lifetime_cdr_model_initial,
                                                             dp_model=_dp_model_initial,
                                                             realized_cdr=self._realized_cdr,
                                                             success_adjust=success_adjust)
            if baye_success:
                self._dp = match_cashflow_dp(df_prediction=self.df_prediction, initial_date=self.initial_date,
                                             last_pool_date=self.history_date, dp=_dp_model_initial,
                                             param_match_method='all')
            else:
                self._lifetime_cdr = _lifetime_cdr_model
                self._dp = _dp_model
                self.warn_lst.append(reason)

        elif self.data_source == 'customize':
            self._dp = self.custom_dp
            self._lifetime_cdr = self.custom_cdr

        return self._lifetime_cdr, self._dp

    def lifetime_rr_npl(self):
        """
        获取不良贷款的基准回收率（项目完整存续期间）

        1. 如果 `data_source` 为 'estimation' 或者 'bayesian_estimation' ， 表示通过估计得到，此时返回:

                其基准回收率 = （当前以回收金额 + 现金流归集表未来应收本金总和） / 入池本金额

        2. 如果 `data_source` 为 'rating_report' , 表示通过评级报告的数据得到，基准回收率 = 毛回收率 * （1 -  催收费用率）
        3. 如果 `data_source` 是 'customize' ，则为输入的自定义值
        """

        if (self.data_source == 'estimation') or (self.data_source == 'bayesian_estimation'):
            self._lifetime_rr = (self.collected_amount + self.df_prediction['current_principal_due'].sum()) / self.initial_principal
        elif self.data_source == 'rating_report': # 净回收率
            try:
                self._lifetime_rr = self.rating_report_info.loc[rating_indicator_dict['RR_MAO'], self.project_seq] * \
                                    (1 - self.rating_report_info.loc[rating_indicator_dict['CS_FEE'], self.project_seq])
            except:
                raise IndexError("评级报告没有维护该不良贷款项目的回收率")
        elif self.data_source == 'customize':
            self._lifetime_rr = self.custom_rr

    def set_rrs_rev(self):
        """
        设置循环购买类ABS的违约回收率

        1. 如果 `data_source` 是 'rating_report' , 提取评级报告数据，由于披露的是以第几年的违约回收率是多少的形式，因此返回 rrs = pd.Series([回收率（最小）], index=[12*回收时间（年）])。如果报告没有披露，则会设为 0.
        2. 如果 `data_source` 是 'customize' ，则为输入的自定义值
        3. 循环购买类的基准累计违约率不支持计算

        Notes:
            评级报告只披露一个违约回收率，不是序列型

        """
        if self.is_revolving and (self.data_source not in ('rating_report', 'custom')):
            raise ValueError('循环购买类的RR只支持从报告获取或者自定义')

        if self.data_source == 'rating_report':
            try:
                self._dpp = self.rating_report_info.loc[rating_indicator_dict['RR_YEAR'], self.project_seq] * 12
                self._rr = self.rating_report_info.loc[rating_indicator_dict['RR_MIN'], self.project_seq]
                self._rrs = pd.Series(self._rr, index=[int(self._dpp)])
            except:
                self._rrs = pd.Series(0., index=[int(self._dpp)])
        elif self.data_source == 'customize':
            self._rrs = self.custom_rrs

    def set_rrs_nonrev(self):
        """
        非循环购买的违约回收率

        1. 如果 `data_source` 为 'estimation' 或者 'bayesian_estimation' ， 表示通过估计得到，此时令违约回收率等于可比项目违约回收率的平均值 ( `staticpools_rrs` 中每个项目的最后一个值的均值)，回收时间无法计算， 因此从评级报告获取回收时间（年）作为替代
        2. 如果 `data_source` 是 'rating_report' , 提取评级报告数据，由于披露的是以第几年的违约回收率是多少的形式，因此返回 rrs = pd.Series([回收率（最小）], index=[12*回收时间（年）])。如果报告没有披露，则会设为 0.
        3. 如果 `data_source` 是 'customize' ，则为输入的自定义值

        """
        if (self.data_source == 'estimation') or (self.data_source == 'bayesian_estimation'):
            if len(self.staticpools_rrs) > 0:
                self._rr = self.staticpools_rrs.ffill().iloc[-1, :].dropna().mean()
                self._dpp = self.rating_report_info.loc[rating_indicator_dict['RR_YEAR'], self.project_seq] * 12 # todo暂时没办法估算
                self._rrs = pd.Series(self._rr, index=[int(self._dpp)])
            else:
                self._rrs = pd.Series(0, index=[1])

        elif self.data_source == 'rating_report':
            try:
                self._dpp = self.rating_report_info.loc[rating_indicator_dict['RR_YEAR'], self.project_seq] * 12
                self._rr = self.rating_report_info.loc[rating_indicator_dict['RR_MIN'], self.project_seq]
                self._rrs = pd.Series(self._rr, index=[int(self._dpp)])
            except:
                raise IndexError("评级报告缺少对该项目的违约回收率的假设")
        elif self.data_source == 'customize':
            self._rrs = self.custom_rrs

    def set_cprs(self):
        """
        早偿率的估计

        1. 如果 `data_source` 为 'estimation' 或者 'bayesian_estimation' ， 表示通过估计得到，此时令违约回收率等于可比项目违约早偿率的平均值（ `staticpools_cprs` 的平均值）
        2. 如果 `data_source` 是 'rating_report' , 提取评级报告数据中的 '提前还款率/早偿率（最大）'，如果没有就设成0
        3. 如果 `data_source` 是 'customize' ，则为输入的自定义值

        """
        if self.data_source == 'rating_report':
            try:
                self._cpr = self.rating_report_info.loc[rating_indicator_dict['CPR'], self.project_seq]
                self._cprs = pd.Series(self._cpr, index=range(1, self.period_num))
            except:
                self._cprs = pd.Series(0., index=range(1, self.period_num))
        elif (self.data_source == 'estimation') or (self.data_source == 'bayesian_estimation'):
            self._cpr = self.staticpools_cprs.mean(1).mean()
            self._cprs = pd.Series(self._cpr, index=range(1, self.period_num))
        elif self.data_source == 'customize':
            self._cprs = self.custom_cprs

    def set_yrs(self):
        """
        收益率 仅循环购买用到

        1. 如果 `data_source` 为 'estimation' 或者 'bayesian_estimation' ， 表示通过估计得到，用现金流归集表的利息收入总和/本金收入总和得到
        2. 如果 `data_source` 是 'rating_report' , 提取评级报告数据中的 '收益率' , 如果没有就设成0
        3. 如果 `data_source` 是 'customize' ，则为输入的自定义值
        """
        # try:

        if self.data_source == 'rating_report':
            try:
                self._yr = self.rating_report_info.loc[rating_indicator_dict['YR'], self.project_seq]
                self._yrs = pd.Series(self._yr, index=range(1, self.period_num))
            except:
                self._yrs = self._estimate_yrs()

        elif self.data_source == 'customize':
            self._yrs = self.custom_yrs
        elif (self.data_source == 'estimation') or (self.data_source == 'bayesian_estimation'):
            self._yrs = self._estimate_yrs()

    def _estimate_yrs(self):

        """根据评级报告估计资产池收益率"""
        if len(self.df_prediction) > 0:
            _yr = self.df_prediction.loc[:, 'current_interest_due'].sum() / \
                       self.df_prediction.loc[:, 'current_principal_due'].sum()
            _yrs = pd.Series(_yr, index=range(1, self.period_num))
        else:
            _yrs = pd.Series(0, index=range(1, self.period_num))
            self.warn_lst.append('未来现金流归集表为空，无法估计资产池收益率,设置收益率为0')

        return _yrs

    def set_pps(self):
        """
        设置循环购买的月回款比例

        1. 如果 `data_source` 为 'estimation' 或者 'bayesian_estimation' ， 表示通过估计得到，用现金流归集表各期回款金额占总回款金额比例的均值得到。
        2. 如果 `data_source` 是 'rating_report' , 提取评级报告数据中的 '月还款率'， 如果没有则报错
        3. 如果 `data_source` 是 'customize' ，则为输入的自定义值

        """
        if self.data_source == 'rating_report':
            try:
                self._pp = self.rating_report_info.loc[rating_indicator_dict['PP'], self.project_seq]
            except:
                try:
                    self._pps = self._estimate_pps()
                except ValueError as e:
                    raise ValueError(e)
                else:
                    self.warn_lst.append("评级报告缺少月还款率数据, 使用现金流归集表的回收进行估算")
            else:
                n = int(np.ceil(1 / self._pp))
                pps = np.full([n, ], self._pp)
                pps[-1] = 1 - self._pp * (n - 1)
                self._pps = pd.Series(pps, index=range(1, n + 1))
        elif (self.data_source == 'estimation') or (self.data_source == 'bayesian_estimation'):
                self._pps = self._estimate_pps()

        elif self.data_source == 'customize':
            self._pps = self.custom_pps


    def _estimate_pps(self):

        if len(self.df_prediction) > 0:
            _pp = (self.df_prediction.loc[:, 'current_principal_due'] / \
                        self.df_prediction.loc[:, 'current_principal_due'].sum()).mean()
            n = int(np.ceil(1 / _pp))
            pps = np.full([n, ], _pp)
            pps[-1] = 1 - _pp * (n - 1)
            _pps = pd.Series(pps, index=range(1, n + 1))
            return _pps

        else:
            raise ValueError('最新现金流归集表未来期次为空，无法估计摊还比例')


    def __cal_dp(self):
        """
        根据评级报告的数据计算违约分布

        1. 首先提取评级报告中对违约分布的假设条件，由于评级报告中的假设以年为单位（披露形式 '违约时间分布_第2年'），因此得到如下形式 ::

                    dp = pd.Series([40, 30, 30], index=[12, 24, 36])


        2. 将读取到的 `dp` 归一化
        3. 通过 ``match_cashflow_dp`` 将假设条件与最新的现金流归集表的各期一一匹配。其中，固定认为评级报告是从初始起算日开始假设的，因此，如果不是发行期，最后输出的违约分布序列总和会小于 1 。
        """
        dp = pd.Series(float('nan'), index=range(1, 20))
        for i in range(1, 20):
            try:
                dp[i] = self.rating_report_info.loc[rating_indicator_dict['RR_' + str(i)], self.project_seq]
            except:
                pass
        dp.dropna(axis=0, inplace=True)
        dp.index = dp.index * 12  # 评级报告的假设以年为单位
        dp = dp / dp.sum()  # 总和为1
        dp_ = match_cashflow_dp(df_prediction=self.df_prediction, initial_date=self.initial_date,
                                last_pool_date=self.history_date, dp=dp, param_match_method='all')
        return dp_

    def bayesian_justification(self, predict_value: float,
                               lifetime_cdr_model, dp_model, realized_cdr,
                               success_adjust):
        """
        对累计违约率进行贝叶斯更新

        Args:
            predict_value (float): 评级报告假设的累计违约率
            lifetime_cdr_model (float): 资产池外推在初始起算日预计的累计违约率
            dp_model (np.array): 资产池违约在初始起算日预计的违约分布
            realized_cdr (float): 当前已实现的累计违约率
            success_adjust (bool): 资产池外推中是否有根据调整因子调整过累计违约率

        Returns:
            float: kalman_cdr: 贝叶斯后的累计违约率


        **逻辑**
        1. 如果 `realize_cdr` 为0，不调整
        2. 如果需要调整，先计算 `dp_model` 中，发生在最近一次历史归集日及以前日期的违约分布总和作为 `dp_realized`,  则::

                    observe_cdr = realized_cdr / dp_realized

        3. 如果 `success_adjust` 为是，设置相关系数 0.5, 否则设置相关系数为 0.7
        4. 根据研究报告中的算法计算卡尔曼增益，另外将调整系数 alpha 的调整指数 e 设为 0.5
        5. 于是有 ::

                kalman_cdr = predict_value + alpha * kalman_gain * (observe_cdr - predict_value)

        Notes:
            返回结果中 cdr 不能超过1， 限制为1及以内
        """
        success_ = False
        reason = None
        # step0 如果没有历史期次披露违约数据，则不进行更新
        if self.realized_cdr == 0:
            reason = "缺少有效观测数据，无法进行卡尔曼滤波调整,直接使用资产池外推结果"
            return lifetime_cdr_model, success_, reason

        # 找到最新一期披露值对应的dp行
        current_t = age_(self.initial_date, self.history_date) # 最新一期披露的账龄
        # step1 根据历史观测值计算累计违约率的理论观测值
        dp_realized = 1 - sum(dp_model[current_t+1:])  # todo
        if dp_realized > 0:
            observe_cdr = realized_cdr / dp_realized
            # 寻找合适时期进行卡尔曼滤波调整----------------------------------------------
            # step2设置相关系数
            if not success_adjust:
                pho = 0.7
            else:
                pho = 0.5

            # step3生成相关系数矩阵
            n = dp_model.index[-1]
            k = np.round(dp_realized * n) # 重新分配期次
            mat_r = np.mat([[pho ** np.abs(i - j) for i in range(n)] for j in range(n)])

            # step4生成权重向量
            h1 = np.mat(np.ones([n]))
            h2 = np.mat([1 / dp_realized - 1 if j < k else -1 for j in range(n)])

            # step5计算卡尔曼增益
            kalman_gain = (h1 @ mat_r @ h1.T / (h1 @ mat_r @ h1.T + h2 @ mat_r @ h2.T))[0, 0]

            # step6计算调整系数alpha
            alpha = dp_realized ** 0.5
            kalman_gain = alpha * kalman_gain
            # step7 计算kalman cdr
            kalman_cdr = predict_value + kalman_gain * (observe_cdr - predict_value)
            kalman_cdr = max(kalman_cdr, realized_cdr)
            success_ = True
            return kalman_cdr, success_, reason
        else:
            reason = '已实现违约分布总和为0，无法进行贝叶斯调整,直接用资产池外推结果'
            return min(lifetime_cdr_model, 1), success_, reason

    @property
    def realized_rr_npl(self):
        """
        不良贷款已实现回收率 = 总本金回收额 / 入池本金额

        """
        self._realized_rr_npl = self.collected_amount / self.initial_principal
        return self._realized_rr_npl

    @property
    def unrealized_rr_npl(self):
        """
        不良贷款未实现回收率为 ``lifetime_rr_npl`` 中计算得到的基准回收率减去已实现回收率( ``realized_rr_npl`` )

        """
        try:
            getattr(self, '_lifetime_rr')
        except:
            self.lifetime_rr_npl()

        return max(0, self._lifetime_rr - self.realized_rr_npl)  # 不能小于0

    @property
    def realized_cdr(self):

        """
        表示当前已实现的累计违约率
        """
        return self._realized_cdr


    @property
    def unrealized_cdr(self):
        """
        基准累计违约率（ `cdr` ） 减去已实现累计违约率( `realized_cdr` )

        """
        if not (self.is_npls):
            try:
                getattr(self, '_unrealized_cdr')
            except:
                self._unrealized_cdr = max(0, self.cdr - self.realized_cdr)

            return self._unrealized_cdr
        else:
            return None

    @property
    def dp(self):
        """
        返回 ``ser_cdr_dp_nonrev`` 得到的违约分布序列，仅非循环且非不良的abs有

        """
        if (not self.is_revolving) and (not self.is_npls):
            try:
                getattr(self, '_dp')
                return self._dp
            except:

                self.ser_cdr_dp_nonrev()
                return self._dp
        else:
            return None

    @property
    def cdr(self):
        """
        为 ``ser_cdr_dp_nonrev`` 或 ``set_cdr_rev`` 得到的累计违约率

        """
        if self.is_npls:
            return 1
        else:
            if not self.is_revolving:
                self.ser_cdr_dp_nonrev()
                return self._lifetime_cdr
            else:
                self.set_cdr_rev()
                return self._cdr_rev

    @property
    def rrs(self):
        """
        为 ``set_rrs_nonrev`` 或 ``set_rrs_rev`` 得到的违约回收率， 不良贷款不用这个参数。

        """
        if self.is_npls:
            return None
        elif not self.is_revolving:
            self.set_rrs_nonrev()
        else:
            self.set_rrs_rev()
        return self._rrs

    @property
    def cprs(self):
        """
        返回 ``set_cprs`` 得到的早偿率
        """

        try:
            return self._cprs
        except:
            self.set_cprs()
            return self._cprs

    @property
    def yrs(self):
        """
        返回 ``set_yrs`` 得到的收益率
        """
        if not self.is_revolving:
            self._yr = None
        else:
            try:

                getattr(self, '_yrs')
            except:
                self.set_yrs()

        return self._yrs

    @property
    def pps(self):
        """
        返回 ``set_pps`` 得到的月还款率
        """
        if not self.is_revolving:
            self._pp = None
        else:
            try:
                getattr(self, '_pps')
            except:
                self.set_pps()
        return self._pps

    @property
    def rp(self):
        """
        返回循环购买率，由于缺少数据，因此，当为自定义输入数据时，返回自定义的循环购买类，否则返回1，即认为百分比用于循环购买
        """
        if not self.is_revolving:
            return None

        else:
            if self.data_source == 'customize':
                return self.custom_rp
            else:
                return 1


    @property
    def staticpools_cdr(self):
        """
        返回可比项目的历史累计违约率

        """
        try:
            getattr(self, 'staticpools_cdrs')
        except:
            valid_projects, self.staticpools_cdrs, staticpools_ucprs, self.staticpools_cprs, staticpools_rrs = \
                comparable_pools_data(self.project_seq, self.rating_date, same_type=self.same_type)
        return self.staticpools_cdrs