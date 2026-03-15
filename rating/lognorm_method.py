# -*- coding: utf-8 -*-


import numpy as np
import scipy.stats as st
from .prepare.config import rating_indicator_dict
from operator import itemgetter
from .prepare.utils import sort_grades
from collections import defaultdict


class lognorm_distribution:

    def __init__(self, project_seq, second_class, info, realized_cdr=0, param_source='rating_report',
                 comparable_project_cdrs=None, scenario_mu=None, scenario_sigma=None):
        """
        对数正态法评级，不适用于不良贷款

        Args:
            project_seq (str): 项目内码
            second_class (str):  项目二级分类
            info (pd.DataFrame): 评级报告数据，index为指标名称，columns为项目内码
            realized_cdr (float): 已实现累计违约率, %
            param_source (str): 对数正态分布参数获取方式,calculation-根据可比项目均值获取, rating_report-直接从评级报告获得, scenario-自定义输入
            comparable_project_cdrs (pd.DataFrame): 可比项目累计违约率数据
            scenario_mu (float): 当为自定义输入参数时，需要输入的期望值
            scenario_sigma (float): 当为自定义输入参数时，需要输入的波动率
        """
        assert param_source in ('calculation', 'rating_report', 'scenario'), f"项目{project_seq}输入错误的对数正态分布参数获取方式"
        self.project_seq = project_seq.strip("'")
        self.realized_cdr = realized_cdr
        self.second_class = second_class
        if param_source == 'scenario':

            if ((scenario_mu is None) or (scenario_sigma is None)):
                raise ValueError(f"项目{project_seq}缺乏自定义的mu,sigma输入")
            else:
                self.mu = scenario_mu
                self.sigma = scenario_sigma

        elif param_source == 'calculation':

            if comparable_project_cdrs is None:
                raise ValueError(f"项目{project_seq}在对数正态分布参数估计中缺乏可比项目cdr输入")
            else:
                self.mu, self.sigma = self.estimate_params(comparable_project_cdrs)
        elif param_source == 'rating_report':
            self.mu, self.sigma = self.__report_params(info)

        self.rating_info = defaultdict(list)

    def default_prob_dict(self, dict_):
        """
        设置违约概率字典，如果自定义输入则用自定义的值，如果不输入则用默认值保存在 ``config`` 中的默认值

        Args:
            dict_: key 是等级（或评级），value是对应的违约概率

        """
        self.dict_ = dict_
        # 排序，保证从高等级到低等级
        keys_ = sort_grades(list(self.dict_.keys()))
        values_ = itemgetter(*keys_)(self.dict_)
        self.dict_ = dict(zip(keys_, values_))

    def rating(self, df_ccdrs):
        """
        评级

        Args:
            df_ccdrs (pd.DataFrame): 所有压力场景下的临界违约率

        Returns:
            dict: rank: 评级, key-证券代码，value-评级


        1. 通过 ``lognorm_tdrs`` 计算得到了目标违约率数据
        2. 对其中每只证券，从 `df_ccdrs` 获取其在所有场景下的最小临界违约率 `min_critical_cdr` ， 从高等级到低等级逐一比对各等级的 `tdr` 和 `min_critical_cdr` ,
        第一个小于 `min_critical_cdr` 的，其对应等级即为给定评级。 如果全部大于 `min_critical_cdr` , 则给出评级为 `NR`
        """

        self.lognorm_tdrs()

        self.grade = np.array(list(self.dict_.keys()))
        n = len(self.grade)
        codes = df_ccdrs.columns
        ranks = {}
        for code_ in codes:
            ranks[code_] = 'NR'
            min_critical_cdr = min(df_ccdrs[code_])
            success_ = False

            for i in range(0, n):
                if self.tdrs[i] <= min_critical_cdr:
                    ranks[code_] = self.grade[i]
                    success_ = True
                    break

            if not success_:
                if min_critical_cdr == 0:
                    self.rating_info[code_].append(
                                           '即使违约率为0该证券仍然违约，请根据估值结果核查表和基准参数预警信息确认估值结果异常原因；')
                else:

                    self.rating_info[code_].append('对数正态分布中证券 %s 的最小临界违约率 %f 小于目标违约率中的最小值 %f , 对应的最低等级为 %s , ' \
                                          '请检查目标违约概率表配置中二级分类 %s 是否足以覆盖各个等级或估值结果是否正常；' %(code_,
                                           min_critical_cdr, self.tdrs[n-1], self.grade[n-1], self.second_class))

        return ranks

    def estimate_params(self, comparable_project_cdrs):
        """
        当 `param_source` 为 `calculation` 时，利用可比项目估计 mu 和 sigma。取可比项目累计违约率中最后一行，计算均值作为 mu, 计算方差作为 sigma

        Args:
            comparable_project_cdrs (pd.DataFrame): 可比项目累计违约率数据

        Returns:
            tuple: tuple contains:
                    mu, sigma

        """

        comparable_project_cdrs = comparable_project_cdrs.ffill()
        cdrs = comparable_project_cdrs.iloc[-1, :]
        log_cdrs = np.log(cdrs)
        log_cdrs.replace(np.inf, np.nan, inplace=True)
        log_cdrs.replace(-np.inf, np.nan, inplace=True)
        mu = log_cdrs.mean()
        sigma = log_cdrs.mean()
        return mu, sigma

    def __report_params(self, info):
        """
        从评级报告获取项目参数，即当 `param_source` 为 `rating_report` 时，读取披露的违约分布参数(μ)和违约分布参数(σ)作为 mu 和sigma，如果没有相关数据则报错

        Args:
            info (pd.DataFrame): 评级报告数据表


        """
        try:
            mu = info.loc[rating_indicator_dict['MU'], self.project_seq]
            sigma = info.loc[rating_indicator_dict['SIGMA'], self.project_seq]
            return mu, sigma
        except Exception:
            raise ValueError("评级报告缺少均值方差信息，不予计算")

    def lognorm_tdrs(self):
        """
        计算各个等级下对应的TDR

        1. 先对违约概率字典 `dict_` 中每个信用等级都计算目标违约率 （通过 ``lognorm_tdr`` )
        2. 统一扣减当前已违约金额 `realized_cdr` ， 并转化为以 % 为单位。（跟临界违约率的计算统一口径）

        """

        self.tdrs = np.zeros(len(self.dict_))

        i = 0
        for grade, prob in self.dict_.items():
            self.tdrs[i] = self.lognorm_tdr(self.mu, self.sigma, prob)
            i += 1

        self.tdrs = (self.tdrs - self.realized_cdr) * 100

    def lognorm_tdr(self, mu, sigma, p):
        """
        计算给定信用等级违约概率下的目标违约率
        
        Args:
            mu (float): ln(累计违约比率)的均值
            sigma (float): ln(累计违约比率)的标准差
            p: 某个信用等级对应的违约概率

        Returns:
            float: TDR


        **公式** ::

            TDR = np.exp(mu + sigma * st.norm.ppf(1 - p))

        """

        TDR = np.exp(mu + sigma * st.norm.ppf(1 - p))

        return TDR

    def return_tdrs(self):
        return dict(zip(self.grade, self.tdrs))