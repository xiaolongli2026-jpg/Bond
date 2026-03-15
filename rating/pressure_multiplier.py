# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from rating.prepare.config import rating_indicator_dict
from operator import itemgetter
from .prepare.utils import sort_grades
from collections import defaultdict


class pressure_multiplier:

    warn_info = []

    def __init__(self, project_seq, secondary_class, report_info, target_param_method='report_date'):
        """
        压力乘数法

        Args:
            project_seq (str): 项目内码
            secondary_class (str): ABS二级分类
            report_info (pd.DataFrame): 评级报告维护的参数信息
            target_param_method (str): 目标违约率/回收率获得方式，calculation-以输入的基准参数乘以倍数获得, rating_report-直接从评级报告获得
        """

        self.project_seq = project_seq.strip("'")
        self.secondary_class = secondary_class
        self.info = report_info
        self.target_param_method = target_param_method
        self.rating_info = defaultdict(list)  # 保存评级过程中的问题

    def multiplier_dict(self, dict_=None):
        """
        设置该二级分类对应的压力乘数字典，如果自定义输入则用自定义的值，如果不输入则用默认值保存在 ``config`` 中的默认值

        Args:
            dict_ (dict): key 是等级，value是对应的乘数

        """

        self.dict_ = dict_
        # 排序，保证从高等级到低等级
        keys_ = sort_grades(list(self.dict_.keys()))
        values_ = itemgetter(*keys_)(self.dict_)
        self.dict_ = dict(zip(keys_, values_))


    def cal_tdrs(self, realized_cdr, unrealized_cdr, custom_tdrs=None):
        """
        计算各评级下对应的TDR （单位 %）

        Args:
            unrealized_cdr (float): 预测累计违约率中未实现的部分
            realized_cdr (float): 已实现累计违约率
            custom_tdrs (dict): 如果 `target_param_method` 为 `customize` , 则需要输入 `custom_tdrs` 。key-信用等级 ， value-需要的目标违约率

        **逻辑**

        1. 如果 `tdr_method` 为 'customize'， 则用 `custom_tdrs` 作为各等级目标违约率
        2. 如果 `tdr_method` 为 'calculation'， 用未实现累计违约率 `unrealized_cdr` 乘以各等级下压力乘数得到相应的目标违约率
        3. 如果 `tdr_method` 为 'rating_report', 从评级报告里读取形式为 '目标违约比率_AAA' 指标的数值，减去已实现累计违约率
         `realized_cdr` , 作为各等级下的目标违约率。如果某一等级的目标违约率不存在，用上一级的目标违约率除以1.1

        """

        tdr_method = self.target_param_method
        if tdr_method == 'customize':

            self.grades = list(custom_tdrs.values())
            self.grades = np.array(sort_grades(self.grades))
            self.tdrs = itemgetter(*self.grades)(custom_tdrs)
            self.tdrs = np.array(self.tdrs)
        else:
            unrealized_cdr = max(unrealized_cdr, 0)
            dict_ = self.dict_
            self.grades = np.array(sort_grades(list(dict_.keys())))
            n = len(self.grades)
            if tdr_method == 'calculation':
                self.tdrs = unrealized_cdr * np.array(itemgetter(*self.grades)(dict_))
            elif tdr_method == 'rating_report':
                self.tdrs = np.ones(len(self.grades))
                for i in range(0, n):
                    grade = self.grades[i]
                    label_ = rating_indicator_dict.get('TDR_' + grade.upper(), None)

                    if label_ in self.info.index and (label_ is not None):

                        TDR = self.info.loc[label_, self.project_seq].copy()
                        TDR = max(TDR - realized_cdr, 0)
                        self.tdrs[i] = TDR
                    else:
                        if i == 0:
                            raise ValueError(f'项目内码：{self.project_seq}评级报告缺乏{grade}等级证券目标违约率')
                        else:
                            TDR = self.tdrs[i-1] / 1.1 # 如果没有，则用上一个等级的TDR/1.1
                            self.tdrs[i] = TDR

        self.tdrs = self.tdrs * 100

    def cal_trrs(self, realized_rr, unrealized_rr=0, customize_trrs=None):
        """
        计算各评级下对应的TRR （单位 %），注意是扣除已实现的，临界回收率中是否违约也是根据未来的现金流判断，不看过去的已实现金额

        Args:
            unrealized_rr (float): 预测回收率中未实现的部分
            realized_rr (float): 已实现回收率，为已回收金额占入池金额的比
            customize_trrs (dict): 如果 `target_param_method` 为 `customize` , 则需要输入 `custom_trrs` 。key-信用等级 ， value-需要的目标回收率


        **逻辑**

        1. 如果 `tdr_method` 为 'customize'， 则用 `custom_trrs` 作为各等级目标回收率
        2. 如果 `tdr_method` 为 'calculation'， 用未实现回收率 `unrealized_rr` 乘以各等级下压力乘数得到相应的目标回收率
        3. 如果 `tdr_method` 为 'rating_report' , 毛回收率*（1-催收费用）*压力乘数，作为各等级下的目标回收率来源1。读取形式为 '目标回收比率_AAA' 指标的数值，减去已实现回收率 `realized_rr` ,作为各等级下的目标回收率的第二个来源，如果某一等级的目标回收率不存在，用上一级的目标回收率， 两者取均值作为各等级下的目标回收率。
        4. 如果 `tdr_method` 为 'both' , 则为 `tdr_method` 为 'rating_report'  和 `tdr_method` 为 'calculation' 下的均值作为目标回收率

        """

        trr_method = self.target_param_method
        if trr_method == 'customize':
            self.grades = list(customize_trrs.values())
            self.grades = np.array(sort_grades(self.grades))
            self.trrs = np.array(itemgetter(*self.grades)(customize_trrs))

        else:
            dict_ = self.dict_
            self.grades = np.array(sort_grades(list(dict_.keys())))
            n = len(self.grades)
            if trr_method == 'calculation':
                self.trrs = unrealized_rr * np.array(itemgetter(*self.grades)(dict_))
            elif trr_method == 'rating_report' or trr_method == 'both':
                try:
                    # 算法一： 毛回收率 * （1-催收费用） * 压力乘数
                    TRRs_1 = self.info.loc[rating_indicator_dict['RR_MAO'], self.project_seq] \
                            * (1 - self.info.loc[rating_indicator_dict['CS_FEE'], self.project_seq]) \
                            * np.array(list(dict_.values()))
                    TRRs_1[TRRs_1 < 0] = 0
                except KeyError as e:
                    self.warn_info.append('trr来源为评级报告，评级报告缺少毛回收率或者催收费用数据')
                    method1_success = False
                else:
                    method1_success = True

                # 算法2 目标回收比例 * （1-催收费用）
                TRRs_2 = np.ones(len(self.grades))
                method2_success = True
                for i in range(0, n):
                    grade = self.grades[i]
                    label_ = rating_indicator_dict['TRR_' + grade.upper()]
                    if label_ in self.info.index:
                        TRR_2 = self.info.loc[label_, self.project_seq].copy() \
                                * (1 - self.info.loc[rating_indicator_dict['CS_FEE'], self.project_seq])

                        TRRs_2[i] = TRR_2
                        TRRs_2[TRRs_2 < 0] = 0
                    else:
                        if i == 0:
                            self.warn_info.append('trr来源为评级报告，评级报告缺少TRR或者催收费用数据')
                            method2_success = False
                        else:
                            # 用上一个等级的值替代作为下一级的目标违约率
                            self.warn_info.append(f'trr来源为评级报告，评级报告缺少{grade}等级TRR或者催收费用数据，用高一等级的结果替代')
                            TRR_2 = TRRs_2[i-1]
                            TRRs_2[i] = TRR_2
                            TRRs_2[TRRs_2 < 0] = 0

                if method1_success and method2_success:
                    self.trrs = pd.Series([TRRs_1, TRRs_2]).mean() - realized_rr # 取均值

                elif method1_success or method2_success:
                    self.trrs = (TRRs_1 - realized_rr if method1_success else TRRs_2 - realized_rr)
                else:
                    raise ValueError(f'项目内码：{self.project_seq}仅根据评级报告数据无法得到目标回收率')

                if trr_method == 'both':
                    trr_cal = unrealized_rr * np.array(itemgetter(*self.grades)(dict_))

                    self.trrs = pd.Series([self.trrs, trr_cal]).mean()
        self.trrs[self.trrs < 0] = 0
        self.trrs = self.trrs * 100


    def rating(self, df_ccdrs):
        """
        除不良贷款以外的项目的评级

        Args:
            df_ccdrs (pd.DataFrame), 所有加压情形下的临界违约率序列, 单位%

        Returns:
            dict: rank: 评级, key-证券代码，value-评级

        **逻辑**

            * 对其中每只证券，从 `df_cdrs` 获取其在所有场景下的最小临界违约率 `min_critical_cdr` ， 从高等级到低等级逐一比对各等级的 `tdr` 和 `min_critical_cdr` ,第一个小于 `min_critical_cdr` 的，其对应等级即为给定评级。 如果全部大于 `min_critical_cdr` , 则给出评级为 `NR`

        """

        # grades: np.array, 评级
        # tdrs: np.array, 评级对应的目标累计违约率（扣除已还部分）, 单位 %
        codes = df_ccdrs.columns
        ranks = {}
        for code_ in codes:
            min_critical_cdr = min(df_ccdrs[code_])
             # 给出评级
            ranks[code_] = 'NR'
            success_ = False
            for grade, tdr in zip(self.grades, self.tdrs):
                if tdr <= min_critical_cdr:
                    ranks[code_] = grade
                    success_ = True
                    break

            if not success_:
                if min_critical_cdr == 0:
                    self.rating_info[code_].append('即使违约率为0该证券仍然违约，请根据结果核查表确认估值结果异常原因')
                else:
                    self.rating_info[code_].append('证券 %s 的最小临界违约率 %f 小于目标违约率中的最小值 %f , 对应的最低等级为 %s , ' \
                                          '目标违约率来源为 %s , 请检查目标来源下目标违约率的等级数量是否不足或估值结果是否正常;' %(code_,
                                                        min_critical_cdr, tdr, grade,
                                                        '评级报告' if self.target_param_method == 'rating_report'
                                                        else ('基准数据加压'
                                                        if (self.target_param_method == 'calculation') or (self.target_param_method == 'both')
                                                        else '自定义目标回收率')))
        return ranks

    def npl_rating(self, df_crrs):
        """
        不良贷款评级

        Args:
            df_crrs (pd.DataFrame), 所有加压情形下的临界回收率序列, 单位%

        Returns:
            dict: rank: 评级, key-证券代码，value-评级

        **逻辑**

          * 对其中每只证券，从 `df_ccrs` 获取其在所有场景下的最大临界回收率 `max_crr` ， 从高等级到低等级逐一比对各等级的 `trr` 和 `max_crr` , 第一个大于 `max_crr` 的，其对应等级即为给定评级。 如果全部小于 `max_crr` , 则给出评级为 `NR`

        """
        codes = df_crrs.columns

        ranks = {}
        for code_ in codes:
            max_crr = max(df_crrs[code_])
            ranks[code_] = 'NR'
            success_ = False
            for grade, trr in zip(self.grades, self.trrs):  # 各二级分类ABS下，各个评级对应的压力乘数映射，从报告采集
                if trr >= max_crr:
                    ranks[code_] = grade
                    success_ = True
                    break

            if not success_:
                if max_crr == 100:
                    self.rating_info[code_].append('即使回收率为100%该证券仍然违约，请根据结果核查表确认估值结果异常原因')
                else:
                    self.rating_info[code_].append('证券 %s 的最大临界回收率 %f 大于目标回收率中的最大值 %f , 对应的最低等级为 %s , ' \
                                          '目标回收率来源为 %s , 请检查目标来源下目标回收率的等级数量是否不足或估值结果是否正常;' % (code_,
                                                                                    max_crr, trr, grade,
                                                                                    '评级报告' if self.target_param_method == 'rating_report'
                                                                                    else (
                                                                                        '自定义目标回收率' if self.target_param_method == 'customize'
                                                                                        else '基准数据加压')))

        return ranks

    def return_tdrs(self):
        """返回各等级下的tdr数据"""
        return dict(zip(self.grades, self.tdrs))

    def return_trrs(self):
        """返回各等级下的trr数据"""
        return dict(zip(self.grades, self.trrs))
