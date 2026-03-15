# -*- coding: utf-8 -*-
"""
模型数据读取
"""

from doc.enumerators import issuerCode, IsRevolving


def get_issuer(project_seq):
    """
    根据项目内码，读取项目发行人
    """

    s = '''
        select project_seq, institution_name
        from csi_abs_project_participant 
        where project_seq = {0}
        and role_type in {1}
    '''.format(project_seq, issuerCode)

    return s


def getSecondClass(project_seq):
    """
    根据项目内码，读取项目的二级分类
    """
    s = '''
        select secondary_classification, initial_date
        from csi_abs_project_basic_info
        where project_seq = {0}
    '''.format(project_seq)
    return s


def projectIssuedBySameIssuer_withSameType(issuer, second_class, initial_date):
    """
    读取某一发行人发行的、某一二级分类的、非循环购买并且初始起算日早于某一日期的所有ABS项目

    Args:
        issuer(str): 发行人的名称
        second_class(str): 二级分类
        initial_date(str): 能提取到的项目的起算日需要早于这个日期

    Notes:
        因为选择可比项目是服务于加压参数预测模型的，而模型不适用于循环购买，故将提取的可比项目范围限制在非循环购买
        用于为标的项目加压参数预测模型进行测算的可比项目最好早于标的项目发行，故限制能提取到的项目的起算日需要早于标的项目的初始起算日

    """

    s = '''
        select A.project_seq
        from csi_abs_project_participant A
        inner join (
        select project_seq
        from csi_abs_project_basic_info
            where secondary_classification = {3}
            and initial_date < {4}
            and is_recurring_pool = {2}) B
        on A.project_seq = B.project_seq 
        where institution_name = {0}
            and role_type in {1} 
    '''.format(issuer, issuerCode, IsRevolving["N"], second_class, initial_date)
    return s


def projectWithSameType(second_class, initial_date):
    """
    读取某二级分类下，初始起算日早于 ``initial_date`` 的所有项目的项目内码

    Args:
        second_class(str): 二级分类
        initial_date(str): 能提取到的项目的起算日需要早于这个日期

    """

    s = '''
        select project_seq
        from csi_abs_project_basic_info
        where secondary_classification = {1}
              and is_recurring_pool = {0}
              and initial_date < {2}
    '''.format(IsRevolving["N"], second_class, initial_date)
    return s


def product_template(project_seq):
    """
    读取预测加压参数模型所需的一些项目基本信息

    """

    s = '''
    select
        project_seq, initial_date, interest_start_date, first_pay_date,
        legal_due_date, is_recurring_pool, primary_classification, secondary_classification
    from csi_abs_project_basic_info
    where project_seq = {0}
    and is_recurring_pool = {1}
    '''.format(project_seq, IsRevolving["N"])
    return s


def get_feature_template(factor_name, projects):
    """
    从入池特征表 ``csi_abs_assetpool_in_feature`` 读取 ``indicator_name`` 为 ``factor_name`` 的数据

    Args:
        factor_name(str): 特征名称
        project(str): 项目内码

    Returns:

    """

    s = '''
        select AP.project_seq, AP.indicator_value
        from csi_abs_assetpool_in_feature AP
        where AP.indicator_name = {0}
            and AP.project_seq in {1}'''.format(factor_name, projects).replace("[", "(").replace("]", ")")
    return s


from doc.enumerators import loan_range_dict


def absorb_status_template(project, fit_date):
    """
    读取马尔可夫模型需要的各期回收资产的金额

    Args:
        project(list(str)): 项目内码
        fit_date(str): 日期

    Returns:
        * 归集日期(或报告结束日期) `end_date` as `date_`
        * 回收贷款状态 `loan_status`
        * 回收金额 `collection_principal` as `principal`

    """
    s = '''select project_seq, end_date as date_, loan_status, collection_principal as principal
    from csi_abs_duration_collection
    where project_seq in {0}
        and report_date <= {1}
        and loan_range = {2}
    order by project_seq, date_
    '''.format(project, "'" + fit_date.strip("'") + "'", "'" + loan_range_dict["prin"] + "'").replace('[', '(').replace(']', ')')
    return s


def temp_status_template(project, fit_date):
    """
    读取马尔可夫模型需要的各期存续的各种状态资产的金额

    Args:
        project(list(str)): 项目内码
        fit_date(str): 日期

    Returns:
        * 归集日期(或报告结束日期) `end_date` as `date_`
        * 贷款状态 `loan_status`
        * 期末剩余金额 `end_principal_balance` as `principal`

    """

    s = '''select project_seq, end_date as date_, loan_status, end_principal_balance as principal
            from csi_abs_duration_status
            where project_seq in {0}
                and report_date <= {1}
                and loan_range = {2}
            union (select A.project_seq, A.initial_date as date_, '2' as loan_status, 
                   B.initial_principal as principal
                   from csi_abs_project_basic_info A
                   left join(select project_seq, indicator_value as initial_principal
                     from csi_abs_assetpool_in_feature
                     where indicator_name= '102'
                     and project_seq in {0}) B
                   on A.project_seq = B.project_seq
                   where A.project_seq in {0})
            order by project_seq, date_
    '''.format(project, "'" + fit_date.strip("'") + "'", "'" + loan_range_dict["prin"] + "'").replace('[', '(').replace(']', ')')
    return s


def get_initial_date(project_seqs):
    """
    读取对应项目内码对应的初始起算日

    Args:
        project_seqs(list(str)): 项目内码

    """

    s = '''select project_seq, initial_date
    from csi_abs_project_basic_info
    where project_seq in {0}
    '''.format(project_seqs).replace("[", "(").replace("]", ")")
    return s


# 发行期资产池特征数据
def get_initial_feature_template(project_seqs, factor_names):
    """
    读取对应项目内码对应的项目的一些入池特征

    Args:
        project_seqs (list(str)): 项目内码
        factor_names (list(str)): 因子名称枚举值

    """
    s = '''
        select project_seq, indicator_value, indicator_name
        from csi_abs_assetpool_in_feature
        where indicator_name in {1}
        and project_seq in {0}
        '''.format(project_seqs, factor_names).replace('[', '(').replace(']', ')')
    return s


def batch_prediction_issue_template(projects):
    """
    读取列表中项目内码对应的初始现金流归集表
    Args:
        projects (list(str)): 项目内码

    """
    s = '''
        select
            project_seq, cf_date as date_,  pay_date, cf_date as pool_date, cif_principal as current_principal_due, 
            cif_accrual as current_interest_due, s_principal_balance as begin_principal_balance, 
            e_principal_balance as end_principal_balance
        from csi_abs_cashflow_issue
        where project_seq in {0}
        order by project_seq, cf_date asc
        '''.format(projects).replace("[", "(").replace("]", ")")
    return s


# 存续期资产池特征数据
def get_duration_feature_template(project_seqs, factor_names, fit_date):
    """
    读取对应项目内码对应的项目的一些存续期特征值

    Args:
        project_seqs (list(str)): 项目内码
        factor_names (list(str)): 因子名称枚举值
        fit_date (str): 日期

    """
    s = '''select project_seq, end_date as date_, indicator_value, indicator_name
           from csi_abs_assets_survive
           where project_seq in {0}
               and indicator_name in {1}
               and report_date <= {2}
           union(select A.project_seq, B.date_, A.indicator_value, A.indicator_name
                from csi_abs_assetpool_in_feature A
                right join(select project_seq, initial_date as date_
                           from csi_abs_project_basic_info
                           where project_seq in {0}) B
                on A.project_seq = B.project_seq
                where A.project_seq in {0}
                and A.indicator_name in {1}) 
           order by project_seq, date_
    '''.format(project_seqs, factor_names, "'" + fit_date.strip("'") + "'").replace('[', '(').replace(']', ')')
    return s
#