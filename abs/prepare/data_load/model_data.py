"""
参数预测模型数据读取
"""
import numpy as np
import pandas as pd
from operator import itemgetter
from doc.enumerators import SecondClass, factor_dict_initial, Markov_absorb_dict, Markov_temp_dict
from utils.timeutils import to_date2
from utils.miscutils import isnull
from prepare.sqltemplate.sqltemplate_basic import prediction_issue_template
from prepare.data_load.load_basic_data import get_param, unpack_params
from prepare.sqltemplate.sqltemplate_model import *
from utils.timeutils import age_
from utils.sql_util import sql_read
from utils.quick_connect import connect_mysql
from calculate.assetmodel.regression_factor_build import RegressionFactorBuild
from doc.global_var import global_var as glv


def get_comparable_project(project_seq, same_type=False, cur=None):
    """
    获取可比项目信息

    Args:
        project_seq (str): 项目内码
        same_type (bool): 可比资产池查找范围

                           * `True` - 查找同二级分类的作为可比资产池，
                           * `False` (默认） - 查找同发行人的作为可比资产池
        cur (cursor):

    Returns:
        tuple: tuple contains:
                comparable_projects_lst: 可比项目内码列表
                success: 可比项目是否足够（超过五个项目）


    **逻辑**


    * ``same_type = False`` 时，如果没有读懂标的项目对应的发起人名称，或者发起人已发项目中同二级分类的项目少于10个，则改用读取同二级分类的数据，以保证数据量
    * ``same_type = True`` 时，读取与标的项目同二级分类的所有项目的内码，此时可能会出现可比项目太多的情况


    Notes:
        也可以选择自建静态池
    """

    if_close = False
    if cur is None:
        conn, is_mysql = connect_mysql()
        cur = conn.cursor()
        if_close = True
    else:
        is_mysql = glv().get('is_mysql')
    # 1. 获取发起人名称
    success = False
    project_seq = "'" + str(project_seq.strip("'")) + "'"
    sql_issuer = get_issuer(project_seq)
    df_issuer = sql_read(sql_str=sql_issuer, cur=cur, is_mysql=is_mysql, single_return=False)

    # 2. 获取项目二级分类
    sql_secondclass = getSecondClass(project_seq)
    df_secondclass = sql_read(sql_str=sql_secondclass, cur=cur, is_mysql=is_mysql, single_return=False)

    # 3. 获取可比项目内码
    try:
        second_class = df_secondclass.loc[0, 'secondary_classification']
        initial_date = df_secondclass.loc[0, 'initial_date']

    except (IndexError, KeyError):
        return [], success

    else:

        if same_type:
            comparable_projects = get_same_class_projects(second_class, initial_date, cur)

        else:
            try:
                issuer = "'" + df_issuer.loc[0, 'institution_name'].strip("'") + "'"
            except (IndexError, KeyError):
                comparable_projects = get_same_class_projects(second_class, initial_date, cur)
            else:
                comparable_projects = get_same_issuer_projects(issuer, second_class, initial_date, cur)
                if len(comparable_projects) < 11:
                    comparable_projects = get_same_class_projects(second_class, initial_date, cur)

        from itertools import chain
        comparable_projects_lst = list(chain.from_iterable(comparable_projects))

        try:
            comparable_projects.remove(project_seq)
        except (AttributeError, ValueError):
            pass

        if if_close:
            cur.close()
            conn.close()

        if len(comparable_projects) > 5:
            success = True

        return comparable_projects_lst, success


def get_same_issuer_projects(issuer, second_class, initial_date, cur=None):
    """
    读取同一发起人发起的同二级分类的项目内码

    Args:
        issuer (str): 发起人名称
        second_class (str): 二级分类枚举值
        initial_date (str): 初始起算日

    Returns:
        tuple: 可比项目

    Notes:
        发起人名称是中文名，且输入的时候未作限制，如果不是100%相同，会出现匹配不到的情况
    """
    sql_comparable_projects = projectIssuedBySameIssuer_withSameType(issuer, second_class,
                                                                     initial_date)
    cur.execute(sql_comparable_projects)
    comparable_projects_lst = cur.fetchall()

    return comparable_projects_lst


def get_same_class_projects(second_class, initial_date, cur=None):
    """
    读取同二级分类项目内码

    Args:
        second_class (str): 二级分类
        initial_date (str): 初始起算日

    Returns:
        tuple: 可比项目
    """
    sql_comparable_projects_supple = projectWithSameType("'" + second_class + "'", initial_date)
    cur.execute(sql_comparable_projects_supple)
    comparable_projects_lst = cur.fetchall()

    return comparable_projects_lst


def delete_outliers_or_cdrs(this_pool_cdrs) -> object:
    """剔除累计违约率序列极端值，包括单期边际违约率超过0.02(2%)或者边际违约率小于0的"""

    n = len(this_pool_cdrs)
    this_pool_mdrs = np.zeros((n,))
    this_pool_mdrs[1::] = this_pool_cdrs[1::] - this_pool_cdrs[0:-1]
    this_pool_cdrs[this_pool_mdrs > 0.02] = float('nan')
    this_pool_cdrs[this_pool_mdrs < -1e-4] = float('nan')
    return this_pool_cdrs


def delete_unvalid_projects(df):
    """
    去掉一些有很多0、nan的无效项目

    Args:
        df (pd.DataFrame): 列为项目内码，行为月

    Returns:
        tuple: tuple contains

                df (pd.DataFrame): 删除掉有很多0、nan的无效项目对应的列后的表
                valid_projects (list): 未被删除的项目
                invalid_projects (list): 被剔除的项目
    """
    df_len = len(df)
    null_num = df.isnull().sum(axis=0).tolist()
    zero_num = (df == 0).sum(axis=0).tolist()
    count_invalid = [x + y for x, y in zip(null_num, zero_num)]
    staticpool_list = df.columns
    invalid_projects = staticpool_list[[True if x == df_len else False for x in count_invalid]]
    df.drop(columns=invalid_projects, inplace=True)
    valid_projects = list(df.columns)
    return df, valid_projects, invalid_projects


def extrapolation_factor_choose(asset_type):
    """
    用于调整基准违约率所需特征因子，输入资产二级分类选取对应的因子名

    Args:
        asset_type: str, 二级分类枚举值

    Returns:
        tuple: tuple contains
                factor: list, 初始入池特征在程序中用到的名字 \n
                factor_dict: dict, key-在数据库的入池特征标中的特征名称枚举值, value-程序中的命名

    """

    if str(asset_type) == SecondClass["RMBS"]:  # 住房抵押贷款
        factor = ['RTL']
    elif str(asset_type) == SecondClass["qichedai"]:  # 个人汽车贷款
        factor = ['LTV']

    if 'factor' in vars():
        factor_cn = itemgetter(*factor)(factor_dict_initial)
        factor_cn = [str(factor_cn)]
        factor_dict = dict(zip(factor_cn, factor))

        return factor, factor_dict
    else:
        raise ValueError("there is no suitable adjusted factor for this asset type, ")


def get_adjust_factor(projects: list, second_class: str, cur) -> pd.DataFrame:
    """
    读取累计违约率的调整因子

    Args:
        projects (list): 项目内码
        second_class (str): 二级分类枚举值
        cur (cursor):

    Returns:
        pd.DataFrame, df_factor_initial, 所需的、用于调整累计违约率的入池特征


    **逻辑**

    根据二级分类从 ``extrapolation_factor_choose`` 读取到因子名称后，从数据库读取项目内码列表中项目的因子值

    """

    try:
        # 获取因子
        factor, factor_dict = \
            extrapolation_factor_choose(second_class)
    except ValueError:
        print("无调整因子，使用静态池违约率均值作为待估资产池累计违约率预测值")
    else:
        factor_sql_name = list(factor_dict.keys())
        df_factor_initial = pd.DataFrame(projects, columns=['project_seq'])

        for ftr in factor_sql_name:
            sql_factor_value = get_feature_template(ftr, projects)
            df_temp = sql_read(sql_factor_value, cur, glv().get('is_mysql'), single_return=False)

            df_temp.rename(columns={'indicator_value': factor_dict[ftr]}, inplace=True)
            df_factor_initial = df_factor_initial.merge(df_temp, how='left', on='project_seq')

        cols = list(factor_dict.values())
        df_factor_initial[cols] = \
            df_factor_initial[cols].applymap(lambda x: float('nan') if isnull(x) else float(x))
        df_factor_initial.drop_duplicates(subset='project_seq', keep='first', inplace=True)

        return df_factor_initial


def get_checkdate_info(project_seq: str, cur):

    """

    获取初始现金流归集表的核算日，账龄由核算日与初始起算日之间的月数四舍五入计算

    Args:
        project_seq (str): 项目内码

    Returns:
         tuple: tuple contains
                schedule (pd.DataFrame): 包含核算日date_, 账龄age
                second_class (str): 二级分类枚举值

    """
    project_seq = "'" + project_seq.strip("'") + "'"

    sql_product = product_template(project_seq)
    df_product = sql_read(sql_product, cur, glv().get('is_mysql'), single_return=False)

    if len(df_product) < 1:
        raise Exception("没有找到该项目的基本信息，或项目为循环购买，无法获取初始核算日信息")

    second_class = df_product.loc[0, 'secondary_classification']
    initial_date = df_product.loc[0, 'initial_date']
    sql_issue_prediction = prediction_issue_template(project_seq)
    df_prediction = sql_read(sql_issue_prediction, cur, glv().get('is_mysql'), single_return=False)

    if len(df_prediction) < 1:
        raise Exception("缺少初始现金流归集表")

    schedule = pd.DataFrame(columns=['date_', 'age'])
    schedule['date_'] = list(df_prediction['date_']) + [initial_date]

    schedule.sort_values(by='date_', inplace=True, ignore_index=True)
    schedule['date_'] = schedule['date_'].apply(to_date2)
    schedule['age'] = schedule['date_'].apply(lambda x: age_(to_date2(initial_date), to_date2(x)))
    schedule['age'] = schedule['age'].apply(int)
    schedule.drop_duplicates(subset='age', keep='first', inplace=True, ignore_index=True)

    return schedule, second_class


def comparable_pools_data(project_seq: str, predict_date: str, same_type=False, cur=None):
    """
    读取可比资产池

    Args:
        project_seq (str): 标的项目
        predict_date (str): 估值日期
        same_type (bool): 可比资产池查找范围

                           * `True` - 查找同二级分类的作为可比资产池，
                           * `False` (默认） - 查找同发行人的作为可比资产池
        cur (cursor):

    Returns:
        tuple: tuple contains:
                valid_projects,
                static_pools_cdrs, staticpools_ucprs, staticpools_cprs, staticpools_rrs

    """
    project_seq = "'" + project_seq.strip("'") + "'"
    predict_date = "'" + str(predict_date.strip("'")) + "'"
    # 1. 获得可比项目内码
    comparable_static_pools, if_read_success = get_comparable_project(project_seq, same_type, cur)
    if not if_read_success:
        raise ValueError(f"项目{project_seq}无法获取足量的可比项目")

    # 2. 可比项目的历史数据
    df_history_param = \
        get_param(comparable_static_pools, predict_date, return_method='series')
    static_pools_cdrs, staticpools_ucprs, staticpools_cprs, staticpools_rrs = unpack_params(df_history_param)

    if len(static_pools_cdrs) < 1:
        if not same_type:
            comparable_static_pools, if_read_success = get_comparable_project(project_seq, same_type=True, cur=cur)
            df_history_param = \
                get_param(comparable_static_pools, predict_date, return_method='series')
            static_pools_cdrs, staticpools_ucprs, staticpools_cprs, staticpools_rrs = unpack_params(df_history_param)

            if len(static_pools_cdrs) < 1:
                raise Exception("缺少可比静态池数据, 无法使用资产池外推模型")

    static_pools_cdrs, valid_projects, invalid_projects = delete_unvalid_projects(static_pools_cdrs)

    if static_pools_cdrs.shape[1] < 2:
        raise ValueError("缺少可比静态池数据, 无法使用资产池外推模型")

    return valid_projects, static_pools_cdrs, staticpools_ucprs, staticpools_cprs, staticpools_rrs


def get_markov_data(projects, fit_date, cur):
    """
    读取马尔可夫转移中的资产池历史状态数据

    Args:
        projects (list, str): 项目内码的list
        fit_date (str): 拟合时为拟合时点，预测时则为预测时点
        cur (cursor):

    Returns:
        pd.DataFrame: result， 至少包含列
                            * 'project_seq' - 项目内码
                            * 'age' - 与初始起算日之间的月数（四舍五入）
                            * 't_n' - 正常本金余额
                            * 't_o_1_30' - 逾期1-30天的本金余额
                            * 't_o_31_60' - 逾期31-60天的本金余额
                            * 't_o_61_90' - 逾期61-90天的本金余额
                            * 't_d' - 违约本金余额
                            * 'a_p' - 当期早偿回收
                            * 'a_o' - 当期逾期回收
                            * 'a_d' - 当期违约回收
                            * 'a_n' - 当期正常回收

    **逻辑**

        * 实际的数据披露上并不都是按以上的口径，这里的一项资产池状态可能是几项枚举值的集合。映射关系见 ``doc.enumerator.Markov_absorb_dict`` 和 ``doc.enumerator.Markov_temp_dict``

    """

    if isinstance(projects, str):
        projects = [projects.strip("'") ]

    fit_date = "'" + fit_date.strip("'") + "'"

    target_cols_ = ['t_n', 't_o_1_30', 't_o_31_60', 't_o_61_90', 't_d', 'a_p', 'a_o', 'a_d', 'a_n']
    temp_status_cols = ['t_n', 't_o_1_30', 't_o_31_60', 't_o_61_90', 't_d']
    absorb_status_cols = ['a_p', 'a_o', 'a_d', 'a_n']

    # 1. 暂态数据
    sql_temp_status = temp_status_template(projects, fit_date)
    df_temp_status_initial = sql_read(sql_temp_status, cur, glv().get('is_mysql'), single_return=False) # 初始数据，还需要处理
    df_temp_status_initial.drop_duplicates(keep='last', inplace=True)
    # 2.吸收态数据
    sql_absorb_status = absorb_status_template(projects, fit_date)
    df_absorb_status_initial = sql_read(sql_absorb_status, cur, glv().get('is_mysql'), single_return=False)

    # 3. 映射数据状态，将暂态和吸收态数据整合到一起
    # 状态起名规则 用“_”隔开，T为暂态，A为吸收态 口径：将逾期90天以上均作为违约
    df_temp_status_initial.loc[:, 'loan_status'] = \
        df_temp_status_initial['loan_status'].apply(lambda x: Markov_temp_dict[x]
        if x in Markov_temp_dict.keys() else float('nan'))
    df_absorb_status_initial['loan_status'] = \
        df_absorb_status_initial['loan_status'].apply(lambda x: Markov_absorb_dict[x]
        if x in Markov_absorb_dict.keys() else float('nan'))

    df_temp_status_initial.dropna(subset=['loan_status', 'principal'], how='any', inplace=True)  # 暂态的标签或者金额缺失则剔除
    df_absorb_status_initial.dropna(subset=['loan_status'], how='all', inplace=True)  # 吸收态如果缺失了标签则剔除，缺失金额当作0处理

    df_status_initial = df_temp_status_initial.append(df_absorb_status_initial, ignore_index=True)

    if len(df_status_initial) < 1:
        return []
    else:

        # 4. 处理数据为列名为状态名称，index为期次的格式
        df_status_initial.loc[:, 'principal'] = \
            df_status_initial['principal'].apply(lambda x: np.nan if isnull(x) else float(x))

        # 4.1 有些项是合并的，比如预期超过90天的都视为违约，所以需要将相关字段的金额加总
        df_status_initial = \
            df_status_initial.groupby(['project_seq', 'date_', 'loan_status'], as_index=False)['principal'].sum()

        # 4.2 数据展开为column为状态名的格式
        df_medium = \
            pd.pivot_table(df_status_initial, index=['project_seq', 'date_', 'loan_status']).unstack()['principal']
        df_medium.reset_index(drop=False, inplace=True)
        df_status = df_medium.copy()
        add_cols = [x for x in target_cols_ if x not in df_status.columns]
        df_status.loc[:, add_cols] = float('nan')

        # 4.3 里面的sum和implement_sum是总金额，当个状态金额缺失时，用这个数据补充用的（但是这个字段不一定有维护，所有列名中可能有缺失）
        if 'a_implement_sum' in df_status.columns:  # 总回收金额
            if 'a_sum' not in df_status.columns:
                df_status.loc[:, 'a_sum'] = float('nan')
            df_status.loc[:, 'a_sum'] = df_status['a_sum'].fillna(df_status['a_implement_sum'])
            df_status.loc[:, 'a_n'] = df_status['a_n'].fillna(df_status['a_sum'] - df_status[['a_p', 'a_o', 'a_d']].sum(axis=1))
            df_status.drop(columns=['a_sum', 'a_implement_sum'], inplace=True)

        if 'a_imple_p' in df_status.columns: # 早偿回收金额
            df_status.loc[:, 'a_p'] = df_status['a_p'].fillna(df_status['a_imple_p'])
            df_status.drop(columns=['a_imple_p'], inplace=True)

        if 't_sum' in df_status.columns:  # 当前本金余额中没有违约也没有早偿的部分
            df_status.loc[:, 't_n'] = df_status['t_n'].fillna(df_status['t_sum'] -
                                                           df_status[['t_o_1_30', 't_o_31_60', 't_o_61_90', 't_d']].sum(axis=1))
            df_status.drop(columns=['t_sum'], inplace=True)

        # 4.4 计算账龄
        df_status['date_'] = df_status['date_'].apply(to_date2)
        initial_dates = df_status.groupby('project_seq')['date_'].min()
        initial_dates_dict = dict(zip(initial_dates.index, initial_dates.values))
        df_status.loc[:, 'age'] = df_status.apply(
            lambda x: age_(initial_dates_dict[x['project_seq']], x['date_']), axis=1)

        df_status[temp_status_cols] = df_status.groupby(['project_seq'])[temp_status_cols].ffill().fillna(0.)
        df_status[absorb_status_cols] = df_status[absorb_status_cols].fillna(0.)
        result = df_status[target_cols_ + ['age', 'project_seq', 'date_']].copy()
        result.loc[:, target_cols_] = result[target_cols_].applymap(float)
        result.drop_duplicates(subset=['project_seq', 'age'], keep='last', inplace=True)
        result.dropna(how='any', inplace=True, axis=1)

        return result


def get_predict_factors(project_seq, second_class, history_date, cur):
    """
    获取需要预测的项目的未来因子数据，主要用到 ``RegressionFactorBuild`` 进行因子构建，选取其中在最新历史归集日以后的数据

    Args:
        project_seq (str): 项目内码
        second_class (str): 二级分类
        history_date (str): 最新历史估值日
        cur (cursor):

    Returns:
        tuple: tuple contains
            df_cdr_factor_for_predict (pd.DataFrame): 累计违约率的回归模型所需的因子
            df_ucpr_factor_for_predict (pd.DataFrame): 无条件早偿率的回归模型所需的因子
            initial_prediction (pd.DataFrame): 所有项目的初始现金流归集表
    """
    project_seqs = [project_seq.strip("'")]
    dt_history_date = to_date2(history_date)
    rfbuild = RegressionFactorBuild(project_seqs, second_class, history_date, cur)
    df_cdr_factor, df_ucpr_factor = rfbuild.all_factors()

    cdr_factor_name = rfbuild.cdr_model_factors
    ucpr_factor_name = rfbuild.ucpr_model_factors

    initial_prediction = rfbuild.initial_prediction

    df_cdr_factor_for_predict = df_cdr_factor.loc[df_cdr_factor['date_'] > dt_history_date, :]
    df_ucpr_factor_for_predict = df_ucpr_factor.loc[df_ucpr_factor['date_'] > dt_history_date, :]

    df_cdr_factor_for_predict = df_cdr_factor_for_predict[set(cdr_factor_name + ['age'])].reset_index(drop=True)
    df_ucpr_factor_for_predict = df_ucpr_factor_for_predict[set(ucpr_factor_name + ['age'])].reset_index(drop=True)

    df_cdr_factor_for_predict.loc[:, 'intercept'] = 1  # 截距项
    df_ucpr_factor_for_predict.loc[:, 'intercept'] = 1

    return df_cdr_factor_for_predict, df_ucpr_factor_for_predict, initial_prediction


#数据读取模块
def get_train_data(fit_date: str, second_class: str, project_seqs: list, cur=None):
    """
    获取回归模型训练所需的因子值、y值（累计违约率cdr或无条件早偿率ucpr)， 线性回归和sigmoid回归用到的因子是一样的

    Args:
        fit_date (str): 数据读取的截止日期，
        second_class (str): 二级分类
        project_seqs (list): 用于拟合的样本范围
        cur (cursor):

    Returns:
        tuple: tuple contains
                df_cdr_trainset (pd.DataFrame): 列名包括累计违约率回归因子+cdr
                df_ucpr_trainset (pd.DataFrame): 列名包括无条件早偿率回归因子+ucpr
    """

    close_cursor = False
    if cur is None:
        conn, is_mysql = connect_mysql()
        cur = conn.cursor()
        close_cursor = True

    # 1. 获取因子值
    rfbuild = RegressionFactorBuild(project_seqs, second_class, fit_date, cur)
    df_cdr_factor, df_ucpr_factor = rfbuild.all_factors()

    cdr_factor_name = rfbuild.cdr_model_factors
    ucpr_factor_name = rfbuild.ucpr_model_factors

    # 2. 历史参数
    df_params = get_param(project_seqs, fit_date, return_method='series', cur=cur)

    if len(df_params) <= 0:
        raise ValueError("可比项目没有违约率和早偿率数据")
    else:
        df_params.loc[:, 'age'] = df_params['age'].apply(np.round).apply(int)
        df_params.drop_duplicates(subset=['project_seq', 'age'], keep='last', inplace=True)
        df_ucpr_factor = \
            df_ucpr_factor.merge(df_params[['project_seq', 'age', 'ucpr']], on=['project_seq', 'age'], how='left')

        df_cdr_factor = \
            df_cdr_factor.merge(df_params[['project_seq', 'age', 'cdr']], on=['project_seq', 'age'], how='left')

    dt_history_date = to_date2(fit_date)
    # 3. 筛选出时间早于拟合日期的数据
    df_cdr_factor.sort_values(by=['project_seq', 'age'], inplace=True)
    df_ucpr_factor.sort_values(by=['project_seq', 'age'], inplace=True)
    df_cdr_trainset = df_cdr_factor.loc[df_cdr_factor['date_'] <= dt_history_date, :]
    df_ucpr_trainset = df_ucpr_factor.loc[df_ucpr_factor['date_'] <= dt_history_date, :]

    # 4. 弃掉一些没有用的列，因为每一行都是一组数据，所以不需要project_seq和日期对数据进行区分，直接将面板数据输入拟合即可
    df_cdr_trainset = df_cdr_trainset[cdr_factor_name + ['cdr']]
    df_ucpr_trainset = df_ucpr_trainset[ucpr_factor_name + ['ucpr']]

    if close_cursor:
        cur.close()
        conn.close()

    return df_cdr_trainset, df_ucpr_trainset
