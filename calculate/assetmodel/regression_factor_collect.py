# -*- coding: utf-8 -*-

from abs.doc.enumerators import SecondClass


def factor_choose(asset_type):
    """
    所有可能用到的初始入池特征因子的集合，输入资产类别选取对应的因子

    Args:
        asset_type: str, 二级分类枚举

    Returns:
        tuple: tuple contains:

            * ucpr_factor (list): 无条件早偿率回归需要用到的因子名称
            * cdr_factor (list): 累计违约率回归需要用到的因子名称
            * duration_feature_dict (dict): 回归因子中需要用到的从资产池存续期特征表格提取的数据，value是程序中用到的名称, 与因子列表中的对应，key是数据库中的枚举值。
            * initial_feature_dict (dict): 回归因子中需要用到的从资产池发行期特征表格提取的数据

    """
    # 3025
    duration_factor_dict = {'loan_number': '110', 'period_begin_real_balance': '102',
                            'wam': '203', 'waa': '202',
                            'wair': '208', 'ltv': '306', 'waic': '302', 'rtl': '303'}

    # 3023
    initial_factor_dict = {'loan_number': '110', 'initial_principal': '102',
                           'wam': '203', 'waa': '202',
                           'wair': '208', 'ltv': '306', 'waic': '302', 'rtl': '303'}

    if str(asset_type) == SecondClass["RMBS"]: #住房抵押贷款
        duration_feature = ['wair', 'wam', 'period_begin_real_balance']
        initial_feature = ['ltv', 'initial_principal']  # 只用发行期数据的因子
        ucpr_factor = ['spread', 'wair', 'previous', 'wam', 'wam^2']
        cdr_factor = ['popb', 'ltv', 'age', 'age^2', 'spread']
    elif str(asset_type) == SecondClass["qichedai"]:
        duration_feature = ['wair', 'wam', 'period_begin_real_balance']
        initial_feature = ['rtl', 'initial_principal']
        ucpr_factor = ['spread', 'wair', 'previous', 'wam', 'wam^2']
        cdr_factor = ['popb', 'rtl', 'age', 'age^2', 'spread']
    else:
        raise ValueError("该二级分类不支持回归模型")

    duration_feature_dict = dict([(duration_factor_dict[key_], key_) for key_ in duration_feature])
    initial_feature_dict = dict([(initial_factor_dict[key_], key_) for key_ in initial_feature])

    return ucpr_factor, cdr_factor, duration_feature_dict, initial_feature_dict


