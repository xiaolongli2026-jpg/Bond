# -*- coding: utf-8 -*-
"""异常结果预警，如果需要对输出指标进行阈值检查，可以在这里添加相应的规则

"""


def result_check(df_other, df_tranches, tranches_obj, distribution_info, is_npls):
    """
    检查估值结果

    Returns:
        tuple: tuple contains:

                * msg (dict): 数据问题信息 ， key-证券代码
                * pass_info (dict): 是否通过阈值检查
                * msg_project (list): 项目数据问题项目，key-项目内码

    """
    threshold_cash = (0.9, float('inf'))  # 未来现金流与面值比的阈值
    threshold_pool = (0.9, 2) # 资产池剩余本金/证券端本金余额
    # 1. 未来现金流与面值比
    tranches_2 = df_tranches.copy()
    thresh = 0.9
    tranches_2['cash_div_balance'] = [(getattr(tranches_obj, level_).prin_payments.sum() +
                                        getattr(tranches_obj, level_).int_payments.sum()) /
                                      getattr(tranches_obj, level_).begin_balance
                              if ('sub' not in level_.lower() and getattr(tranches_obj, level_).begin_balance > 0)
                              else float('inf') for level_ in tranches_2['security_level']]

    underpay_cash = tranches_2['cash_div_balance'] < threshold_cash[0]

    # 2. 节点缺失或者无法正常进入问题
    lst_ = [",".join(distribution_info[x]) for x in tranches_2['security_seq']]

    msg = {x: f"未来现金流面值比小于阈值{thresh}" + ";" + z if y else z for (x, y, z) in zip(tranches_2['security_code'], underpay_cash, lst_)}

    # 3. 资产池剩余本金与证券端本金余额比

    asset_div_debt = df_other.loc[0, 'pool_divide_security']
    out_of_range = (asset_div_debt < threshold_pool[0]) or (asset_div_debt > threshold_pool[1])
    if (out_of_range and is_npls):
        msg_project = [f'资产池与证券本金比{asset_div_debt}超阈值{threshold_pool}']
    else:
        msg_project = []

    pass_info = {x: len(msg[x]) < 5 if isinstance(msg, str) else True for x in msg}

    return msg, pass_info, msg_project