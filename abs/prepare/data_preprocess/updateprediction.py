# -*- coding: utf-8 -*-


def update_prediction(initial, current_balance):
    """
    根据存续期数据等比例更新现金流归集表,使得现金流归集表未来首期的期初本金余额等于 `current_balance`

    Args:
        initial (pd.DataFrame): 基准现金流预测,仅包含用于现金流瀑布的日期
        current_balance (float): 数值，当前剩余本金( `split_default` 为 `True` 时不包括违约本金，否则是包括当期最新违约本金的值）

    Returns:
        pd.DataFrame: 未来的现金流预测
    """
    if len(initial) == 0:  # 如果不是，不update但是也不报错，因为要对应付未付的本金进行分配
        return initial

    initial_begin_balance = initial.loc[0, 'begin_principal_balance']
    if initial_begin_balance > 0:
        ratio = current_balance / initial_begin_balance
        updated = initial.copy()

        updated.loc[:, ["begin_principal_balance", "current_principal_due",
                        "current_interest_due", "end_principal_balance"]] *= ratio
    else:
        updated = initial.copy()

    return updated