"""
逻辑运算函数
"""


def and_(expressions):
    """
    与

    Args
        expressions (list, tuple): 由 bool 值构成的列表

    Returns:
        bool: 列表中的值是否都为 `True`
    """
    result = True
    for ex in expressions:
        result = result and ex
    return result


def or_(expressions):
    """
    或

    Args
        expressions (list, tuple): 由 bool 值构成的列表

    Returns:
        bool: 列表中的值是否有任意一个为 `True`
    """
    result = False
    for ex in expressions:
        result = result or ex
    return result


def not_(expression):
    """
    非

    Args
        expressions (bool):

    Returns:
        bool: 返回非 `expression`
    """
    return not expression
