
"""
空值运算函数
"""


def isscalar(str):
    """
    判断是否是标量
    Args:
        str (str):

    Returns:
        bool: `str` 是否是数值
    """
    try:
        float(str)
    except:
        return False
    else:
        return True


def isnull(x):
    """
    是否是空值

    Args:
        x: 可以是None, float('nan'), NaT 等

    Returns:
        bool: 是否是空值
    """
    if str(x) in ("None", "nan", "none", "", " ", "NaT", "nat"):
        return True
    else:
        return False


def ifnull(a, b):
    """如果 `a` 是空值，返回 `b` ，否则返回 `a` """

    if not isnull(a):
        return a
    else:
        return b


def fill_array(arr, method='ffill', axis=1, value=0):
    """
    对于含有 float('nan') 的 `array` 进行填充

    Args:
        arr (np.array): 存在空值的序列
        method: 'fill' -用前值补后值，'bfill' -相反，'constant' -固定值填充
        axis: 1-按列，0-按行

    Returns:
        np.array: 填充空值后的序列
    """
    try:
        (row_, column_) = arr.shape
    except:
        arr = arr.reshape(len(arr), 1)
        (row_, column_) = arr.shape

    import numpy as np
    if method == 'constant':
        arr[np.isnan(arr)] = value
        return arr
    elif method == 'ffill':
        if axis == 0:
            arr_copy = arr.T
            temp_ = row_
            row_ = column_
            column_ = temp_
        else:
            arr_copy = arr.copy()

        nans = np.isnan(arr_copy)

        for j in range(0, column_):
            for i in range(0, row_):
                arr_copy[i, j] = arr_copy[i-1, j] if ((i-1)>0) and (nans[i,j]) else arr_copy[i, j]

        if axis == 0:
            return arr_copy.T
        else:
            return arr_copy