# -*- coding: utf-8 -*-
import numpy as np


def sort_grades(ratings):
    """
    将评级从高到低排序

    Args:
        ratings (np.array, list): 评级，eg ['a+', 'a-', 'aa']  , 要求不能有+-以外的符号

    Returns:
        np.array: eg. 前述的序列变成 np.array(['aa', 'a+', 'a-'])
    """
    aim_digit = max([len(x) for x in ratings])
    scores = np.zeros(len(ratings))
    upper_limit = ord('z') + 10  # 用于产生 a - z 逐渐增大的数字, a-z对应 10-35
    i = 0
    for rt in ratings:
        no_ = 0
        score_ = 0
        for charac in rt:
            no_ += 1
            if charac == '+':
                score_ += 1
            elif charac == '-':
                score_ -= 1
            else:
                ascii_ = ord(charac)  # ascii码从 a - z 逐渐增大， 大写的小于小写的字符
                ascii_ = ascii_ + 32 if ((ascii_ >= 65) and (ascii_ <= 90)) else ascii_  # 大写转为小写
                if ascii_ >= 97 and ascii_ <= 122:
                    ascii_ = upper_limit - ascii_   # a-z对应 10-35
                score_ += ascii_ * (100 ** (aim_digit - no_))

        scores[i] = score_
        i += 1

    order_ = np.argsort(-scores)
    if isinstance(ratings, list):
        ratings = np.array(ratings)

    return ratings[order_]

