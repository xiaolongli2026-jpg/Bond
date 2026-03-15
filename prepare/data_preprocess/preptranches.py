
import numpy as np
import pandas as pd
import re
from utils.miscutils import ifnull, isnull
from utils.timeutils import to_date2
from abs.doc.enumerators import interest_type_dict, frency_dict, amort_type_dict, DCC_dict


def transform_tranches(df_tranches, df_sequence, is_rev):
    """
    证券信息处理

    Args:
        df_tranches (pd.DataFrame): 证券基本要素表
        df_sequence (pd.DataFrame): 支付顺序表
        is_rev (bool): 是否循环购买

    Returns:
        df_tranches,
        warn_info


    **逻辑**

        1. 用支付顺序表中的次级期间收益率补充证券基本信息中期间收益率的缺失。由于表格架构关系，期间收益率字段取的是估值库字段，
        而这个字段并未维护次级的期间收益，但是在ABS的支付顺序维护中，期间收益率体现在了支付上限上。故通过提取支付顺序中，资金去向为 ``sub.int`` ,
        支付上限为 ``0.08 * sub.prin`` 的放在 ``df_tranches`` 中固定利息 ``fixed_rate`` 那一列中次级对应的行

        2. 其他细节处理。

            * 将付息频率、计息方式等由枚举值映射到程序能处理的
            * 将利率、利差等数值由百分数、BP变为小数。
            * 将日期相关字段由字符串变为 `datetime.date` 类型。
            * 对必填字段如证券等级进行空缺检查
            * 如果非次级超过预期到期日则预警

    Notes:
        还本付息频率不重要，因为建立了支付日序列表，不需要再通过频率推算

    """

    warn_info = []
    df_tranches.sort_values(by='security_level', inplace=True, ignore_index=True)
    df_tranches.drop_duplicates(inplace=True)

    # 1.处理付息方式

    df_tranches.loc[:, 'interest_type'] = \
        df_tranches['interest_type'].apply(lambda x: interest_type_dict[str(x)]
        if not isnull(x) else float('nan'))  # 利息类型：浮动or 固定

    # 1.1 用支付顺序表中的次级期间收益率补充证券基本信息中期间收益率的缺失
    seq_ = df_sequence.copy()
    seq_ = seq_.astype(str)
    sub_income = seq_.query("money_destination.notna() and money_destination.str.contains('sub')", engine='python')
    sub_income = sub_income.query("upper_limit.notna() and upper_limit.str.contains('prin')", engine='python') # 类似于0.08*Sub.prin的格式，则前面的数据就是次级期间收益率
    sub_income = sub_income.query("money_destination.str.contains('int')", engine='python')  # 次级期间收益对应的支付顺序去向为sub.int
    sub_income.reset_index(drop=True, inplace=True)
    n_sub = len(sub_income)
    sub_key, sub_value = [], []
    if n_sub > 0:
        for i in range(0, n_sub):
            md = sub_income.loc[i, 'money_destination']
            ul = sub_income.loc[i, 'upper_limit']
            if not isnull(ul):
                sub_key = sub_key + md.split(",") if "," in md else [md]
                sub_value = sub_value + ul.split(",") if "," in ul else [ul]
        sub_key = [re.match('sub[\d]*', x.strip()).group() for x in sub_key]
        sub_value = [re.match('([\d\.]+)', x).group() if 'sub' in x else 0 for x in
                     sub_value]
        sub_int_dict = dict(zip(sub_key, float(sub_value) * 100))

        df_tranches.loc[:, 'fixed_rate'] = df_tranches[['security_level', 'fixed_rate']].apply(
            lambda row: float(sub_int_dict[row[0]]) if (row[0] in sub_int_dict) and (
                    isnull(row[1]) or row[1] <= 0) else row[1], axis=1)

    # 2.处理证券级别 证券级别不能为空也不能重复，否则对不上支付顺序
    if pd.isna(df_tranches["security_level"]).any():
        raise ValueError(f"{df_tranches.loc[df_tranches['security_level'].isna(), 'security_code']}证券等级为空")
    elif df_tranches.duplicated(subset=['security_level'], keep='first').any():
        raise ValueError(f"存在重复的证券等级{df_tranches.loc[:, ['security_code', 'security_level']]}")
    df_tranches["security_level"] = df_tranches["security_level"].str.lower()

    # 3.处理利率数值
    df_tranches["fixed_rate"] = ifnull(df_tranches["fixed_rate"], 0) / 100

    # 4.处理浮动利率基准和利差
    df_tranches["floating_rate_benchmark"] = df_tranches["floating_rate_benchmark"] / 100.0
    df_tranches["floating_rate_spread"] = ifnull(df_tranches["floating_rate_spread"], 0) / 100.0

    # 5.次级固定资金成本
    df_tranches["sub_fcc"] = ifnull(df_tranches["sub_fcc"], 0) / 100
    
    # 6.处理预期到期日为datetime.date
    for i, ed in enumerate(df_tranches["legal_maturity_date"]):
        df_tranches.loc[i, "legal_maturity_date"] = to_date2(ed)

    # 7.处理付息频率 （不重要可以不关注）
    # 7.1 数据缺失情况
    if is_rev:

        lack_freqs = df_tranches[['rev_pay_interest_frequency',
                                  'rev_pay_principal_frequency',
                                  'amo_pay_interest_frequency',
                                  'amo_pay_principal_frequency']].applymap(lambda x: isnull(x))
    else:
        lack_freqs = df_tranches[['pay_interest_frequency', 'pay_principal_frequency']].applymap(lambda x: isnull(x))

    if lack_freqs.any().any():
        raise ValueError(f"存在还本付息频率缺失，请检查{'循环购买' if is_rev else '非循环购买'}对应的还本付息频率字段")

    # 7.2 枚举值转化
    try:
        freq_cols = ['pay_interest_frequency', 'pay_principal_frequency',
                     'rev_pay_interest_frequency', 'rev_pay_principal_frequency',
                     'amo_pay_interest_frequency', 'amo_pay_principal_frequency']
        df_tranches[freq_cols] = df_tranches[freq_cols].applymap(lambda x: frency_dict[str(x)] \
            if not isnull(x) else np.nan) # 频率具体选哪个看是否循环
    except KeyError as e:
        raise ValueError(f"字典里不存在的付息频率{e}")

    # 8.本金偿付方式和支付频率的相互关系
    for i, row_ in df_tranches.iterrows():
        if str(row_.amort_type) == '3':  # 8.1 到期一次还本

            if (df_tranches.loc[i, ['pay_principal_frequency', 'amo_pay_principal_frequency']]!='once').all():
                warn_info.append(
                    ("证券基本信息数据矛盾", f"证券{df_tranches.loc[i, 'security_code']}的还本频率与"
                                   f"到期一次还本付息的本金偿付方式不匹配"))

            if is_rev:
                df_tranches.loc[i, 'amo_pay_principal_frequency'] = 'once'
            else:
                df_tranches.loc[i, 'pay_principal_frequency'] = 'once' # 强制转化
            df_tranches.loc[i, 'rev_pay_principal_frequency'] = float('nan') # 默认循环期不还本

        # 8.2 除了到期一次，剩下的固定摊还和过手摊还，不能出现还本频率是“无”(除了循环期），会影响本息支付
        else:
            # (1) 本金支付为无则报错
            if (is_rev and isnull(df_tranches.loc[i, 'amo_pay_principal_frequency'])) or \
                    ((not is_rev) and isnull(df_tranches.loc[i, 'pay_principal_frequency'])):
                warn_info.append(("证券基本信息数据矛盾", f"非到期一次还本的证券{df_tranches.loc[i, 'security_code']}"
                                                f"未填还本频率或还本频率为无(关注维护的时候是否区分了是否循环并维护在适当的位置）"))
            # （2）利息支付为无则用本金频率补
            if (is_rev and isnull(df_tranches.loc[i, 'amo_pay_interest_frequency'])) or \
                    ((not is_rev) and isnull(df_tranches.loc[i, 'pay_interest_frequency'])) and \
                    ('sub' not in df_tranches.loc[i, 'security_level'].lower()):
                warn_info.append(("证券基本信息错误", f"优先级证券{df_tranches.loc[i, 'security_code']}"
                                              f"未填付息频率或付息频率为无,用本金频率补充(关注维护的时候是否区分了是否循环并维护在适当的位置）"))
                if is_rev:
                    df_tranches.loc[i, 'amo_pay_interest_frequency'] = 'once' \
                        if 'sub' in df_tranches.loc[i, 'security_level'].lower() \
                        else df_tranches.loc[i, 'amo_pay_principal_frequency']
                else:
                    df_tranches.loc[i, 'pay_interest_frequency'] = 'once' \
                        if 'sub' in df_tranches.loc[i, 'security_level'].lower() \
                        else df_tranches.loc[i, 'pay_principal_frequency']

    # 9.摊还方式
    df_tranches['amort_type'] = df_tranches.apply(lambda x: amort_type_dict[str(x['amort_type'])], axis=1)
    if sum((df_tranches['amort_type'] != 'fixed') & df_tranches['int_pay_with_prin']):
        raise ValueError("非固定摊还证券为利随本清模式，无法处理")

    # 10. 计息方式处理
    df_tranches['daycount'] = df_tranches['daycount'].apply(lambda x: DCC_dict[str(x)] if not isnull(x) else 'ACT/365')
    # 11. 处理日期格式
    df_tranches[['period_end_date', 'period_begin_date', 'payment_date', 'legal_maturity_date']] = \
        df_tranches[['period_end_date', 'period_begin_date', 'payment_date', 'legal_maturity_date']].applymap(to_date2)

    # 12. 如果优先级超过了预期到期日还没有完成还款，则预警
    exceed_mat = \
        [x and y and z for x, y, z in zip((df_tranches['payment_date'] >= df_tranches['legal_maturity_date']),
                                        (df_tranches['period_end_balance'] > 0),
                                        ['sub' not in x for x in df_tranches['security_level']])]
    if sum(exceed_mat) > 0:
        warn_info.append(("超过预期到期日未完成还款",
                          f"证券{df_tranches.loc[exceed_mat, 'security_code'].values}超过预期到期日未完成还款"))

    return df_tranches, warn_info
