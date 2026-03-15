# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
from utils.timeutils import to_date2, holiday_adjsut


def trans_plan(df_plan_initial, df_tranches, calendar=None):
    """
    处理摊还计划表

    Args:
        df_plan_initial (pd.DataFrame): 初始摊还计划表.
        df_tranches (pd.DataFrame): 证券要素表.

    Returns:
        tuple: tuple contains:
                df_plan : 调整后的摊还计划表.
                df_tranches: 将摊还方式中固定摊还根据摊还计划表处理后的结果

    **处理逻辑**

        1. 当目标本金支付和目标本金余额有任一为空时，用其中的另一个补充。
        2. 对于固定摊还的一定要有摊还计划表，如果没有则报错。
        3. 检查当前剩余金额，与摊还计划表的未来计划总额是否一样
    """
    warn_lst = []
    if len(df_plan_initial) > 0:
        df_plan_initial.loc[:, 'date_'] = df_plan_initial['date_'].apply(lambda x: to_date2(x))

        if calendar is not None:  # 进行节假日调整，避免与支付日序列对不上
            before_ = np.array(list(set(df_plan_initial['date_'])))
            before_.sort()
            after_ = holiday_adjsut(date_series=before_, schedule=calendar, holiday_rule='forward')
            dict_ = dict(zip(before_, after_))
            df_plan_initial.loc[:, 'date_'] = df_plan_initial['date_'].apply(lambda x: dict_[x])

        df_plan_initial.reset_index(inplace=True, drop=True)

        df_plan_initial.loc[:, 'security_level'] = df_plan_initial.loc[:, 'security_level'].apply(str.lower)

        df_plan = pd.DataFrame()
      
        for d, ini_prin in df_tranches.loc[:,
                           ['security_level', 'interest_type', 'initial_principal', 'amort_type',
                            'security_code', 'clear', 'payment_date', 'period_end_balance']].iterrows():
            sec_level = ini_prin.security_level
            sec_code = ini_prin.security_code
            if ini_prin.amort_type != 'fixed':
                continue

            sec_initial_principal = ini_prin.initial_principal
            sec_plan = df_plan_initial[df_plan_initial['security_level'] == sec_level]
            sec_plan.reset_index(inplace=True)

            if len(sec_plan) > 0:
                if (not pd.isna(sec_plan.target_balance).all()) and pd.isna(sec_plan.target_principal_payment).all():  # 目标摊还金额列为空，目标余额不为空
                    df_tranches.loc[d, 'amort_type'] = 'target-balance'
                    sec_plan.loc[:, 'target_balance'].fillna(method='ffill', inplace=True)
                    sec_plan.loc[1::, 'target_principal_payment'] = \
                        sec_plan.iloc[0:-1]['target_balance'].values - sec_plan.loc[1::, 'target_balance'].values
                    sec_plan.loc[0, 'target_principal_payment'] = \
                        sec_initial_principal - sec_plan.loc[0, 'target_balance']

                #  检查当前剩余金额，与摊还计划表的未来计划总额是否一样
                if not ini_prin.clear:
                    prin_sum = \
                        sum(sec_plan.loc[sec_plan['date_'] > to_date2(ini_prin.payment_date),
                                         'target_principal_payment'])
                    if prin_sum <= 0 and ini_prin.period_end_balance > 0:
                        warn_lst.append(("摊还计划表问题", f"根据摊还计划证券{sec_code}应已完成还款,实际未完成"))

                    if prin_sum > 0 and ini_prin.period_end_balance > 0:
                        if abs(prin_sum - ini_prin.period_end_balance) > 10:

                            warn_lst.append(('摊还计划表金额问题', f"证券{sec_code}支付日{ini_prin.payment_date}"
                                                          f"以后的摊还计划应付本金总额{prin_sum}, 当前证券端剩余本金额为"
                                                          f"{ini_prin.period_end_balance}。当前等比例调整到与剩余本金额一致"))
                            sec_plan.loc[:, 'target_principal_payment'] = sec_plan['target_principal_payment'] / prin_sum\
                                                                   * ini_prin.period_end_balance
                            sec_plan.loc[:, 'target_balance'] = float('nan')

                # 补目标本金余额
                sec_plan.loc[:, 'target_balance'] = \
                    sec_plan['target_principal_payment'][::-1].cumsum()[::-1].shift(-1).fillna(0.)
                df_plan = df_plan.append(sec_plan)

            else:
                raise ValueError(f"证券{sec_code}的固定摊还证券缺少摊还计划")

        return df_plan, df_tranches, warn_lst
    else:
        return df_plan_initial, df_tranches, warn_lst
