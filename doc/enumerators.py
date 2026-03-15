# -*- coding: utf-8 -*-
"""
保存枚举值映射关系

"""
import numpy as np


freq_to_month = {'monthly': (1, 'M'), 'quarterly': (3, 'M'), 'semiannual': (6, 'M'), 'annual': (12, 'M'),
                 'twomonths': (2, 'M'), 'twoweeks': (2, 'W')}
"""dict, 付息频率映射成 数字+频率类型"""

freq_to_month2 = {'monthly': 1, 'quarterly': 3, 'semiannual': 6, 'annual': 12, 
                  'twomonths': 2, 'twoweeks': 0.5,
                  'fixed': 1e4, 'once': 1e4, 'none': 1e4, 'nan': 1e4}

#  入池特征枚举
factor_dict_initial = {'loan_number': '110', 'initial_balance': '102', 'WAM': '203', 'WAA': '202',
                       'WAIR': '208', 'LTV': '306', 'WAIC': '302', 'RTL': '303'}

# 3034
loan_range_dict = {'prin_plus_int': '1', 'prin': '2', 'int': '3'}

# 3036
# 20210628 一般情况下10201、10200 枚举项和等于10200枚举项
Markov_absorb_dict = {'1': 'a_implement_sum', '2': 'a_n', '3': 'a_o', '4': 'a_d', '5': 'a_p',
                      '6': 'a_d', '7': 'a_n', '8': 'a_p',
                      '9': 'a_sum', '12': 'a_d', '14': 'a_d', '17': 'a_imple_p', '18': 'a_imple_p', '19': 'a_o',
                      '20': 'a_n', '21': 'a_d', '22': 'a_d'}  #清仓回购、赎回都视作违约回收
"""dict, 将资产池回收状态 ``csi_abs_duration_collection`` 表中的状态枚举值映射到马尔可夫转移矩阵能够处理的状态。
其中带 '_imple'的指的是用于补充的数据，比如 'a_p' 对应的枚举值'5'在数据库中的值是0时，将 'a_imple_p'对应的 '17', '18'对应的值加总，
因为枚举值5指的是早偿，而'17'、'18'分别指的是部分早偿和全额早偿，相当于是'5'的子集，加总起来可以用来补枚举为 '5'的数据的缺失。 \n
而带 '_sum' 的表示合计值，比如枚举为'1' 的是不区分状态的、当期资产回收合计值
"""
# 3035
# 20210628 严重拖欠贷款不算违约贷款
Markov_temp_dict = {'1': 't_sum', '2': 't_n', '3': 't_o_sum', '4': 't_o_1_30',
                    '5': 't_o_31_60', '6': 't_o_61_90', '7': 't_d',
                    '8': 't_d', '9': 't_d', '10': 't_d', '11': 'a_d', '12': 't_o_1_30',
                    '13': 't_d', '14': 't_o_31_90', '15': 't_o_31_90', '16': 't_o_31_90',
                    '22': 't_d', '23': 't_d', '24': 't_d',  '25': 't_d',
                    '27': 't_d', '28': 't_d', '30': 't_o_1_30', '31': 't_o_1_30'}

SecondClass = {
    "RMBS": '1',
    "qichedai": '2',
    "NPLS": '6'}

IsRevolving = {
    "Y": '1',
    "N": '2'}

interest_type_dict = {'1': 'fixed', '2':'floating', '3': 'fixed', '4': 'fixed', '5': 'fixed'}
if_qjsy_dict = {'1': np.nan, '2': np.nan, '3': 1, '4': np.nan, '5': np.nan}

frency_dict = {'1': np.nan, '2': 'monthly', '3': 'quarterly', '4': 'semiannual', '5':'annual', '6':'twoweeks',
                '7': 'twomonths', '8': 'once', '9': 'fixed', '10': np.nan, '97': 'fixed', '98': np.nan, '99':np.nan}


amort_type_dict = {'2': 'fixed', '1': 'pass-through', '3': 'once'}

DCC_dict = {'1': 'ACT/ACT', '2':'ACT/ACT', '3': 'ACT/365', '4': 'ACT/365','5': 'ACT/360', '6': '30/360'}


# 日期规则
# 3038 
date_name_dict = {'1': 'pool_date', '2': 'prin_date', '4': 'prin_date', '5': 'int_date', '6': 'rev_date', '7': 'rev_date'}

# 3039
date_type_dict = {'1': 'fix_day', '2': 'fix', '3': 'fix_weekday', '4': 'fix_interval', '5': 'relative'}

# 3040 
date_cond_dict = {'condless': '1', 'pref_clear': '2', 'eoaa': '3', 'not_inamortperiod': '4', 'inamortperiod': '5'}

# 3041
day_type_dict = {'1': 'weekday', '2': 'workday'}

# 3042 
holiday_rule_dict = {'keep': '1', 'next_bd': '2', 'last_bd': '3'}

# 支付顺序表
# 3043
node_type_dict = {'1': 'other', '2': 'judge', '3': 'branch', '4': 'pay', '5': 'pay', '6': 'pay', '7': 'pay'}

# 3044
money_source_dict = {'1': 'int_col', '2': 'prin_col', '3': 'total_col'}

issuerCode = ('8', '32', '5')

#3031 触发细节 只能完成部分
trigger_cond_dict = {'3': 'not(priority.expect_maturity_prin_clear)',
                     '4': 'not(priority.expect_maturity_target_clear)',
                     '5': 'and(cdr>in_thresh,cdr<out_thresh,not(inamortperiod))',
                     '6': 'and(cdr>in_thresh,cdr<out_thresh,inamortperiod)',
                     '8': 'not(priority.int_clear)',
                     '9': 'not(priority.legal_maturity_balance_clear)',
                     '10': 'not(priority.int_clear)',
                     '11': 'and(cdr>in_thresh,cdr<out_thresh)',
                     '12': 'not(priority.prin_clear)',
                     '13': 'return_decline>in_thresh',
                     '15': 'not(exp_clear)',
                     '16': 'not(priority.int_clear)',
                     '17': 'not(priority.prin_clear)',
                     '24': 'and(pool_principal/all_sec.balance>in_thresh,pool_principal/all_sec.balance<out_thresh,asset_under_debt_period>2)',
                     '25': 'and(pool_principal/all_sec.balance>in_thresh,pool_principal/all_sec.balance<out_thresh,not(inamortperiod))',
                     '33': 'and(cdr>in_thresh,cdr<out_thresh)',
                     '36': 'total_col/all_sec.notional>in_thresh',
                     '38': 'and(cdr>in_thresh,cdr<out_thresh)',
                     '44': 'not(and(exp_clear,priority.prin_clear,priority.int_clear))',
                     '45': 'vacant_period>6',
                     '47': 'not(exp_clear)',
                     '49': 'not(A.clear)',
                     '50': 'not(B.clear)',
                     '51': 'not(C.clear)',
                     '52': 'not(D.clear)',
                     '53': 'not(E.clear)',
                     '54': 'not(A1.clear)',
                     '55': 'not(A2.clear)',
                     '56': 'not(A3.clear)',
                     '57': 'not(A4.clear)',
                     '58': 'not(A5.clear)',}

# 将 触发事件表事件类型字段(3030) 重大事项表 事项类型(3032) 支付顺序表分支条件/节点条件 联系起来
# 3030
trigger_event_dict = {'1': 'eoaa', '2': 'eod', '3': 'eoce', '4': 'liq_support', '5': 'pre_amort',
                      '6': 'pre_term', '7': 'expect_pay', '8': 'diff_pay', '9': 'repur', '10': 'etrp',
                      '11': 'payment_start', '12': 'fbb', '13': 'liq_support', '14': 'trustee_warn', '15': 'guarantee',
                      '16': 'right_complete', '17': 'early_collect', '18': 'etrp'}
# 重大事项
happend_event_dict = {'1': 'eoaa', '2': 'eod', '3': 'server_dismiss', '4': 'right_complete', '6': 'eoce', '7': 'fbb',
                      '8': 'loss_ability', '9': 'trust_dismiss', '10': 'harmful_event',
                      '11': 'loan_server_dismiss', '12': 'keeper_dismiss', '17': 'rank_change', '19': 'liq_support',
                      '21': 'liq_support', '22': 'etrp', '25': 'diff_pay', '27': 'harmful_event', '29': 'trust_term',
                      '30': 'coupon_adjust', '34': 'trust_dismiss'
                      } # 将重大事项与支付顺序匹配

# 事件类型与对应中文
event_meaning_dict = dict([('eoaa', '加速清偿事件'),
      ('eod', '违约事件'),
      ('eoce', '强制执行事件'),
      ('liq_support', '流动性支持触发事件'),
      ('pre_amort', '提前摊还事件'),
      ('pre_term', '提前终止事件'),
      ('expect_pay', '预期兑付事件'),
      ('diff_pay', '差额支付启动事件'),
      ('trustee_warn', '信托预警事件'),
      ('repur', '循环购买事件'),
      ('etrp', '提前结束循环期事件'),
      ('guarantee', '担保启动事件'),
      ('payment_start', '付款启动事件'),
      ('fbb', '清仓回购'),
      ('earply_collect', '加速归集事件'),
      ('server_dismiss', '资产服务机构解任'),
      ('right_complete', '权利完善事件'),
      ('loss_ability', '丧失清偿能力'),
      ('trust_dismiss', '受托机构解任事件'),
      ('harmful_event', '重大不利影响事件'),
      ('loan_server_dismiss', '贷款服务机构解任'),
      ('keeper_dismiss', '资金保管机构解任'),
      ('rank_change', '优先级资产支持证券的信用评级发生变化'),
      ('trust_term', '信托终止事件'),
      ('coupon_adjust', '委托人行驶票面利率调整权'),])

# 将支付顺序中节点条件进行隐射, 表示非xxx的字段映射为not(xxx), 处理后的结果与tranche对象、trigger对象属性名称一致。
cond_pairs = [('noteoaa', 'not(eoaa)'),
                   ('notinamortperiod', 'not(inamortperiod)'),
                   ('noteod', 'not(eod)'),
                   ('notetrp', 'not(etrp)'),
                   ('notfbb', 'not(fbb)'),
                   ('noteoce', 'not(eoce)'),
                   ('notclear', 'not_clear'),# 是否清算
                   ('eodnotcausedbyservicer', 'not(eodcausedbyservicer)'),
                   ('eoaanotcausedbyservicer', 'not(eoaacausedbyservicer)'),
                   ('eodnotcausedbyguarantor', 'not(eodcausedbyguarantor)'),
                   ('eoaanotcausedbyguarantor', 'not(eoaacausedbyguarantor)'),
                   ('onintdates', 'on_int_date'),
                   ('onprindates', 'on_prin_date'),
                   ('beforeintdates', 'before_int_date'),
                   ('beforeprindates', 'before_prin_date'),
                   ('afterintdates', 'after_int_date'),
                   ('afterprindates', 'after_prin_date'),
                   ('fcc.clear', 'fcc_clear'),
                   # 以前的枚举值,现在没有
                   ('notbigeoaa', 'not(eoaa)'),
                   ("afterplan", "after_plan"),
                   ("intcol", 'eternal_True'),  # 本金帐收益帐节点一定会进入
                   ("princol", 'eternal_True'),
                   ("intday", 'eternal_True'),
                   ("prinday", 'eternal_True'),
                   ("notprinday", 'eternal_True'),
                   ]

# 将支付上限进行映射
limit_pairs = [
    ("cumloss", "cum_loss"),
    ("cumprintoint", "cum_prin_to_int"),
    ("cuminttoprin", "cum_int_to_prin"),
    ('cumpaiddiff', 'cum_paid_diff'),
    ("targetprinpayment", "target_prin_payment"),
    ("targetbalance", "target_balance"),
    ("targetendbalance", "target_balance"),  # 录第一批数据时目标余额是录成 TargetEndBalance
    ("princol", "prin_col"),
    ("intcol", "int_col"),
    ("expectedtax", "expected_tax"),
    ("expectedexp", "expected_exp"),
    ("expectedint", "expected_int"),]

# 资金去向一些科目需要忽略
target_pairs = [('defint', 'ignore'),
                 ('int_col', 'ignore'), #遇到维护成本金帐转收益帐的暂时忽略，程序默认本金帐会补收益帐的不足
                 ('guarantor', 'ignore'),
                 ('optionpremium', 'ignore'),
                 ('liqres', 'ignore'),
                 ('serres', 'ignore'),
                 ('princol', 'prin_col'),
                 ('intcol', 'int_col'),
                 ('incentivefee', 'incentive_fee')]


# 一般用本息和模式的二级分类
mix_prin_interest_class = []


# 市场后缀
market_suffix = {'1': 'SH', '2': 'SZ', '3': 'IB'}