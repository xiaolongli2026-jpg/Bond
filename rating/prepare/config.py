# -*- coding: utf-8 -*-

# 二级分类枚举映射
second_class_dict = {
    'RMBS': '1',
    'NPL': '6',
}

# 是否循环购买
is_revolving_dict = {
    'Y': '1',
    'N': '2'
}

# 评级映射
ranks = ['AAA+', 'AAA', 'AAA-', 'AA+','AA', 'AA-', 'A+', 'A', 'A-', 'BBB+', 'BBB', 'BBB-',
         'BB+', 'BB', 'BB-', 'B+', 'B', 'B-', 'CCC+', 'CCC', 'CCC-', 'CC+', 'CC', 'CC-', 'C+', 'C', 'C-']
rating_dict = dict(zip(ranks, range(len(ranks))))
rating_dict['CAAA'] = rating_dict['AAA+']
rating_dict['nan'] = float('nan')
rating_dict['None'] = float('nan')
rating_dict['NR'] = len(ranks) + 10  # NR 的区分度更大一些
"""将评级映射到数字，便于进行结果与隐含评级的对比
"""

# 压力乘数映射
multiplier_dict = {
    '1': {
        'AAA': 5,
        'AAA-': 4.8,
        'AA+': 4.5,
        'AA': 4.1,
        'AA-': 3.7,
        'A+': 3.3,
        'A': 3,
        'A-': 2.7,
        'BBB+': 2.5,
        'BBB': 2.3,
        'BBB-': 2.0,
        'BB+': 1.8,
        'BB': 1.5,
        'BB-': 1.2

    },
    '2': {
        'AAA': 5,
        'AAA-': 4.8,
        'AA+': 4.5,
        'AA': 4.1,
        'AA-': 3.7,
        'A+': 3.3,
        'A': 3,
        'A-': 2.7,
        'BBB+': 2.5,
        'BBB': 2.3,
        'BBB-': 2.0,
        'BB+': 1.8,
        'BB': 1.5,
        'BB-': 1.2
    },
    '3': {
        'AAA': 5,
        'AAA-': 4.8,
        'AA+': 4.5,
        'AA': 4.1,
        'AA-': 3.7,
        'A+': 3.3,
        'A': 3,
        'A-': 2.7,
        'BBB+': 2.5,
        'BBB': 2.3,
        'BBB-': 2.0,
        'BB+': 1.8,
        'BB': 1.5,
        'BB-': 1.2
    },
    '4': {
        'AAA': 5,
        'AAA-': 4.8,
        'AA+': 4.5,
        'AA': 4.1,
        'AA-': 3.7,
        'A+': 3.3,
        'A': 3,
        'A-': 2.7,
        'BBB+': 2.5,
        'BBB': 2.3,
        'BBB-': 2.0,
        'BB+': 1.8,
        'BB': 1.5,
        'BB-': 1.2
    },
    '5': {
        'AAA': 20.5,
        'AAA-': 18.9,
        'AA+': 17.0,
        'AA': 16.3,
        'AA-': 14,
        'A+': 12.9,
        'A': 11.8,
        'A-': 10.5,
        'BBB+': 9.5,
        'BBB': 5.5,
        'BB+': 5,
        'BB': 4.5,
        'BB-': 4,
    },
    '6': {
        'AAA': 0.8,
        'AAA-': 0.813,
        'AA+': 0.844,
        'AA': 0.875,
        'AA-': 0.906,
        'A+': 0.936,
        'A': 0.967,
        'A-': 0.988,
    },
    '7': {
        'AAA': 5,
        'AAA-': 4.8,
        'AA+': 4.5,
        'AA': 4.1,
        'AA-': 3.7,
        'A+': 3.3,
        'A': 3,
        'A-': 2.7,
        'BBB+': 2.5,
        'BBB': 2.3,
        'BBB-': 2.0,
        'BB+': 1.8,
        'BB': 1.5,
        'BB-': 1.2
    },
    '9': {
        'AAA': 5,
        'AAA-': 4.8,
        'AA+': 4.5,
        'AA': 4.1,
        'AA-': 3.7,
        'A+': 3.3,
        'A': 3,
        'A-': 2.7,
        'BBB+': 2.5,
        'BBB': 2.3,
        'BBB-': 2.0,
        'BB+': 1.8,
        'BB': 1.5,
        'BB-': 1.2
    },

}
"""dict: 各二级分类的压力乘数映射
"""

# 目标违约概率映射
default_prob_dict = {
    '1': {
        'AAA': 0.001,
        'AAA-': 0.003,
        'AA+': 0.007,
        'AA': 0.01,
        'AA-': 0.015,
        'A+': 0.02,
        'A': 0.028,
        'BBB+': 0.042,
        'BBB': 0.05,
        'BBB-': 0.077,
        'BB+': 0.11,
        'BB': 0.17,
        'BB-': 0.26
    },
    '2': {
        'AAA': 0.001,
        'AAA-': 0.003,
        'AA+': 0.007,
        'AA': 0.01,
        'AA-': 0.015,
        'A+': 0.02,
        'A': 0.028,
        'BBB+': 0.042,
        'BBB': 0.05,
        'BBB-': 0.077,
        'BB+': 0.11,
        'BB': 0.17,
        'BB-': 0.26
    },

    '3': {
        'AAA': 0.001,
        'AAA-': 0.003,
        'AA+': 0.007,
        'AA': 0.01,
        'AA-': 0.015,
        'A+': 0.02,
        'A': 0.028,
        'BBB+': 0.042,
        'BBB': 0.05,
        'BBB-': 0.077,
        'BB+': 0.11,
        'BB': 0.17,
        'BB-': 0.26
    },
    # 对公贷款已更新
    '4': {
        'AAA': 0.0002,
        'AAA-': 0.0004,
        'AA+': 0.0009,
        'AA': 0.0011,
        'AA-': 0.0017,
        'A+': 0.0041,
        'A': 0.0053,
        'A-': 0.0082,
    },
    '5': {
        'AAA': 0.0002,
        'AAA-': 0.0003,
        'AA+': 0.0005,
        'AA': 0.0008,
        'AA-': 0.0011,
        'A+': 0.0018,
        'A': 0.0024,
        'A-': 0.0036,
    },

    '7': {
        'AAA': 0.0006,
        'AAA-': 0.0012,
        'AA+': 0.0022,
        'AA': 0.0027,
        'AA-': 0.0035,
        'A+': 0.0099,
        'A': 0.0114,
        'A-': 0.0184,
        'BBB+': 0.0231,
        'BBB': 0.046,
        'BBB-': 0.0537,
        'BB+': 0.0796,
    },

}
"""dict: 各个二级分类下的目标违约概率映射
"""

# 情景映射
scenario_dict = {
    0: '基准条件',
    1: '回收率降为0，其他为基准条件',
    2: '回收率下降10%，其他为基准条件',  # 16
    3: '回收率降低20%，其他为基准条件',  # 2
    4: '回收率降低30%，其他为基准条件',  # 3
    5: '回收率降低50%，其他为基准条件',  # new
    6: '年化条件早偿率提高/降低25%，其他为基准条件',  # 4
    7: '年化条件早偿率提高/降低50%，其他为基准条件',  # 5
    8: '年化早偿率下降提高/降低75%，其他为基准条件',  # 20
    9: '年化条件早偿率提高200%/降低50%,其他为基准条件',  # 26
    10: '年化条件早偿率提高300%/降低75%,其他为基准条件',  # 27
    11: '年化条件早偿率提高100%,其他为基准条件',  # 17
    12: '年化条件早偿率提高300%,其他为基准条件',  # 23
    13: '违约分布前置/后置10%，其他为基准条件',  # 6
    14: '违约分布前置/后置20%，其他为基准条件',  # 7
    15: '违约分布前置10%，其他为基准条件',  # 12
    16: '违约分布前置20%，其他为基准条件',  # 13
    17: '利差缩减25个bp，其他为基准条件',  # 8
    18: '利差缩减50个bp，其他为基准条件',  # 9
    19: '回收率降低20%；年化条件早偿率上升/下降25%；违约分布前置/后置10%；利差缩减25个bp',  # 10
    20: '回收率降低30%；年化条件早偿率上升/下降50%；违约分布前置/后置20%；利差缩减50个bp',  # 11
    21: '回收率降低20%；年化条件早偿率上升/下降25%；违约分布前置10%；利差缩减25个bp',  # 14
    22: '回收率降低30%；年化条件早偿率上升/下降50%；违约分布前置20%；利差缩减50个bp',  # 15
    23: '回收率下降10%，年化条件早偿率上升50%，违约分布前置10%，利差缩减25bp',  # 18
    24: '回收率下降20%，年化条件早偿率上升100%，违约分布前置20%，利差缩减50bp',  # 19
    25: '回收率下降10%，年化条件早偿率下降50%，违约分布前置10%，利差缩减25bp',  # 21
    26: '回收率下降20%，年化条件早偿率下降75%，违约分布前置20%，利差缩减50bp',  # 22
    27: '回收率下降10%，年化条件早偿率上升100%，违约分布前置10%，利差缩减25bp',  # 24
    28: '回收率降低20%；年化条件早偿率上升300%；违约分布前置20%；利差缩减50个bp',  # 25
    29: '回收率下降10%，年化条件早偿率上升200%/下降50%，违约分布前置10%，利差缩减25bp',  # 28
    30: '回收率降低20%；年化条件早偿率上升300%/下降75%；违约分布前置20%；利差缩减50个bp',  # 29
    31: '回收率降低10%；年化条件早偿率上升/下降25%；违约分布前置10%；利差缩减25个bp',  # 30
    32: '回收率降低20%；年化条件早偿率上升/下降50%；违约分布前置20%；利差缩减50个bp',  # 31
    33: '回收金额30%向后延长6个月，其他为基准条件',  # 32
    34: '利差缩减25个bp，回收金额30%向后延长6个月',  # 33
    35: '利差缩减50个bp，回收金额30%向后延长6个月',  # 34
    36: '月还款率上升/下降20%',
    37: '月还款率上升/下降30%',
    38: '收益率降低20%',
    39: '收益率降低30%',
    40: '收益率降低50%',
    41: '循环购买率下降50%',
    42: '回收率下降20%，月还款率上升/下降20%，收益率降低20%，利差缩减25个bp',
    43: '回收率下降30%，月还款率上升/下降30%，收益率降低30%，利差缩减50个bp',
    44: '回收率下降20%，提前还款率下降50%，收益率降低50%，循环购买率下降50%，利差缩减50个bp'
}
"""dict: 各个加压场景的说明
"""

# 情景映射（数字）
scenario_nums_dict = {
    0: [{"rr_rate": 100, "cpr_rate": 100., "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 0.}],
    1: [{"rr_rate": 0., "cpr_rate": 100., "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 0.}],
    2: [{"rr_rate": 90, "cpr_rate": 100., "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 0.}],# 16
    3: [{"rr_rate": 80, "cpr_rate": 100., "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 0.}],# 2
    4: [{"rr_rate": 70, "cpr_rate": 100., "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 0.}],# 3
    5: [{"rr_rate": 50, "cpr_rate": 100., "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 0.}],# new
    6: [{"rr_rate": 100., "cpr_rate": 75, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 0.},
        {"rr_rate": 100., "cpr_rate": 125, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 0.}],# 4
    7: [{"rr_rate": 100., "cpr_rate": 50, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 0.},
        {"rr_rate": 100., "cpr_rate": 150, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 0.}],# 5
    8: [{"rr_rate": 100., "cpr_rate": 25, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 0.},
        {"rr_rate": 100., "cpr_rate": 175, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 0.}],# 20
    9: [{"rr_rate": 100., "cpr_rate": 50, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 0.},
        {"rr_rate": 100., "cpr_rate": 300, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 0.}],# 26
    10: [{"rr_rate": 100., "cpr_rate": 25, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 0.},
         {"rr_rate": 100., "cpr_rate": 400, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 0.}],# 27
    11: [{"rr_rate": 100., "cpr_rate": 200, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 0.}],# 17
    12: [{"rr_rate": 100., "cpr_rate": 400, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 0.}],  # 23
    13: [{"rr_rate": 100., "cpr_rate": 100., "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 10,
          "dp_direction": "back"},
         {"rr_rate": 100., "cpr_rate": 100., "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 10,
         "dp_direction": "front"}],  # 6
    14: [{"rr_rate": 100., "cpr_rate": 100., "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 20,
          "dp_direction": "back"},
         {"rr_rate": 100., "cpr_rate": 100., "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 20,
         "dp_direction": "front"}],  # 7
    15: [{"rr_rate": 100., "cpr_rate": 100., "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 10,
         "dp_direction": "front"}],  # 12
    16: [{"rr_rate": 100., "cpr_rate": 100., "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 20,
         "dp_direction": "front"}],  # 13
    17: [{"rr_rate": 100., "cpr_rate": 100., "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 25, "dp_rate": 0.}],# 8
    18: [{"rr_rate": 100., "cpr_rate": 100., "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 50, "dp_rate": 0.}],  # 9
    19: [{"rr_rate": 80, "cpr_rate": 125, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 25, "dp_rate": 10,
          "dp_direction": "back"},
         {"rr_rate": 80, "cpr_rate": 125, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 25, "dp_rate": 10,
         "dp_direction": "front"},
         {"rr_rate": 80, "cpr_rate": 75, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 25,
          "dp_rate": 10, "dp_direction": "back"},
         {"rr_rate": 80, "cpr_rate": 75, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 25,
          "dp_rate": 10, "dp_direction": "front"}],  # 10
    20: [{"rr_rate": 70, "cpr_rate": 150, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 50, "dp_rate": 20,
          "dp_direction": "back"},
         {"rr_rate": 70, "cpr_rate": 150, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 50, "dp_rate": 20,
         "dp_direction": "front"},
         {"rr_rate": 70, "cpr_rate": 50, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 50,
          "dp_rate": 20, "dp_direction": "back"},
         {"rr_rate": 70, "cpr_rate": 50, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 50,
          "dp_rate": 20, "dp_direction": "front"}], # 11
    21: [{"rr_rate": 80, "cpr_rate": 125, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 25, "dp_rate": 10,
          "dp_direction": "front"},
         {"rr_rate": 80, "cpr_rate": 75, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 25,
          "dp_rate": 10, "dp_direction": "front"}],  # 14
    22: [{"rr_rate": 70, "cpr_rate": 150, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 50, "dp_rate":20,
          "dp_direction": "front"},
         {"rr_rate": 70, "cpr_rate": 50, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 50,
          "dp_rate": 20, "dp_direction": "front"}],  # 15
    23: [{"rr_rate": 90, "cpr_rate": 150, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 25, "dp_rate": 10,
          "dp_direction": "front"}],  # 18
    24: [{"rr_rate": 80, "cpr_rate": 200, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 50, "dp_rate": 20,
          "dp_direction": "front"}],  # 19
    25: [{"rr_rate": 90, "cpr_rate": 50, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 25, "dp_rate": 10,
          "dp_direction": "front"}],  # 21
    26: [{"rr_rate": 80, "cpr_rate": 25, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 50, "dp_rate": 20,
          "dp_direction": "front"}],  # 22
    27: [{"rr_rate": 90, "cpr_rate": 200, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 25, "dp_rate": 10,
          "dp_direction": "front"}],  # 24
    28: [{"rr_rate": 80, "cpr_rate": 400, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 50, "dp_rate": 20,
          "dp_direction": "front"}],  # 25
    29: [{"rr_rate": 90, "cpr_rate": 300, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 25, "dp_rate": 10,
          "dp_direction": "front"},
         {"rr_rate": 90, "cpr_rate": 50, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 25, "dp_rate": 10,
          "dp_direction": "front"}],  # 28
    30: [{"rr_rate": 80, "cpr_rate": 400, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 50, "dp_rate": 20,
          "dp_direction": "front"},
         {"rr_rate": 80, "cpr_rate": 25, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 50, "dp_rate": 20,
          "dp_direction": "front"}],  # 29
    31: [{"rr_rate": 90, "cpr_rate": 125, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 25, "dp_rate": 10,
          "dp_direction": "front"},
         {"rr_rate": 90, "cpr_rate": 75, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 25, "dp_rate": 10,
          "dp_direction": "front"}],  # 30
    32: [{"rr_rate": 80, "cpr_rate": 100.5, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 50, "dp_rate": 20,
          "dp_direction": "front"},
         {"rr_rate": 80, "cpr_rate": 50, "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 50, "dp_rate": 20,
          "dp_direction": "front"}],  # 31
    33: [{"rr_rate": 100., "cpr_rate": 100., "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 0.,
          "dpp": 6, "portion": 30}],  # 32
    34: [{"rr_rate": 100., "cpr_rate": 100., "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 25, "dp_rate": 0.,
          "dpp": 6, "portion": 30}],  # 33
    35: [{"rr_rate": 100., "cpr_rate": 100., "pp_rate": 100., "yr_rate": 100., "rp_rate": 100., "bp_change": 50, "dp_rate": 0.,
          "dpp": 6, "portion": 30}],  # 34
    36: [{"rr_rate": 100., "cpr_rate": 100., "pp_rate": 120, "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 0.},
         {"rr_rate": 100., "cpr_rate": 100., "pp_rate": 80, "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 0.}],
    37: [{"rr_rate": 100., "cpr_rate": 100., "pp_rate": 130, "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 0.},
         {"rr_rate": 100., "cpr_rate": 100., "pp_rate": 70, "yr_rate": 100., "rp_rate": 100., "bp_change": 0., "dp_rate": 0.}],
    38: [{"rr_rate": 100., "cpr_rate": 100., "pp_rate": 100., "yr_rate": 80, "rp_rate": 100., "bp_change": 0., "dp_rate": 0.}],
    39: [{"rr_rate": 100., "cpr_rate": 100., "pp_rate": 100., "yr_rate": 70, "rp_rate": 100., "bp_change": 0., "dp_rate": 0.}],
    40: [{"rr_rate": 100., "cpr_rate": 100., "pp_rate": 100., "yr_rate": 50, "rp_rate": 100., "bp_change": 0., "dp_rate": 0.}],
    41: [{"rr_rate": 100., "cpr_rate": 100., "pp_rate": 100., "yr_rate": 100, "rp_rate": 50, "bp_change": 0., "dp_rate": 0.}],
    42: [{"rr_rate": 80, "cpr_rate": 100., "pp_rate": 120, "yr_rate": 80, "rp_rate": 100., "bp_change": 25, "dp_rate": 0.},
         {"rr_rate": 80, "cpr_rate": 100., "pp_rate": 80, "yr_rate": 80, "rp_rate": 100., "bp_change": 25, "dp_rate": 0.}],
    43: [{"rr_rate": 70, "cpr_rate": 100., "pp_rate": 130, "yr_rate": 70, "rp_rate": 100., "bp_change": 50, "dp_rate": 0.},
         {"rr_rate": 70, "cpr_rate": 100., "pp_rate": 70, "yr_rate": 70, "rp_rate": 100., "bp_change": 50, "dp_rate": 0.}],
    44: [{"rr_rate": 80, "cpr_rate": 50, "pp_rate": 100., "yr_rate": 50, "rp_rate": 50, "bp_change": 50, "dp_rate": 0.}]}
"""dict: 各个加压场景下，各压力参数占基准参数的比例
"""


# 情景设置
scenario_combination = {
    'non-revolve': {
        '1': [20, 19, 18, 17, 14, 13, 7, 6, 4, 3, 0],  # RMBS
        '2': [26, 25, 18, 17, 16, 15, 8, 7, 3, 2, 1, 0],  # 汽车贷
        '3': [24, 23, 18, 17, 16, 15, 11, 7, 3, 2, 1, 0],  # 消费贷
        '4': [28, 27, 18, 17, 16, 15, 12, 11, 3, 2, 1, 0],  # 企业贷
        '5': [30, 29, 18, 17, 16, 15, 10, 9, 3, 2, 1, 0],  # 小微企业贷
        '6': [35, 34, 33, 18, 17, 0],  # 不良贷
        '7': [22, 21, 18, 17, 16, 15, 7, 6, 4, 3, 0],  # 租赁贷
        '9': [22, 21, 18, 17, 16, 15, 7, 6, 4, 3, 0],  # 住房公积金贷款
    },
    'revolve': {
        '2': [43, 42, 4, 3, 37, 36, 39, 38, 18, 17, 0],  # 汽车贷
        # '3': [44, 3, 7, 40, 41, 18, 0],  # 消费贷
        '3': [43, 42, 4, 3, 37, 36, 39, 38, 18, 17, 0],  # 消费贷
        '5': [43, 42, 4, 3, 37, 36, 39, 38, 18, 17, 0],  # 小微企业贷
    }

}
"""dict: 各二级分类选择的加压情景
"""

# 评级方法映射
# data_source, rating_method, lognorm_source, tdr_source
rating_method_dict = {
    '1': ['bayesian_estimation', 'pressure_multiplier', None, 'calculation'],  # RMBS
    '2': ['rating_report', 'lognorm_distribution', 'rating_report', 'calculation'],  # 汽车贷
    '3': ['rating_report', 'pressure_multiplier', None, 'calculation'],  # 消费贷
    '4': ['rating_report', 'pressure_multiplier', None, 'rating_report'],  # 企业贷
    '5': ['rating_report', 'pressure_multiplier', None, 'calculation'],  # 小微企业贷
    '6': ['rating_report', 'pressure_multiplier', None, 'both'],  # 不良贷
    '7': ['rating_report', 'pressure_multiplier', None, 'rating_report'],  # 租赁贷
    '9': ['rating_report', 'pressure_multiplier', None, 'calculation'],  # 住房公积金贷款
}
"""dict: 各个二级分类选择的评级方法、数据来源
"""


rating_method_meanings = {'pressure_multiplier': '压力乘数法', 'lognorm_distribution': '对数正态分布法'}
data_source_meanings = {'rating_report': '评级报告', 'bayesian_estimation': '贝叶斯估计', 'calculation': '资产池外推', 'customize': '自定义'}
lognorm_source_meanings = {'rating_report': '评级报告', 'calculation': '根据同类项目数据估计', 'customize': '自定义'}
tdr_source_meanings = {'rating_report': '评级报告', 'calculation': '评级模型计算', 'customize': '自定义', 'both': "评级报告和模型均值"}


rating_indicator_dict = {
    'CDR': '1',
    'CPR': '2',
    'RR_MIN': '18',
    'RR_1': '19',
    'RR_2': '20',
    'RR_3': '21',
    'RR_4': '22',
    'RR_5': '23',
    'RR_6': '24',
    'RR_7': '25',
    'RR_8': '26',
    'RR_9': '27',
    'RR_10': '28',
    'RR_11': '29',
    'RR_12': '30',
    'RR_13': '31',
    'RR_14': '32',
    'RR_15': '33',
    'RR_YEAR': '34',
    'YR': '35',
    'PP': '36',
    'PR': '37',
    'HXL': '38',
    'MU': '39',
    'SIGMA': '40',
    'TDR_AAA': '41',
    'TDR_AAA-': '42',
    'TDR_AA+': '43',
    'TDR_AA': '44',
    'TDR_AA-': '45',
    'TDR_A+': '46',
    'TDR_A': '47',
    'TDR_A-': '48',
    'TRR_AAA': '49',
    'TRR_AAA-': '50',
    'TRR_AA+': '51',
    'TRR_AA': '52',
    'TRR_AA-': '53',
    'TRR_A+' : '54',
    'TRR_A': '55',
    'TRR_A-': '56',
    'CS_FEE': '57',
    'RR_MAO': '58',
    'RR_NET': '59'
}
"""dict: key-程序用到的值，value-评级报告中的名词，如果维护的跟value中的不一样，修改value即可
"""

threshold_npls = {'rr': [0.01, 0.8]}
"""不良贷款基准参数应有的阈值
"""

threshold = {'cdr': [0, 0.2],
             'cpr': [0, 0.5]}
"""非循环非不良的类型，基准参数的阈值
"""

threshold_rev = {'yr': [0.001, 1],
                 'cdr': [0, 0.2],
                 'cpr': [0, 0.9],
                 'pp': [0.01, 1],
                 'rp': [0.9, 1]}
"""循环购买基准参数的阈值
"""

implied_rank_track = "Z:\王西琦\估值结果监控"


threshold_rank = [-3, 3]
"""评级差异的阈值
"""