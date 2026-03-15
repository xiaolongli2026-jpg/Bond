# -*- coding: utf-8 -*-
"""将输入参数处理成模型处理的标准化格式

"""

import numpy as np
import pandas as pd
from conf.conf import default_params


def calculator_input_preprocess(params):
    """将输入的参数从dict中取出，并处理成现金流计算器所需的标准化格式

    Returns:
        见 ``abs_calculator`` 中对入参的说明，只不过原来带单位的输入全部转为了无单位
    """

    security_code, trade_date, value_method, curve_name,scenario_type, model_type, bayes, n_markov, input_type, \
    input_value, coupon_rate_change, recal_cashflow,\
    CDR, CPR, RR, exist_tax_exp, exp_rate, tax_rate, DPP, suoe, PP, YR, RP, upload,\
    DPs, CPRs, RRs, DR, CDRs,PPs, YRs, DRs, YCDR, YCDRs, is_security, module_type, cur, \
    split_default, begin_default_recover, add_remain_account, portion, dp_match_perfectly, day_count_method, \
    param_match_method, minus_CDR, cpr_type, same_type, compensate\
        = unpack_params(params)

    interest_method = None
    if scenario_type == 'model':  # 模型加压时，统一调整参数为序列型
        module_type = 'series_normal1'
        DPs = None # 不需要，由模型生成
        CDR = None
    else:

        if not ((DR is None) and (DRs is None)):
            interest_method = 'discount'  # 循环购买池的利息计算方式
        else:
            interest_method = 'yield_'

    if module_type == 'static_rev_recal':  # 循环购买重新算的都转成序列型的参数使用
        module_type = 'series_rev_recal'
        if interest_method == 'yield_':
            YRs = pd.Series(YR, index=range(1, 2))
        elif interest_method == 'discount':
            DRs = pd.Series(DR, index=range(1, 2))
        RRs = pd.Series([RR], index=[int(DPP)])
        CDRs = pd.Series(CDR, index=range(1, 2))
        CPRs = pd.Series(CPR, index=range(1, 2))
        month_payback = int(np.ceil(1 / PP))
        PPs = pd.Series(PP, index=range(1, month_payback + 1))
        PPs.iloc[-1] = 1 - PP * (month_payback - 1)

    coupon_rate_change, CDR, CPR, RR, exp_rate, tax_rate, DPP, PP, YR, RP, \
    DPs, CPRs, RRs, DR, CDRs, PPs, YRs, DRs, YCDR, YCDRs, module_type = \
        params_unit_transform(exist_tax_exp, coupon_rate_change,
                          CDR, CPR, RR, exp_rate, tax_rate, DPP, PP, YR, RP, DPs, CPRs, RRs, DR,
                          CDRs, PPs, YRs, DRs, YCDR, YCDRs, portion, module_type, scenario_type, model_type)

    return security_code, trade_date, value_method, curve_name,\
        scenario_type, model_type, bayes, n_markov, input_type, input_value, coupon_rate_change, recal_cashflow,\
        CDR, CPR, RR, exist_tax_exp, exp_rate, tax_rate, DPP, suoe, PP, YR, RP, upload,\
        DPs, CPRs, RRs, DR, CDRs, PPs, YRs, DRs, YCDR, YCDRs, portion, module_type, interest_method, is_security, \
        cur, split_default, begin_default_recover, add_remain_account, dp_match_perfectly, \
        day_count_method, param_match_method, minus_CDR, cpr_type, same_type, compensate


def params_unit_transform(exist_tax_exp, coupon_rate_change,
                          CDR: (float, int)=None, CPR: (float, int)=None, RR: (float, int)=None,
                          exp_rate: (float, int)=None, tax_rate: (float, int)=None,
                          DPP: int=None, PP: (float, int)=None, YR: (float, int)=None, RP: (float, int)=None,
                          DPs: pd.Series=None, CPRs: pd.Series=None, RRs: pd.Series=None, DR: (float, int)=None,
                          CDRs: pd.Series=None, PPs: pd.Series=None, YRs: pd.Series=None,
                          DRs: pd.Series=None, YCDR: int=None, YCDRs: pd.Series=None, portion=0.,
                          module_type='static_normal1', scenario_type='user', model_type=None):
    """
    将带单位的输入参数转化为模型需要的形式

    Args:
        coupon_rate_change: (float, int), 利差变动
        CDR: (float, int), 累计违约率, 单位:%
        CPR: (float, int), 年化早偿率, 单位:%
        RR: (float, int), 违约回收率, 单位:%
        exist_tax_exp: bool, 是否计算税费
        exp_rate: (float, int), 费率, 单位:%
        tax_rate: (float, int), 税率, 单位:%
        DPP: int, 延迟回收月份
        PP: (float, int), 月摊还比例, 单位:%
        YR: (float, int), 资产池收益率, 单位:%
        RP: (float, int), 循环购买率, 单位:%
        DPs: pd.Series, 违约分布, 单位:%
        CPRs: pd.Series, 年化违约率序列, 单位:%
        RRs: pd.Series, 违约回收率序列, index是延迟回收月份, value是对应时间的回收率, 单位:%
        DR: (float, int), 折价率，与YR为替代关系, 单位:%
        CDRs: pd.Series, 累计违约率序列, 适用于循环购买中的动态假设, 单位:%
        PPs: pd.Series, 摊还比例序列, 单位:%
        YRs: pd.Series, 收益率序列, 单位:%
        DRs: pd.Series, 折价率序列, 单位:%
        YCDR: (float, int), 年化违约率, 单位:%
        YCDRs: pd.Seriesk, 年化违约率序列, 单位:%

    Returns:
        tuple: 输出结果全部转化为了无单位，比如原来单位是%的除以100，原来单位是BP的除以1000

    """
    coupon_rate_change = float(coupon_rate_change) * 0.0001

    if exist_tax_exp:
        exp_rate = float(exp_rate) * 0.01
        tax_rate = float(tax_rate) * 0.01

    else:
        exp_rate = 0
        tax_rate = 0

    # 关键参数的单位转换
    if scenario_type == 'model':  # 模型估计的模式下只用series_normal1
        if model_type == 'linear_model' or model_type == 'sigmoid_model':
            RRs = pd.Series(RR * 0.01, index=[DPP]) if RRs is None else RRs * 0.01
        elif model_type == 'extrapolate_model':
            RRs = pd.Series(RR * 0.01, index=[DPP]) if RRs is None else RRs * 0.01
            CPRs = pd.Series(CPR * 0.01, index=[1]) if CPRs is None else CPRs * 0.01
    else:
        if module_type == 'static_normal1':
            CDR, CPR, RR = CDR * 0.01, CPR * 0.01, RR * 0.01
            DPP = int(DPP)
        elif module_type == 'static_normal2':
            YCDR, CPR, RR = YCDR * 0.01, CPR * 0.01, RR * 0.01
            DPP = int(DPP)
        elif module_type == 'series_normal1':
            CDR, DPs, CPRs, RRs = CDR * 0.01, DPs * 0.01, CPRs * 0.01, RRs * 0.01
        elif module_type == 'series_normal2':
            YCDRs, CPRs, RRs = YCDRs * 0.01, CPRs * 0.01, RRs * 0.01
        elif module_type == 'static_npl_recal':
            RR *= 0.01
            portion *= 0.01
            DPP = int(DPP) if DPP is not None else 0
        elif module_type == 'static_rev_recal':
            CDR, CPR, RR, PP, RP = CDR * 0.01, CPR * 0.01, RR * 0.01, PP * 0.01, RP * 0.01
            YR = YR * 0.01 if YR is not None else None
            DR = DR * 0.01 if DR is not None else None
            DPP = int(DPP)
        elif module_type == 'series_rev_recal':
            CDRs, CPRs, RRs, PPs, RP = CDRs * 0.01, CPRs * 0.01, RRs * 0.01, PPs * 0.01, RP * 0.01
            YRs = YRs * 0.01 if YRs is not None else None
            DRs = DRs * 0.01 if DRs is not None else None

    return coupon_rate_change, CDR, CPR, RR, exp_rate, tax_rate, DPP, PP, YR, RP,\
        DPs, CPRs, RRs, DR, CDRs, PPs, YRs, DRs, YCDR, YCDRs, module_type


def unpack_params(params_0: dict):
    """将输入的params: dict中的参数拆出来, 因为初始版本的代码是这种模式，后面不会再用这种入参模式。


    Notes:
        部分参数规定了默认参数，具体在配置文件 `doc.conf` 中规定。也可以对单个项目进行自定义。
    """
    params = params_0.copy()
    for x in params_0: # 转化下格式，避免string形式的True False None
        if str(params_0[x]) == 'None':
            params.pop(x)

        if str(params_0[x]).lower() == 'true' or str(params_0[x]).lower() == 'false':
            params[x] = bool(params_0[x])

    # 页面输入根据种类做区分，这里尚未读取种类，故不做区分，未输入的参数均设置为None
    params = dict(zip([x.lower() for x in params], params.values()))

    security_code = params["security_code"]
    trade_date = params["trade_date"]

    scenario_type = params.get('scenario_type', 'user')
    module_type = params.get('module_type', None)
    model_type = params.get("model_type", None)

    input_type = params.get('input_type', None)
    input_value = float(params.get('input_value', 'nan'))

    if input_type == 'yield_':
        value_method = 'yield_'
    elif input_type == 'spread':
        value_method = 'curve'
    else:
        value_method = params.get('value_method', None)

    curve_name = params.get('curve_name', None)

    coupon_rate_change = params.get('coupon_rate_change', 0.)

    recal_cashflow = params.get("recal_cashflow", False)

    CDR = params.get("cdr", None)
    CPR = params.get("cpr", None)
    RR = params.get("rr", None)
    YCDR = params.get("ycdr", None)

    DPs = params.get('dps', None)
    CPRs = params.get("cprs", None)
    RRs = params.get("rrs", None)
    DPP = params.get("dpp", None)  # 违约回收延迟月数
    portion = params.get('portion', 0.)
    YCDRs = params.get("ycdrs", None)

    exist_tax_exp = params['exist_tax_exp']
    exist_tax_exp = {"true": True, "false": False}[str(exist_tax_exp).lower()]
    exp_rate = params.get("exp_rate", 0.)
    tax_rate = params.get("tax_rate", 0.)

    suoe = params["suoe"]
    PP = params.get("pp", None)
    YR = params.get("yr", None)
    RP = params.get("rp", None)
    DR = params.get("dr", None)

    CDRs = params.get("cdrs", None)
    PPs = params.get("pps", None)
    YRs = params.get("yrs", None)
    DRs = params.get("drs", None)

    upload = params.get('upload', False)

    is_security = params.get('is_security', default_params['is_security'])  # 区分计算范围，True只计算单个证券估值，False计算项目下所有证券的估值
    split_default = params.get('split_default', default_params['split_default'])  # 是否从最新剩余本金中扣除违约部分
    begin_default_recover = params.get('begin_default_recover', default_params['begin_default_recover']) # 当前剩余违约金额是否需要计算违约回收
    add_remain_account = params.get('add_remain_account', default_params['add_remain_account'])  # 是否把当前剩余资金加到现金流里面
    cur = params.get('cur', None)

    day_count_method = params.get('day_count_method', default_params['day_count_method'])
    dp_match_perfectly = params.get('dp_match_perfectly', default_params['dp_match_perfectly'])
    param_match_method = params.get('param_match_method', default_params['param_match_method'])  # 默认值
    minus_CDR = params.get('minus_CDR', default_params['minus_CDR'])
    cpr_type = params.get('cpr_type', default_params['cpr_type'])
    same_type = params.get('same_type', default_params['same_type'])
    compensate = params.get('compensate', default_params['compensate'])
    bayes = params.get("bayes", default_params['bayes'])
    n_markov = params.get("n_markov", default_params['n_markov'])

    # 检查参数参数类型是否正确
    assert scenario_type in ('user', 'model'), "错误的scenario_type %s" %scenario_type
    assert module_type in ('static_normal1', 'static_normal2', 'series_normal1', 'series_normal2', 'static_npl_recal',
                           'static_rev_recal', 'series_rev_recal', None) , "错误的module_type %s" %module_type
    assert model_type in ('linear_model', 'sigmoid_model', 'markov_model', 'extrapolate_model', None), "model_type %s" %model_type
    assert type(bayes) is bool or bayes is None, "错误的bayes %s" %bayes
    assert type(n_markov) is bool or n_markov is None, "错误的n_markov %s" % n_markov
    assert type(recal_cashflow) is bool or recal_cashflow is None, "错误的recal_cashflow %s" % recal_cashflow
    assert type(exist_tax_exp) is bool, "错误的exist_tax_exp %s" % exist_tax_exp
    assert type(suoe) is bool, "错误的suoe %s" % suoe
    assert type(is_security) is bool, "错误的is_security %s" % is_security
    assert type(split_default) is bool, "错误的split_default %s" % split_default
    assert type(begin_default_recover) is bool, \
        "错误的begin_default_recover %s" % begin_default_recover
    assert type(add_remain_account) is bool, "错误的add_remain_account %s" % add_remain_account
    assert day_count_method in ('begin', 'end'), "错误的day_count_method %s" % day_count_method
    assert type(dp_match_perfectly) is bool, "错误的dp_match_perfectly %s" % dp_match_perfectly
    assert type(minus_CDR) is bool, "错误的minus_CDR %s" % minus_CDR
    assert type(same_type) is bool, "错误的same_type %s" % same_type
    assert cpr_type in ('type1', 'type2'), "错误的cpr_type %s" % cpr_type
    assert param_match_method in ('remain', 'all'), "错误的param_match_method %s" % param_match_method
    assert type(compensate) is bool, "错误的compensate %s" % compensate

    return (
        security_code,
        trade_date,
        value_method,
        curve_name,
        scenario_type,
        model_type,
        bayes,
        n_markov,
        input_type,
        input_value,
        coupon_rate_change,
        recal_cashflow,
        CDR,
        CPR,
        RR,
        exist_tax_exp,
        exp_rate,
        tax_rate,
        DPP,
        suoe,
        PP,
        YR,
        RP,
        upload,
        DPs,
        CPRs,
        RRs,
        DR,
        CDRs,
        PPs,
        YRs,
        DRs,
        YCDR,
        YCDRs,
        is_security,
        module_type,
        cur,
        split_default,
        begin_default_recover,
        add_remain_account, portion,
        dp_match_perfectly,
        day_count_method,
        param_match_method,
        minus_CDR,
        cpr_type,
        same_type,
        compensate)

