# -*- coding: utf-8 -*-


from utils.timeutils import to_date2

from .prepsequence import transform_sequence
from .preptranches import transform_tranches
from .prepproduct import transform_product

from .preplan import trans_plan
from .preptrigger import transform_trigger
from .DateRule_new import gen_schedule, add_pool_date
from .prepcashflow_new import transform_cashflow
from .updateprediction import update_prediction


def data_full(df_product, df_tranches, df_prediction, df_sequence, df_plan, df_other, df_trigger, events,
              df_date, if_recal, split_default, df_daterule, df_calendar):
    """
    数据检查和填充，输入未经处理的数据，1. 缺少必要数据时进行报错；2. 将数据转化为模型需要的格式

    Args:
        df_product: DF, 项目基本信息
        df_tranches: DF, 证券基本信息
        df_prediction: DF, 现金流归集表
        df_sequence: DF, 支付顺序
        df_plan: DF, 摊还计划表
        df_other: DF, 其他信息
        df_trigger: DF, 触发事件
        df_date: DF, 证券日期
        events: dict, 重大事项发生情况
        if_recal: bool, 循环购买类是否重新计算现金流归集表
        df_daterule: 日期规则表
        df_calendar: 日历表
        split_default: bool, 是否将违约本金从剩余本金扣除，如果是，现金流归集表将可能减少正常回收，增加违约回收金额；如果否，则剩余违约本金与未违约的本金在现金流加压中完全视作一样

    Returns:
        tuple: 处理后的数据


    **逻辑**

    1. 根据 ``split_default`` 判断是否从资产池余额中扣除违约金额。在上一步读取数据时，读取的是包含了违约本金的合计值。
    如 ``split_default=True`` 则从资产池余额中扣除违约金额，同时设置违约本金额为读取到的值。
    如果 ``split_default=False`` , 则资产池余额包含了违约金额，同时设置违约本金额为0。即后面会将违约本金也视作正常的资产金额早偿、违约的模拟。

    2. 将资产池剩余本金余额乘以调整比例作为调整后的资产池剩余金额。

    3.  ``transform_product`` 处理 `df_prodcut`
    4. ``transform_tranches`` 处理 `df_tranches`
    5. ``gen_schedule`` 生成日历 `df_schedule` , 此时不包含归集日，
    6. 现金流归集表的处理分两步， 先用 ``transform_cashflow`` 处理现金流归集表。如果是非循环购买重新测算的情况，然后截取现金流归集表中 'date_' 晚于历史证券支付日的部分，
    再用 ``update_prediction`` 根据调整后的资产池剩余金额调整现金流归集表，使得期初本金余额等于调整后的资产池剩余金额。
    完成这一步后将归集日加入到日历中。此时如果是循环购买，归集日是从日期规则表读取的，如果是其他情况，则是从处理完的现金流归集表直接提取的 'date_' 列

    7. ``trans_plan`` 处理 `df_plan`
    8. ``trans_sequence`` 处理支付顺序 `df_sequence`

    TODO:
       现金流归集表的支付日列可能存在维护不正确
    """

    # 0. prepare
    warns_lst = []

    prediction_report_date = to_date2(df_other.loc[0, 'prediction_report_date'])
    default_principal = df_other.loc[0, 'default_principal']
    is_rev = df_product.loc[0, 'is_revolving_pool']
    prin_adjust_factor = df_other.loc[0, 'adjust_factor']
    virtual_history_date = to_date2(df_other.loc[0, 'virtual_history_date'])

    if split_default:
        remaining_principal = max(df_other.loc[0, 'remaining_principal'] - default_principal, 0)
    else:
        remaining_principal = df_other.loc[0, 'remaining_principal']
    # 0.1 调整资产池剩余本金余额

    remaining_principal *= prin_adjust_factor
    default_principal *= prin_adjust_factor

    # 1. 项目基本信息
    max_maturity_date = to_date2(df_tranches['legal_maturity_date'].dropna().max())
    df_product, warns_lst_1 = transform_product(df_product, max_maturity_date)
    warns_lst = warns_lst + warns_lst_1
    product = list(df_product.itertuples(index=False))[0]

    # 1.2 检查现金流归集表是否存在且日期要在将来（如果是循环购买重新计算现金流归集表的不需要必须存在）
    if (len(df_prediction) < 1) and (not (if_recal and is_rev)):
        if to_date2(prediction_report_date) == product.initial_date:
            raise ValueError("该项目没有初始现金流归集表，也没有存续归集表")
        else:
            raise ValueError(f"报告期为{prediction_report_date}的资产归集表为空")

    # 2. 证券信息处理
    df_tranches, warns_lst1 = transform_tranches(df_tranches, df_sequence, is_rev)
    warns_lst = warns_lst + warns_lst1
    tranches = list(df_tranches.itertuples(index=False))

    workday_schedule = df_calendar

    # 3. 摊还计划表
    df_plan, df_tranches, warns_lst_plan = trans_plan(df_plan, df_tranches, workday_schedule)
    warns_lst = warns_lst + warns_lst_plan
    # 4. 生成日历
    df_schedule, warns_lst_sche = gen_schedule(tranches=tranches, product=product, date_rule=df_date, df_calendar=df_calendar)
    warns_lst = warns_lst + warns_lst_sche

    # 5. 根据最新资产池数据，更新现金流归集表(截掉历史数据，并根据最新披露的剩余本金进行调整）

    df_prediction.loc[:, ['date_', 'pay_date', 'pool_date']] = \
        df_prediction[['date_', 'pay_date', 'pool_date']].applymap(lambda x:
                                                                   to_date2(x) if str(x) != 'nan' else float('nan'))

    df_prediction = transform_cashflow(df_prediction, product.initial_date,
                                       product.revolving_expiry_date, is_rev, workday_schedule)

    df_schedule = add_pool_date(df_schedule, df_prediction, is_rev, if_recal, product, df_daterule,
                                                eoaa=events.get('eoaa', False),
                                                rev_end_date=product.revolving_expiry_date)

    if not (if_recal and is_rev) :  # 一般情形下需要现金流归集表的金额很重要，需要根据资产池余额更新。如果需要从新测算现金流的则不重要

        df_prediction = df_prediction[df_prediction.date_ > to_date2(virtual_history_date)].reset_index(drop=True)
        df_prediction = update_prediction(initial=df_prediction, current_balance=remaining_principal)
        if len(df_prediction) < 1:
            raise ValueError("现金流归集表过短")
    # 6. 支付顺序表导入与调整
    _levels = df_tranches['security_level'].values
    pre_sub_levels = [x for x in _levels if 'sub' not in x]
    df_sequence = transform_sequence(df_sequence, pre_sub_levels)

    df_other['remaining_principal'] = remaining_principal # 更新资产池余额
    df_other['default_principal'] = default_principal
    df_other["pool_divide_security"] = \
        float(remaining_principal)/df_tranches['period_end_balance'].sum() \
            if df_tranches['period_end_balance'].sum() > 0 else float('nan') # 资产池剩余金额与证券剩余金额的比

    # 7. 触发事件表处理
    df_trigger = transform_trigger(df_trigger, product.initial_date)

    return df_product, df_tranches, df_prediction, df_sequence, df_plan, df_other, df_trigger, df_schedule, warns_lst
