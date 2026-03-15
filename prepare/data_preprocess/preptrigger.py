# -*- coding: utf-8 -*-

from abs.doc.enumerators import trigger_cond_dict, trigger_event_dict


def transform_trigger(df_trigger, initial_date):
    """
    处理触发事项表

    Args:
        df_trigger (pd.DataFrame): 原始触发事项表
        initial_date (datetime.date): 初始起算日

    Returns:
        df_trigger (pd.DataFrame): 处理后的触发事件表


    **逻辑**

        主要是将枚举值映射到程序能够读取的值, 映射关系见 ``abs.doc.enumerators`` 。映射关系里面没有的枚举值，则视作程序无法处理，予以剔除

    """
    # 1. 枚举值映射，数字单位转换
    df_trigger.rename(columns={'change_type': 'event_type',
                               'trigger_condition_segment': 'trigger_condition'}, inplace=True)

    df_trigger.loc[:, 'event_type'] = df_trigger['event_type'].map(trigger_event_dict) # 事件类型
    df_trigger.loc[:, 'trigger_condition'] = df_trigger['trigger_condition'].map(trigger_cond_dict)  # 触发条件
    df_trigger[['in_thresh', 'out_thresh']] = df_trigger[['in_thresh', 'out_thresh']].astype(float) / 100  # 敲入敲出条件
    df_trigger[['start_date', 'end_date']] = \
        df_trigger[['start_year', 'end_year']].applymap(lambda x:
                                                        initial_date.replace(initial_date.year + int(x))
                                                        if isinstance(x, (int, float)) else float('nan'))
    # 2. 如果仍是数字型的枚举值，或者是空的触发时间细分，是无法考虑的触发情况，予以剔除
    df_trigger.dropna(subset=['trigger_condition'], how='any', inplace=True)
    df_trigger = \
        df_trigger.loc[~(df_trigger['event_type'].str.isdigit()&df_trigger['trigger_condition'].str.isdigit()), :].reset_index(drop=True)

    # 3. 直接将in,out数据替换到条件表达式
    for i, row in df_trigger.iterrows():
        in_thresh = row.in_thresh
        out_thresh = row.out_thresh
        if in_thresh == in_thresh and out_thresh == out_thresh:
            lower_limit, upper_limit = in_thresh, out_thresh
        elif in_thresh == in_thresh and out_thresh != out_thresh:
            lower_limit, upper_limit = in_thresh, 'Inf'
        elif in_thresh != in_thresh and out_thresh == out_thresh:
            lower_limit, upper_limit = 0, out_thresh
        else:
            lower_limit, upper_limit = 'Inf', 'Inf'
        df_trigger.loc[i, 'trigger_condition'] = \
            row.trigger_condition.replace('in_thresh', str(lower_limit)).replace('out_thresh', str(upper_limit))

    df_trigger.drop(columns=['in_thresh', 'out_thresh', 'start_year', 'end_year'], inplace=True)
    df_trigger.dropna(subset=['event_type'], axis=0, inplace=True)
    df_trigger.reset_index(drop=True, inplace=True)
    return df_trigger
