# -*- coding: utf-8 -*-
"""日常量化评级
"""


from main.production.pscheck_add import main_batch_rating
from datetime import datetime, timedelta
from main.production.filter import (new_project, updated_project_list, get_last_rundate,
                                    get_last_date_list, read_input_rating)


if __name__ == '__main__':
    full_run = True
    trade_date = '20240119'
    # trade_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    # trade_date = datetime.now().strftime("%Y%m%d")
    test_df = new_project(check_date=trade_date, return_module='list', aim='rating') # 样本筛选

    if not full_run:
        last_date = get_last_rundate(trade_date)
        last_date_list = get_last_date_list(last_date)
        update_projects = updated_project_list(last_date=last_date, check_date=trade_date)

        #为前一个计算日没有的，和前一个计算日至今天有更新报告的打上标签
        update_projects = \
            update_projects.union(set(test_df['project_seq']).difference(set(last_date_list['PROJECT_SEQ'])))
        test_df.loc[:, 'update'] = 0
        test_df.loc[test_df['project_seq'].isin(update_projects), 'update'] = 1
    else:
        test_df.loc[:, 'update'] = 1

    df_params, non_param_projects = read_input_rating(test_df[test_df['update']==1].reset_index(), trade_date) # 参数读取

    if len(df_params) < 1:
        print("没有更新数据的项目，直接沿用前日评级")
    else:
        df_ranks_dict, df_critical_dict, df_ranks_cn_dict, df_critical_cn_dict, df_model_params_dict, \
        patch_info, df_fail, df_params, df_fail_project = \
            main_batch_rating(test_df, test_module='rating', check_date=trade_date, df_params=df_params) # 批量计算
