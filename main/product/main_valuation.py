# -*- coding: utf-8 -*-
"""每日估值，调用module_abs_new

"""

from datetime import datetime, timedelta
from main.production.pscheck_add import main_batch_run
from main.production.filter import new_project, read_input, updated_project_list, get_last_rundate, get_last_date_list


if __name__ == '__main__':
    # trade_date = '20240301'
    full_run = True
    trade_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

    test_df = new_project(check_date=trade_date, return_module='list', aim='valuation')
    if not full_run:
        last_date, invalid_securities = get_last_rundate(trade_date)
        last_date_list = get_last_date_list(last_date)
        update_projects = updated_project_list(last_date=last_date, check_date=trade_date)

        #为新入库的的，前一个计算日至今天有更新报告的和上一日计算失败的打上标签
        update_projects = \
            update_projects.union(set(test_df['project_seq']).difference(set(last_date_list['PROJECT_SEQ'])))
        test_df.loc[:, 'update'] = 0
        test_df.loc[(test_df['project_seq'].isin(update_projects)| \
                    test_df['security_seq'].isin(invalid_securities)), 'update'] = 1
    else:
        test_df.loc[:, 'update'] = 1

    df_params, non_param_projects = read_input(test_df[test_df['update']==1], trade_date)  # 只读取更新了的，未更新的沿用前日的数据

    if len(df_params) > 0:
        patch_info, df_fail= \
            main_batch_run(test_df, test_module='valuation', check_date=trade_date, df_params=df_params, model_type=None)
    else:
        print("无数据更新，沿用昨日现金流")










