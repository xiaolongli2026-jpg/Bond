# -*- coding: utf-8 -*-
import pandas as pd

from rating.prepare.config import (multiplier_dict, default_prob_dict, scenario_nums_dict, scenario_dict,\
                        scenario_combination)

param_set_date = '20220101'

df_multiplier = pd.DataFrame([[i, j, multiplier_dict[i][j]] for i in multiplier_dict.keys() \
                              for j in multiplier_dict[i].keys()], columns=['SECOND_CLASS', 'RANK', 'MULTIPLIER'])

df_multiplier['DATE'] = param_set_date

df_default_prob = pd.DataFrame([[i, j, default_prob_dict[i][j]] for i in default_prob_dict.keys() \
                              for j in default_prob_dict[i].keys()], columns=['SECOND_CLASS', 'RANK', 'DEFAULT_RATE'])

df_default_prob['DATE'] = param_set_date

scenario_dict2 = scenario_nums_dict.copy()
for x in scenario_dict2:
    lsts = scenario_dict2[x]
    str_ = " ".join([";".join([i + ',' + str(dict_[i]) for i in dict_ ]) for dict_ in lsts])
    scenario_dict2[x] = str_

df_scenario = pd.DataFrame.from_dict(scenario_dict2, orient='index',)
df_scenario.reset_index(inplace=True, drop=False)
df_scenario.columns = ['SCENARIO_SEQ', 'SCENARIO']
df_scenario['SCENARIO_DETAIL'] = df_scenario['SCENARIO_SEQ'].apply(lambda x: scenario_dict[x]
                                                                   )
df_scenario['SCENARIO_SEQ'] = df_scenario['SCENARIO_SEQ'].astype(str)
df_scenario['DATE'] = param_set_date

dict_1 = scenario_combination['non-revolve']
dict_1 = dict(zip(dict_1.keys(), [",".join([str(y) for y in dict_1[x]]) for x in dict_1]))

df_scenario_combine = pd.DataFrame.from_dict(dict_1, orient='index')
df_scenario_combine.reset_index(inplace=True, drop=False)
df_scenario_combine.columns=['SECOND_CLASS', 'SCENARIO_SEQS']
df_scenario_combine['IS_REV'] = '0'

dict_2 = scenario_combination['revolve']
dict_2 = dict(zip(dict_2.keys(), [",".join([str(y) for y in dict_2[x]]) for x in dict_2]))
df_temp = pd.DataFrame.from_dict(dict_2, orient='index')
df_temp.reset_index(inplace=True, drop=False)
df_temp.columns=['SECOND_CLASS', 'SCENARIO_SEQS']
df_temp['IS_REV'] = '1'
df_scenario_combine = df_scenario_combine.append(df_temp)

df_scenario_combine['DATE'] = param_set_date


# data_source, rating_method, lognorm_source, tdr_source
rating_method_dict = {
    '1': ['5', '2', '', '3'],  # RMBS
    '2': ['3', '3', '2', '3'],  # 汽车贷
    '3': ['3', '2', '', '3'],  # 消费贷
    '4': ['3', '2', '', '2'],  # 企业贷
    '5': ['3', '2', '', '3'],  # 小微企业贷
    '6': ['3', '2', '', '4'],  # 不良贷
    '7': ['3', '2', '', '2'],  # 租赁贷
    '9': ['3', '2', '', '3'],  # 住房公积金贷款
}

df_method = pd.DataFrame.from_dict(rating_method_dict, orient='index')
df_method.reset_index(drop=False, inplace=True)
df_method.columns = ['SECOND_CLASS', 'DATA_SOURCE', 'RATING_METHOD', 'LOGNORM_SOURCE', 'TDR_SOURCE']
df_method['DATE'] = param_set_date

from utils.db_util import UploadLib

db = UploadLib('test')
db.connect()
db.insert('CSI_ABS_RATING_MULTIPLIER', df_multiplier)
db.insert('CSI_ABS_RATING_TARGET_DEFAULT_RATE', df_default_prob)
db.insert('CSI_ABS_RATING_SCENARIO', df_scenario)
db.insert('CSI_ABS_RATING_SCENARIO_COMBINATION', df_scenario_combine)
db.insert('CSI_ABS_RATING_CONFIG', df_method)
db.close()