# -*- coding: utf-8 -*-
"""保存所有需要上传到任意数据库的数据结构，以及入参的枚举值映射，如果修改了入参的枚举值或者表结构，修改ABS_table_specification.xlsx替换并且删掉对应的两个json文件即可

"""
import pandas as pd
import os
import json


def table_info():
    """数据上传表格信息

    Returns:
        dict: key-表名， value-tuples构成，每个tuple包括字段名、字段精度、是否允许为空、是否主键、字段中文释义

    """
    filename = os.path.join(os.path.dirname(__file__), 'table_info.json')
    if os.path.exists(filename):
        with open(filename) as f:
            table_info = json.load(f)
    else:
        # 上传到估值库的数据

        table_info = {"CSI_ABS_GZ_CF": (("SECURITY_SEQ", "VARCHAR(16)", False, True, "证券内码"),
                                        ("CF_DATE", "VARCHAR(8)", False, True, "现金流日期"),
                                        ("ACCRUAL", "DECIMAL(15, 8)", True, False, "应计利息（支付日）"),
                                        ("PRINCIPAL", "DECIMAL(9, 4)", False, False, "本金"),
                                        ("CASH_FLOW", "DECIMAL(16, 0)", False, False, "现金流"),
                                        ("TRADE_DATE", "VARCHAR(8)", False, True, "估值日"),
                                        ("PRIN_REPLENISH", "DECIMAL(15, 8)", True, False, "补充支付本金"),
                                        ("INT_REPLENISH", "DECIMAL(15, 8)", True, False, "补充支付利息"),
                                        ("EXRET_PAYMENT", "DECIMAL(15, 8)", True, False, "超额收益"),
                                        ("FCC", "DECIMAL(15, 8)", True, False, "固定资金成本")),
                      "CSI_ABS_GZ_RESULT": (("SECURITY_SEQ", "VARCHAR(16)", False, True, "证券内码"),
                                            ('INPUT_SEQ', 'VARCHAR(200)', False, True, "参数内码"),
                                            ("DATE", "VARCHAR(8)", False, True, "估值日"),
                                            ("PRICE", "DECIMAL(9, 4)", False, False, "全价"),
                                            ("CLEAN_PRICE", "DECIMAL(9, 4)", True, False, "净价"),
                                            ("ACCRUAL_INTEREST", "DECIMAL(9, 4)", False, False, "应计利息"),
                                            ),
                      'SUMMARY': (('DATE', 'VARCHAR(8)', False, True, "日期"),
                                  ('CALCULATION_NUMBER', 'DECIMAL(16, 0)', False, False, "当日计算项目数"),
                                  ('SUCCESS_NUMBER', 'DECIMAL(16, 0)', False, False, "计算成功项目数量"),
                                  ('FAIL_NUMBER', 'DECIMAL(16, 0)', False, False, "计算失败项目数量"),
                                  ('CALCULATION_SECURITY_NUMBER','DECIMAL(16, 0)', False, False, '单日计算证券数'),
                                  ('SUCCESS_SECURITY_NUMBER', 'DECIMAL(16, 0)', False, False, '计算成功证券数量'),
                                  ('FAIL_SECURITY_NUMBER', 'DECIMAL(16, 0)', False, False, '计算失败证券数量'),
                                  ('CALCULATION_TIME', 'DECIMAL(16, 4)', True, False, "计算总时长"),
                                  ('MODULE', 'VARCHAR(200)', False, True, "计算模块"),
                                  ('PROJECT_NUMBER', 'DECIMAL(16, 0)', False, False, "估值项目清单数量"),
                                  ('SECURITY_NUMBER', 'DECIMAL(16, 0)', False, False, '证券总数'),
                                   ('INVALID_SECURITIES', 'BLOB', True, False, '结果无效的证券'))}

        table_info['CSI_ABS_GZ_CF_CUSTOM'] = tuple(list(table_info['CSI_ABS_GZ_CF'])+
                                                        [('INPUT_SEQ', 'VARCHAR(200)', False, True, "参数内码")])
        table_info['CSI_ABS_GZ_CF_HIS'] = table_info['CSI_ABS_GZ_CF']

        filename = os.path.join(os.path.dirname(__file__), 'ABS_table_specification.xlsx')
        sheet_names = ['入参表-通用', '默认参数配置表-通用', '输出表-通用', '数据检查表-生产']
        for sn in sheet_names:
            tables = pd.read_excel(filename, sheet_name=sn, index_col=None, header=0, )
            tables = tables[['TABLE_NAME', 'TABLE_COMMENT', 'COLUMN', 'COLUMN_TYPE_SQLITE', 'ALLOW_NAN', 'PRIMARY_KEY', 'COMMENT']]
            tables.ffill(inplace=True)
            table_names = list(set((tables['TABLE_NAME'])))
            for x in table_names:
                df = tables[tables['TABLE_NAME']==x]
                table_info[x] = tuple(zip(df['COLUMN'], df['COLUMN_TYPE_SQLITE'], df['ALLOW_NAN'], df['PRIMARY_KEY'], df['COMMENT']))

        with open(os.path.join(os.path.dirname(__file__), 'table_info.json'), "w") as f:
            info_str = json.dumps(table_info)
            f.write(info_str)
            f.close()

    return table_info


def data_enumerators():
    """获取枚举值和对应的中文

    Returns:
        dict: key-table, value-dict（key-字段名，value-由字段名中的枚举值和中文名组成）
    """
    filename = os.path.join(os.path.dirname(__file__), 'data_name_enumerators.json')
    if os.path.exists(filename):
        with open(filename) as f:
            reflections = json.load(f)
    else:

        filename = os.path.join(os.path.dirname(__file__), 'ABS_table_specification.xlsx')
        if os.path.exists(filename):
            table_ = pd.read_excel(filename, sheet_name='入参表-通用', index_col=None, header=0, )
            table2 = pd.read_excel(filename, sheet_name='默认参数配置表-通用', index_col=None, header=0, )
            table_ = table_.append(table2)
            table_ = table_[
                ['TABLE_NAME', 'COLUMN', 'ENUMERATOR']]
            table_['TABLE_NAME'] = table_['TABLE_NAME'].ffill()
            table_.dropna(subset=['ENUMERATOR'], how='any', axis=0, inplace=True)
            table_['ENUMERATOR'] = table_['ENUMERATOR'].astype(int).astype(str)
            enumerator_initial = pd.read_excel(filename, sheet_name='枚举值', index_col=None, header=0, )
            enumerator_initial = enumerator_initial[['CODE', 'PROGRAM_REFLECTION']]
            enumerator_initial['CODE'] = enumerator_initial['CODE'].astype(int).astype(str)
            table_ = table_.merge(enumerator_initial, left_on='ENUMERATOR', right_on='CODE')
            reflections = {x: {} for x in list(set(table_['TABLE_NAME']))}
            for i in range(len(table_)):
                if str(table_.loc[i, 'PROGRAM_REFLECTION']) not in ('nan', 'None'):
                    str_ = table_.loc[i, 'PROGRAM_REFLECTION']
                    str_ = str_.replace(" ", "")
                    lst = str_.split(";")
                    dict_ = {x.split("-")[0]: x.split("-")[1] for x in lst}
                    reflections[table_.loc[i, 'TABLE_NAME']][table_.loc[i, 'COLUMN']] = dict_

            json_str = json.dumps(reflections, indent=0)
            with open(os.path.join(os.path.dirname(__file__), "data_name_enumerators.json"), "w") as json_file:
                json_file.write(json_str)
                json_file.close()

        else:
            raise FileNotFoundError("没有参数枚举值映射文件")

    return reflections

