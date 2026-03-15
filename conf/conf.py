import os

ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(os.path.dirname(ROOT_PATH), 'data')  # 数据存储目录
EXCEL_DIR = os.path.join(DATA_DIR, 'excel')
DATABASE_DIR = os.path.join(DATA_DIR, 'database')
LOG_DIR = os.path.join(os.path.dirname(ROOT_PATH), 'log')

# 1. 数据库配置
# 1.1 ABS外网数据库
DB_CONF = {
    "host": "10.98.101.7",
    "port": 3306,
    "user": "root",
    "passwd": "Csindex123!",
    "db": "bond"
}
"""dict: 数据库地址（MySQL） 
"""

# 1.2 估值库
DB_CONF_ORC = {
    "host": "10.12.11.10",
    "port": 1521,
    "sid": "jq012011010a",
    "user": "csi_bond_input1",
    "passwd": "input1_h73d3_j6A",
    "db": "csi_bond_input1"
}
"""dict: 生产库 (oracle)
"""

# 1.3. 测试
DB_CONF_TEST = {
    "host": "10.13.1.107",
    "port": 1521,
    "sid": "jq013001107",
    "user": "csi_bond_input1",
    "passwd": "csi_bond_input1",
    "db": "csi_bond_input1"
}
"""dict: 测试库 (oracle)
"""

# 1.3 数据来源默认用估值库 `db_abs` ，如需切换修改默认值为 `db_abs`  （DB_CONF）
default_db = 'db_test'
param_source_db = 'db_test' # 参数来源 TODO 入参测试库当前未建表
config_source_db = 'test'  # 评级的配置数据来源
local_db = 'test'  # 保存估值和评级数据，此时需提取param_source的参数
local_db2 = 'temp'  # 用统一参数进行的分模块测试结果保存在此,参数无需从表格提取，

upload_db = 'db_test'  # 数据上传地址
db_names = {'db_bond': {'info': DB_CONF_ORC, 'type': 'oracle'}, 'db_abs': {'info': DB_CONF, 'type': 'mysql'},
            'db_test': {'info': DB_CONF_TEST, 'type': 'oracle'}}


# 2. 默认入参配置
default_params = {}
default_params['split_default'] = False
"""是否从最新剩余本金中扣除违约部分
"""

default_params['begin_default_recover'] = False
"""当前剩余违约金额是否需要计算违约回收
"""

default_params['add_remain_account'] = False
"""是否把当前剩余资金加到现金流里面
"""

default_params['dp_match_perfectly'] = False
"""输入的违约分布（如有），是否跟现金流归集表完全匹配
"""

default_params['param_match_method'] = 'remain'
"""输入的假设条件是从初始起算日还是最近历史归集日开始，默认从最近历史归集日开始
"""

default_params['day_count_method'] = 'begin'
"""计息天数计算方式，默认算头不算尾
"""

default_params['minus_CDR'] = True
"""是否从输入的 `CDR` 中考虑当期已发生的累计违约率的影响, 默认为是
"""

default_params['cpr_type'] = 'type1'
"""早偿率计算方式，默认为方式1
"""


default_params['is_security'] = True
"""计算范围，默认只计算证券
"""

default_params['same_type'] = True
"""当用模型时，用同类项目还是同发起人项目，默认同二级分类
"""

default_params['compensate'] = True
"""对于固定摊还和到期一次还本付息，是否假设有外部资金源自动补齐不足额的还款, 默认为是
"""

default_params['compensate_rating'] = False
"""评级时不补足现金流
"""

default_params['bayes'] = False
default_params['n_markov'] = False
