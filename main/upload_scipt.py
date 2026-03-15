# -*- coding: utf-8 -*-
"""上传保存在文件夹的数据到云数据库, 确认本地保存的数据后再上传。（在db文件中看或者 ``output_excel`` 导出后看 ）

TODO
    目前只保持了证券端现金流，需要保存更多数据则在建表后增加这里的上传语句即可，但是需要保证见的表格式跟 conf/ABS_table_specification.xlsx 规定的一致。
"""
from datetime import datetime, timedelta
from conf.conf import upload_db, local_db
from utils.db_util import UploadLib


# trade_date = datetime.now().strftime("%Y%m%d")
# trade_date = '20240301'
trade_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
db_local = UploadLib(local_db)
db_cloud = UploadLib(upload_db)

db_local.connect()
db_cloud.connect()

cf = db_local.select("SELECT * FROM CSI_ABS_GZ_CF")
cf = cf[['SECURITY_SEQ', 'CF_DATE', 'ACCRUAL', 'PRINCIPAL', 'CASH_FLOW', 'TRADE_DATE']]
db_cloud.delete('CSI_BOND_ABS.CSI_ABS_GZ_CF', None)
db_cloud.insert('CSI_BOND_ABS.CSI_ABS_GZ_CF', cf)
db_cloud.delete('CSI_BOND_ABS.CSI_ABS_GZ_CF_HIS', 'TRADE_DATE=%s'%trade_date)
db_cloud.insert('CSI_BOND_ABS.CSI_ABS_GZ_CF_HIS', cf)

db_local.close()
db_cloud.close()
