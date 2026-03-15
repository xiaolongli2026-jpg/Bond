# -*- coding: utf-8 -*-


from conf.table_structure import table_info
from utils.db_util import UploadLib
from conf.conf import local_db, local_db2


table_info = table_info()
db = UploadLib(local_db)
db.connect()
for table_ in table_info:
    db.create_table(table_, table_info[table_])

db.close()

db2 = UploadLib(local_db2)
db2.connect()

for table_ in table_info:
    db2.create_table(table_, table_info[table_])

db2.close()

