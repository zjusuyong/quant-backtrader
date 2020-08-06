# coding=gbk

from sqlalchemy import create_engine
from tookit.database.configure.config import to_format

conn_qt = create_engine(to_format(db_name='quant'), echo=False)
conn_qt_read = create_engine(to_format(db_name='quant', username='read'), echo=False)


conn_cn_daily = create_engine(to_format(db_name='cn_daily'), echo=False)
conn_cn_daily_read = create_engine(to_format(db_name='cn_daily', username='read'), echo=False)

conn_economic = create_engine(to_format(db_name='economic_data'), echo=False)
conn_economic_read = create_engine(to_format(db_name='economic_data', username='read'), echo=False)

conn_logs = create_engine(to_format(db_name='log'), echo=False)
