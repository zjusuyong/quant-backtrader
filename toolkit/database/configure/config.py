# coding=gbk

def to_format(db='mysql',
              db_conn='pymysql',
              username='mysql',
              password='123!@#',
              host='127.0.0.1',
              port='3306',
              db_name='database'):
    return f'{db}+{db_conn}://{username}:{password}@{host}:{port}/{db_name}?charset=utf8mb4'
