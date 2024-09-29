import mysql.connector

import pandas as pd


# 创建数据库连接
connection = mysql.connector.connect(
    host="122.9.44.102",  # MySQL服务器地址
    user="admin",   # 用户名
    password="admin",  # 密码
    database="fintest111"  # 数据库名称
)

query=f"SELECT * FROM user_data"

df = pd.read_sql(query, con=connection)


connection.close()



print(df)
df.to_csv('data.csv',index=False)
