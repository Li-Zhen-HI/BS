
import matplotlib.pyplot as plt
import seaborn as sns

import mysql.connector

import pandas as pd




data=pd.read_csv('data.csv')
print("已经去除重复值：%d个"%data.duplicated().sum())
data.drop_duplicates(inplace=True)

data.dropna(how='any',inplace=True)



# 创建数据库连接
connection = mysql.connector.connect(
    host="122.9.44.102",  # MySQL服务器地址
    user="admin",   # 用户名
    password="admin",  # 密码
    database="fintest111"  # 数据库名称
)

query=f"SELECT * FROM train_after"

data2 = pd.read_sql(query, con=connection)
data2.columns=['user_id','age_range','gender','merchant_id','label']

data3=data2[['user_id','label']]


data3['user_id'] = data3['user_id'].astype('int64')

merged_df = pd.merge(data, data3, on='user_id', how='left')

merged_df = merged_df.fillna()

print(merged_df)
merged_df.to_csv('datatest.csv')

cursor = connection.cursor()
table_name = 'new_data'

for index, row in merged_df.iterrows():



    sql = f"INSERT INTO {table_name} (user_id, item_id,cat_id,merchant_id,brand_id,month,day,action,age_range,gender,province,label) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cursor.execute(sql, tuple(row))
cursor.close()
connection.close()