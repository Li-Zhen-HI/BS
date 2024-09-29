import mysql.connector

import pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns
# # 创建数据库连接
# connection = mysql.connector.connect(
#     host="122.9.44.102",  # MySQL服务器地址
#     user="admin",   # 用户名
#     password="admin",  # 密码
#     database="fintest111"  # 数据库名称
# )
#
# query=f"SELECT * FROM new_data"
#
# df = pd.read_sql(query, con=connection)
#
# connection.close()
#
#
# plt.rcParams['font.sans-serif'] = ['SimHei']
# plt.rcParams['axes.unicode_minus'] = False
#
# fig,ax = plt.subplots(1, 4,figsize=(16,4))
# sns.distplot(df['action'],ax=ax[0])
# sns.distplot(df['age_range'],ax=ax[1])
# sns.distplot(df['gender'],ax=ax[2])
# sns.distplot(df['label'],ax=ax[3])
#
# plt.show()


# connection = mysql.connector.connect(
#     host="122.9.44.102",
#     user="admin",
#     password="admin",
#     database="fintest111"
# )
# cursor = connection.cursor()
# cursor.execute("SELECT gender, COUNT(*) AS count FROM new_data GROUP BY gender;")
# rata_data = cursor.fetchall()
# print(rata_data)
# cursor.close()
# connection.close()

#
# connection = mysql.connector.connect(
#     host="122.9.44.102",
#     user="admin",
#     password="admin",
#     database="fintest111"
# )
# cursor = connection.cursor()
# cursor.execute("SELECT gender, action, COUNT(*) AS count FROM new_data WHERE gender IN ('0', '1') GROUP BY gender, action;")
# action_data = cursor.fetchall()
#
# cursor.close()
# connection.close()
# print(action_data)

# connection = mysql.connector.connect(
#     host="122.9.44.102",
#     user="admin",
#     password="admin",
#     database="fintest111"
# )
# cursor = connection.cursor()
# cursor.execute("SELECT gender, age_range, COUNT(*) AS count FROM new_data WHERE gender IN ('0', '1','2') GROUP BY gender, age_range;")
# action_data = cursor.fetchall()
#
# cursor.close()
# connection.close()
#
# print(action_data)

#
# connection = mysql.connector.connect(
#     host="122.9.44.102",
#     user="admin",
#     password="admin",
#     database="fintest111"
# )
# cursor = connection.cursor()
#
# cursor.execute("SELECT brand_id, COUNT(*) AS count FROM new_data GROUP BY brand_id ORDER BY count DESC LIMIT 23;")
# age_data = cursor.fetchall()
#
# cursor.close()
# connection.close()
#
# print(age_data)



# connection = mysql.connector.connect(
#     host="122.9.44.102",
#     user="admin",
#     password="admin",
#     database="fintest111"
# )
# cursor = connection.cursor()
#
# cursor.execute("SELECT province, COUNT(*) AS count FROM new_data GROUP BY province;")
# age_data = cursor.fetchall()
#
# cursor.close()
# connection.close()
#
# print(age_data)