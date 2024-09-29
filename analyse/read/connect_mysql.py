import mysql.connector

def get_tables(database_name):
    # 连接到MySQL
    connection = mysql.connector.connect(
        host="122.9.44.102",
        user="admin",
        password="admin",
        database=database_name
    )

    # 检查连接是否成功
    if connection.is_connected():
        print("已连接到MySQL！")
        print(connection)

        # 创建游标对象
        cursor = connection.cursor()

        try:
            # 执行查询获取表信息
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s", (database_name,))

            # 获取所有表名
            tables = cursor.fetchall()

            # 打印表名
            for table in tables:
                print(table[0])

        except mysql.connector.Error as e:
            print("获取表信息时出错:", e)

        finally:
            # 关闭游标和连接
            cursor.close()
            connection.close()
            print("MySQL连接已关闭。")

    else:
        print("无法连接到MySQL。")

# 调用函数获取指定数据库中的所有表
get_tables("fintest111")

