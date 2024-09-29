from flask import Flask, render_template, request

import findspark
import pyspark
import seaborn
import matplotlib
import numpy
import pandas
findspark.init()

from pyspark.sql.functions import col

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder

findspark.init()


from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("MySQL Connection") \
    .config("spark.jars", "D:\\FILES\\PYcharm\\BS\\analyse\\data\\jar\\mysql-connector-java-5.1.48.jar") \
    .config("spark.driver.extraClassPath", "D:\\FILES\\PYcharm\\BS\\analyse\\data\\jar\\mysql-connector-java-5.1.48.jar") \
    .getOrCreate()

df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://122.9.44.102:3306/fintest111") \
    .option("dbtable", "new_data") \
    .option("user", "admin") \
    .option("password", "admin") \
    .load()

df.show()

# 删除列
columns_to_drop = ["user_id", "item_id","cat_id","merchant_id","brand_id","month","day"]

# 使用drop()方法删除指定列
df = df.drop(*columns_to_drop)

# 显示删除列后的DataFrame
df.show()

categorical_cols = ['action', 'age_range', 'gender', 'province']
enc_cols = categorical_cols
enc_cols_ind = ['{}_ind'.format(s) for s in enc_cols]
enc_cols_val = ['{}_val'.format(s) for s in enc_cols]

# 特征处理,输出字段与输出字段名称
stringIndexer = StringIndexer(inputCols=enc_cols, outputCols=enc_cols_ind)

# 独热编码器，设置输入与输出属性,注意输入字段为通过标签编码后的值类型名称。
onehotenc = OneHotEncoder(inputCols=enc_cols_ind, outputCols=enc_cols_val) \
    .setHandleInvalid("keep")

from pyspark.ml import Pipeline,PipelineModel
# 指定输入输出
v = VectorAssembler().setInputCols(enc_cols_val).setOutputCol("features")

# 构建回归模型, labelCol,相对于featrues列，表示要进行预测的列
from pyspark.ml.classification import LogisticRegression

log_Reg = LogisticRegression(labelCol='label',featuresCol='features')


# 创建一个Pipeline，并将所有的数据处理和模型拟合步骤添加到Pipeline中
pipeline = Pipeline(stages=[stringIndexer, onehotenc, v, log_Reg])


# 添加数据
pipeline_fit = pipeline.fit(df)
# 编码
new_df = pipeline_fit.transform(df)
print(new_df.show())

selected_df = new_df.select("label", "prediction")
print(selected_df.show())

#selected_df.coalesce(1).write.format("csv").save("out.csv")

#保存整个Pipeline作为管道模型文件
pipeline_fit.write().overwrite().save("model3/")


# #计算准确率
# from pyspark.sql.functions import col
# total_count = selected_df.count()
# print(total_count)
# selected_df = selected_df.withColumn("prediction", col("prediction").cast("int"))
# correct_count = selected_df.filter(col("label") == col("prediction")).count()
#
# accuracy = correct_count / total_count
# print("Accuracy: {:.2f}".format(accuracy))  #Accuracy: 0.96



# 停止Spark会话
spark.stop()
