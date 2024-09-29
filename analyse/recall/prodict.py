from pyspark.ml import PipelineModel
import findspark

from pyspark.sql import SparkSession
from pyspark.sql import Row
findspark.init()
# 创建Spark会话
spark = SparkSession.builder.appName("PredictionApp").getOrCreate()

# 创建要预测的数据
data = [(1, 1, 2, "上海市")]
columns = ["action", "age_range", "gender", "province"]
df = spark.createDataFrame(data, columns)
print(df.show())


pipeline_model = PipelineModel.load("D:\\FILES\\PYcharm\\BS\\analyse\\recall\\model3")

# 使用Pipeline模型进行预测

predictions = pipeline_model.transform(df)

# 提取预测结果
selected = predictions.select("prediction")
print(selected.show())
# 提取预测结果

data_list = selected.collect()


# 打印预测结果
# print(int(data_list[0][0]))
print(int(selected.first()[0]))

spark.stop()

