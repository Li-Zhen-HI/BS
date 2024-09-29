import sqlite3

from flask import render_template, jsonify
import mysql.connector
import findspark
from flask import Flask, request
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import Row
findspark.init()


app = Flask(__name__)

spark = SparkSession.builder.appName("PredictionApp").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "8")
pipeline_model = PipelineModel.load("D:/FILES/PYcharm/BS/analyse/recall/model3")


@app.route('/')
def index():  # put application's code here
    return render_template('index.html')

@app.route('/home')
def home():
    return render_template('index.html')

@app.route('/rata')
def rata():
    # connection=sqlite3.Connection("new_data.idb")
    # cursor = connection.cursor()
    # cursor.execute("SELECT gender, COUNT(*) AS count FROM new_data GROUP BY gender;")
    # rata_data = cursor.fetchall()
    # cursor.close()
    # connection.close()
    # return render_template('rata.html', canshu=rata_data)


    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="12345678",
        database="test"
    )
    cursor = connection.cursor()
    cursor.execute("SELECT gender, COUNT(*) AS count FROM new_data GROUP BY gender;")
    rata_data = cursor.fetchall()
    cursor.close()
    connection.close()


    return render_template('rata.html',canshu=rata_data)

@app.route('/action')
def action():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="12345678",
        database="test"
    )
    cursor = connection.cursor()
    cursor.execute("SELECT gender, action, COUNT(*) AS count FROM new_data WHERE gender IN ('0', '1') GROUP BY gender, action;")
    action_data = cursor.fetchall()

    cursor.close()
    connection.close()
    return render_template('action.html',canshu=action_data)
@app.route('/age')
def age():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="12345678",
        database="test"
    )
    cursor = connection.cursor()
    cursor.execute(
        "SELECT gender, age_range, COUNT(*) AS count FROM new_data WHERE gender IN ('0', '1','2') GROUP BY gender, age_range;")
    age_data = cursor.fetchall()

    cursor.close()
    connection.close()

    return render_template('age.html',canshu=age_data)
@app.route('/item')
def item():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="12345678",
        database="test"
    )
    cursor = connection.cursor()

    cursor.execute("SELECT brand_id, COUNT(*) AS count FROM new_data GROUP BY brand_id ORDER BY count DESC LIMIT 23;")
    item_data = cursor.fetchall()

    cursor.close()
    connection.close()
    return render_template('item.html',canshu=item_data)
@app.route('/province')
def province():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="12345678",
        database="test"
    )
    cursor = connection.cursor()

    cursor.execute("SELECT province, COUNT(*) AS count FROM new_data GROUP BY province;")
    province_data = cursor.fetchall()

    cursor.close()
    connection.close()

    return render_template('province.html',canshu=province_data)


@app.route('/recall',methods=['GET','POST'])
def recall(spark=spark,pipeline_model=pipeline_model):
    if request.method == 'POST':
        print(request.method)
        behavior = request.form.get('action')
        age = request.form.get('age')
        gender = request.form.get('gender')
        province = request.form.get('province')

        data = [(int(behavior), int(age), int(gender), province)]

        schema = ['action','age_range', 'gender','province']
        #
        df = spark.createDataFrame(data, schema)

        # # 使用Pipeline模型进行预测
        predictions = pipeline_model.transform(df)
        #
        # # 提取预测结果
        selected = predictions.select("prediction")

        # results = selected.collect()[0].asDict().get('prediction')
        #
        results=int(selected.first()[0])
        print(results)
        return render_template('recall.html',canshu=results)
    if request.method =='GET':
        return render_template('recall.html')
    #done






if __name__ == '__main__':
    app.run()

