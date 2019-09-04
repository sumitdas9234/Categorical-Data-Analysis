from pyspark import StorageLevel, SparkFiles, SparkContext
from pyspark.sql import SparkSession
import sys
from flask import Flask, jsonify, request
from flask import render_template
from flaskwebgui import FlaskUI
import datetime
import os
from helpers import convert_bytes
from spark_jobs import SparkJobs
from millify import prettify

app = Flask(__name__)

ui = FlaskUI(app)


# Resources
APP_DIR = os.path.dirname(os.path.realpath(__file__))
spark_jobs_file = os.path.join(APP_DIR, "spark_jobs.py")
helpers_file = os.path.join(APP_DIR, "helpers.py")


# Create Pyspark Configuration and App Nmae and cluster URL
def init_spark(appName):
    SparkContext.stop(appName)
    spark = (
        SparkSession.builder.config(
            "spark.sql.warehouse.dir",
            "file:///C:/Users/sumitd.XEBIAINDIA/Documents/Official/Apache%20Spark/spark-warehouse",
        )
        .appName(appName)
        .getOrCreate()
    )
    # set verbosity to WARNINGS
    spark.sparkContext.setLogLevel("WARN")

    # add the spark_jobs.py to sparkContext
    spark.sparkContext.addPyFile(spark_jobs_file)
    spark.sparkContext.addPyFile(helpers_file)

    # set the root directory for spark files
    sys.path.insert(0, SparkFiles.getRootDirectory())

    return spark



@app.route("/")
def index():
    return render_template('index.html')

@app.route("/full_report")
def full_report():
    global transactions
    transactions = spark_jobs.load_file('../data/transactions.csv')
    country_count = transactions.select('Country').distinct().count()
    stores = spark_jobs.load_file('../data/stores.csv')
    users = spark_jobs.load_file('../data/cards.csv')
    return render_template('report.html', transaction_count = transactions.count(), country_count = country_count, store_count= stores.count(), user_count = users.count(), product_count = transactions.select('Product_Code').distinct().count())

@app.route("/load_file")
def load_file():
    filepath = os.path.abspath(request.args.get('filepath', ''))
    # get the schema of the file as df
    df = spark_jobs.load_file(filepath)
    return jsonify(df.schema.json())

@app.route("/get_data_points")
def get_data_points():
    return jsonify(spark_jobs.get_monthwise_sales(transactions))


@app.template_filter('fileManager')
def fileManager(root_dir):
    for r, d, f in os.walk(root_dir):
        dirs = []
        for directory in d:
            dirs.append([directory, os.path.abspath(directory)])
        files = []
        for file in f:
            files.append([file, convert_bytes(os.path.getsize(os.path.join(root_dir, file))), os.path.join(root_dir, file)])

        return [os.path.abspath(r).split('\\')[-1], dirs, files]

@app.template_filter('pretify')
def pretify(number):
    return prettify(number)

if __name__ == '__main__':   
    # Initialise Spark
    spark = init_spark("App Name");
    global  spark_jobs
    # instansiate the spark jobs
    filepath = "../data/data.csv"
    spark_jobs = SparkJobs(spark)
    app.run(debug=True)
    # ui.run() 