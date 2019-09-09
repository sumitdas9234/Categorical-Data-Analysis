
# Import the pyspark modules
from pyspark.sql import SparkSession
import pyspark.sql.functions as Func
import json

class SparkJobs():
    def __init__(self, appName):
        self.spark = SparkSession.builder.config("spark.sql.warehouse.dir","file:///C:/Users/sumitd.XEBIAINDIA/Documents/Official/Apache%20Spark/spark-warehouse").appName(appName).getOrCreate()
        # set verbosity to WARNINGS
        self.spark.sparkContext.setLogLevel("WARN")

    def getAppID(self):
        return str(self.spark.sparkContext.applicationId)

    def loadCSV(self, filepath, _inferSchema=True, _multiLine=True, _header=True):
        df = self.spark.read.csv(filepath, inferSchema=_inferSchema, multiLine=_multiLine, header=_header)
        return df

    def get_monthwise_sales(self,df):
        mw_sales = df.groupBy(Func.month('date_id').alias('month'), Func.year('date_id').alias('year')).sum('net_spend_amt').orderBy('month')
        l = mw_sales.select(Func.col("month"),Func.col("year"), Func.round(Func.col("sum(net_spend_amt)"), 2).alias('net_spend_amt') ).toJSON().collect()
        res = []
        for i in l:
            res.append(json.loads(i))
        return res