
# Import the pyspark modules
from pyspark.sql import SparkSession


class SparkJobs():
    def __init__(self, appName):
        self.spark = SparkSession.builder.config("spark.sql.warehouse.dir","file:///C:/Users/sumitd.XEBIAINDIA/Documents/Official/Apache%20Spark/spark-warehouse",).appName(appName).getOrCreate()
        # set verbosity to WARNINGS
        self.spark.sparkContext.setLogLevel("WARN")

    def getAppID(self):
        return str(self.spark.sparkContext.applicationId)

    def loadCSV(self, filepath, _inferSchema=True, _multiLine=True, _header=True):
        df = self.spark.read.csv(filepath, inferSchema=_inferSchema, multiLine=_multiLine, header=_header)
        return df
