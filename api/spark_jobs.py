
# Import the pyspark modules
from pyspark.sql import SparkSession
import pyspark.sql.functions as Func
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import round
from pyspark.sql.types import *
import json

SHABIT_CODES = {
        'PR': 'Premium',
        'VL': 'Valuable',
        'PO': 'Potential',
        'UN': 'Uncommitted',
        'LP': 'Lapsing',
        'GO': 'GoneAway',
        '': 'Missing'}

SHAB_LOOKUP = {'11': 'PR', '12': 'PR', '13': 'PR',
                            '14': 'VL', '15': 'VL', '16': 'PO', '19': 'LP',
                            '21': 'VL', '22': 'VL', '23': 'VL',
                            '24': 'PO', '25': 'PO', '26': 'UN', '29': 'LP',
                            '31': 'PO', '32': 'PO', '33': 'PO',
                            '34': 'UN', '35': 'UN', '36': 'UN', '39': 'LP'}

SPEND_LOOKUP = {'1': 'High', '2': 'Medium', '3': 'Low'}

VISIT_PATTERN_LOOKUP = {'1': 'Daily', '2': 'Twice Weekly',
                             '3': 'Weekly', '4': 'Stop Start',
                             '5': 'Now & Then', '6': 'Hardly Ever',
                             '9': 'Lapsing'}

BAND_HIGH = 70
BAND_LOW = 30



def flag_visit_pattern1(weeks_shopped,max_consec_weeks, avg_visit, distinct_periods):
    """ Assign Flag according to visit pattern """
    if (weeks_shopped >= 7) or ((weeks_shopped == 6) and (max_consec_weeks == 6)):
        if avg_visit < 2:
            return '3'
        elif 2 <= avg_visit < 3:
            return '2'
        elif avg_visit >= 3:
            return '1'
    elif (weeks_shopped >= 4):
        return '5'
    elif 1 <= weeks_shopped <= 3:
        return '6'
    else:
        return '99'


def flag_spend(med_spend, band_high, band_low):
    """ Assign Flag according to spend """
    if med_spend < BAND_LOW:
        return '3'
    if BAND_LOW <= med_spend <= BAND_HIGH:
        return '2'
    if med_spend > BAND_HIGH:
        return '1'

def higher_level_cat(visit_pattern, spend_flag):
    category = str(spend_flag) + str(visit_pattern)
    return category

def lookup_shab(val):
    return SHAB_LOOKUP[val]

def lookup_spend(val):
    return SPEND_LOOKUP[val]

def lookup_visit_pattern(val):
    return VISIT_PATTERN_LOOKUP[val]

def med(tile_1, tile_2, count):
    if count % 2 != 0:
        return float(tile_1)
    else:
        return (float(tile_1) + float(tile_2)) / 2.0

def max_consec_weeks(weeks):
        """ Retruns max consecutive weeks shopped"""
        weeks.sort()
        differences = [j - i for i, j in zip(weeks[:-1], weeks[1:])]
        max_weeks = 1
        tempweeks = 1
        for k in differences:
            if k == 1:
                tempweeks += 1
            elif k > 1:
                tempweeks = 1
            if tempweeks > max_weeks:
                max_weeks = tempweeks
        return max_weeks

def distinct_periods(weeks):
        """ Retuns distinct period shopped"""
        weeks.sort()
        differences = [j - i for i, j in zip(weeks[:-1], weeks[1:])]
        numperiods = 1
        for k in differences:
            if k > 1:
                numperiods += 1
        return numperiods

class SparkJobs():
    def __init__(self, appName):
        self.spark = SparkSession.builder.config("spark.sql.warehouse.dir","file:///C:/Users/sumitd.XEBIAINDIA/Documents/Official/Apache%20Spark/spark-warehouse").appName(appName).getOrCreate()
        # set verbosity to WARNINGS
        self.spark.sparkContext.setLogLevel("WARN")
        self.visit_pattern_udf = Func.udf(flag_visit_pattern1,StringType())
        self.spend_level_udf = Func.udf(flag_spend,StringType()) 
        self.higher_level_udf = Func.udf(higher_level_cat, StringType())
        self.lookup_shab_udf = Func.udf(lookup_shab, StringType())
        self.lookup_spend_udf = Func.udf(lookup_spend, StringType())
        self.lookup_visit_pattern_udf = Func.udf(lookup_visit_pattern, StringType())
        self.median_udf = Func.udf(med,FloatType())
        self.max_consec_weeks_udf = Func.udf(max_consec_weeks, IntegerType())
        self.distinct_periods_udf = Func.udf(distinct_periods, IntegerType())
        self.loyalties = self.spark.read.csv("C:\\Users\\sumitd.XEBIAINDIA\\Documents\\Official\\Categorical_Analysis\\data\\loyalty.csv", inferSchema=True, multiLine=True, header=True)
        self.transactions = self.spark.read.csv("C:\\Users\\sumitd.XEBIAINDIA\\Documents\\Official\\Categorical_Analysis\\data\\transactions.csv", inferSchema=True, multiLine=True, header=True)
        self.users = self.spark.read.csv("C:\\Users\\sumitd.XEBIAINDIA\\Documents\\Official\\Categorical_Analysis\\data\\cards.csv", inferSchema=True, multiLine=True, header=True)
        self.stores = self.spark.read.csv("C:\\Users\\sumitd.XEBIAINDIA\\Documents\\Official\\Categorical_Analysis\\data\\stores.csv", inferSchema=True, multiLine=True, header=True)


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

    def get_cust_week_summary(self, df):
        # filter the df based on fis_week_id
        return df.groupBy('CustomerID', 'fis_week_id').agg(
            Func.count('TRANSACTION_ID').alias('transaction_count'),
            Func.sum('item_qty').alias('qty'),
            Func.round(Func.sum('net_spend_amt'), 2).alias('spend'),
            Func.collect_set("fis_week_id").over(Window.partitionBy(
                "CustomerID")).alias("cycle_list")).orderBy('CustomerID')

    def get_cust_summary(self, df):
        return df.groupBy('CustomerID', 'cycle_list').agg(
            Func.round(Func.avg('spend'), 2).alias('avg_spend'),
            Func.max('spend').alias('max_spend'),
            Func.min('spend').alias('min_spend'),
            Func.round(Func.sum('spend'), 2).alias('total_spend'),
            Func.round(Func.avg('qty'), 2).alias('avg_qty'),
            Func.max('qty').alias('max_qty'),
            Func.min('qty').alias('min_qty'),
            self.distinct_periods_udf('cycle_list').alias('distinct_periods'),
            self.max_consec_weeks_udf('cycle_list').alias('max_consec_weeks'),
            Func.sum('qty').alias('total_qty'),
            Func.count('fis_week_id').alias('weeks_shopped'),
            Func.round(Func.avg('transaction_count')).cast(
                IntegerType()).alias('avg_visit'))

    def median(self, data_frame, partition_column, calculation_column):
        """Returns median accross a partition"""
        _window = Window().partitionBy(
            partition_column).orderBy(calculation_column) 

        ntile = data_frame.withColumn('ntile', Func.ntile(2).over(_window))

        median_data = ntile.groupBy(partition_column, 'ntile').agg(
            Func.count(Func.col(calculation_column)).alias('count'),
            Func.max(Func.col(calculation_column)).alias('max'),
            Func.min(Func.col(calculation_column)).alias('min'))

        median_data = median_data.groupBy(partition_column).agg(
            Func.min('max').alias('1st_tile'),
            Func.max('min').alias('2nd_tile'),
            Func.sum('count').alias('count'))

        median_data = median_data.select(
                partition_column,
                self.median_udf(Func.col('1st_tile'),
                        Func.col('2nd_tile'),
                        Func.col('count')).alias('med_spend'))

        return median_data

    def customer_metrics(self, df):
        cust_week_summary= self.get_cust_week_summary(df)
        cust_summary= self.get_cust_summary(cust_week_summary)        
        customer_summary = cust_summary.withColumn(
            'distinct_periods',
            self.distinct_periods_udf(Func.col('cycle_list'))).withColumn(
                'max_consec_weeks',
                self.max_consec_weeks_udf(Func.col('cycle_list')))
        # determine median spend
        median_spend = self.median(
            data_frame=cust_week_summary,partition_column='CustomerID',calculation_column='spend')
        customer_summary = customer_summary.join(
            median_spend, on='CustomerID', how='left')
        return customer_summary

    def get_loyalty(self):
        # customer_summary = self.customer_metrics(transactions)
        # flagged_summary = customer_summary.withColumn(
        #         'visit_pattern',
        #         self.visit_pattern_udf(
        #                         Func.col('weeks_shopped'),
        #                         Func.col('max_consec_weeks'),
        #                         Func.col('avg_visit'),
        #                         Func.col('distinct_periods')))
        # # assign spend flag
        # flagged_summary = flagged_summary.withColumn(
        #     'spend_flag',
        #     self.spend_level_udf(Func.col('med_spend'),
        #                     Func.lit(BAND_HIGH),
        #                     Func.lit(BAND_LOW)))
        # flagged_summary = flagged_summary.withColumn(
        #     'higher_level_cat',
        #     self.higher_level_udf(Func.col('visit_pattern'),
        #                     Func.col('spend_flag')))
        # flagged_summary = flagged_summary.withColumn(
        #     'shabit',
        #     self.lookup_shab_udf(Func.col('higher_level_cat'))).withColumn(
        #         'spend_desc',
        #         self.lookup_spend_udf(Func.col('spend_flag'))).withColumn(
        #             'visit_desc',
        #             self.lookup_visit_pattern_udf(Func.col('visit_pattern'))).toJSON().collect()
        res = []
        for i in self.loyalties.groupBy('shabit').count().alias('Count').select('shabit', (Func.col('count')*100/self.loyalties.count()).alias("perc")).toJSON().collect():
            res.append(json.loads(i))
        return res

    def get_users(self):
        res = []
        for i in self.loyalties.select("CustomerID", "total_spend", "weeks_shopped","shabit", "spend_desc", "visit_desc").toJSON().collect():
            res.append(json.loads(i))
        return res

    def get_users_distribution(self):
        res = []
        for i in self.loyalties.groupBy('shabit').agg(Func.avg('weeks_shopped').alias('weeks_shopped'), Func.avg('max_consec_weeks').alias('max_consec_weeks'), Func.avg('avg_visit').alias('avg_visit'), Func.avg('distinct_periods').alias('distinct_periods')).toJSON().collect():
            res.append(json.loads(i))

        for i in self.loyalties.groupBy('shabit').agg(Func.avg('avg_spend').alias('avg_spend'), Func.avg('med_spend').alias('med_spend'), Func.avg('total_spend').alias('total_spend')).toJSON().collect():
            res.append(json.loads(i))
        
        return res