from pyspark.sql.functions import round
import pyspark.sql.functions as Func
from pyspark.sql import Window
from pyspark.sql.types import *
import secrets
import json


def distinct_periods(weeks):
    """ Retuns distinct period shopped"""
    weeks.sort()
    differences = [j - i for i, j in zip(weeks[:-1], weeks[1:])]
    numperiods = 1
    for k in differences:
        if k > 1:
            numperiods += 1
    return numperiods

def max_consec_weeks(weeks):
    """ Retuns max consecutive weeks shopped"""
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


#  Creating a function to generate Unique Identifiers of length 6
def add_card_code(length=6):
    # Generate the token and append it to "C0"
    return "C0" + secrets.token_hex(int(length / 2))

# Convert the function to a Spark UDF
udf_add_card_code = Func.udf(add_card_code, StringType())
max_consec_weeks_udf = Func.udf(max_consec_weeks, IntegerType())
distinct_periods_udf = Func.udf(distinct_periods, IntegerType())




class SparkJobs:
    def __init__(self, _spark):
        self.spark = _spark


    def preprocess_transactions(self, df):
        # Drop rows with null values in CustomerID
        df = df.dropna(subset=["CustomerID"])

        # Drop the rows with TRANSACTION_ID Strating with 'C'
        df = df.filter(~Func.col("TRANSACTION_ID").startswith("C"))
        # split the entries of the column by " "
        splits = Func.split(df["date_id"], " ")
        # removing the HH:MM from the timestamp
        df = df.withColumn("date_id", splits.getItem(0))
        # Apply the new Date Format
        df = self.convertColToDate(df, "date_id", "MM/dd/yyyy")
        return df


    # A function to convert any Column of String Objects into Date Objects
    def convertColToDate(self, df, colName, currentDateFormat):
        return df.withColumn(
            colName,
            Func.to_date(
                Func.unix_timestamp(Func.col(colName), currentDateFormat).cast(
                    "timestamp"
                )
            ),
        )

    # A function to find the Amount spend by all the customers b/w a date range
    def spendSummaryByCustomer(self, initial_date, final_date):
        if initial_date == final_date:
            return (
                transaction_item_mft.filter(
                    Func.col("date_id") == Func.lit(initial_date)
                )
                .groupBy("CustomerID")
                .agg(Func.round(Func.sum("net_spend_amt"), 2).alias("net_spend_amt"))
            )
        else:
            return (
                transaction_item_mft.filter(
                    (Func.col("date_id") >= Func.lit(initial_date))
                    & (Func.col("date_id") <= Func.lit(final_date))
                )
                .groupBy("CustomerID")
                .agg(Func.round(Func.sum("net_spend_amt"), 2).alias("net_spend_amt"))
            )

    def get_cust_week_summary(self, df):
        # filter the df based on fis_week_id
        return (
            df.groupBy("CustomerID", "fis_week_id")
            .agg(
                Func.count("TRANSACTION_ID").alias("transaction_count"),
                Func.sum("item_qty").alias("qty"),
                Func.round(Func.sum("net_spend_amt"), 2).alias("spend"),
                Func.collect_set("fis_week_id")
                .over(Window.partitionBy("CustomerID"))
                .alias("cycle_list"),
            )
            .orderBy("CustomerID")
        )

    def get_cust_summary(self, df):
        return df.groupBy("CustomerID", "cycle_list").agg(
            Func.round(Func.avg("spend"), 2).alias("avg_spend"),
            Func.max("spend").alias("max_spend"),
            Func.min("spend").alias("min_spend"),
            Func.round(Func.sum("spend"), 2).alias("total_spend"),
            Func.round(Func.avg("qty"), 2).alias("avg_qty"),
            Func.max("qty").alias("max_qty"),
            Func.min("qty").alias("min_qty"),
            distinct_periods_udf("cycle_list").alias("distinct_periods"),
            max_consec_weeks_udf("cycle_list").alias("max_consec_weeks"),
            Func.sum("qty").alias("total_qty"),
            Func.count("fis_week_id").alias("weeks_shopped"),
            Func.round(Func.avg("transaction_count"))
            .cast(IntegerType())
            .alias("avg_visit"),
        )
    def load_file(self, filepath):
        df = self.spark.read.csv(filepath, inferSchema=True, multiLine=True, header=True)
        return df

    def get_monthwise_sales(self,df):
        mw_sales = df.groupBy(Func.month('date_id').alias('month'), Func.year('date_id').alias('year')).sum('net_spend_amt').orderBy('month')
        l = mw_sales.select(Func.col("month"),Func.col("year"), Func.round(Func.col("sum(net_spend_amt)"), 2).alias('net_spend_amt') ).toJSON().collect()
        res = []
        for i in l:
            res.append(json.loads(i))
        return res
