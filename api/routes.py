# ADD ONLY ROUTES, WHICH ARE SPECIFIC TO THE API and BACKEND 


# Import Flask.Blueprint to create a modular code 
from flask import Blueprint, jsonify
from flask_restful import Resource
from .spark_jobs import SparkJobs
import json

# Instantiate the Blueprint named "rest_api" with following config
rest_api = Blueprint('rest_api', __name__)

# Creating new routes --> Example
class GetAppID(Resource):
    def get(self):     
        # Return the json
        return "{info: "+sparkSession.getAppID()+"}"

class LoadCSV(Resource):
    def get(self, filepath):
        df = sparkSession.loadCSV(filepath)
        return jsonify(df.schema.json())
        # return jsonify(filepath)
class GetMonthwiseSales(Resource):
    def get(self):
        df = sparkSession.loadCSV("C:\\Users\\sumitd.XEBIAINDIA\\Documents\\Official\\Categorical_Analysis\\data\\transactions.csv")
        res = sparkSession.get_monthwise_sales(df)
        return res

class GetKPI(Resource):
    def get(self):
        transactions = sparkSession.loadCSV("C:\\Users\\sumitd.XEBIAINDIA\\Documents\\Official\\Categorical_Analysis\\data\\transactions.csv")
        users = sparkSession.loadCSV("C:\\Users\\sumitd.XEBIAINDIA\\Documents\\Official\\Categorical_Analysis\\data\\cards.csv")
        stores = sparkSession.loadCSV("C:\\Users\\sumitd.XEBIAINDIA\\Documents\\Official\\Categorical_Analysis\\data\\stores.csv")
        country_count = transactions.select('Country').distinct().count()

        res  = {}
        res['transaction_count'] = transactions.count()
        res['store_count'] = stores.count()
        res['product_count'] =  transactions.select('Product_Code').distinct().count()
        res['country_count'] = transactions.select('Country').distinct().count()
        res['user_count'] = users.count()
        return json.dumps(res)

def initSparkSession(appName):
    global sparkSession
    sparkSession = SparkJobs(appName)
