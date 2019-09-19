# ADD ONLY ROUTES, WHICH ARE SPECIFIC TO THE API and BACKEND 


# Import Flask.Blueprint to create a modular code 
from flask import Blueprint, jsonify
from flask_restful import Resource
from .spark_jobs import SparkJobs
import json
from millify import prettify

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
        res  = {}
        res['transaction_count'] = prettify(sparkSession.transactions.count())
        res['store_count'] = prettify(sparkSession.stores.count())
        res['product_count'] =  prettify(sparkSession.transactions.select('Product_Code').distinct().count())
        res['country_count'] = prettify(sparkSession.transactions.select('Country').distinct().count())
        res['user_count'] = prettify(sparkSession.users.count())
        return json.dumps(res)

class GetLoyalty(Resource):
    def get(self):
        return sparkSession.get_loyalty()


class GetTransactions(Resource):
    def get(self):
        return "{hello : world}"

class GetCountries(Resource):
    def get(self):
        return "{hello : world}"

class GetStores(Resource):
    def get(self):
        return "{hello : world}"

class GetUsers(Resource):
    def get(self):
        return sparkSession.get_users()

class GetUsersDistribution(Resource):
    def get(self):
        return sparkSession.get_users_distribution()

class GetProducts(Resource):
    def get(self):
        return "{hello : world}"

class GetTopCustomers(Resource):
    def get(self):
        return sparkSession.get_top_customers()

def initSparkSession(appName):
    global sparkSession
    sparkSession = SparkJobs(appName)
