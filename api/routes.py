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



def initSparkSession(appName):
    global sparkSession
    sparkSession = SparkJobs(appName)
