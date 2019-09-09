# Import Flask, flask_restful and CORS
from flask import Flask
from flask_restful import Resource, Api
from flask_cors import CORS
# Import rest_api from api.routes
from api.routes import rest_api
from api.routes import *
# Import web from web.routes
from web.routes import web


if __name__ == '__main__':
    # Setting up the App Name for the Flask Server
    app = Flask(__name__, template_folder='./web/templates', static_folder='./web/static')

    # Enable Cross Origin Support for the App (if Required)
    CORS(app)

    # Setting up the API class
    api = Api(rest_api, prefix='/api')

    # Adding the API endpoints
    api.add_resource(GetAppID, '/getStuff')
    api.add_resource(LoadCSV, '/load_csv/<string:filepath>')
    api.add_resource(GetMonthwiseSales, '/getMonthwiseSales')
    api.add_resource(GetKPI, '/getKPI')

    # Register the blueprints registered
    app.register_blueprint(rest_api)
    app.register_blueprint(web)

    #Initilise the sparkSession from api.routes
    initSparkSession("my_app")

    # Run the Flask Application
    app.run(debug = True)