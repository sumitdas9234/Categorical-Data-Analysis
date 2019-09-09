# ADD ONLY ROUTES, WHICH ARE SPECIFIC TO THE fRONTEND DASHBOARD


# Import Flask.Blueprint to create a modular code 
from flask import Blueprint
from flask import render_template
import os
import requests
import prettify

# Instantiate the Blueprint named "web" with following config
web = Blueprint('web', __name__)

def convert_bytes(num):
    """
    this function will convert bytes to MB.... GB... etc
    """
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0


# Creating new routes --> Example
@web.route("/")
def index():
    return render_template('index.html', title="My Dashboard")


@web.route("/full_report")
def full_report():
    return render_template('report.html', title="Charts")

@web.app_template_filter('fileManager')
def fileManager(root_dir):
    for r, d, f in os.walk(root_dir):
        dirs = []
        for directory in d:
            dirs.append([directory, os.path.abspath(directory)])
        files = []
        for file in f:
            files.append([file, convert_bytes(os.path.getsize(os.path.join(root_dir, file))), os.path.abspath(os.path.join(root_dir, file))])

        return [os.path.abspath(r).split('\\')[-1], dirs, files]

@web.app_template_filter('pretify')
def pretify(number):
    return prettify(number)

@web.app_template_filter('spark_api')
def spark_api(x):
    url = "http://localhost:4041/api/v1/applications"
    return requests.post(url)
