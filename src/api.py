# -*- coding: UTF-8 -*-

from flasgger import Swagger
from flask import Flask
# from flask_pymongo import PyMongo
from flask_restful import Api
from pyspark.sql import SparkSession

from resources.v1.traffic_data_get import *
from resources.v1.traffic_data_upload import *

app = Flask(__name__)
# app.config["MONGO_URI"] = "mongodb://localhost:27017/traffic_data"

api = Api(app)
# API版面風格管理
swagger_config = Swagger.DEFAULT_CONFIG
swagger_config['title'] = 'CECI-運輸大數據平台-V1'
swagger_config['description'] = '路況資料整合系統'
swagger_config['version'] = '0.1.0'
swagger_config['termsOfService'] = ''
swagger = Swagger(app, config=swagger_config)

# mongo = PyMongo(app)
# 格式是："mongodb://IP:Port/database.collection"
input_uri = "mongodb://127.0.0.1:27017/traffic_data_nfb.vd"
output_uri = "mongodb://127.0.0.1:27017/traffic_data_nfb.vd"
# 要把mongo-spark-connector_2.12-3.0.0.jar和mongo-java-driver-3.12.6.jar放在\pyspark\jars裡
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("CECI_traffic_data") \
    .config("spark.mongodb.input.uri", input_uri) \
    .config("spark.mongodb.output.uri", output_uri) \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0") \
    .getOrCreate()
sc = spark.sparkContext

api.add_resource(Get, "/v1/traffic_data/class/<dataclass>/authority/<authority>", endpoint="get")
api.add_resource(Upload, "/v1/traffic_data/class/<dataclass>/authority/<authority>/standard/MOTC_traffic_v2")
api.add_resource(Upload_one_record,
                 "/v1/traffic_data/class/<dataclass>/authority/<authority>/standard/MOTC_traffic_v2/one_record")

if __name__ == '__main__':
    app.config['JSON_AS_ASCII'] = False
    app.config.update(RESTFUL_JSON=dict(ensure_ascii=False))
    app.run(host='0.0.0.0', debug=True)
