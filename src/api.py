# -*- coding: UTF-8 -*-
import logging

from flasgger import Swagger
from flask import Flask
from flask_restful import Api
from pymongo import MongoClient
from pyspark.sql import SparkSession

from resources.v1.traffic_data_converter import *
from resources.v1.traffic_data_get import *
from resources.v1.traffic_data_upload import *

FORMAT = '%(asctime)s %(levelname)s: %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT)

app = Flask(__name__)
api = Api(app)

# API版面風格管理
# flasgger UI: http://localhost:5000/apidocs
swagger_config = Swagger.DEFAULT_CONFIG
swagger_config['title'] = 'CECI-運輸大數據平台-V1'
swagger_config['description'] = """
● 路況資料整合系統
● 本平台資料資料介接來源：「交通部即時路況與停車資訊流通平臺」https://traffic.transportdata.tw/
● 聯絡人：平台系統問題及異常回報-電機部 王翔正(ext.3018) wxz159d2@ceci.com.tw
"""
swagger_config['version'] = '0.1.2-bate'
swagger_config['termsOfService'] = ''
swagger = Swagger(app, config=swagger_config)

# Spark連接MongoDB設定
# mongo url格式是："mongodb://IP:Port/database.collection"
token_file = open('./token', mode='r')
token = token_file.readline()
token_file.close()
mongo_settings = {
    # test host
    # 'MONGO_HOST': '127.0.0.1',
    # k8s host
    'MONGO_HOST': 'mongodb',
    'MONGO_PORT': '27017',
    'MONGO_DB': '',
    'MONGO_USER': 'tbdUSER',
    'MONGO_PSW': token,
    'MONGO_MACH': 'SCRAM-SHA-1',
}
mongo_url = 'mongodb://' + mongo_settings['MONGO_USER'] + ':' + mongo_settings['MONGO_PSW'] + '@' \
            + mongo_settings['MONGO_HOST'] + ':' + mongo_settings['MONGO_PORT'] \
            + '/?authMechanism=' + mongo_settings['MONGO_MACH']
mongo_client = MongoClient(mongo_url)

# 要把mongo-spark-connector_2.12-3.0.0.jar和mongo-java-driver-3.12.6.jar放在\pyspark\jars裡
# 線上環境可用自動配置 .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0")
# spark UI: http://localhost:4041/
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("CECI_traffic_data") \
    .config("spark.jars",
            "/usr/local/spark/jars/mongo-spark-connector_2.12-3.0.0.jar,/usr/local/spark/jars/mongodb-driver-sync-4.0.5.jar,/usr/local/spark/jars/bson-4.0.5.jar,/usr/local/spark/jars/mongo-java-driver-3.12.6.jar") \
    .getOrCreate()
sc = spark.sparkContext

# flask_restful資源設定
api.add_resource(Get_t2_one_record,
                 "/v1/traffic_data/authority/<authority>/class/<dataclass>/oid/<oid>/date/<date>/standard/MOTC_traffic_v2/")
api.add_resource(Get_t2_time_range,
                 "/v1/traffic_data/authority/<authority>/class/<dataclass>/oid/<oid>/date/<sdate>/to/<edate>/standard/MOTC_traffic_v2/")
api.add_resource(Get_one_record_slsv,
                 "/v1/traffic_data/authority/<authority>/oid/<oid>/date/<date>/method/sum_lanes/sum_vehicles/")
api.add_resource(Get_one_record_slpv,
                 "/v1/traffic_data/authority/<authority>/oid/<oid>/date/<date>/method/sum_lanes/per_vehicles/")
api.add_resource(Get_one_record_plsv,
                 "/v1/traffic_data/authority/<authority>/oid/<oid>/date/<date>/method/per_lanes/sum_vehicles/")
api.add_resource(Get_one_record_plpv,
                 "/v1/traffic_data/authority/<authority>/oid/<oid>/date/<date>/method/per_lanes/per_vehicles/")
api.add_resource(Get_time_range_slsv,
                 "/v1/traffic_data/authority/<authority>/oid/<oid>/date/<sdate>/to/<edate>/method/sum_lanes/sum_vehicles/")
api.add_resource(Upload_batch,
                 "/v1/traffic_data/authority/<authority>/class/<dataclass>/standard/MOTC_traffic_v2/method/batch")
# api.add_resource(Upload_repeat_check,
#                  "/v1/traffic_data/authority/<authority>/class/<dataclass>/standard/MOTC_traffic_v2/method/repeat_check")
api.add_resource(Upload_one_record_live,
                 "/v1/traffic_data/authority/<authority>/class/<dataclass>/standard/MOTC_traffic_v2/method/one_record")
api.add_resource(Upload_one_record_static,
                 "/v1/traffic_data/authority/<authority>/class/<dataclass>/update/<date>/standard/MOTC_traffic_v2/method/one_record")
api.add_resource(Converter_batch_xml_to_json,
                 "/v1/traffic_data/class/<dataclass>/standard/MOTC_traffic_v2/method/batch/xml_to_json")

if __name__ == '__main__':
    app.config['JSON_AS_ASCII'] = False
    app.config.update(RESTFUL_JSON=dict(ensure_ascii=False))
    app.run(host='0.0.0.0', debug=True)
