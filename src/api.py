# -*- coding: UTF-8 -*-

from flasgger import Swagger
from flask import Flask
from flask_restful import Api
from pyspark.sql import SparkSession

from resources.v1.traffic_data_converter import *
from resources.v1.traffic_data_get import *
from resources.v1.traffic_data_upload import *

app = Flask(__name__)
api = Api(app)

# API版面風格管理
# flasgger UI: http://localhost:5000/apidocs
swagger_config = Swagger.DEFAULT_CONFIG
swagger_config['title'] = 'CECI-運輸大數據平台-V1'
swagger_config['description'] = '路況資料整合系統'
swagger_config['version'] = '0.1.0'
swagger_config['termsOfService'] = ''
swagger = Swagger(app, config=swagger_config)

# Spark連接MongoDB設定
# mongo url格式是："mongodb://IP:Port/database.collection"
mongo_url = 'mongodb://127.0.0.1:27017/'
# 要把mongo-spark-connector_2.12-3.0.0.jar和mongo-java-driver-3.12.6.jar放在\pyspark\jars裡
# spark UI: http://localhost:4041/
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("CECI_traffic_data") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0") \
    .getOrCreate()
sc = spark.sparkContext

# flask_restful資源設定
api.add_resource(Get_t2_one_record,
                 "/v1/traffic_data/class/<dataclass>/authority/<authority>/oid/<oid>/date/<date>/standard/MOTC_traffic_v2/method/one_record")
api.add_resource(Get_t2_time_range,
                 "/v1/traffic_data/class/<dataclass>/authority/<authority>/oid/<oid>/date/<sdate>/to/<edate>/standard/MOTC_traffic_v2/method/time_range")
api.add_resource(Upload_batch,
                 "/v1/traffic_data/class/<dataclass>/authority/<authority>/standard/MOTC_traffic_v2/method/batch")
api.add_resource(Upload_repeat_check,
                 "/v1/traffic_data/class/<dataclass>/authority/<authority>/standard/MOTC_traffic_v2/method/repeat_check")
api.add_resource(Upload_one_record_live,
                 "/v1/traffic_data/class/<dataclass>/authority/<authority>/standard/MOTC_traffic_v2/method/one_record")
api.add_resource(Upload_one_record_static,
                 "/v1/traffic_data/class/<dataclass>/authority/<authority>/update/<date>/standard/MOTC_traffic_v2/method/one_record")
api.add_resource(Converter_batch_xml_to_json,
                 "/v1/traffic_data/class/<dataclass>/standard/MOTC_traffic_v2/method/batch_xml_to_json")

if __name__ == '__main__':
    app.config['JSON_AS_ASCII'] = False
    app.config.update(RESTFUL_JSON=dict(ensure_ascii=False))
    app.run(host='0.0.0.0', debug=True)
