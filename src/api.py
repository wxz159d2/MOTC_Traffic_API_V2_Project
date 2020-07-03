# -*- coding: UTF-8 -*-

from flasgger import Swagger
from flask import Flask
from flask_pymongo import PyMongo
from flask_restful import Api

from resources.v1.traffic_data_get import Get
from resources.v1.traffic_data_upload import Upload

app = Flask(__name__)
app.config["MONGO_URI"] = "mongodb://localhost:27017/traffic_data"

api = Api(app)
# API版面風格管理
swagger_config = Swagger.DEFAULT_CONFIG
swagger_config['title'] = 'CECI-運輸大數據平台-V1'
swagger_config['description'] = '路況資料整合系統'
swagger_config['version'] = '0.1.0'
swagger_config['termsOfService'] = ''
swagger = Swagger(app, config=swagger_config)

mongo = PyMongo(app)

api.add_resource(Get, "/v1/get/<string:xxx>/", endpoint="get")
api.add_resource(Upload, "/v1/upload/", endpoint="upload")

if __name__ == '__main__':
    app.config['JSON_AS_ASCII'] = False
    app.config.update(RESTFUL_JSON=dict(ensure_ascii=False))
    app.run(host='0.0.0.0', debug=True)
