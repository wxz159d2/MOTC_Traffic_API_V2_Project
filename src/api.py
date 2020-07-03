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
swagger = Swagger(app)
mongo = PyMongo(app)

api.add_resource(Get, "/v1/get/<string:xxx>/", endpoint="get")
api.add_resource(Upload, "/v1/upload/", endpoint="upload")

if __name__ == '__main__':
    app.config['JSON_AS_ASCII'] = False
    app.config.update(RESTFUL_JSON=dict(ensure_ascii=False))
    app.run(host='0.0.0.0', debug=True)
