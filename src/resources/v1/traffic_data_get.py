# -*- coding: UTF-8 -*-

from flask_restful import Resource, reqparse


class Get(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('format', type=str, required=True, help='Param error: Format',
                                 choices=['JSON', 'XML'])

    def get(self, authority, dataclass):
        # flasgger 預設入口 http://localhost:5000/apidocs
        """
        本API提供查詢OOOO格式資料。
        命令格式： /v1/traffic_data/class/{dataclass}/authority/{authority}?format={format}
        ---
        tags:
          - Traffic Get API
        parameters:
          - in: path
            name: dataclass
            type: string
            required: true
            description: 資料型態(依即時路況資料標準V2.0資料類型訂定，如VD、VDLive、LiveTraffic...)
            enum: ['VD', 'VDLive', 'CCTV', 'CMS', 'CMSLive', 'ETag', 'ETagPair', 'ETagPairLive',
                   'Section', 'SectionLink', 'LiveTraffic', 'CongestionLevel', 'SectionShape',
                   'News']
          - in: path
            name: authority
            type: string
            required: true
            description: 業管機關簡碼(https://traffic-api-documentation.gitbook.io/traffic/xiang-dai-zhao-biao)
            enum: ['NFB', 'THB', 'TNN']
          - in: query
            name: format
            type: string
            required: true
            description: 資料格式(支援JSON、XML)
            enum: ['JSON', 'XML']
        responses:
          200:
            description: OK
         """

        args = self.parser.parse_args()
        format = args['format']

        # pyspark讀取語法
        from api import spark
        df = spark.read.format("mongo").option("spark.mongodb.input.uri",
                                               "mongodb://127.0.0.1:27017/tisv.vd_value").load()
        df.show()

        return {
                   'message': 'OK',
                   'authority': authority,
                   'dataclass': dataclass,
                   'Format': format
               }, 200

    def post(self):
        pass

    def put(self):
        pass

    def delete(self):
        pass
