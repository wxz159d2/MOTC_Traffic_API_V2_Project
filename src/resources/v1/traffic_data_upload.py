# -*- coding: UTF-8 -*-
from flask import request
from flask_restful import Resource, reqparse


class Upload(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('format', type=str, required=False, help='Param error: Format',
                                 choices=['JSON', 'XML'])

    def get(self):
        pass

    def post(self, authority, dataclass):
        # test com: curl http://127.0.0.1:5000/v1/traffic_data/class/VDLive/authority/NFB/standard/MOTC_traffic_v2?format=JSON/ -X POST -d {"VDID":"VD-N3-S-236-I-WS-1X-南下入口2","SubAuthorityCode":"NFB-CR","LinkFlows":[{"LinkID":"0000301047050M","Lanes":[{"LaneID":0,"LaneType":2,"Speed":61.0,"Occupancy":2.0,"Vehicles":[{"VehicleType":"S","Volume":3,"Speed":61.0},{"VehicleType":"L","Volume":1,"Speed":61.0},{"VehicleType":"T","Volume":0,"Speed":0.0}]}]}],"Status":0,"DataCollectTime":"2020-07-02T15:17:00+08:00"}

        # flasgger 預設入口 http://localhost:5000/apidocs
        """
        本API提供上傳交通部「即時路況資料標準(V2.0)」MOTC Traffic API V2格式資料。
        命令格式： /v1/traffic_data/class/{dataclass}/authority/{authority}/standard/MOTC_traffic_v2?format={format} -X POST -d {data}
        ---
        tags:
          - Traffic Upload API
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
          - in: body
            name: data
            required: true
            description: 輸入一組資料(支援JSON、XML)
            example: {}
        responses:
          200:
            description: OK
         """

        args = self.parser.parse_args()
        format = args['format']

        # pyspark讀取語法
        # 輸入JSON文件
        from api import spark, sc
        data = request.get_json()
        df = spark.read.json(sc.parallelize([data]))
        df.write.format("mongo").mode("append").option("spark.mongodb.output.uri",
                                                       "mongodb://127.0.0.1:27017/traffic_data_nfb.vd").save()

        return {
                   'message': 'ok',
                   'authority': authority,
                   'data_type': dataclass,
                   'format': format,
                   'data': data
               }, 200

    def put(self):
        pass

    def delete(self):
        pass


class Upload_one_record(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('format', type=str, required=False, help='Param error: Format',
                                 choices=['JSON', 'XML'])

    def get(self):
        pass

    def post(self, authority, dataclass):
        # test com: curl http://127.0.0.1:5000/v1/traffic_data/class/VDLive/authority/NFB/standard/MOTC_traffic_v2/one_record?format=JSON/ -X POST -d {"VDID":"VD-N3-S-236-I-WS-1X-南下入口2","SubAuthorityCode":"NFB-CR","LinkFlows":[{"LinkID":"0000301047050M","Lanes":[{"LaneID":0,"LaneType":2,"Speed":61.0,"Occupancy":2.0,"Vehicles":[{"VehicleType":"S","Volume":3,"Speed":61.0},{"VehicleType":"L","Volume":1,"Speed":61.0},{"VehicleType":"T","Volume":0,"Speed":0.0}]}]}],"Status":0,"DataCollectTime":"2020-07-02T15:17:00+08:00"}

        # flasgger 預設入口 http://localhost:5000/apidocs
        """
        本API提供上傳交通部「即時路況資料標準(V2.0)」MOTC Traffic API V2格式資料。
        命令格式： /v1/traffic_data/class/{dataclass}/authority/{authority}/standard/MOTC_traffic_v2/one_record?format={format} -X POST -d {data}
        ---
        tags:
          - Traffic Upload API
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
          - in: body
            name: data
            required: true
            description: 輸入一筆資料(支援JSON、XML)
            example: {}
        responses:
          200:
            description: OK
         """

        args = self.parser.parse_args()
        format = args['format']

        data = request.get_json()

        return {
                   'message': 'ok',
                   'authority': authority,
                   'data_type': dataclass,
                   'format': format,
                   'data': data
               }, 200

    def put(self):
        pass

    def delete(self):
        pass
