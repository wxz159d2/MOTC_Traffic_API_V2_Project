# -*- coding: UTF-8 -*-

# 以交通部「即時路況資料標準(V2.0)」上傳的方法
# 寫入目標MongoDB之資料表，要以下命令建立唯一索引(以VD為例)
#     db.vdlive.createIndex({"VDID":1,"DataCollectTime":1},{unique: true})

from flask import request
from flask_restful import Resource, reqparse

dataclass_record_name = {
    'VD': 'VDs',
    'VDLive': 'VDLives',
    'CCTV': 'CCTVs',
    'CMS': 'CMSs',
    'CMSLive': 'CMSLives',
    'ETag': 'ETags',
    'ETagPair': 'ETagPairs',
    'ETagPairLive': 'ETagPairLives',
    'Section': 'Sections',
    'SectionLink': 'SectionLinks',
    'LiveTraffic': 'LiveTraffics',
    'CongestionLevel': 'CongestionLevels',
    'SectionShape': 'SectionShapes',
    'News': 'Newses'
}


class Upload(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('dataclass', type=str, required=False, help='Param error: dataclass',
                                 choices=['VD', 'VDLive', 'CCTV', 'CMS', 'CMSLive', 'ETag', 'ETagPair', 'ETagPairLive',
                                          'Section', 'SectionLink', 'LiveTraffic', 'CongestionLevel', 'SectionShape',
                                          'News'])
        self.parser.add_argument('authority', type=str, required=False, help='Param error: authority',
                                 choices=['NFB', 'THB', 'TNN'])

    def get(self):
        pass

    def post(self, dataclass, authority):
        # test com: curl http://127.0.0.1:5000/v1/traffic_data/class/VDLive/authority/NFB/standard/MOTC_traffic_v2?format=JSON/ -X POST -d {"VDID":"VD-N3-S-236-I-WS-1X-南下入口2","SubAuthorityCode":"NFB-CR","LinkFlows":[{"LinkID":"0000301047050M","Lanes":[{"LaneID":0,"LaneType":2,"Speed":61.0,"Occupancy":2.0,"Vehicles":[{"VehicleType":"S","Volume":3,"Speed":61.0},{"VehicleType":"L","Volume":1,"Speed":61.0},{"VehicleType":"T","Volume":0,"Speed":0.0}]}]}],"Status":0,"DataCollectTime":"2020-07-02T15:17:00+08:00"}
        """
        本API提供上傳交通部「即時路況資料標準(V2.0)」MOTC Traffic API V2格式資料：資料集模式。
        命令格式： /v1/traffic_data/class/{dataclass}/authority/{authority}/standard/MOTC_traffic_v2 -X POST -d {data}
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
          - in: body
            name: data
            required: true
            description: 輸入一組資料(JSON格式)
        responses:
          200:
            description: OK
         """

        from api import spark
        from api import sc
        from api import mongo_url

        message = ''

        # 讀取API傳入參數
        args = self.parser.parse_args()

        # 參數轉小寫處裡
        dataclass_lower = dataclass.lower()
        authority_lower = authority.lower()

        # MongoDB連結設定參數處裡
        database = 'traffic_data_' + authority_lower
        collection = dataclass_lower
        mongo_url_db = mongo_url + database + '.' + collection

        # pyspark寫入語法
        # 輸入JSON文件
        data = request.get_json()
        df = spark.read.json(sc.parallelize(data[dataclass_record_name[dataclass]]))
        df.write.format('mongo').mode('append').option('uri', mongo_url_db).save()
        message = 'upload succeeded'

        return {
                   'message': message
               }, 200

    def put(self):
        pass

    def delete(self):
        pass


class Upload_one_record(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('dataclass', type=str, required=False, help='Param error: dataclass',
                                 choices=['VD', 'VDLive', 'CCTV', 'CMS', 'CMSLive', 'ETag', 'ETagPair', 'ETagPairLive',
                                          'Section', 'SectionLink', 'LiveTraffic', 'CongestionLevel', 'SectionShape',
                                          'News'])
        self.parser.add_argument('authority', type=str, required=False, help='Param error: authority',
                                 choices=['NFB', 'THB', 'TNN'])

    def get(self):
        pass

    def post(self, dataclass, authority):
        """
        本API提供上傳交通部「即時路況資料標準(V2.0)」MOTC Traffic API V2格式資料：單筆資料模式。
        命令格式： /v1/traffic_data/class/{dataclass}/authority/{authority}/standard/MOTC_traffic_v2/one_record -X POST -d {data}
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
          - in: body
            name: data
            required: true
            description: 輸入一筆資料(JSON格式)
        responses:
          200:
            description: OK
         """

        from api import spark
        from api import sc
        from api import mongo_url

        message = ''

        # 讀取API傳入參數
        args = self.parser.parse_args()

        # 參數轉小寫處裡
        dataclass_lower = dataclass.lower()
        authority_lower = authority.lower()

        # MongoDB連結設定參數處裡
        database = 'traffic_data_' + authority_lower
        collection = dataclass_lower
        mongo_url_db = mongo_url + database + '.' + collection

        # pyspark寫入語法
        # 輸入JSON文件
        data = request.get_json()
        df = spark.read.json(sc.parallelize([data]))
        df.write.format('mongo').mode('append').option('uri', mongo_url_db).save()
        message = 'upload succeeded'

        return {
                   'message': message
               }, 200

    def put(self):
        pass

    def delete(self):
        pass
