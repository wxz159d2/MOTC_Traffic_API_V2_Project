# -*- coding: UTF-8 -*-

# 以交通部「即時路況資料標準(V2.0)」上傳的方法
# 寫入目標MongoDB之資料表，要以下命令建立唯一索引(以VD為例)
#     db.vdlive.createIndex({"VDID":1,"DataCollectTime":1},{unique: true})

from flask import request
from flask_restful import Resource, reqparse
from py4j.protocol import Py4JJavaError
from pyspark.sql.utils import convert_exception

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
        """
        [資料集批次寫入模式]
        處理速度快，但含重複資料則整批廢棄
        命令格式： /v1/traffic_data/class/{dataclass}/authority/{authority}/standard/MOTC_traffic_v2 -X POST -d {data}
        ---
        tags:
          - MOTC Traffic v2 Upload API (提供上傳交通部「即時路況資料標準(V2.0)」格式資料)
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
        one_records = data[dataclass_record_name[dataclass]]
        df = spark.read.json(sc.parallelize(one_records))
        try:
            df.write.format('mongo').mode('append').option('uri', mongo_url_db).save()
        except Py4JJavaError as e:
            message = convert_exception(e.java_exception)
        message = 'upload succeeded'

        return {
                   'message': message
               }, 200

    def put(self):
        pass

    def delete(self):
        pass


class Upload_repeat_check(Resource):
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
        [資料集重複驗證模式]
        處理速度較慢，可逐筆寫入未重複資料
        命令格式： /v1/traffic_data/class/{dataclass}/authority/{authority}/standard/MOTC_traffic_v2/repeat_check -X POST -d {data}
        ---
        tags:
          - MOTC Traffic v2 Upload API (提供上傳交通部「即時路況資料標準(V2.0)」格式資料)
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
        one_records = data[dataclass_record_name[dataclass]]
        for one_record in one_records:
            df = spark.read.json(sc.parallelize([one_record]))
            try:
                df.write.format('mongo').mode('append').option('uri', mongo_url_db).save()
            except Py4JJavaError as e:
                message = convert_exception(e.java_exception)
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
        [單筆資料模式]
        僅可寫入未重複資料
        命令格式： /v1/traffic_data/class/{dataclass}/authority/{authority}/standard/MOTC_traffic_v2/one_record -X POST -d {data}
        ---
        tags:
          - MOTC Traffic v2 Upload API (提供上傳交通部「即時路況資料標準(V2.0)」格式資料)
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
        one_record = request.get_json()
        df = spark.read.json(sc.parallelize([one_record]))
        try:
            df.write.format('mongo').mode('append').option('uri', mongo_url_db).save()
        except Py4JJavaError as e:
            message = convert_exception(e.java_exception)
        message = 'upload succeeded'

        return {
                   'message': message
               }, 200

    def put(self):
        pass

    def delete(self):
        pass
