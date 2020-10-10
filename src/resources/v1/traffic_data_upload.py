# -*- coding: UTF-8 -*-

# 以交通部「即時路況資料標準(V2.0)」上傳的方法
# 寫入目標MongoDB之資料表，要以下命令建立唯一索引
"""
db.vd.createIndex({"VDID":1,"UpdateTime":1},{unique: true})
db.vdlive.createIndex({"VDID":1,"DataCollectTime":1},{unique: true})
db.cctv.createIndex({"CCTVID":1,"UpdateTime":1},{unique: true})
db.cms.createIndex({"CMSID":1,"UpdateTime":1},{unique: true})
db.cmslive.createIndex({"CMSID":1,"DataCollectTime":1},{unique: true})
db.avi.createIndex({"AVIID":1,"UpdateTime":1},{unique: true})
db.avipair.createIndex({"AVIPairID":1,"UpdateTime":1},{unique: true})
db.avipairlive.createIndex({"AVIPairID":1,"DataCollectTime":1},{unique: true})
db.etag.createIndex({"ETagGantryID":1,"UpdateTime":1},{unique: true})
db.etagpair.createIndex({"ETagPairID":1,"UpdateTime":1},{unique: true})
db.etagpairlive.createIndex({"ETagPairID":1,"DataCollectTime":1},{unique: true})
db.gvplivetraffic.createIndex({"SectionID":1,"DataCollectTime":1},{unique: true})
db.cvplivetraffic.createIndex({"SectionID":1,"DataCollectTime":1},{unique: true})
db.section.createIndex({"SectionID":1,"UpdateTime":1},{unique: true})
db.sectionlink.createIndex({"SectionID":1,"UpdateTime":1},{unique: true})
db.livetraffic.createIndex({"SectionID":1,"DataCollectTime":1},{unique: true})
db.congestionlevel.createIndex({"CongestionLevelID":1,"UpdateTime":1},{unique: true})
db.sectionshape.createIndex({"SectionID":1,"UpdateTime":1},{unique: true})
db.news.createIndex({"NewsID":1,"UpdateTime":1},{unique: true})
"""
import aniso8601
from flask import request
from flask_restful import Resource, reqparse
from py4j.protocol import Py4JJavaError
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from pyspark.sql.utils import convert_exception

dataclass_record = {
    'VD': 'VDs',
    'VDLive': 'VDLives',
    'CCTV': 'CCTVs',
    'CMS': 'CMSs',
    'CMSLive': 'CMSLives',
    'AVI': 'AVIs',
    'AVIPair': 'AVIPairs',
    'AVIPairLive': 'AVIPairs',
    'ETag': 'ETags',
    'ETagPair': 'ETagPairs',
    'ETagPairLive': 'ETagPairLives',
    'GVPLiveTraffic': 'GVPLiveTraffics',
    'CVPLiveTraffic': 'CVPLiveTraffics',
    'Section': 'Sections',
    'SectionLink': 'SectionLinks',
    'LiveTraffic': 'LiveTraffics',
    'CongestionLevel': 'CongestionLevels',
    'SectionShape': 'SectionShapes',
    'News': 'Newses'
}

dataclass_id = {
    'VD': 'VDID',
    'VDLive': 'VDID',
    'CCTV': 'CCTVID',
    'CMS': 'CMSID',
    'CMSLive': 'CMSID',
    'AVI': 'AVIID',
    'AVIPair': 'AVIPairID',
    'AVIPairLive': 'AVIPairID',
    'ETag': 'ETagGantryID',
    'ETagPair': 'ETagPairID',
    'ETagPairLive': 'ETagPairID',
    'GVPLiveTraffic': 'SectionID',  # 依標準可用LinkID填列，未見有機關使用，暫未開發
    'CVPLiveTraffic': 'SectionID',  # 依標準可用LinkID填列，未見有機關使用，暫未開發
    'Section': 'SectionID',
    'SectionLink': 'SectionID',
    'LiveTraffic': 'SectionID',  # 依標準可用LinkID填列，未見有機關使用，暫未開發
    'CongestionLevel': 'CongestionLevelID',
    'SectionShape': 'SectionID',
    'News': 'NewsID'
}


class Upload_batch(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('authority', type=str, required=False, help='Param error: authority',
                                 choices=['NFB', 'THB', 'TNN'])
        self.parser.add_argument('dataclass', type=str, required=False, help='Param error: dataclass',
                                 choices=['VD', 'VDLive', 'CCTV', 'CMS', 'CMSLive', 'AVI', 'AVIPair', 'AVIPairLive',
                                          'ETag', 'ETagPair', 'ETagPairLive', 'GVPLiveTraffic', 'CVPLiveTraffic',
                                          'Section', 'SectionLink', 'LiveTraffic', 'CongestionLevel', 'SectionShape',
                                          'News'])

    def get(self):
        pass

    def post(self, authority, dataclass):
        """
        [資料集批次寫入模式]
        可上傳一組完整格式的資料，且重複資料複寫
        命令格式： /v1/traffic_data/authority/{authority}/class/{dataclass}/standard/MOTC_traffic_v2/method/batch -X POST -d {data}
        ---
        tags:
          - MOTC Traffic v2 Upload API (提供上傳交通部「即時路況資料標準(V2.0)」格式資料)
        parameters:
          - in: path
            name: authority
            type: string
            required: true
            description: 業管機關簡碼(https://traffic-api-documentation.gitbook.io/traffic/xiang-dai-zhao-biao)
            enum: ['NFB', 'THB', 'TNN']
          - in: path
            name: dataclass
            type: string
            required: true
            description: 資料型態(依即時路況資料標準V2.0資料類型訂定，如VD、VDLive、LiveTraffic...)
            enum: ['VD', 'VDLive', 'CCTV', 'CMS', 'CMSLive', 'AVI', 'AVIPair', 'AVIPairLive',
                   'ETag', 'ETagPair', 'ETagPairLive', 'GVPLiveTraffic', 'CVPLiveTraffic',
                   'Section', 'SectionLink', 'LiveTraffic', 'CongestionLevel', 'SectionShape',
                   'News']
          - in: body
            name: data
            required: true
            description: 輸入一組資料(JSON格式)
        responses:
          200:
            description: OK
         """

        from api import mongo_client

        message = ''

        # 讀取API傳入參數
        args = self.parser.parse_args()

        # 參數轉小寫處理
        dataclass_lower = dataclass.lower()
        authority_lower = authority.lower()

        # MongoDB連結設定參數處理
        database = 'traffic_data_' + authority_lower
        collection = dataclass_lower
        db = mongo_client[database]
        collection = db[collection]

        # pyspark寫入語法
        # 輸入JSON文件
        data = request.get_json()
        update_time = data['UpdateTime']
        one_records = data[dataclass_record[dataclass]]
        update_records = []
        for one_record in one_records:
            if not ('DataCollectTime' in one_record) and not ('UpdateTime' in one_record):
                # 靜態資料附加UpdateTime資訊
                one_record.update({'UpdateTime': aniso8601.parse_datetime(update_time)})
                update_records.append(
                    UpdateOne(
                        {dataclass_id[dataclass]: one_record[dataclass_id[dataclass]],
                         'UpdateTime': one_record['UpdateTime']},
                        {"$set": one_record},
                        upsert=True
                    )
                )
            else:
                one_record.update({'DataCollectTime': aniso8601.parse_datetime(one_record['DataCollectTime'])})
                update_records.append(
                    UpdateOne(
                        {dataclass_id[dataclass]: one_record[dataclass_id[dataclass]],
                         'DataCollectTime': one_record['DataCollectTime']},
                        {"$set": one_record},
                        upsert=True
                    )
                )
        try:
            collection.bulk_write(update_records)
        except BulkWriteError as e:
            message = e.details
            return {
                       'message': message
                   }, 404

        message = 'upload succeeded'

        return {
                   'message': message
               }, 200

    def put(self):
        pass

    def delete(self):
        pass


# 20201010 改以pymongo上傳，重複驗證方式棄用
# class Upload_repeat_check(Resource):
#     def __init__(self):
#         self.parser = reqparse.RequestParser()
#         self.parser.add_argument('authority', type=str, required=False, help='Param error: authority',
#                                  choices=['NFB', 'THB', 'TNN'])
#         self.parser.add_argument('dataclass', type=str, required=False, help='Param error: dataclass',
#                                  choices=['VD', 'VDLive', 'CCTV', 'CMS', 'CMSLive', 'AVI', 'AVIPair', 'AVIPairLive',
#                                           'ETag', 'ETagPair', 'ETagPairLive', 'GVPLiveTraffic', 'CVPLiveTraffic',
#                                           'Section', 'SectionLink', 'LiveTraffic', 'CongestionLevel', 'SectionShape',
#                                           'News'])
#
#     def get(self):
#         pass
#
#     def post(self, authority, dataclass):
#         """
#         [資料集重複驗證模式]
#         處理速度較慢，可逐筆寫入未重複資料
#         命令格式： /v1/traffic_data/authority/{authority}/class/{dataclass}/standard/MOTC_traffic_v2/method/repeat_check -X POST -d {data}
#         ---
#         tags:
#           - MOTC Traffic v2 Upload API (提供上傳交通部「即時路況資料標準(V2.0)」格式資料)
#         parameters:
#           - in: path
#             name: authority
#             type: string
#             required: true
#             description: 業管機關簡碼(https://traffic-api-documentation.gitbook.io/traffic/xiang-dai-zhao-biao)
#             enum: ['NFB', 'THB', 'TNN']
#           - in: path
#             name: dataclass
#             type: string
#             required: true
#             description: 資料型態(依即時路況資料標準V2.0資料類型訂定，如VD、VDLive、LiveTraffic...)
#             enum: ['VD', 'VDLive', 'CCTV', 'CMS', 'CMSLive', 'AVI', 'AVIPair', 'AVIPairLive',
#                    'ETag', 'ETagPair', 'ETagPairLive', 'GVPLiveTraffic', 'CVPLiveTraffic',
#                    'Section', 'SectionLink', 'LiveTraffic', 'CongestionLevel', 'SectionShape',
#                    'News']
#           - in: body
#             name: data
#             required: true
#             description: 輸入一組資料(JSON格式)
#         responses:
#           200:
#             description: OK
#          """
#
#         from api import spark
#         from api import sc
#         from api import mongo_url
#
#         message = ''
#
#         # 讀取API傳入參數
#         args = self.parser.parse_args()
#
#         # 參數轉小寫處理
#         dataclass_lower = dataclass.lower()
#         authority_lower = authority.lower()
#
#         # MongoDB連結設定參數處理
#         database = 'traffic_data_' + authority_lower
#         collection = dataclass_lower
#
#         # pyspark寫入語法
#         # 輸入JSON文件
#         data = request.get_json()
#         update_time = data['UpdateTime']
#         one_records = data[dataclass_record[dataclass]]
#         for one_record in one_records:
#             # 靜態資料附加UpdateTime資訊
#             if not ('DataCollectTime' in one_record) and not ('UpdateTime' in one_record):
#                 one_record.update({'UpdateTime': update_time})
#             df = spark.read.json(sc.parallelize([one_record]))
#             try:
#                 df.write.format('mongo') \
#                     .mode('append') \
#                     .option('uri', mongo_url) \
#                     .option("database", database) \
#                     .option("collection", collection) \
#                     .save()
#             except Py4JJavaError as e:
#                 message = convert_exception(e.java_exception)
#         message = 'upload succeeded'
#
#         return {
#                    'message': message
#                }, 200
#
#     def put(self):
#         pass
#
#     def delete(self):
#         pass


class Upload_one_record_live(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('authority', type=str, required=False, help='Param error: authority',
                                 choices=['NFB', 'THB', 'TNN'])
        self.parser.add_argument('dataclass', type=str, required=False, help='Param error: dataclass',
                                 choices=['VDLive', 'CMSLive', 'AVIPairLive',
                                          'ETagPairLive', 'GVPLiveTraffic', 'CVPLiveTraffic',
                                          'LiveTraffic',
                                          'News'])

    def get(self):
        pass

    def post(self, authority, dataclass):
        """
        [單筆動態資料模式]
        僅可寫入單筆動態資料，且重複資料複寫
        命令格式： /v1/traffic_data/authority/{authority}/class/{dataclass}/standard/MOTC_traffic_v2/method/one_record -X POST -d {data}
        ---
        tags:
          - MOTC Traffic v2 Upload API (提供上傳交通部「即時路況資料標準(V2.0)」格式資料)
        parameters:
          - in: path
            name: authority
            type: string
            required: true
            description: 業管機關簡碼(https://traffic-api-documentation.gitbook.io/traffic/xiang-dai-zhao-biao)
            enum: ['NFB', 'THB', 'TNN']
          - in: path
            name: dataclass
            type: string
            required: true
            description: 資料型態(依即時路況資料標準V2.0資料類型訂定，如VD、VDLive、LiveTraffic...)
            enum: ['VDLive', 'CMSLive', 'AVIPairLive',
                   'ETagPairLive', 'GVPLiveTraffic', 'CVPLiveTraffic',
                   'LiveTraffic',
                   'News']
          - in: body
            name: data
            required: true
            description: 輸入一筆資料(JSON格式)
        responses:
          200:
            description: OK
         """

        from api import mongo_client

        message = ''

        # 讀取API傳入參數
        args = self.parser.parse_args()

        # 參數轉小寫處理
        dataclass_lower = dataclass.lower()
        authority_lower = authority.lower()

        # MongoDB連結設定參數處理
        database = 'traffic_data_' + authority_lower
        collection = dataclass_lower
        db = mongo_client[database]
        collection = db[collection]

        # pyspark寫入語法
        # 輸入JSON文件
        one_record = request.get_json()
        update_records = []
        if 'DataCollectTime' in one_record:
            one_record.update({'DataCollectTime': aniso8601.parse_datetime(one_record['DataCollectTime'])})
            update_records.append(
                UpdateOne(
                    {dataclass_id[dataclass]: one_record[dataclass_id[dataclass]],
                     'DataCollectTime': one_record['DataCollectTime']},
                    {"$set": one_record},
                    upsert=True
                )
            )
        else:
            one_record.update({'UpdateTime': aniso8601.parse_datetime(one_record['UpdateTime'])})
            update_records.append(
                UpdateOne(
                    {dataclass_id[dataclass]: one_record[dataclass_id[dataclass]],
                     'UpdateTime': one_record['UpdateTime']},
                    {"$set": one_record},
                    upsert=True
                )
            )
        try:
            collection.bulk_write(update_records)
        except BulkWriteError as e:
            message = e.details
            return {
                       'message': message
                   }, 404

        message = 'upload succeeded'

        return {
                   'message': message
               }, 200

    def put(self):
        pass

    def delete(self):
        pass


class Upload_one_record_static(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('dataclass', type=str, required=False, help='Param error: dataclass',
                                 choices=['VD', 'CCTV', 'CMS', 'AVI', 'AVIPair',
                                          'ETag', 'ETagPair',
                                          'Section', 'SectionLink', 'CongestionLevel', 'SectionShape'])
        self.parser.add_argument('authority', type=str, required=False, help='Param error: authority',
                                 choices=['NFB', 'THB', 'TNN'])

    def get(self):
        pass

    def post(self, authority, dataclass, date):
        """
        [單筆靜態資料模式]
        僅可寫入單筆靜態資料，且重複資料複寫
        命令格式： /v1/traffic_data/authority/{authority}/class/{dataclass}/update/{date}/standard/MOTC_traffic_v2/method/one_record -X POST -d {data}
        ---
        tags:
          - MOTC Traffic v2 Upload API (提供上傳交通部「即時路況資料標準(V2.0)」格式資料)
        parameters:
          - in: path
            name: authority
            type: string
            required: true
            description: 業管機關簡碼(https://traffic-api-documentation.gitbook.io/traffic/xiang-dai-zhao-biao)
            enum: ['NFB', 'THB', 'TNN']
          - in: path
            name: dataclass
            type: string
            required: true
            description: 資料型態(依即時路況資料標準V2.0資料類型訂定，如VD、VDLive、LiveTraffic...)
            enum: ['VD', 'CCTV', 'CMS', 'AVI', 'AVIPair',
                   'ETag', 'ETagPair',
                   'Section', 'SectionLink', 'CongestionLevel', 'SectionShape']
          - in: path
            name: date
            type: string
            required: true
            description: 資料代表之時間(靜態資料參照欄位：UpdateTime)[格式：ISO8601]
            default: '2020-08-13T10:49:00+08:00'
          - in: body
            name: data
            required: true
            description: 輸入一筆資料(JSON格式)
        responses:
          200:
            description: OK
         """

        from api import mongo_client

        message = ''

        # 讀取API傳入參數
        args = self.parser.parse_args()

        # 參數轉小寫處理
        dataclass_lower = dataclass.lower()
        authority_lower = authority.lower()

        # MongoDB連結設定參數處理
        database = 'traffic_data_' + authority_lower
        collection = dataclass_lower
        db = mongo_client[database]
        collection = db[collection]

        # pyspark寫入語法
        # 輸入JSON文件
        one_record = request.get_json()
        update_time = date
        update_records = []
        one_record.update({'UpdateTime': aniso8601.parse_datetime(update_time)})
        update_records.append(
            UpdateOne(
                {dataclass_id[dataclass]: one_record[dataclass_id[dataclass]],
                 'UpdateTime': one_record['UpdateTime']},
                {"$set": one_record},
                upsert=True
            )
        )
        try:
            collection.bulk_write(update_records)
        except BulkWriteError as e:
            message = e.details
            return {
                       'message': message
                   }, 404

        message = 'upload succeeded'

        return {
                   'message': message
               }, 200

    def put(self):
        pass

    def delete(self):
        pass
