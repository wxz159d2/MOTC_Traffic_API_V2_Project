# -*- coding: UTF-8 -*-
import json

from flask_restful import Resource, reqparse

dataclass_id_name = {
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


class Get_t2_one_record(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('dataclass', type=str, required=False, help='Param error: dataclass',
                                 choices=['VD', 'VDLive', 'CCTV', 'CMS', 'CMSLive', 'AVI', 'AVIPair', 'AVIPairLive',
                                          'ETag', 'ETagPair', 'ETagPairLive', 'GVPLiveTraffic', 'CVPLiveTraffic',
                                          'Section', 'SectionLink', 'LiveTraffic', 'CongestionLevel', 'SectionShape',
                                          'News'])
        self.parser.add_argument('authority', type=str, required=False, help='Param error: authority',
                                 choices=['NFB', 'THB', 'TNN'])
        self.parser.add_argument('format', type=str, required=True, help='Param error: Format',
                                 choices=['JSON', 'XML'])

    def get(self, dataclass, authority, oid, date):
        """
        [路況標準2.0][單筆歷史資料查詢]
        提供查詢指定設備及資料時間之單筆[路況標準2.0]格式資料
        命令格式： /v1/traffic_data/class/{dataclass}/authority/{authority}/id/{id}/date/{date}/standard/MOTC_traffic_v2/method/one_record?format={format}
        ---
        tags:
          - Traffic Data Query API (交通資料查詢API)
        parameters:
          - in: path
            name: dataclass
            type: string
            required: true
            description: 資料型態(依即時路況資料標準V2.0資料類型訂定，如VD、VDLive、LiveTraffic...)
            enum: ['VD', 'VDLive', 'CCTV', 'CMS', 'CMSLive', 'AVI', 'AVIPair', 'AVIPairLive',
                                          'ETag', 'ETagPair', 'ETagPairLive', 'GVPLiveTraffic', 'CVPLiveTraffic',
                                          'Section', 'SectionLink', 'LiveTraffic', 'CongestionLevel', 'SectionShape',
                                          'News']
          - in: path
            name: authority
            type: string
            required: true
            description: 業管機關簡碼(https://traffic-api-documentation.gitbook.io/traffic/xiang-dai-zhao-biao)
            enum: ['NFB', 'THB', 'TNN']
          - in: path
            name: oid
            type: string
            required: true
            description: 設備、資料之ID
          - in: path
            name: date
            type: string
            required: true
            description: 資料代表之時間(動態資料參照欄位：DataCollectTime、靜態資料參照欄位：UpdateTime)[格式：ISO8601]
            default: '2020-08-13T10:49:00+08:00'
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

        from api import mongo_url

        message = ''

        # 讀取API傳入參數
        args = self.parser.parse_args()
        format = args['format']

        # 參數轉小寫處裡
        dataclass_lower = dataclass.lower()
        authority_lower = authority.lower()

        # MongoDB連結設定參數處裡
        database = 'traffic_data_' + authority_lower
        collection = dataclass_lower
        mongo_url_db = mongo_url + database + '.' + collection

        # pyspark讀取語法
        from api import spark
        id_name = dataclass_id_name[dataclass]
        if ('Live' in dataclass) or dataclass == 'News':
            # 動態資料查詢管道指令
            pipeline = "{'$match':{'$and':[{'" + id_name + "':'" + oid + "'},{'DataCollectTime':{'$date':'" + date + "'}}]}}"
        else:
            # 靜態資料查詢管道指令
            pipeline = "{'$match':{'$and':[{'" + id_name + "':'" + oid + "'},{'UpdateTime':{'$date':'" + date + "'}}]}}"
        df = spark.read.format('mongo').option('uri', mongo_url_db).option('pipeline', pipeline).load()
        json_data_list = df.toJSON().collect()
        json_data = []
        for values in json_data_list:
            json_dict = json.loads(values)
            del json_dict['_id']  # 刪除momgo的資料編號
            json_data.append(json_dict)

        return json_data, 200

    def post(self):
        pass

    def put(self):
        pass

    def delete(self):
        pass
