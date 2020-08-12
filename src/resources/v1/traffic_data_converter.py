# -*- coding: UTF-8 -*-

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


class xml_to_json(Resource):
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
        [資料集批次轉換模式]
        適用一組資料轉換json
        命令格式： /v1/traffic_data/class/{dataclass}/authority/{authority}/standard/MOTC_traffic_v2/xml_to_json -X POST -d {data}
        ---
        tags:
          - MOTC Traffic v2 Converter API (提供轉換交通部「即時路況資料標準(V2.0)」格式資料)
        consumes:
          - application/xml
        produces:
          - application/json
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
            description: 輸入一組資料(XML格式)
        responses:
          200:
            description: OK
         """

        message = ''

        # 讀取API傳入參數
        args = self.parser.parse_args()

        # 參數轉小寫處裡
        dataclass_lower = dataclass.lower()
        authority_lower = authority.lower()

        # 輸入JSON文件
        data = request.get_json()
        one_records = data[dataclass_record_name[dataclass]]

        message = 'upload succeeded'

        return {
                   'message': message
               }, 200

    def put(self):
        pass

    def delete(self):
        pass
