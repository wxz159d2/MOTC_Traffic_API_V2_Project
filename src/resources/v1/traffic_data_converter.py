# -*- coding: UTF-8 -*-

import xmltodict
from flask import request
from flask_restful import Resource, reqparse

dataclass_record_name = {
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

dataclass_list_name = {
    'VD': 'VDList',
    'VDLive': 'VDLiveList',
    'CCTV': 'CCTVList',
    'CMS': 'CMSList',
    'CMSLive': 'CMSLiveList',
    'AVI': 'AVIList',
    'AVIPair': 'AVIPairList',
    'AVIPairLive': 'AVIPairList',
    'ETag': 'ETagList',
    'ETagPair': 'ETagPairList',
    'ETagPairLive': 'ETagPairLiveList',
    'GVPLiveTraffic': 'GVPLiveTrafficList',
    'CVPLiveTraffic': 'CVPLiveTrafficList',
    'Section': 'SectionList',
    'SectionLink': 'SectionLinkList',
    'LiveTraffic': 'LiveTrafficList',
    'CongestionLevel': 'CongestionLevelList',
    'SectionShape': 'SectionShapeList',
    'News': 'NewsList'
}


class Converter_batch_xml_to_json(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('dataclass', type=str, required=False, help='Param error: dataclass',
                                 choices=['VD', 'VDLive', 'CCTV', 'CMS', 'CMSLive', 'AVI', 'AVIPair', 'AVIPairLive',
                                          'ETag', 'ETagPair', 'ETagPairLive', 'GVPLiveTraffic', 'CVPLiveTraffic',
                                          'Section', 'SectionLink', 'LiveTraffic', 'CongestionLevel', 'SectionShape',
                                          'News'])

    def get(self):
        pass

    def post(self, dataclass):
        """
        [路況標準2.0][資料集批次轉換json模式]
        適用一組「即時路況資料標準(V2.0)」xml資料轉換json
        命令格式： /v1/traffic_data/class/{dataclass}/standard/MOTC_traffic_v2/method/batch/xml_to_json -X POST -d {data}
        ---
        tags:
          - Traffic Data Converter API (資料轉換工具API)
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
            enum: ['VD', 'VDLive', 'CCTV', 'CMS', 'CMSLive', 'AVI', 'AVIPair', 'AVIPairLive',
                   'ETag', 'ETagPair', 'ETagPairLive', 'GVPLiveTraffic', 'CVPLiveTraffic',
                   'Section', 'SectionLink', 'LiveTraffic', 'CongestionLevel', 'SectionShape',
                   'News']
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

        # 輸入XML文件
        data = request.data
        json_dict = xmltodict.parse(data)
        json_dict = json_dict[dataclass_list_name[dataclass]]
        del json_dict['@xsi:schemaLocation']
        del json_dict['@xmlns:xsi']
        del json_dict['@xmlns']
        json_dict[dataclass_record_name[dataclass]] = json_dict[dataclass_record_name[dataclass]][dataclass]
        del json_dict['Count']

        message = 'succeeded'

        return json_dict, 200

    def put(self):
        pass

    def delete(self):
        pass
