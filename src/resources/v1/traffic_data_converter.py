# -*- coding: UTF-8 -*-

# 僅提供交通2.0標準 XML格式轉JSON使用

import xmltodict
from flask import request
from flask_restful import Resource, reqparse

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

dataclass_list = {
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


def del_json_dict(json_dict, del_key):
    if del_key in json_dict:
        del json_dict[del_key]
    return json_dict


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

        # 參數轉小寫處理
        dataclass_lower = dataclass.lower()

        # 輸入XML文件
        data = request.data
        json_dict = xmltodict.parse(data)
        json_dict = json_dict[dataclass_list[dataclass]]
        json_dict = del_json_dict(json_dict=json_dict, del_key='@xsi:schemaLocation')
        json_dict = del_json_dict(json_dict=json_dict, del_key='@xmlns:xsi')
        json_dict = del_json_dict(json_dict=json_dict, del_key='@xmlns')
        json_dict[dataclass_record[dataclass]] = json_dict[dataclass_record[dataclass]][dataclass]
        json_dict = del_json_dict(json_dict=json_dict, del_key='Count')

        # 轉換類型：VD
        if dataclass == 'VD':
            for i in range(len(json_dict[dataclass_record[dataclass]])):
                if 'SubAuthorityCode' in json_dict[dataclass_record[dataclass]][i]:
                    if isinstance(json_dict[dataclass_record[dataclass]][i]['SubAuthorityCode'], dict):
                        del json_dict[dataclass_record[dataclass]][i]['SubAuthorityCode']
                if not isinstance(json_dict[dataclass_record[dataclass]][i], dict):
                    json_data = []
                    for listdata in json_dict[dataclass_record[dataclass]][i]:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
            for i in range(len(json_dict[dataclass_record[dataclass]])):
                if isinstance(json_dict[dataclass_record[dataclass]][i]['DetectionLinks'], dict):  # 無資料特例處理
                    json_dict[dataclass_record[dataclass]][i]['DetectionLinks'] = [
                        json_dict[dataclass_record[dataclass]][i]['DetectionLinks']['DetectionLink']]
        # 轉換類型：VDLive
        elif dataclass == 'VDLive':
            # 第一層轉換
            for i in range(len(json_dict[dataclass_record[dataclass]])):
                if not isinstance(json_dict[dataclass_record[dataclass]][i], dict):
                    json_data = []
                    for listdata in json_dict[dataclass_record[dataclass]][i]:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
            for i in range(len(json_dict[dataclass_record[dataclass]])):
                json_dict[dataclass_record[dataclass]][i]['LinkFlows'] = [
                    json_dict[dataclass_record[dataclass]][i]['LinkFlows']['LinkFlow']]
            # 第二層轉換
            for i in range(len(json_dict[dataclass_record[dataclass]])):
                for j in range(len(json_dict[dataclass_record[dataclass]][i]['LinkFlows'])):
                    if not isinstance(json_dict[dataclass_record[dataclass]][i]['LinkFlows'][j], dict):
                        json_data = []
                        for listdata in json_dict[dataclass_record[dataclass]][i]['LinkFlows'][j]:
                            json_data.append(listdata)
                        json_dict[dataclass_record[dataclass]][i]['LinkFlows'] = json_data
            for i in range(len(json_dict[dataclass_record[dataclass]])):
                for j in range(len(json_dict[dataclass_record[dataclass]][i]['LinkFlows'])):
                    json_dict[dataclass_record[dataclass]][i]['LinkFlows'][j]['Lanes'] = [
                        json_dict[dataclass_record[dataclass]][i]['LinkFlows'][j]['Lanes']['Lane']]
            # 第三層轉換
            for i in range(len(json_dict[dataclass_record[dataclass]])):
                for j in range(len(json_dict[dataclass_record[dataclass]][i]['LinkFlows'])):
                    for k in range(len(json_dict[dataclass_record[dataclass]][i]['LinkFlows'][j]['Lanes'])):
                        if not isinstance(json_dict[dataclass_record[dataclass]][i]['LinkFlows'][j]['Lanes'][k], dict):
                            json_data = []
                            for listdata in json_dict[dataclass_record[dataclass]][i]['LinkFlows'][j]['Lanes'][k]:
                                json_data.append(listdata)
                            json_dict[dataclass_record[dataclass]][i]['LinkFlows'][j]['Lanes'] = json_data
            for i in range(len(json_dict[dataclass_record[dataclass]])):
                for j in range(len(json_dict[dataclass_record[dataclass]][i]['LinkFlows'])):
                    for k in range(len(json_dict[dataclass_record[dataclass]][i]['LinkFlows'][j]['Lanes'])):
                        json_dict[dataclass_record[dataclass]][i]['LinkFlows'][j]['Lanes'][k]['Vehicles'] = (
                            json_dict[dataclass_record[dataclass]][i]['LinkFlows'][j]['Lanes'][k]['Vehicles'][
                                'Vehicle'])
            # 第四層轉換(選填資料清除)
            for i in range(len(json_dict[dataclass_record[dataclass]])):
                for j in range(len(json_dict[dataclass_record[dataclass]][i]['LinkFlows'])):
                    for k in range(len(json_dict[dataclass_record[dataclass]][i]['LinkFlows'][j]['Lanes'])):
                        vehicles_list = json_dict[dataclass_record[dataclass]][i]['LinkFlows'][j]['Lanes'][k][
                            'Vehicles']  # 變數太長，轉換縮減用
                        for l in range(len(vehicles_list)):
                            if 'Speed' in vehicles_list[l]:
                                if isinstance(vehicles_list[l]['Speed'], dict):
                                    del json_dict[dataclass_record[dataclass]][i]['LinkFlows'][j]['Lanes'][k][
                                        'Vehicles'][l]['Speed']
        # 轉換類型：CCTV
        elif dataclass == 'CCTV':
            for i in range(len(json_dict[dataclass_record[dataclass]])):
                if 'SubAuthorityCode' in json_dict[dataclass_record[dataclass]][i]:
                    if isinstance(json_dict[dataclass_record[dataclass]][i]['SubAuthorityCode'], dict):
                        del json_dict[dataclass_record[dataclass]][i]['SubAuthorityCode']
                if not isinstance(json_dict[dataclass_record[dataclass]][i], dict):
                    json_data = []
                    for listdata in json_dict[dataclass_record[dataclass]][i]:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
        # 轉換類型：CMS
        elif dataclass == 'CMS':
            for i in range(len(json_dict[dataclass_record[dataclass]])):
                if 'SubAuthorityCode' in json_dict[dataclass_record[dataclass]][i]:
                    if isinstance(json_dict[dataclass_record[dataclass]][i]['SubAuthorityCode'], dict):
                        del json_dict[dataclass_record[dataclass]][i]['SubAuthorityCode']
                if not isinstance(json_dict[dataclass_record[dataclass]][i], dict):
                    json_data = []
                    for listdata in json_dict[dataclass_record[dataclass]][i]:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
        # 轉換類型：CMSLive
        elif dataclass == 'CMSLive':
            for i in range(len(json_dict[dataclass_record[dataclass]])):
                if not isinstance(json_dict[dataclass_record[dataclass]][i], dict):
                    json_data = []
                    for listdata in json_dict[dataclass_record[dataclass]][i]:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
        # 轉換類型：AVI or ETag
        elif dataclass == 'AVI' or dataclass == 'ETag':
            for i in range(len(json_dict[dataclass_record[dataclass]])):
                if 'SubAuthorityCode' in json_dict[dataclass_record[dataclass]][i]:
                    if isinstance(json_dict[dataclass_record[dataclass]][i]['SubAuthorityCode'], dict):
                        del json_dict[dataclass_record[dataclass]][i]['SubAuthorityCode']
                if not isinstance(json_dict[dataclass_record[dataclass]][i], dict):
                    json_data = []
                    for listdata in json_dict[dataclass_record[dataclass]][i]:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
        # 轉換類型：AVIPair or ETagPair
        elif dataclass == 'AVIPair' or dataclass == 'ETagPair':
            for i in range(len(json_dict[dataclass_record[dataclass]])):
                if 'SubAuthorityCode' in json_dict[dataclass_record[dataclass]][i]:
                    if isinstance(json_dict[dataclass_record[dataclass]][i]['SubAuthorityCode'], dict):
                        del json_dict[dataclass_record[dataclass]][i]['SubAuthorityCode']
                if not isinstance(json_dict[dataclass_record[dataclass]][i], dict):
                    json_data = []
                    for listdata in json_dict[dataclass_record[dataclass]][i]:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
        # 轉換類型：AVIPairLive
        elif dataclass == 'AVIPairLive':
            for i in range(len(json_dict[dataclass_record[dataclass]])):
                if not isinstance(json_dict[dataclass_record[dataclass]][i], dict):
                    json_data = []
                    for listdata in json_dict[dataclass_record[dataclass]][i]:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
        # 轉換類型：ETagPairLive
        elif dataclass == 'AVIPairLive':
            for i in range(len(json_dict[dataclass_record[dataclass]])):
                if not isinstance(json_dict[dataclass_record[dataclass]][i], dict):
                    json_data = []
                    for listdata in json_dict[dataclass_record[dataclass]][i]:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
            for i in range(len(json_dict[dataclass_record[dataclass]])):
                json_dict[dataclass_record[dataclass]][i]['Flows'] = [
                    json_dict[dataclass_record[dataclass]][i]['Flows']['Flow']]
        # 轉換類型：GVPLiveTraffic or CVPLiveTraffic
        elif dataclass == 'GVPLiveTraffic' or dataclass == 'CVPLiveTraffic':
            for i in range(len(json_dict[dataclass_record[dataclass]])):
                if 'SubAuthorityCode' in json_dict[dataclass_record[dataclass]][i]:
                    if isinstance(json_dict[dataclass_record[dataclass]][i]['SubAuthorityCode'], dict):
                        del json_dict[dataclass_record[dataclass]][i]['SubAuthorityCode']
                if not isinstance(json_dict[dataclass_record[dataclass]][i], dict):
                    json_data = []
                    for listdata in json_dict[dataclass_record[dataclass]][i]:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
        # 轉換類型：Section
        elif dataclass == 'Section':
            for i in range(len(json_dict[dataclass_record[dataclass]])):
                if 'SubAuthorityCode' in json_dict[dataclass_record[dataclass]][i]:
                    if isinstance(json_dict[dataclass_record[dataclass]][i]['SubAuthorityCode'], dict):
                        del json_dict[dataclass_record[dataclass]][i]['SubAuthorityCode']
                if not isinstance(json_dict[dataclass_record[dataclass]][i], dict):
                    json_data = []
                    for listdata in json_dict[dataclass_record[dataclass]][i]:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
        # 轉換類型：SectionLink
        elif dataclass == 'SectionLink':
            for i in range(len(json_dict[dataclass_record[dataclass]])):
                if 'SubAuthorityCode' in json_dict[dataclass_record[dataclass]][i]:
                    if isinstance(json_dict[dataclass_record[dataclass]][i]['SubAuthorityCode'], dict):
                        del json_dict[dataclass_record[dataclass]][i]['SubAuthorityCode']
                if not isinstance(json_dict[dataclass_record[dataclass]][i], dict):
                    json_data = []
                    for listdata in json_dict[dataclass_record[dataclass]][i]:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
        # 轉換類型：LiveTraffic
        elif dataclass == 'LiveTraffic':
            for i in range(len(json_dict[dataclass_record[dataclass]])):
                if not isinstance(json_dict[dataclass_record[dataclass]][i], dict):
                    json_data = []
                    for listdata in json_dict[dataclass_record[dataclass]][i]:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
        # 轉換類型：CongestionLevel
        elif dataclass == 'CongestionLevel':
            if not isinstance(json_dict[dataclass_record[dataclass]], dict):
                for i in range(len(json_dict[dataclass_record[dataclass]])):
                    if 'SubAuthorityCode' in json_dict[dataclass_record[dataclass]][i]:
                        if isinstance(json_dict[dataclass_record[dataclass]][i]['SubAuthorityCode'], dict):
                            del json_dict[dataclass_record[dataclass]][i]['SubAuthorityCode']
                    if 'Description' in json_dict[dataclass_record[dataclass]]:
                        if isinstance(json_dict[dataclass_record[dataclass]]['Description'], dict):
                            del json_dict[dataclass_record[dataclass]]['Description']
                # 第二層轉換
                for i in range(len(json_dict[dataclass_record[dataclass]])):
                    json_dict[dataclass_record[dataclass]][i]['Levels'] = (
                        json_dict[dataclass_record[dataclass]][i]['Levels']['LevelItem'])
            else:
                if 'SubAuthorityCode' in json_dict[dataclass_record[dataclass]]:
                    if isinstance(json_dict[dataclass_record[dataclass]]['SubAuthorityCode'], dict):
                        del json_dict[dataclass_record[dataclass]]['SubAuthorityCode']
                if 'Description' in json_dict[dataclass_record[dataclass]]:
                    if isinstance(json_dict[dataclass_record[dataclass]]['Description'], dict):
                        del json_dict[dataclass_record[dataclass]]['Description']
                json_dict[dataclass_record[dataclass]] = [json_dict[dataclass_record[dataclass]]]
                for i in range(len(json_dict[dataclass_record[dataclass]])):
                    json_data = []
                    for listdata in json_dict[dataclass_record[dataclass]][i]['Levels']['LevelItem']:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]][i]['Levels'] = json_data
            # (選填資料清除)
            for i in range(len(json_dict[dataclass_record[dataclass]])):
                for j in range(len(json_dict[dataclass_record[dataclass]][i]['Levels'])):
                    if 'TopValue' in json_dict[dataclass_record[dataclass]][i]['Levels'][j]:
                        if isinstance(json_dict[dataclass_record[dataclass]][i]['Levels'][j]['TopValue'], dict):
                            del json_dict[dataclass_record[dataclass]][i]['Levels'][j]['TopValue']
                    if 'LowValue' in json_dict[dataclass_record[dataclass]][i]['Levels'][j]:
                        if isinstance(json_dict[dataclass_record[dataclass]][i]['Levels'][j]['LowValue'], dict):
                            del json_dict[dataclass_record[dataclass]][i]['Levels'][j]['LowValue']
        # 轉換類型：SectionShape
        elif dataclass == 'SectionShape':
            for i in range(len(json_dict[dataclass_record[dataclass]])):
                if 'SubAuthorityCode' in json_dict[dataclass_record[dataclass]][i]:
                    if isinstance(json_dict[dataclass_record[dataclass]][i]['SubAuthorityCode'], dict):
                        del json_dict[dataclass_record[dataclass]][i]['SubAuthorityCode']
                if not isinstance(json_dict[dataclass_record[dataclass]][i], dict):
                    json_data = []
                    for listdata in json_dict[dataclass_record[dataclass]][i]:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
        # 轉換類型：News
        elif dataclass == 'News':
            for i in range(len(json_dict[dataclass_record[dataclass]])):
                if 'SubAuthorityCode' in json_dict[dataclass_record[dataclass]][i]:
                    if isinstance(json_dict[dataclass_record[dataclass]][i]['SubAuthorityCode'], dict):
                        del json_dict[dataclass_record[dataclass]][i]['SubAuthorityCode']
                if 'Department' in json_dict[dataclass_record[dataclass]][i]:
                    if isinstance(json_dict[dataclass_record[dataclass]][i]['Department'], dict):
                        del json_dict[dataclass_record[dataclass]][i]['Department']
                if 'NewsURL' in json_dict[dataclass_record[dataclass]][i]:
                    if isinstance(json_dict[dataclass_record[dataclass]][i]['NewsURL'], dict):
                        del json_dict[dataclass_record[dataclass]][i]['NewsURL']
                if 'AttachmentURL' in json_dict[dataclass_record[dataclass]][i]:
                    if isinstance(json_dict[dataclass_record[dataclass]][i]['AttachmentURL'], dict):
                        del json_dict[dataclass_record[dataclass]][i]['AttachmentURL']
                if 'StartTime' in json_dict[dataclass_record[dataclass]][i]:
                    if isinstance(json_dict[dataclass_record[dataclass]][i]['StartTime'], dict):
                        del json_dict[dataclass_record[dataclass]][i]['StartTime']
                if 'EndTime' in json_dict[dataclass_record[dataclass]][i]:
                    if isinstance(json_dict[dataclass_record[dataclass]][i]['EndTime'], dict):
                        del json_dict[dataclass_record[dataclass]][i]['EndTime']
                if not isinstance(json_dict[dataclass_record[dataclass]][i], dict):
                    json_data = []
                    for listdata in json_dict[dataclass_record[dataclass]][i]:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
        else:
            message = 'The class type is no function.'
        message = 'succeeded'

        return json_dict, 200

    def put(self):
        pass

    def delete(self):
        pass
