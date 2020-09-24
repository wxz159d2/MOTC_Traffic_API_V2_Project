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


# 鍵值刪除程序
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
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                if 'SubAuthorityCode' in layer_1:
                    if isinstance(layer_1['SubAuthorityCode'], dict):
                        del layer_1['SubAuthorityCode']
                if not isinstance(layer_1, dict):
                    json_data = []
                    for listdata in layer_1:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                if isinstance(layer_1['DetectionLinks'], dict):
                    layer_1['DetectionLinks'] = layer_1['DetectionLinks']['DetectionLink']
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                if isinstance(layer_1['DetectionLinks'], dict):
                    layer_1['DetectionLinks'] = [layer_1['DetectionLinks']]
        # 轉換類型：VDLive
        elif dataclass == 'VDLive':
            # 第一層轉換
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                if not isinstance(layer_1, dict):
                    json_data = []
                    for listdata in layer_1:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                layer_1['LinkFlows'] = [layer_1['LinkFlows']['LinkFlow']]
            # 第二層轉換
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                for layer_2 in layer_1['LinkFlows']:
                    if not isinstance(layer_2, dict):
                        json_data = []
                        for listdata in layer_2:
                            json_data.append(listdata)
                        layer_1['LinkFlows'] = json_data
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                for layer_2 in layer_1['LinkFlows']:
                    layer_2['Lanes'] = [layer_2['Lanes']['Lane']]
            # 第三層轉換
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                for layer_2 in layer_1['LinkFlows']:
                    for layer_3 in layer_2['Lanes']:
                        if not isinstance(layer_3, dict):
                            json_data = []
                            for listdata in layer_3:
                                json_data.append(listdata)
                            layer_2['Lanes'] = json_data
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                for layer_2 in layer_1['LinkFlows']:
                    for layer_3 in layer_2['Lanes']:
                        layer_3['Vehicles'] = (layer_3['Vehicles']['Vehicle'])
            # 第四層轉換(選填資料無值清除)
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                for layer_2 in layer_1['LinkFlows']:
                    for layer_3 in layer_2['Lanes']:
                        for layer_4 in layer_3['Vehicles']:
                            if 'Speed' in layer_4:
                                if isinstance(layer_4['Speed'], dict):
                                    del layer_4['Speed']
        # 轉換類型：CCTV
        elif dataclass == 'CCTV':
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                if 'SubAuthorityCode' in layer_1:
                    if isinstance(layer_1['SubAuthorityCode'], dict):
                        del layer_1['SubAuthorityCode']
                if not isinstance(layer_1, dict):
                    json_data = []
                    for listdata in layer_1:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
        # 轉換類型：CMS
        elif dataclass == 'CMS':
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                if 'SubAuthorityCode' in layer_1:
                    if isinstance(layer_1['SubAuthorityCode'], dict):
                        del layer_1['SubAuthorityCode']
                if not isinstance(layer_1, dict):
                    json_data = []
                    for listdata in layer_1:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
        # 轉換類型：CMSLive
        elif dataclass == 'CMSLive':
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                if not isinstance(layer_1, dict):
                    json_data = []
                    for listdata in layer_1:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
        # 轉換類型：AVI or ETag
        elif dataclass == 'AVI' or dataclass == 'ETag':
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                if 'SubAuthorityCode' in layer_1:
                    if isinstance(layer_1['SubAuthorityCode'], dict):
                        del layer_1['SubAuthorityCode']
                if not isinstance(layer_1, dict):
                    json_data = []
                    for listdata in layer_1:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
        # 轉換類型：AVIPair or ETagPair
        elif dataclass == 'AVIPair' or dataclass == 'ETagPair':
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                if 'SubAuthorityCode' in layer_1:
                    if isinstance(layer_1['SubAuthorityCode'], dict):
                        del layer_1['SubAuthorityCode']
                if not isinstance(layer_1, dict):
                    json_data = []
                    for listdata in layer_1:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                if 'LinkIDs' in layer_1:
                    if isinstance(layer_1['LinkIDs'], dict):
                        if 'LinkIDItem' in layer_1['LinkIDs']:
                            json_data = []
                            for layer_2 in layer_1['LinkIDs']['LinkIDItem']:
                                json_data.append(layer_2)
                            layer_1['LinkIDs'] = json_data
                        else:
                            json_data = []
                            for layer_2 in layer_1['LinkIDs']['LinkID']:
                                json_data.append({'LinkID': layer_2})
                            layer_1['LinkIDs'] = json_data
        # 轉換類型：AVIPairLive
        elif dataclass == 'AVIPairLive':
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                if not isinstance(layer_1, dict):
                    json_data = []
                    for listdata in layer_1:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
        # 轉換類型：ETagPairLive
        elif dataclass == 'ETagPairLive':
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                if not isinstance(layer_1, dict):
                    json_data = []
                    for listdata in layer_1:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                layer_1['Flows'] = [layer_1['Flows']['Flow']]
        # 轉換類型：GVPLiveTraffic or CVPLiveTraffic
        elif dataclass == 'GVPLiveTraffic' or dataclass == 'CVPLiveTraffic':
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                if 'SubAuthorityCode' in layer_1:
                    if isinstance(layer_1['SubAuthorityCode'], dict):
                        del layer_1['SubAuthorityCode']
                if not isinstance(layer_1, dict):
                    json_data = []
                    for listdata in layer_1:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
        # 轉換類型：Section
        elif dataclass == 'Section':
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                if 'SubAuthorityCode' in layer_1:
                    if isinstance(layer_1['SubAuthorityCode'], dict):
                        del layer_1['SubAuthorityCode']
                if not isinstance(layer_1, dict):
                    json_data = []
                    for listdata in layer_1:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
        # 轉換類型：SectionLink
        elif dataclass == 'SectionLink':
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                if 'SubAuthorityCode' in layer_1:
                    if isinstance(layer_1['SubAuthorityCode'], dict):
                        del layer_1['SubAuthorityCode']
                if not isinstance(layer_1, dict):
                    json_data = []
                    for listdata in layer_1:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
        # 轉換類型：LiveTraffic
        elif dataclass == 'LiveTraffic':
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                if not isinstance(layer_1, dict):
                    json_data = []
                    for listdata in layer_1:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
        # 轉換類型：CongestionLevel
        elif dataclass == 'CongestionLevel':
            if not isinstance(json_dict[dataclass_record[dataclass]], dict):
                for layer_1 in json_dict[dataclass_record[dataclass]]:
                    if 'SubAuthorityCode' in layer_1:
                        if isinstance(layer_1['SubAuthorityCode'], dict):
                            del layer_1['SubAuthorityCode']
                    if 'Description' in json_dict[dataclass_record[dataclass]]:
                        if isinstance(json_dict[dataclass_record[dataclass]]['Description'], dict):
                            del json_dict[dataclass_record[dataclass]]['Description']
                # 第二層轉換
                for layer_1 in json_dict[dataclass_record[dataclass]]:
                    if 'LevelItem' in layer_1['Levels']:
                        json_data = []
                        for layer_2 in layer_1['Levels']['LevelItem']:
                            json_data.append(layer_2)
                        layer_1['Levels'] = json_data
                    else:
                        json_data = []
                        for layer_2 in layer_1['Levels']['Level']:
                            json_data.append(layer_2)
                        layer_1['Levels'] = json_data
            else:
                if 'SubAuthorityCode' in json_dict[dataclass_record[dataclass]]:
                    if isinstance(json_dict[dataclass_record[dataclass]]['SubAuthorityCode'], dict):
                        del json_dict[dataclass_record[dataclass]]['SubAuthorityCode']
                if 'Description' in json_dict[dataclass_record[dataclass]]:
                    if isinstance(json_dict[dataclass_record[dataclass]]['Description'], dict):
                        del json_dict[dataclass_record[dataclass]]['Description']
                json_dict[dataclass_record[dataclass]] = [json_dict[dataclass_record[dataclass]]]
                for layer_1 in json_dict[dataclass_record[dataclass]]:
                    if 'LevelItem' in layer_1['Levels']:
                        json_data = []
                        for layer_2 in layer_1['Levels']['LevelItem']:
                            json_data.append(layer_2)
                        layer_1['Levels'] = json_data
                    else:
                        json_data = []
                        for layer_2 in layer_1['Levels']['Level']:
                            json_data.append(layer_2)
                        layer_1['Levels'] = json_data
            # (選填資料無值清除)
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                for layer_2 in layer_1['Levels']:
                    if 'TopValue' in layer_2:
                        if isinstance(layer_2['TopValue'], dict):
                            del layer_2['TopValue']
                    if 'LowValue' in layer_2:
                        if isinstance(layer_2['LowValue'], dict):
                            del layer_2['LowValue']
        # 轉換類型：SectionShape
        elif dataclass == 'SectionShape':
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                if 'SubAuthorityCode' in layer_1:
                    if isinstance(layer_1['SubAuthorityCode'], dict):
                        del layer_1['SubAuthorityCode']
                if not isinstance(layer_1, dict):
                    json_data = []
                    for listdata in layer_1:
                        json_data.append(listdata)
                    json_dict[dataclass_record[dataclass]] = json_data
        # 轉換類型：News
        elif dataclass == 'News':
            for layer_1 in json_dict[dataclass_record[dataclass]]:
                if 'SubAuthorityCode' in layer_1:
                    if isinstance(layer_1['SubAuthorityCode'], dict):
                        del layer_1['SubAuthorityCode']
                if 'Department' in layer_1:
                    if isinstance(layer_1['Department'], dict):
                        del layer_1['Department']
                if 'NewsURL' in layer_1:
                    if isinstance(layer_1['NewsURL'], dict):
                        del layer_1['NewsURL']
                if 'AttachmentURL' in layer_1:
                    if isinstance(layer_1['AttachmentURL'], dict):
                        del layer_1['AttachmentURL']
                if 'StartTime' in layer_1:
                    if isinstance(layer_1['StartTime'], dict):
                        del layer_1['StartTime']
                if 'EndTime' in layer_1:
                    if isinstance(layer_1['EndTime'], dict):
                        del layer_1['EndTime']
                if not isinstance(layer_1, dict):
                    json_data = []
                    for listdata in layer_1:
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
