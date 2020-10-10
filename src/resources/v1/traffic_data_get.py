# -*- coding: UTF-8 -*-
import copy
import datetime
import json

import aniso8601
from flask_restful import Resource, reqparse
from flask_restful.inputs import positive

# 取代值
REPLACE_VALUE = -1

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

# 資料狀態
data_status = {
    'normal': 0,  # 正常值
    'nodata': 1,  # 無資料
    'abnormal': 2,  # 異常值
    'repair': 3  # 修補值
}

# 流量正常值範圍(1分鐘)
flows_normal_range = {
    'occupancy_min': 0,
    'occupancy_max': 100,
    'speed_min': 0,
    'speed_max': 150,
    'volume_min': 0,
    'volume_max': 311,  # 參照公路容量手冊2011 式(18.10)以W=10、h=-0.1推算
}


# 鍵值刪除程序
def del_json_dict(json_dict, del_key):
    if del_key in json_dict:
        del json_dict[del_key]
    return json_dict


# 正常VDLive合併車道車種資料處理
def vdlive_data_slsv_normal_process(json_data, m_pce, s_pce, l_pce, t_pce):
    for json_dict in json_data:
        # 標註資料狀態(先假設正常)
        json_dict['DataStatus'] = data_status['normal']
        # 檢查設備狀態，如異常則標註資料異常
        if not (json_dict['Status'] == 0 or json_dict['Status'] == '0'):
            json_dict['DataStatus'] = data_status['abnormal']
        for link_list in json_dict['LinkFlows']:
            l_occupancy = 0
            l_speed = 0
            l_volume = 0
            for lane_list in link_list['Lanes']:
                v_volume = 0
                for vehicle_list in lane_list['Vehicles']:
                    # 檢查流量，如異常則標註資料異常
                    if int(vehicle_list['Volume']) < flows_normal_range['volume_min'] or \
                            int(vehicle_list['Volume']) > flows_normal_range['volume_max']:
                        json_dict['DataStatus'] = data_status['abnormal']
                    # 合併各車種流量
                    if vehicle_list['VehicleType'] == 'M':
                        v_volume = v_volume + int(vehicle_list['Volume']) * m_pce
                    elif vehicle_list['VehicleType'] == 'S':
                        v_volume = v_volume + int(vehicle_list['Volume']) * s_pce
                    elif vehicle_list['VehicleType'] == 'L':
                        v_volume = v_volume + int(vehicle_list['Volume']) * l_pce
                    elif vehicle_list['VehicleType'] == 'T':
                        v_volume = v_volume + int(vehicle_list['Volume']) * t_pce
                    else:
                        v_volume = v_volume + int(vehicle_list['Volume']) * 1
                lane_list['Volume'] = v_volume
                del_json_dict(json_dict=lane_list, del_key='Vehicles')
                # 檢查速率，如異常則標註資料異常
                if float(lane_list['Speed']) < flows_normal_range['speed_min'] or \
                        float(lane_list['Speed']) > flows_normal_range['speed_max']:
                    json_dict['DataStatus'] = data_status['abnormal']
                # 檢查佔有率，如異常則標註資料異常
                if float(lane_list['Occupancy']) < flows_normal_range['occupancy_min'] or \
                        float(lane_list['Occupancy']) > flows_normal_range['occupancy_max']:
                    json_dict['DataStatus'] = data_status['abnormal']
                # 合併各車道
                l_volume = l_volume + float(lane_list['Volume'])
                l_speed = l_speed + float(lane_list['Speed']) * float(lane_list['Volume'])
                l_occupancy = l_occupancy + float(lane_list['Occupancy'])
            if l_volume > 0:
                l_speed = l_speed / l_volume
            if len(link_list['Lanes']) > 0:
                l_occupancy = l_occupancy / len(link_list['Lanes'])
            # 呈現資料
            link_list['Volume'] = l_volume
            link_list['Speed'] = l_speed
            link_list['Occupancy'] = l_occupancy
            # 隱藏資料
            del_json_dict(json_dict=link_list, del_key='Lanes')
        # 轉換時間鍵值名稱
        if 'DataCollectTime' in json_dict:
            json_dict['Time'] = json_dict['DataCollectTime']
            del json_dict['DataCollectTime']
        # 隱藏資料
        del_json_dict(json_dict=json_dict, del_key='SubAuthorityCode')
        del_json_dict(json_dict=json_dict, del_key='Status')
    return json_data


# 異常VDLive合併車道車種資料處理
def vdlive_data_slsv_abnormal_process(json_data, error_process):
    fulltime_json_data = []
    for json_dict in json_data:
        if json_dict['DataStatus'] == data_status['normal']:
            fulltime_json_data.append(json_dict)
            continue
        if not error_process == 1:
            for link_list in json_dict['LinkFlows']:
                link_list['Volume'] = REPLACE_VALUE
                link_list['Speed'] = REPLACE_VALUE
                link_list['Occupancy'] = REPLACE_VALUE
            fulltime_json_data.append(json_dict)
    return fulltime_json_data


# VDLive缺漏時間填補處理
def vdlive_data_null_time_process(json_dict, time):
    json_dict['Time'] = time
    for link_list in json_dict['LinkFlows']:
        link_list['Volume'] = REPLACE_VALUE
        link_list['Speed'] = REPLACE_VALUE
        link_list['Occupancy'] = REPLACE_VALUE
    json_dict['DataStatus'] = data_status['nodata']
    return json_dict


# VDLive缺漏時間檢查
def vdlive_data_null_time_check(json_data, json_dict_sample, time):
    for json_dict in json_data:
        if json_dict['Time'] == time.isoformat():
            return json_dict
    json_dict = copy.deepcopy(vdlive_data_null_time_process(json_dict_sample, time.isoformat()))
    return json_dict


class Get_t2_one_record(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('authority', type=str, required=False, help='Param error: authority',
                                 choices=['NFB', 'THB', 'TNN'])
        self.parser.add_argument('dataclass', type=str, required=False, help='Param error: dataclass',
                                 choices=['VD', 'VDLive', 'CCTV', 'CMS', 'CMSLive', 'AVI', 'AVIPair', 'AVIPairLive',
                                          'ETag', 'ETagPair', 'ETagPairLive', 'GVPLiveTraffic', 'CVPLiveTraffic',
                                          'Section', 'SectionLink', 'LiveTraffic', 'CongestionLevel', 'SectionShape',
                                          'News'])
        self.parser.add_argument('format', type=str, default='JSON', required=False, help='Param error: Format',
                                 choices=['JSON', 'XML'])

    def get(self, authority, dataclass, oid, date):
        """
        [路況標準2.0][單筆歷史資料查詢]
        提供查詢指定設備及資料時間之單筆[路況標準2.0]格式資料
        命令格式： /v1/traffic_data/authority/{authority}/class/{dataclass}/oid/{oid}/date/{date}/standard/MOTC_traffic_v2/?format={format}
        ---
        tags:
          - Traffic Data Query API (交通資料查詢API)
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
          - in: path
            name: oid
            type: string
            required: true
            description: 設備、資料之ID
            default: 'VD-N3-S-300.000-N-Loop'
          - in: path
            name: date
            type: string
            required: true
            description: 資料代表之時間(動態資料參照欄位：DataCollectTime、靜態資料參照欄位：UpdateTime)[格式：ISO8601]
            default: '2020-08-18T17:50:00+08:00'
          - in: query
            name: format
            type: string
            required: false
            description: 資料格式(支援JSON、XML)
            enum: ['JSON', 'XML']
            default: 'JSON'
        responses:
          200:
            description: OK
         """

        from api import mongo_url

        message = ''

        # 讀取API傳入參數
        args = self.parser.parse_args()
        format = args['format']

        # 參數轉小寫處理
        dataclass_lower = dataclass.lower()
        authority_lower = authority.lower()

        # MongoDB連結設定參數處理
        database = 'traffic_data_' + authority_lower
        collection = dataclass_lower

        # pyspark讀取語法
        from api import spark
        id_name = dataclass_id[dataclass]
        pipeline = ""
        if 'Live' in dataclass:
            # 動態資料查詢管道指令
            pipeline = pipeline + "{'$match':"
            pipeline = pipeline + "    {'$and':["
            pipeline = pipeline + "        {'" + id_name + "':'" + oid + "'"
            pipeline = pipeline + "        },"
            pipeline = pipeline + "        {'DataCollectTime':"
            pipeline = pipeline + "            {'$date':'" + date + "'}"
            pipeline = pipeline + "        }"
            pipeline = pipeline + "    ]}"
            pipeline = pipeline + "}"
        else:
            # 靜態資料查詢管道指令 (News因採UpdateTime時間戳記，故以本方法處理)
            pipeline = pipeline + "{'$match':"
            pipeline = pipeline + "    {'$and':["
            pipeline = pipeline + "        {'" + id_name + "':'" + oid + "'"
            pipeline = pipeline + "        },"
            pipeline = pipeline + "        {'UpdateTime':"
            pipeline = pipeline + "            {'$date':'" + date + "'}"
            pipeline = pipeline + "        }"
            pipeline = pipeline + "    ]}"
            pipeline = pipeline + "}"

        df = spark.read.format('mongo') \
            .option('uri', mongo_url) \
            .option("database", database) \
            .option("collection", collection) \
            .option('pipeline', pipeline) \
            .load()
        json_data_list = df.toJSON().collect()
        json_data = []
        for values in json_data_list:
            json_dict = json.loads(values)
            del json_dict['_id']  # 刪除momgo的資料編號
            json_data.append(json_dict)

        output_json = json_data
        return output_json, 200

    def post(self):
        pass

    def put(self):
        pass

    def delete(self):
        pass


class Get_t2_time_range(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('authority', type=str, required=False, help='Param error: authority',
                                 choices=['NFB', 'THB', 'TNN'])
        self.parser.add_argument('dataclass', type=str, required=False, help='Param error: dataclass',
                                 choices=['VD', 'VDLive', 'CCTV', 'CMS', 'CMSLive', 'AVI', 'AVIPair', 'AVIPairLive',
                                          'ETag', 'ETagPair', 'ETagPairLive', 'GVPLiveTraffic', 'CVPLiveTraffic',
                                          'Section', 'SectionLink', 'LiveTraffic', 'CongestionLevel', 'SectionShape',
                                          'News'])
        self.parser.add_argument('format', type=str, default='JSON', required=False, help='Param error: Format',
                                 choices=['JSON', 'XML'])
        self.parser.add_argument('sort', type=int, required=False, help='Param error: sort',
                                 choices=[1, -1])

    def get(self, dataclass, authority, oid, sdate, edate):
        """
        [路況標準2.0][時段歷史資料查詢]
        提供查詢指定設備及資料時段範圍之多筆[路況標準2.0]格式資料
        命令格式： /v1/traffic_data/authority/{authority}/class/{dataclass}/oid/{oid}/date/{sdate}/to/{edate}/standard/MOTC_traffic_v2/?format={format}&sort={sort}
        ---
        tags:
          - Traffic Data Query API (交通資料查詢API)
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
          - in: path
            name: oid
            type: string
            required: true
            description: 設備、資料之ID
            default: 'VD-N3-S-300.000-N-Loop'
          - in: path
            name: sdate
            type: string
            required: true
            description: 資料代表之開始時間(含)(動態資料參照欄位：DataCollectTime、靜態資料參照欄位：UpdateTime)[格式：ISO8601]
            default: '2020-08-18T17:00:00+08:00'
          - in: path
            name: edate
            type: string
            required: true
            description: 資料代表之結束時間(含)(動態資料參照欄位：DataCollectTime、靜態資料參照欄位：UpdateTime)[格式：ISO8601]
            default: '2020-08-18T18:00:00+08:00'
          - in: query
            name: format
            type: string
            required: false
            description: 資料格式(支援JSON、XML)
            enum: ['JSON', 'XML']
            default: 'JSON'
          - in: query
            name: sort
            type: int
            required: false
            description: 依資料時間排序(遞增：1、遞減：-1)
            enum: [1, -1]
        responses:
          200:
            description: OK
         """

        from api import mongo_url

        message = ''

        # 讀取API傳入參數
        args = self.parser.parse_args()
        format = args['format']
        sort = args['sort']

        # 參數轉小寫處理
        dataclass_lower = dataclass.lower()
        authority_lower = authority.lower()

        # MongoDB連結設定參數處理
        database = 'traffic_data_' + authority_lower
        collection = dataclass_lower

        # pyspark讀取語法
        from api import spark
        id_name = dataclass_id[dataclass]
        pipeline = ""
        if 'Live' in dataclass:
            # 動態資料查詢管道指令
            pipeline = pipeline + "{'$match':"
            pipeline = pipeline + "    {'$and':["
            pipeline = pipeline + "        {'" + id_name + "':'" + oid + "'"
            pipeline = pipeline + "        },"
            pipeline = pipeline + "        {'DataCollectTime':"
            pipeline = pipeline + "            {"
            pipeline = pipeline + "                '$gte':{'$date':'" + sdate + "'},"
            pipeline = pipeline + "                '$lte':{'$date':'" + edate + "'}"
            pipeline = pipeline + "            }"
            pipeline = pipeline + "        }"
            pipeline = pipeline + "    ]}"
            pipeline = pipeline + "}"
            if sort == 1 or sort == -1:
                pipeline = "[" + pipeline + ",{'$sort':{'DataCollectTime':" + str(sort) + "}}]"
        else:
            # 靜態資料查詢管道指令 (News因採UpdateTime時間戳記，故以本方法處理)
            pipeline = pipeline + "{'$match':"
            pipeline = pipeline + "    {'$and':["
            pipeline = pipeline + "        {'" + id_name + "':'" + oid + "'"
            pipeline = pipeline + "        },"
            pipeline = pipeline + "        {'UpdateTime':"
            pipeline = pipeline + "            {"
            pipeline = pipeline + "                '$gte':{'$date':'" + sdate + "'},"
            pipeline = pipeline + "                '$lte':{'$date':'" + edate + "'}"
            pipeline = pipeline + "            }"
            pipeline = pipeline + "        }"
            pipeline = pipeline + "    ]}"
            pipeline = pipeline + "}"
            if sort == 1 or sort == -1:
                pipeline = "[" + pipeline + ",{'$sort':{'UpdateTime':" + str(sort) + "}}]"

        df = spark.read.format('mongo') \
            .option('uri', mongo_url) \
            .option("database", database) \
            .option("collection", collection) \
            .option('pipeline', pipeline) \
            .option('pipe', 'allowDiskUse=True').load()
        json_data_list = df.toJSON().collect()
        json_data = []
        for values in json_data_list:
            json_dict = json.loads(values)
            del json_dict['_id']  # 刪除momgo的資料編號
            json_data.append(json_dict)

        output_json = json_data
        return output_json, 200

    def post(self):
        pass

    def put(self):
        pass

    def delete(self):
        pass


class Get_one_record_slsv(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('authority', type=str, required=False, help='Param error: authority',
                                 choices=['NFB', 'THB', 'TNN'])
        self.parser.add_argument('format', type=str, default='JSON', required=False, help='Param error: Format',
                                 choices=['JSON', 'XML'])
        self.parser.add_argument('m_pce', type=float, default=1.0, required=False, help='Param error: m_pce')
        self.parser.add_argument('s_pce', type=float, default=1.0, required=False, help='Param error: s_pce')
        self.parser.add_argument('l_pce', type=float, default=1.0, required=False, help='Param error: l_pce')
        self.parser.add_argument('t_pce', type=float, default=1.0, required=False, help='Param error: t_pce')
        self.parser.add_argument('error_check', type=int, default=1, required=False, help='Param error: error_check',
                                 choices=[1, 2, 3])
        self.parser.add_argument('error_process', type=int, default=0, required=False,
                                 help='Param error: error_process',
                                 choices=[0, 1, 2, 30, 31, 32])

    def get(self, authority, oid, date):
        """
        [單筆車流資料查詢][合併車道][合併車種]
        提供查詢指定VD設備及資料時間之單筆合併車道及車種之車流資料，並具車當量(PCE)轉換、異常資料排除、資料修補等功能
        命令格式： /v1/traffic_data/authority/{authority}/oid/{oid}/date/{date}/method/sum_lanes/sum_vehicles/?format={format}&m_pce={m_pce}&s_pce={s_pce}&l_pce={l_pce}&t_pce={t_pce}&error_check={error_check}&error_process={error_process}
        ---
        tags:
          - Traffic Data Query API (交通資料查詢API)
        parameters:
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
            default: 'VD-N3-S-300.000-N-Loop'
          - in: path
            name: date
            type: string
            required: true
            description: 資料代表之時間(動態資料參照欄位：DataCollectTime)[格式：ISO8601]
            default: '2020-08-18T17:50:00+08:00'
          - in: query
            name: format
            type: string
            required: false
            description: 資料格式(支援JSON、XML)
            enum: ['JSON', 'XML']
            default: 'JSON'
          - in: query
            name: m_pce
            type: float
            minimum: 0
            required: false
            description: 機車當量
            default: 1.0
          - in: query
            name: s_pce
            type: float
            minimum: 0
            required: false
            description: 小型車當量
            default: 1.0
          - in: query
            name: l_pce
            type: float
            minimum: 0
            required: false
            description: 大型車當量
            default: 1.0
          - in: query
            name: t_pce
            type: float
            minimum: 0
            required: false
            description: 連結車當量
            default: 1.0
          - in: query
            name: error_check
            type: int
            required: false
            description: 數值資料異常偵測模式(1:基本規則法、2:巨觀車流模式驗證法、3:AI辨識法)
            enum: [1, 2, 3]
            default: 1
          - in: query
            name: error_process
            type: int
            required: false
            description: 數值資料異常處理模式(0:不做處理、1:刪除資料、2:清除並填補[-1]、30:數值修補-線性插值法、31:數值修補-多項式插值法、32:數值修補-AI修補模式)
            enum: [0, 1, 2, 30, 31, 32]
            default: 0
        responses:
          200:
            description: OK
         """

        from api import mongo_url

        message = ''

        # 讀取API傳入參數
        args = self.parser.parse_args()
        format = args['format']
        m_pce = args['m_pce']
        s_pce = args['s_pce']
        l_pce = args['l_pce']
        t_pce = args['t_pce']
        error_check = args['error_check']
        error_process = args['error_process']

        dataclass = 'VDLive'

        # 參數轉小寫處理
        dataclass_lower = dataclass.lower()
        authority_lower = authority.lower()

        # MongoDB連結設定參數處理
        database = 'traffic_data_' + authority_lower
        collection = dataclass_lower

        # pyspark讀取語法
        from api import spark
        id_name = dataclass_id[dataclass]
        pipeline = ""
        # 動態資料查詢管道指令
        pipeline = pipeline + "{'$match':"
        pipeline = pipeline + "    {'$and':["
        pipeline = pipeline + "        {'" + id_name + "':'" + oid + "'"
        pipeline = pipeline + "        },"
        pipeline = pipeline + "        {'DataCollectTime':"
        pipeline = pipeline + "            {'$date':'" + date + "'}"
        pipeline = pipeline + "        }"
        pipeline = pipeline + "    ]}"
        pipeline = pipeline + "}"

        df = spark.read.format('mongo') \
            .option('uri', mongo_url) \
            .option("database", database) \
            .option("collection", collection) \
            .option('pipeline', pipeline) \
            .load()
        json_data_list = df.toJSON().collect()
        json_data = []

        for values in json_data_list:
            json_dict = json.loads(values)
            del json_dict['_id']  # 刪除momgo的資料編號
            json_dict['DataCollectTime'] = aniso8601.parse_datetime(json_dict['DataCollectTime'])
            # 無條件捨去秒以下時間
            json_dict['DataCollectTime'] = datetime.datetime(year=json_dict['DataCollectTime'].year,
                                                             month=json_dict['DataCollectTime'].month,
                                                             day=json_dict['DataCollectTime'].day,
                                                             hour=json_dict['DataCollectTime'].hour,
                                                             minute=json_dict['DataCollectTime'].minute,
                                                             second=0,
                                                             microsecond=0,
                                                             tzinfo=json_dict['DataCollectTime'].tzinfo)
            json_dict['DataCollectTime'] = json_dict['DataCollectTime'].isoformat()
            json_data.append(json_dict)

        # 正常VDLive資料處理
        json_data = vdlive_data_slsv_normal_process(json_data, m_pce, s_pce, l_pce, t_pce)

        # 異常資料不處理
        if error_process == 0:
            output_json = json_data
            return output_json, 200

        # 異常資料不輸出
        if error_process == 1:
            output_json = []
            return output_json, 200

        output_json = json_data
        return output_json, 200

    def post(self):
        pass

    def put(self):
        pass

    def delete(self):
        pass


class Get_one_record_slpv(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('authority', type=str, required=False, help='Param error: authority',
                                 choices=['NFB', 'THB', 'TNN'])
        self.parser.add_argument('format', type=str, default='JSON', required=False, help='Param error: Format',
                                 choices=['JSON', 'XML'])
        self.parser.add_argument('m_pce', type=float, default=1.0, required=False, help='Param error: m_pce')
        self.parser.add_argument('s_pce', type=float, default=1.0, required=False, help='Param error: s_pce')
        self.parser.add_argument('l_pce', type=float, default=1.0, required=False, help='Param error: l_pce')
        self.parser.add_argument('t_pce', type=float, default=1.0, required=False, help='Param error: t_pce')

    def get(self, authority, oid, date):
        """
        [單筆車流資料查詢][合併車道][各別車種]
        提供查詢指定VD設備及資料時間之單筆合併車道及各別車種之車流資料，並具車當量(PCE)轉換功能
        命令格式： /v1/traffic_data/authority/{authority}/oid/{oid}/date/{date}/method/sum_lanes/per_vehicles/?format={format}&m_pce={m_pce}&s_pce={s_pce}&l_pce={l_pce}&t_pce={t_pce}
        ---
        tags:
          - Traffic Data Query API (交通資料查詢API)
        parameters:
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
            default: 'VD-N3-S-300.000-N-Loop'
          - in: path
            name: date
            type: string
            required: true
            description: 資料代表之時間(動態資料參照欄位：DataCollectTime、靜態資料參照欄位：UpdateTime)[格式：ISO8601]
            default: '2020-08-18T17:50:00+08:00'
          - in: query
            name: format
            type: string
            required: false
            description: 資料格式(支援JSON、XML)
            enum: ['JSON', 'XML']
            default: 'JSON'
          - in: query
            name: m_pce
            type: float
            minimum: 0
            required: false
            description: 機車當量
            default: 1.0
          - in: query
            name: s_pce
            type: float
            minimum: 0
            required: false
            description: 小型車當量
            default: 1.0
          - in: query
            name: l_pce
            type: float
            minimum: 0
            required: false
            description: 大型車當量
            default: 1.0
          - in: query
            name: t_pce
            type: float
            minimum: 0
            required: false
            description: 連結車當量
            default: 1.0
        responses:
          200:
            description: OK
         """

        from api import mongo_url

        message = ''

        # 讀取API傳入參數
        args = self.parser.parse_args()
        format = args['format']
        m_pce = args['m_pce']
        s_pce = args['s_pce']
        l_pce = args['l_pce']
        t_pce = args['t_pce']

        dataclass = 'VDLive'

        # 參數轉小寫處理
        dataclass_lower = dataclass.lower()
        authority_lower = authority.lower()

        # MongoDB連結設定參數處理
        database = 'traffic_data_' + authority_lower
        collection = dataclass_lower

        # pyspark讀取語法
        from api import spark
        id_name = dataclass_id[dataclass]
        pipeline = ""
        # 動態資料查詢管道指令
        pipeline = pipeline + "{'$match':"
        pipeline = pipeline + "    {'$and':["
        pipeline = pipeline + "        {'" + id_name + "':'" + oid + "'"
        pipeline = pipeline + "        },"
        pipeline = pipeline + "        {'DataCollectTime':"
        pipeline = pipeline + "            {'$date':'" + date + "'}"
        pipeline = pipeline + "        }"
        pipeline = pipeline + "    ]}"
        pipeline = pipeline + "}"

        df = spark.read.format('mongo') \
            .option('uri', mongo_url) \
            .option("database", database) \
            .option("collection", collection) \
            .option('pipeline', pipeline) \
            .load()
        json_data_list = df.toJSON().collect()
        json_data = []
        for values in json_data_list:
            json_dict = json.loads(values)
            del json_dict['_id']  # 刪除momgo的資料編號
            json_data.append(json_dict)

        output_json = json_data
        return output_json, 200

    def post(self):
        pass

    def put(self):
        pass

    def delete(self):
        pass


class Get_one_record_plsv(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('authority', type=str, required=False, help='Param error: authority',
                                 choices=['NFB', 'THB', 'TNN'])
        self.parser.add_argument('format', type=str, default='JSON', required=False, help='Param error: Format',
                                 choices=['JSON', 'XML'])
        self.parser.add_argument('m_pce', type=float, default=1.0, required=False, help='Param error: m_pce')
        self.parser.add_argument('s_pce', type=float, default=1.0, required=False, help='Param error: s_pce')
        self.parser.add_argument('l_pce', type=float, default=1.0, required=False, help='Param error: l_pce')
        self.parser.add_argument('t_pce', type=float, default=1.0, required=False, help='Param error: t_pce')

    def get(self, authority, oid, date):
        """
        [單筆車流資料查詢][各別車道][合併車種]
        提供查詢指定VD設備及資料時間之單筆各別車道及合併車種之車流資料，並具車當量(PCE)轉換功能
        命令格式： /v1/traffic_data/authority/{authority}/oid/{oid}/date/{date}/method/per_lanes/sum_vehicles/?format={format}&m_pce={m_pce}&s_pce={s_pce}&l_pce={l_pce}&t_pce={t_pce}
        ---
        tags:
          - Traffic Data Query API (交通資料查詢API)
        parameters:
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
            default: 'VD-N3-S-300.000-N-Loop'
          - in: path
            name: date
            type: string
            required: true
            description: 資料代表之時間(動態資料參照欄位：DataCollectTime、靜態資料參照欄位：UpdateTime)[格式：ISO8601]
            default: '2020-08-18T17:50:00+08:00'
          - in: query
            name: format
            type: string
            required: false
            description: 資料格式(支援JSON、XML)
            enum: ['JSON', 'XML']
            default: 'JSON'
          - in: query
            name: m_pce
            type: float
            minimum: 0
            required: false
            description: 機車當量
            default: 1.0
          - in: query
            name: s_pce
            type: float
            minimum: 0
            required: false
            description: 小型車當量
            default: 1.0
          - in: query
            name: l_pce
            type: float
            minimum: 0
            required: false
            description: 大型車當量
            default: 1.0
          - in: query
            name: t_pce
            type: float
            minimum: 0
            required: false
            description: 連結車當量
            default: 1.0
        responses:
          200:
            description: OK
         """

        from api import mongo_url

        message = ''

        # 讀取API傳入參數
        args = self.parser.parse_args()
        format = args['format']
        m_pce = args['m_pce']
        s_pce = args['s_pce']
        l_pce = args['l_pce']
        t_pce = args['t_pce']

        dataclass = 'VDLive'

        # 參數轉小寫處理
        dataclass_lower = dataclass.lower()
        authority_lower = authority.lower()

        # MongoDB連結設定參數處理
        database = 'traffic_data_' + authority_lower
        collection = dataclass_lower

        # pyspark讀取語法
        from api import spark
        id_name = dataclass_id[dataclass]
        pipeline = ""
        # 動態資料查詢管道指令
        pipeline = pipeline + "{'$match':"
        pipeline = pipeline + "    {'$and':["
        pipeline = pipeline + "        {'" + id_name + "':'" + oid + "'"
        pipeline = pipeline + "        },"
        pipeline = pipeline + "        {'DataCollectTime':"
        pipeline = pipeline + "            {'$date':'" + date + "'}"
        pipeline = pipeline + "        }"
        pipeline = pipeline + "    ]}"
        pipeline = pipeline + "}"

        df = spark.read.format('mongo') \
            .option('uri', mongo_url) \
            .option("database", database) \
            .option("collection", collection) \
            .option('pipeline', pipeline) \
            .load()
        json_data_list = df.toJSON().collect()
        json_data = []
        for values in json_data_list:
            json_dict = json.loads(values)
            del json_dict['_id']  # 刪除momgo的資料編號
            json_data.append(json_dict)

        output_json = json_data
        return output_json, 200

    def post(self):
        pass

    def put(self):
        pass

    def delete(self):
        pass


class Get_one_record_plpv(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('authority', type=str, required=False, help='Param error: authority',
                                 choices=['NFB', 'THB', 'TNN'])
        self.parser.add_argument('format', type=str, default='JSON', required=False, help='Param error: Format',
                                 choices=['JSON', 'XML'])
        self.parser.add_argument('m_pce', type=float, default=1.0, required=False, help='Param error: m_pce')
        self.parser.add_argument('s_pce', type=float, default=1.0, required=False, help='Param error: s_pce')
        self.parser.add_argument('l_pce', type=float, default=1.0, required=False, help='Param error: l_pce')
        self.parser.add_argument('t_pce', type=float, default=1.0, required=False, help='Param error: t_pce')

    def get(self, authority, oid, date):
        """
        [單筆車流資料查詢][各別車道][各別車種]
        提供查詢指定VD設備及資料時間之單筆各別車道及車種之車流資料，並具車當量(PCE)轉換功能
        命令格式： /v1/traffic_data/authority/{authority}/oid/{oid}/date/{date}/method/per_lanes/per_vehicles/?format={format}&m_pce={m_pce}&s_pce={s_pce}&l_pce={l_pce}&t_pce={t_pce}
        ---
        tags:
          - Traffic Data Query API (交通資料查詢API)
        parameters:
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
            default: 'VD-N3-S-300.000-N-Loop'
          - in: path
            name: date
            type: string
            required: true
            description: 資料代表之時間(動態資料參照欄位：DataCollectTime、靜態資料參照欄位：UpdateTime)[格式：ISO8601]
            default: '2020-08-18T17:50:00+08:00'
          - in: query
            name: format
            type: string
            required: false
            description: 資料格式(支援JSON、XML)
            enum: ['JSON', 'XML']
            default: 'JSON'
          - in: query
            name: m_pce
            type: float
            minimum: 0
            required: false
            description: 機車當量
            default: 1.0
          - in: query
            name: s_pce
            type: float
            minimum: 0
            required: false
            description: 小型車當量
            default: 1.0
          - in: query
            name: l_pce
            type: float
            minimum: 0
            required: false
            description: 大型車當量
            default: 1.0
          - in: query
            name: t_pce
            type: float
            minimum: 0
            required: false
            description: 連結車當量
            default: 1.0
        responses:
          200:
            description: OK
         """

        from api import mongo_url

        message = ''

        # 讀取API傳入參數
        args = self.parser.parse_args()
        format = args['format']
        m_pce = args['m_pce']
        s_pce = args['s_pce']
        l_pce = args['l_pce']
        t_pce = args['t_pce']

        dataclass = 'VDLive'

        # 參數轉小寫處理
        dataclass_lower = dataclass.lower()
        authority_lower = authority.lower()

        # MongoDB連結設定參數處理
        database = 'traffic_data_' + authority_lower
        collection = dataclass_lower

        # pyspark讀取語法
        from api import spark
        id_name = dataclass_id[dataclass]
        pipeline = ""
        # 動態資料查詢管道指令
        pipeline = pipeline + "{'$match':"
        pipeline = pipeline + "    {'$and':["
        pipeline = pipeline + "        {'" + id_name + "':'" + oid + "'"
        pipeline = pipeline + "        },"
        pipeline = pipeline + "        {'DataCollectTime':"
        pipeline = pipeline + "            {'$date':'" + date + "'}"
        pipeline = pipeline + "        }"
        pipeline = pipeline + "    ]}"
        pipeline = pipeline + "}"

        df = spark.read.format('mongo') \
            .option('uri', mongo_url) \
            .option("database", database) \
            .option("collection", collection) \
            .option('pipeline', pipeline) \
            .load()
        json_data_list = df.toJSON().collect()
        json_data = []
        for values in json_data_list:
            json_dict = json.loads(values)
            del json_dict['_id']  # 刪除momgo的資料編號
            json_data.append(json_dict)

        output_json = json_data
        return output_json, 200

    def post(self):
        pass

    def put(self):
        pass

    def delete(self):
        pass


class Get_time_range_slsv(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('authority', type=str, required=False, help='Param error: authority',
                                 choices=['NFB', 'THB', 'TNN'])
        self.parser.add_argument('format', type=str, default='JSON', required=False, help='Param error: Format',
                                 choices=['JSON', 'XML'])
        self.parser.add_argument('m_pce', type=float, default=1.0, required=False, help='Param error: m_pce')
        self.parser.add_argument('s_pce', type=float, default=1.0, required=False, help='Param error: s_pce')
        self.parser.add_argument('l_pce', type=float, default=1.0, required=False, help='Param error: l_pce')
        self.parser.add_argument('t_pce', type=float, default=1.0, required=False, help='Param error: t_pce')
        self.parser.add_argument('error_check', type=int, default=1, required=False, help='Param error: error_check',
                                 choices=[1, 2, 3])
        self.parser.add_argument('error_process', type=int, default=0, required=False,
                                 help='Param error: error_process',
                                 choices=[0, 1, 2, 30, 31, 32])
        self.parser.add_argument('null_time', type=int, default=0, required=False, help='Param error: null_time',
                                 choices=[0, 1])
        self.parser.add_argument('time_interval', type=positive, default=1, required=False,
                                 help='Param error: time_interval')
        self.parser.add_argument('time_rolling', type=positive, default=1, required=False,
                                 help='Param error: time_rolling')
        self.parser.add_argument('sort', type=int, required=False, help='Param error: sort',
                                 choices=[1, -1])

    def get(self, authority, oid, sdate, edate):
        """
        [時段車流資料查詢][合併車道][合併車種]
        提供查詢指定VD設備及資料時段範圍之固定或滾動週期之多筆合併車道及車種之車流資料，並具車當量(PCE)轉換、異常資料排除、資料修補等功能
        命令格式： /v1/traffic_data/authority/{authority}/oid/{oid}/date/{sdate}/to/{edate}/method/sum_lanes/sum_vehicles/?format={format}&m_pce={m_pce}&s_pce={s_pce}&l_pce={l_pce}&t_pce={t_pce}&error_check={error_check}&error_process={error_process}&null_time={null_time}&time_interval={time_interval}&time_rolling={time_rolling}&sort={sort}
        ---
        tags:
          - Traffic Data Query API (交通資料查詢API)
        parameters:
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
            default: 'VD-N3-S-300.000-N-Loop'
          - in: path
            name: sdate
            type: string
            required: true
            description: 資料代表之開始時間(含)(動態資料參照欄位：DataCollectTime)[格式：ISO8601]
            default: '2020-08-18T17:00:00+08:00'
          - in: path
            name: edate
            type: string
            required: true
            description: 資料代表之結束時間(含)(動態資料參照欄位：DataCollectTime)[格式：ISO8601]
            default: '2020-08-18T18:00:00+08:00'
          - in: query
            name: format
            type: string
            required: false
            description: 資料格式(支援JSON、XML)
            enum: ['JSON', 'XML']
            default: 'JSON'
          - in: query
            name: m_pce
            type: float
            minimum: 0
            required: false
            description: 機車當量
            default: 1.0
          - in: query
            name: s_pce
            type: float
            minimum: 0
            required: false
            description: 小型車當量
            default: 1.0
          - in: query
            name: l_pce
            type: float
            minimum: 0
            required: false
            description: 大型車當量
            default: 1.0
          - in: query
            name: t_pce
            type: float
            minimum: 0
            required: false
            description: 連結車當量
            default: 1.0
          - in: query
            name: error_check
            type: int
            required: false
            description: 數值資料異常偵測模式(1:基本規則法、2:巨觀車流模式驗證法、3:AI辨識法)
            enum: [1, 2, 3]
            default: 1
          - in: query
            name: error_process
            type: int
            required: false
            description: 數值資料異常處理模式(0:不做處理、1:刪除資料、2:清除並填補[-1]、30:數值修補-線性插值法、31:數值修補-多項式插值法、32:數值修補-AI修補模式)
            enum: [0, 1, 2, 30, 31, 32]
            default: 0
          - in: query
            name: null_time
            type: int
            required: false
            description: 該時段無資料處理模式(0:不輸出、1:輸出並套用數值資料異常處理模式)
            enum: [0, 1]
            default: 0
          - in: query
            name: time_interval
            type: int
            minimum: 1
            required: false
            description: 資料時間間隔長(分鐘)，例：設定5，則資料時間輸出為07:00、07:05、07:10...
            default: 1
          - in: query
            name: time_rolling
            type: int
            minimum: 1
            required: false
            description: 資料滾動時段長(分鐘)，例：設定15、time_interval設定5，則輸出07:00資料為06:46至07:00之有效資料合併、07:05資料為06:51至07:05之有效資料合併。有效資料為正常或數值修補後資料，如滾動時段內皆無有效資料則套用時段無資料及數值資料異常處理模式
            default: 1
          - in: query
            name: sort
            type: int
            required: false
            description: 依資料時間排序(遞增：1、遞減：-1)
            enum: [1, -1]
        responses:
          200:
            description: OK
         """

        from api import mongo_url

        message = ''

        # 讀取API傳入參數
        args = self.parser.parse_args()
        format = args['format']
        m_pce = args['m_pce']
        s_pce = args['s_pce']
        l_pce = args['l_pce']
        t_pce = args['t_pce']
        error_check = args['error_check']
        error_process = args['error_process']
        null_time = args['null_time']
        time_interval = args['time_interval']
        time_rolling = args['time_rolling']
        sort = args['sort']

        # 滾動時段向前延伸
        sdate = (aniso8601.parse_datetime(sdate) - datetime.timedelta(minutes=time_rolling - 1)).isoformat()

        dataclass = 'VDLive'

        # 參數轉小寫處理
        dataclass_lower = dataclass.lower()
        authority_lower = authority.lower()

        # MongoDB連結設定參數處理
        database = 'traffic_data_' + authority_lower
        collection = dataclass_lower

        # pyspark讀取語法
        from api import spark
        id_name = dataclass_id[dataclass]
        pipeline = ""
        # 動態資料查詢管道指令
        pipeline = pipeline + "{'$match':"
        pipeline = pipeline + "    {'$and':["
        pipeline = pipeline + "        {'" + id_name + "':'" + oid + "'"
        pipeline = pipeline + "        },"
        pipeline = pipeline + "        {'DataCollectTime':"
        pipeline = pipeline + "            {"
        pipeline = pipeline + "                '$gte':{'$date':'" + sdate + "'},"
        pipeline = pipeline + "                '$lte':{'$date':'" + edate + "'}"
        pipeline = pipeline + "            }"
        pipeline = pipeline + "        }"
        pipeline = pipeline + "    ]}"
        pipeline = pipeline + "}"
        if sort == 1 or sort == -1:
            pipeline = "[" + pipeline + ",{'$sort':{'DataCollectTime':" + str(sort) + "}}]"

        df = spark.read.format('mongo') \
            .option('uri', mongo_url) \
            .option("database", database) \
            .option("collection", collection) \
            .option('pipeline', pipeline) \
            .option('pipe', 'allowDiskUse=True') \
            .load()
        json_data_list = df.toJSON().collect()
        # 原始資料表
        json_data = []

        # 查無資料回應
        if len(json_data_list) == 0:
            output_json = json_data
            return output_json, 200

        for values in json_data_list:
            json_dict = json.loads(values)
            del json_dict['_id']  # 刪除momgo的資料編號
            json_dict['DataCollectTime'] = aniso8601.parse_datetime(json_dict['DataCollectTime'])
            # 無條件捨去秒以下時間
            json_dict['DataCollectTime'] = datetime.datetime(year=json_dict['DataCollectTime'].year,
                                                             month=json_dict['DataCollectTime'].month,
                                                             day=json_dict['DataCollectTime'].day,
                                                             hour=json_dict['DataCollectTime'].hour,
                                                             minute=json_dict['DataCollectTime'].minute,
                                                             second=0,
                                                             microsecond=0,
                                                             tzinfo=json_dict['DataCollectTime'].tzinfo)
            json_dict['DataCollectTime'] = json_dict['DataCollectTime'].isoformat()
            json_data.append(json_dict)

        # 正常VDLive資料處理
        json_data = vdlive_data_slsv_normal_process(json_data, m_pce, s_pce, l_pce, t_pce)

        # 正常資料處理後，如異常資料不處理&缺漏時間不輸出則回傳
        if (time_interval == 1 and time_rolling == 1) and (error_process == 0 and null_time == 0):
            output_json = json_data
            return output_json, 200

        # 異常VDLive資料處理
        json_data = vdlive_data_slsv_abnormal_process(json_data, error_process)

        # 時間填補後資料表
        fulltime_json_data = []
        # 正常資料樣板
        json_dict_sample = {}

        # 異常資料處理後，如缺漏時間不輸出則回傳，否則進行時間填補
        # 如採時間填補但又採用刪除異常資料，等同缺漏時間不輸出處理
        if (time_interval == 1 and time_rolling == 1) and (null_time == 0 or (error_process == 1 and null_time == 1)):
            output_json = json_data
            return output_json, 200
        else:
            # 於時間範圍內產生逐分鐘資料
            start_time = aniso8601.parse_datetime(sdate)
            end_time = aniso8601.parse_datetime(edate)
            step_range = datetime.timedelta(minutes=1)
            # 複製一份正常資料樣板
            for json_dict in json_data:
                if not (json_dict['DataStatus'] == data_status['nodata']):
                    json_dict_sample = copy.deepcopy(json_dict)
                    for link_list in json_dict_sample['LinkFlows']:
                        link_list['Volume'] = REPLACE_VALUE
                        link_list['Speed'] = REPLACE_VALUE
                        link_list['Occupancy'] = REPLACE_VALUE
                    break
            # 時間填補程序
            while start_time <= end_time:
                fulltime_json_data.append(vdlive_data_null_time_check(json_data, json_dict_sample, start_time))
                start_time = start_time + step_range
            # 如採填補[-1]，則此階段輸出
            if (time_interval == 1 and time_rolling == 1) and error_process == 2:
                output_json = fulltime_json_data
                if sort == -1:
                    output_json.reverse()
                return output_json, 200

        # 滾動處理後資料表
        rolling_json_data = []

        # 滾動時段輸出處理
        for index in range(len(fulltime_json_data)):
            if index < time_rolling - 1:
                continue
            if index % time_interval == (time_interval - 1):
                json_dict_temp = {}
                effective_cont = 0
                nodata_cont = 0
                for rolling_index in range(index - (time_rolling - 1), index + 1):
                    if len(json_dict_temp) == 0 and \
                            (fulltime_json_data[rolling_index]['DataStatus'] == data_status['normal'] or
                             fulltime_json_data[rolling_index]['DataStatus'] == data_status['repair']):
                        # 第一筆待合併資料
                        json_dict_temp = fulltime_json_data[rolling_index]
                        for linkflows_list in json_dict_temp['LinkFlows']:
                            # 轉為加權速度
                            linkflows_list['Speed'] = linkflows_list['Speed'] * linkflows_list['Volume']
                        effective_cont = effective_cont + 1
                    elif len(json_dict_temp) > 0 and \
                            (fulltime_json_data[rolling_index]['DataStatus'] == data_status['normal'] or
                             fulltime_json_data[rolling_index]['DataStatus'] == data_status['repair']):
                        # 其他合併資料處理
                        for linkflows_list in json_dict_temp['LinkFlows']:
                            for linkflows_list_temp in fulltime_json_data[rolling_index]['LinkFlows']:
                                if linkflows_list['LinkID'] == linkflows_list_temp['LinkID']:
                                    # 計算流量和
                                    linkflows_list['Volume'] = linkflows_list['Volume'] + linkflows_list_temp['Volume']
                                    # 轉為加權速度
                                    if not linkflows_list_temp['Volume'] == 0:
                                        linkflows_list['Speed'] = linkflows_list['Speed'] + \
                                                                  linkflows_list_temp['Speed'] * \
                                                                  linkflows_list_temp['Volume']
                                    # 計算佔有率和
                                    linkflows_list['Occupancy'] = linkflows_list['Occupancy'] + \
                                                                  linkflows_list_temp['Occupancy']
                        if fulltime_json_data[rolling_index]['DataStatus'] == data_status['repair']:
                            json_dict_temp['DataStatus'] = data_status['repair']
                        effective_cont = effective_cont + 1
                    elif fulltime_json_data[rolling_index]['DataStatus'] == data_status['nodata']:
                        nodata_cont = nodata_cont + 1
                # 滾動時段內皆無有效資料，標示異常
                if len(json_dict_temp) == 0:
                    if error_process == 2:
                        json_dict_temp = json_dict_sample
                    if nodata_cont == 0:
                        json_dict_temp['DataStatus'] = data_status['abnormal']
                    else:
                        json_dict_temp['DataStatus'] = data_status['nodata']
                else:
                    for linkflows_list in json_dict_temp['LinkFlows']:
                        if linkflows_list['Volume'] == 0:
                            linkflows_list['Speed'] = 0
                        else:
                            linkflows_list['Speed'] = linkflows_list['Speed'] / linkflows_list['Volume']
                        linkflows_list['Occupancy'] = linkflows_list['Occupancy'] / effective_cont
                # 資料押上輸出時段
                json_dict_temp['Time'] = fulltime_json_data[index]['Time']
                if json_dict_temp['DataStatus'] == data_status['nodata'] or \
                        json_dict_temp['DataStatus'] == data_status['abnormal']:
                    if not (error_process == 1 or null_time == 0):
                        rolling_json_data.append(json_dict_temp)
                else:
                    rolling_json_data.append(json_dict_temp)
        output_json = rolling_json_data
        if sort == -1:
            output_json.reverse()
        return output_json, 200

        output_json = json_data
        return output_json, 200

    def post(self):
        pass

    def put(self):
        pass

    def delete(self):
        pass
