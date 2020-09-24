# -*- coding: UTF-8 -*-
import json

from flask_restful import Resource, reqparse

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
def vdlive_data_slsu_normal_process(json_data, m_pce, s_pce, l_pce, t_pce):
    for data_list in json_data:
        # 標註資料狀態(先假設正常)
        data_list['DataStatus'] = data_status['normal']
        # 檢查設備狀態，如異常則標註資料異常
        if not data_list['Status'] == '0':
            data_list['DataStatus'] = data_status['abnormal']
        for link_list in data_list['LinkFlows']:
            l_occupancy = 0
            l_speed = 0
            l_volume = 0
            for lane_list in link_list['Lanes']:
                v_volume = 0
                for vehicle_list in lane_list['Vehicles']:
                    # 檢查流量，如異常則標註資料異常
                    if int(vehicle_list['Volume']) < flows_normal_range['volume_min'] or \
                            int(vehicle_list['Volume']) > flows_normal_range['volume_max']:
                        data_list['DataStatus'] = data_status['abnormal']
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
                    data_list['DataStatus'] = data_status['abnormal']
                # 檢查佔有率，如異常則標註資料異常
                if float(lane_list['Occupancy']) < flows_normal_range['occupancy_min'] or \
                        float(lane_list['Occupancy']) > flows_normal_range['occupancy_max']:
                    data_list['DataStatus'] = data_status['abnormal']
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
        if 'DataCollectTime' in data_list:
            data_list['Time'] = data_list['DataCollectTime']
            del data_list['DataCollectTime']
        # 隱藏資料
        del_json_dict(json_dict=data_list, del_key='SubAuthorityCode')
        del_json_dict(json_dict=data_list, del_key='Status')
    return json_data


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
        mongo_url_db = mongo_url + database + '.' + collection

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

        df = spark.read.format('mongo').option('uri', mongo_url_db).option('pipeline', pipeline).load()
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
        self.parser.add_argument('sort', type=str, required=False, help='Param error: sort',
                                 choices=['1', '-1'])

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
            type: string
            required: false
            description: 依資料時間排序(遞增：1、遞減：-1)
            enum: ['1', '-1']
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
        mongo_url_db = mongo_url + database + '.' + collection

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
            if sort == '1' or sort == '-1':
                pipeline = "[" + pipeline + ",{'$sort':{'DataCollectTime':" + sort + "}}]"
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
            if sort == '1' or sort == '-1':
                pipeline = "[" + pipeline + ",{'$sort':{'UpdateTime':" + sort + "}}]"

        df = spark.read.format('mongo').option('uri', mongo_url_db) \
            .option('pipeline', pipeline).option('pipe', 'allowDiskUse=True').load()
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


class Get_one_record_slsu(Resource):
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
        self.parser.add_argument('err_data', type=int, default=0, required=False, help='Param error: err_data')

    def get(self, authority, oid, date):
        """
        [單筆車流資料查詢][合併車道][合併車種]
        提供查詢指定VD設備及資料時間之單筆合併車道及車種之車流資料，並具車當量(PCE)轉換、異常資料排除、資料修補等功能
        命令格式： /v1/traffic_data/authority/{authority}/oid/{oid}/date/{date}/method/sum_lanes/sum_vehicles/?format={format}&m_pce={m_pce}&s_pce={s_pce}&l_pce={l_pce}&t_pce={t_pce}&err_data={err_data}
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
            required: false
            description: 機車當量
            default: 1.0
          - in: query
            name: s_pce
            type: float
            required: false
            description: 小型車當量
            default: 1.0
          - in: query
            name: l_pce
            type: float
            required: false
            description: 大型車當量
            default: 1.0
          - in: query
            name: t_pce
            type: float
            required: false
            description: 連結車當量
            default: 1.0
          - in: query
            name: err_data
            type: int
            required: false
            description: 數值資料異常處理模式(0:不做處理、1:清除並填補[-1]、2:清除並修補數值)
            enum: ['0', '1', '2']
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
        err_data = args['err_data']

        dataclass = 'VDLive'

        # 參數轉小寫處理
        dataclass_lower = dataclass.lower()
        authority_lower = authority.lower()

        # MongoDB連結設定參數處理
        database = 'traffic_data_' + authority_lower
        collection = dataclass_lower
        mongo_url_db = mongo_url + database + '.' + collection

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

        df = spark.read.format('mongo').option('uri', mongo_url_db).option('pipeline', pipeline).load()
        json_data_list = df.toJSON().collect()
        json_data = []

        # 查無資料回應
        if len(json_data_list) == 0:
            json_dict = {}
            json_dict['VDID'] = oid
            json_dict['Time'] = date
            json_dict['DataStatus'] = data_status['nodata']
            json_data.append(json_dict)

        for values in json_data_list:
            json_dict = json.loads(values)
            del json_dict['_id']  # 刪除momgo的資料編號
            json_data.append(json_dict)

        # 正常VDLive資料處理
        json_data = vdlive_data_slsu_normal_process(json_data, m_pce, s_pce, l_pce, t_pce)

        # 異常資料不處理
        if err_data == 0:
            output_json = json_data
            return output_json, 200

        output_json = json_data
        return output_json, 200

    def post(self):
        pass

    def put(self):
        pass

    def delete(self):
        pass


class Get_one_record_slpu(Resource):
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
            required: false
            description: 機車當量
            default: 1.0
          - in: query
            name: s_pce
            type: float
            required: false
            description: 小型車當量
            default: 1.0
          - in: query
            name: l_pce
            type: float
            required: false
            description: 大型車當量
            default: 1.0
          - in: query
            name: t_pce
            type: float
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
        mongo_url_db = mongo_url + database + '.' + collection

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

        df = spark.read.format('mongo').option('uri', mongo_url_db).option('pipeline', pipeline).load()
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


class Get_one_record_plsu(Resource):
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
            required: false
            description: 機車當量
            default: 1.0
          - in: query
            name: s_pce
            type: float
            required: false
            description: 小型車當量
            default: 1.0
          - in: query
            name: l_pce
            type: float
            required: false
            description: 大型車當量
            default: 1.0
          - in: query
            name: t_pce
            type: float
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
        mongo_url_db = mongo_url + database + '.' + collection

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

        df = spark.read.format('mongo').option('uri', mongo_url_db).option('pipeline', pipeline).load()
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


class Get_one_record_plpu(Resource):
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
            required: false
            description: 機車當量
            default: 1.0
          - in: query
            name: s_pce
            type: float
            required: false
            description: 小型車當量
            default: 1.0
          - in: query
            name: l_pce
            type: float
            required: false
            description: 大型車當量
            default: 1.0
          - in: query
            name: t_pce
            type: float
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
        mongo_url_db = mongo_url + database + '.' + collection

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

        df = spark.read.format('mongo').option('uri', mongo_url_db).option('pipeline', pipeline).load()
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


class Get_time_range_slsu(Resource):
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
        self.parser.add_argument('err_data', type=int, default=0, required=False, help='Param error: err_data')
        self.parser.add_argument('null_time', type=int, default=0, required=False, help='Param error: null_time')
        self.parser.add_argument('sort', type=str, required=False, help='Param error: sort',
                                 choices=['1', '-1'])

    def get(self, authority, oid, sdate, edate):
        """
        [時段車流資料查詢][合併車道][合併車種]
        提供查詢指定VD設備及資料時段範圍之多筆合併車道及車種之車流資料，並具車當量(PCE)轉換、異常資料排除、資料修補等功能
        命令格式： /v1/traffic_data/authority/{authority}/oid/{oid}/date/{sdate}/to/{edate}/method/sum_lanes/sum_vehicles/?format={format}&m_pce={m_pce}&s_pce={s_pce}&l_pce={l_pce}&t_pce={t_pce}&err_data={err_data}&null_time={null_time}&sort={sort}
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
            name: m_pce
            type: float
            required: false
            description: 機車當量
            default: 1.0
          - in: query
            name: s_pce
            type: float
            required: false
            description: 小型車當量
            default: 1.0
          - in: query
            name: l_pce
            type: float
            required: false
            description: 大型車當量
            default: 1.0
          - in: query
            name: t_pce
            type: float
            required: false
            description: 連結車當量
            default: 1.0
          - in: query
            name: err_data
            type: int
            required: false
            description: 數值資料異常處理模式(0:不做處理、1:清除資料、2:清除並填補[-1]、3:清除並修補數值)
            enum: ['0', '1', '2', '3']
            default: 0
          - in: query
            name: null_time
            type: int
            required: false
            description: 該時段無資料處理模式(0:不輸出、1:輸出並套用數值資料異常處理模式)
            enum: ['0', '1']
            default: 0
          - in: query
            name: sort
            type: string
            required: false
            description: 依資料時間排序(遞增：1、遞減：-1)
            enum: ['1', '-1']
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
        err_data = args['err_data']
        null_time = args['null_time']
        sort = args['sort']

        dataclass = 'VDLive'

        # 參數轉小寫處理
        dataclass_lower = dataclass.lower()
        authority_lower = authority.lower()

        # MongoDB連結設定參數處理
        database = 'traffic_data_' + authority_lower
        collection = dataclass_lower
        mongo_url_db = mongo_url + database + '.' + collection

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
        if sort == '1' or sort == '-1':
            pipeline = "[" + pipeline + ",{'$sort':{'DataCollectTime':" + sort + "}}]"

        df = spark.read.format('mongo').option('uri', mongo_url_db) \
            .option('pipeline', pipeline).option('pipe', 'allowDiskUse=True').load()
        json_data_list = df.toJSON().collect()
        json_data = []

        # 查無資料回應
        if len(json_data_list) == 0:
            json_dict = {}
            json_dict['VDID'] = oid
            json_dict['Time'] = sdate
            json_dict['DataStatus'] = data_status['nodata']
            json_data.append(json_dict)
            output_json = json_data
            return output_json, 200

        for values in json_data_list:
            json_dict = json.loads(values)
            del json_dict['_id']  # 刪除momgo的資料編號
            json_data.append(json_dict)

        # 正常VDLive資料處理
        json_data = vdlive_data_slsu_normal_process(json_data, m_pce, s_pce, l_pce, t_pce)

        # 異常資料不處理&缺漏時間不輸出
        if err_data == 0 and null_time == 0:
            output_json = json_data
            return output_json, 200

        output_json = json_data
        return output_json, 200

    def post(self):
        pass

    def put(self):
        pass

    def delete(self):
        pass
