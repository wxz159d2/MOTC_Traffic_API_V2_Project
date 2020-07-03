# -*- coding: UTF-8 -*-

from flask_restful import Resource, reqparse


class Upload(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('AuthorityCode', type=str, required=True, help='Param error: AuthorityCode',
                                 choices=['NFB', 'THB', 'TNN'])
        self.parser.add_argument('DataType', type=str, required=True, help='Param error: DataType',
                                 choices=['VD', 'VDLive', 'CCTV', 'CMS', 'CMSLive', 'ETag', 'ETagPair', 'ETagPairLive',
                                          'Section', 'SectionLink', 'LiveTraffic', 'CongestionLevel', 'SectionShape',
                                          'News'])
        self.parser.add_argument('Format', type=str, required=True, help='Param error: Format',
                                 choices=['JSON', 'XML'])
        self.parser.add_argument('Data', type=str, required=True, help='Param error: Data')

    def get(self):
        pass

    def post(self):
        # test com: curl http://127.0.0.1:5000/v1/upload/ -X POST -d AuthorityCode=NFB -d DataType=VDLive -d Format=JSON -d Data={"VDID":"VD-N3-S-236-I-WS-1X-南下入口2","SubAuthorityCode":"NFB-CR","LinkFlows":[{"LinkID":"0000301047050M","Lanes":[{"LaneID":0,"LaneType":2,"Speed":61.0,"Occupancy":2.0,"Vehicles":[{"VehicleType":"S","Volume":3,"Speed":61.0},{"VehicleType":"L","Volume":1,"Speed":61.0},{"VehicleType":"T","Volume":0,"Speed":0.0}]}]}],"Status":0,"DataCollectTime":"2020-07-02T15:17:00+08:00"}

        # flasgger 預設入口 http://localhost:5000/apidocs
        """
        本API提供上傳交通部「即時路況資料標準(V2.0)」MOTC Traffic API V2格式資料。
        命令格式： /v1/upload/ -X POST -d AuthorityCode={AuthorityCode} -d DataType={DataType} -d Format={Format} -d Data={Data}
        ---
        tags:
          - Traffic Upload API
        parameters:
          - name: AuthorityCode
            type: string
            in: POST Data
            required: true
            description: 業管機關簡碼(https://traffic-api-documentation.gitbook.io/traffic/xiang-dai-zhao-biao)
          - name: DataType
            type: string
            in: POST Data
            required: true
            description: 資料型態(依即時路況資料標準V2.0資料類型訂定，如VD、VDLive、LiveTraffic...)
          - name: Format
            type: string
            in: POST Data
            required: true
            description: 資料格式(支援JSON、XML)
          - name: Data
            type: string
            in: POST Data
            required: true
            description: 資料內容(一筆資料之JSON或XML值)
        responses:
          200:
            description: OK
         """

        args = self.parser.parse_args()
        authorityCode = args['AuthorityCode']
        dataType = args['DataType']
        format = args['Format']
        data = args['Data']

        return {
                   'message': 'ok',
                   'authorityCode': authorityCode,
                   'dataType': dataType,
                   'format': format,
                   'data': data
               }, 200

    def put(self):
        pass

    def delete(self):
        pass
