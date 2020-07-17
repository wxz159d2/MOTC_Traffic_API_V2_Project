# -*- coding: UTF-8 -*-

from flask_restful import Resource, reqparse


class Upload(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()

    def get(self):
        pass

    def post(self):
        # test com: curl http://127.0.0.1:5000/v1/upload/ -X POST -d AuthorityCode=NFB -d DataType=VDLive -d Format=JSON -d Data={"VDID":"VD-N3-S-236-I-WS-1X-南下入口2","SubAuthorityCode":"NFB-CR","LinkFlows":[{"LinkID":"0000301047050M","Lanes":[{"LaneID":0,"LaneType":2,"Speed":61.0,"Occupancy":2.0,"Vehicles":[{"VehicleType":"S","Volume":3,"Speed":61.0},{"VehicleType":"L","Volume":1,"Speed":61.0},{"VehicleType":"T","Volume":0,"Speed":0.0}]}]}],"Status":0,"DataCollectTime":"2020-07-02T15:17:00+08:00"}

        # flasgger 預設入口 http://localhost:5000/apidocs
        """
        本API提供上傳交通部「即時路況資料標準(V2.0)」MOTC Traffic API V2格式資料。
        命令格式： /v1/traffic_data/ -X POST -d post_data={AuthorityCode}
        ---
        tags:
          - Traffic Upload API
        parameters:
          - in: body
            name: post_data
            required: true
            schema:
              required:
                - authoritycode
                - data_type
                - format
                - data
              properties:
                authoritycode:
                  type: string
                  description: 業管機關簡碼(https://traffic-api-documentation.gitbook.io/traffic/xiang-dai-zhao-biao)
                data_type:
                  type: string
                  description: 資料型態(依即時路況資料標準V2.0資料類型訂定，如VD、VDLive、LiveTraffic...)
                format:
                  type: string
                  description: 資料格式(支援JSON、XML)
                data:
                  type: string
                  description: 資料內容(一筆資料之JSON或XML值)
        responses:
          200:
            description: OK
         """

        return {
                   'message': 'ok',
                   'authoritycode': authoritycode,
                   'data_type': data_type,
                   'format': format,
                   'data': data
               }, 200

    def put(self):
        pass

    def delete(self):
        pass
