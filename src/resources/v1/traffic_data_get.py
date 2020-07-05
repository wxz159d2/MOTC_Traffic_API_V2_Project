# -*- coding: UTF-8 -*-

from flask_restful import Resource, reqparse


class Get(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()

    def get(self, authorityCode, dataType, format):
        # flasgger 預設入口 http://localhost:5000/apidocs
        """
        本API提供查詢OOOO格式資料。
        命令格式： /v1/get/{authorityCode}/{cataType}/{format}/
        ---
        tags:
          - Traffic Get API
        parameters:
          - name: authorityCode
            type: string
            in: path
            required: true
            description: 業管機關簡碼(https://traffic-api-documentation.gitbook.io/traffic/xiang-dai-zhao-biao)
          - name: dataType
            type: string
            in: path
            required: true
            description: 資料型態(依即時路況資料標準V2.0資料類型訂定，如VD、VDLive、LiveTraffic...)
          - name: format
            type: string
            in: path
            required: true
            description: 資料格式(支援JSON、XML)
        responses:
          200:
            description: OK
         """

        args = self.parser.parse_args()

        return {
                   'message': 'OK',
                   'AuthorityCode': authorityCode,
                   'DataType': dataType,
                   'Format': format
               }, 200

    def post(self, name):
        pass

    def put(self, name):
        pass

    def delete(self, name):
        pass
