# -*- coding: UTF-8 -*-
import sys

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
        args = self.parser.parse_args()
        authorityCode = args['AuthorityCode']
        dataType = args['DataType']
        format = args['Format']
        data = args['Data']

        codetype =sys.getdefaultencoding()
        print(codetype)
        return {
                   'message': 'ok',
                   'authorityCode': authorityCode,
                   'dataType': dataType,
                   'format': format,
                   'data': data.encode('unicode-escape').decode(codetype)
               }, 200

    def put(self):
        pass

    def delete(self):
        pass
