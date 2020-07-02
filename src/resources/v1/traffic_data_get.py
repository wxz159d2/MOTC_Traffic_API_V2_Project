# -*- coding: UTF-8 -*-

from flask_restful import Resource, reqparse


class Get(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('bt', type=str, required=True)
        self.parser.add_argument('ch', type=str, required=True)
        self.parser.add_argument('rr', type=str, required=True)
        self.parser.add_argument('qd', type=str, required=True)

    def get(self, dbKey, rampName, locationId_start, locationId_end, mainDirection, timeRangeCapacity,
            timeRangeOD_start, timeRangeOD_end):
        data = self.parser.parse_args()
        bt = data.get('bt')
        ch = data.get('ch')
        rr = data.get('rr')
        qd = data.get('qd')
        return {
                   'message': 'true',
                   'dbKey': dbKey,
                   'rampName': rampName,
                   'locationId_start': locationId_start,
                   'locationId_end': locationId_end,
                   'mainDirection': mainDirection,
                   'timeRangeCapacity': timeRangeCapacity,
                   'timeRangeOD_start': timeRangeOD_start,
                   'timeRangeOD_end': timeRangeOD_end,
                   'bt': bt,
                   'ch': ch,
                   'rr': rr,
                   'qd': qd
               }, 200

    def post(self, name):
        pass

    def put(self, name):
        pass

    def delete(self, name):
        pass
