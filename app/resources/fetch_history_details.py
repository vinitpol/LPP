from flask import Flask, jsonify, request, make_response
from .merge_data import HistoryInfo
from flask_restful import Resource
from ..serializers import HistoryInfoSchema


class HistoryDetails(Resource):

    def get(self):
        try:
            history_data = HistoryInfo.query.all()
            history_info_schema = HistoryInfoSchema(many=True)
            serialized_data = history_info_schema.dump(history_data)

            return make_response({"data": serialized_data},200)
        except Exception as e:
            return make_response({"message": f"Error occurred: {str(e)}"}, 400)