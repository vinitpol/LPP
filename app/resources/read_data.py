from flask_restful import Resource
from flask import Flask,jsonify,make_response,request
from ..models import MergeData,OptimizeData
from ..serializers import MergeDataSchema,OptimizeDataSchema
from sqlalchemy import text,inspect
from ..settings import flask_app,db


class ReadData(Resource):
    def get(self):
        try:
            unique_id = request.args.get("unique_id")
            table_name = request.args.get("table_name")
            page_no = int(request.args.get("page_no"))
            size = int(request.args.get("size"))
            offset_no = (page_no-1)*size
            serialized_data = []
            total_count = 0
            inspector = inspect(db.engine)
            if table_name == MergeData.__tablename__:
                if not inspector.has_table(MergeData.__tablename__):
                    return make_response({"message":"Table Does Not Exist"},400)
                merged_data = MergeData.query.filter_by(unique_id=unique_id).order_by(MergeData.destinationname.asc(),MergeData.benefits.desc()).offset(offset_no).limit(size)
                total_count = MergeData.query.filter_by(unique_id=unique_id).count()
                merged_data_schema = MergeDataSchema(many=True)
                serialized_data = merged_data_schema.dump(merged_data)
            elif table_name == OptimizeData.__tablename__:
                if not inspector.has_table(OptimizeData.__tablename__):
                    return make_response({"message":"Table Does Not Exist"},400)
                optimize_data = OptimizeData.query.filter_by(unique_id=unique_id).order_by(OptimizeData.destinationname.asc()).offset(offset_no).limit(size)
                total_count = OptimizeData.query.filter_by(unique_id=unique_id).count()
                optimize_data_schema = OptimizeDataSchema(many=True)
                serialized_data = optimize_data_schema.dump(optimize_data)
            return make_response({"data": serialized_data,"total_count":total_count,"message":"success"}, 200)
        except Exception as e:
            return make_response({"message": f"Error occurred: {str(e)}"}, 400)