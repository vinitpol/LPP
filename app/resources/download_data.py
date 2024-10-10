from flask_restful import Resource
from flask import Flask,jsonify,make_response,request
from ..models import MergeData,OptimizeData
from ..serializers import MergeDataSchema,OptimizeDataSchema
from sqlalchemy import inspect
from ..settings import db
import pandas as pd


class DownloadData(Resource):
    def get(self):
        try:
            unique_id = request.args.get("unique_id")
            table_name = request.args.get("table_name")
            serialized_data = []
            inspector = inspect(db.engine)
            if table_name == MergeData.__tablename__:
                if not inspector.has_table(MergeData.__tablename__):
                    return make_response({"message":"Table Does Not Exist"},400)
                merged_data = MergeData.query.filter_by(unique_id=unique_id).order_by(MergeData.destinationname.asc(),MergeData.benefits.desc())
                merged_data_schema = MergeDataSchema(many=True)
                serialized_data = merged_data_schema.dump(merged_data)
            elif table_name == OptimizeData.__tablename__:
                if not inspector.has_table(OptimizeData.__tablename__):
                    return make_response({"message":"Table Does Not Exist"},400)
                optimize_data = OptimizeData.query.filter_by(unique_id=unique_id).order_by(OptimizeData.destinationname.asc())
                optimize_data_schema = OptimizeDataSchema(many=True)
                serialized_data = optimize_data_schema.dump(optimize_data)
            if serialized_data:
                df = pd.DataFrame(serialized_data)
                return make_response({"data": df.to_csv(index=False, header=True),"message":"success"}, 200)
            return make_response({"data": serialized_data,"message":"No Data"}, 400)
        except Exception as e:
            return make_response({"message": f"Error occurred: {str(e)}"}, 400)