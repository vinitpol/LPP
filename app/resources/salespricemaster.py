from flask import Flask, jsonify, request, make_response
import pandas as pd
from flask_restful import Resource
from ..settings import flask_app,db
from sqlalchemy import inspect
from sqlalchemy import text


class SalespriceMaster(Resource):
    class SalespriceModel(db.Model):
        __tablename__ = 'salespricemaster'
        id = db.Column(db.Integer, primary_key=True)
        destinationCode = db.Column(db.String(length=255),nullable=False)
        subProductCode = db.Column(db.String(length=255),nullable=False)
        destributionChannelCode = db.Column(db.String(length=255),nullable=False)
        incoterm = db.Column(db.String(length=255),nullable=False)
        timePeriod = db.Column(db.Integer,nullable=False)
        salesPrice = db.Column(db.Float,nullable=False)

    def check_null_or_empty(self, row):
        error_message = ''
        for column, value in row.items():
            if pd.isnull(value) or value == '':
                error_message += f"{column},"
        if error_message:
            error_message = error_message.rstrip(', ') + " is/are null or empty"
        return error_message

    def validate_data(self, df,validate_flag):
        # Check for null values
        df['error'] = df.apply(lambda row: self.check_null_or_empty(row), axis=1)
        if df['error'].any():
            validate_flag = True
        # Check for duplicates based on all columns
        subset_of_columns = ['destinationCode', 'subProductCode', 'destributionChannelCode']
        df['is_duplicate'] = df.duplicated(subset=subset_of_columns, keep='first')
        duplicate_rows_df = df[df['is_duplicate']]
        print('Inserted Database',df)
        if not duplicate_rows_df.empty:
            df.loc[duplicate_rows_df.index, 'error'] = ' Data Already Exist'
            validate_flag = True
        df = df.drop('is_duplicate', axis=1)
        print('Check Databases ',df)
        return df, validate_flag

    def post(self):
        try:
            file = request.files['file']
            if file.filename.endswith('.csv') or file.filename.endswith('.xlsx'):
                # try:
                df = pd.read_csv(file) if file.filename.endswith('.csv') else pd.read_excel(file)
                # Validation: Check for missing column in df with the defined schema
                missing_columns = set(SalespriceMaster.SalespriceModel.__table__.columns.keys()) - set(df.columns) - {"id"}
                if missing_columns:
                    missing_columns_message = ", ".join(missing_columns)
                    return make_response({"message": f"{missing_columns_message} is/are missing"},400)
                common_columns = list(set(df.columns) & set(SalespriceMaster.SalespriceModel.__table__.columns.keys()))
                df = df[common_columns]
                validate_flag = False
                df, validate_flag = self.validate_data(df,validate_flag)
                if validate_flag:
                    return make_response({"error_attachment": df.to_csv(index=False, header=True),"message":"Invalid Data"}, 400)
                # Check if the table already exists
                df = df.drop('error', axis=1)
                inspector = inspect(db.engine)
                if not inspector.has_table(SalespriceMaster.SalespriceModel.__tablename__):
                    db.create_all()
                else:
                    db.session.execute(text(f"DELETE FROM {SalespriceMaster.SalespriceModel.__tablename__}"))
                    db.session.commit()
                df.to_sql(SalespriceMaster.SalespriceModel.__tablename__, db.engine, if_exists='append', index=False)
                db.session.commit()
                # result_df = pd.read_sql(f"SELECT * FROM {DemandMaster.DemandModel.__tablename__}", db.engine)
                return make_response({"message": "Data Inserted Successfully"}, 200)
        except ValueError as ve:
            return make_response({"message": str(ve)}, 400)
        except Exception as e:
            return make_response({"message": f"Error occurred: {str(e)}"}, 400)
        else:
            return make_response({"message": "Unsupported or Invalid File Format. Please upload Excel or CSV File."}, 400)
