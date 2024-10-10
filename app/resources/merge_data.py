from flask import Flask, jsonify, request, make_response
from flask_restful import Resource
from ..settings import flask_app,db
from sqlalchemy.exc import OperationalError
from sqlalchemy import text,inspect
import polars as pl
import pandas as pd
from datetime import datetime, timezone
from ..models import MergeData,OptimizeData
from sqlalchemy import MetaData
from pulp import *
import time
from openpyxl import Workbook
from openpyxl.styles import PatternFill


class HistoryInfo(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    unique_id = db.Column(db.String(length=255), nullable=False)
    unique_name = db.Column(db.String(length=255), nullable=False)


def generate_id_and_datetime():
    '''generating timestamp for unique id and converting it to datetome format too'''
    # print('tim111111111111',datetime.now())
    timestamp = int(time.time() * 1000)
    datetime_obj = datetime.fromtimestamp(timestamp / 1000.0, timezone.utc)
    formatted_datetime = datetime_obj.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    return timestamp,formatted_datetime


def execute_queries():
    queries = {
        "demandmaster": "SELECT * FROM demandmaster;",
        "destinationmaster": "SELECT * FROM destinationmaster;",
        "salespricemaster": "SELECT * FROM salespricemaster;",
        "subproductmaster": "SELECT * FROM subproductmaster;",
        "pdfreightmaster": "SELECT * FROM pdfreightmaster;",
        "costmaster": "SELECT * FROM costmaster;",
        "wdfreightmaster": "SELECT * FROM wdfreightmaster;",
        "pwfreightmaster": "SELECT * FROM pwfreightmaster;",
        "plantmaster": "SELECT * FROM plantmaster;",
        "warehousemaster": "SELECT * FROM warehousemaster;",
        "supplymaster": "SELECT * FROM supplymaster;"
    }

    try:
        connection = db.engine.connect()
        dict1 = {}
        for key, query in queries.items():
            result = connection.execute(text(query))
            data = result.fetchall()
            column_names = result.keys()
            df = pl.DataFrame({col: [row[i] for row in data] for i, col in enumerate(column_names)})
            globals()[key] = df
            # print(f"DataFrame for {key} stored in variable '{key}'")
            if globals()[key] is not None:
                globals()[key] = globals()[key].rename({col: col.lower() for col in globals()[key].columns})
            dict1[key] = globals()[key]
        return dict1
    except OperationalError as e:
        return e


def preprocess_dataframes(dict1):
    try:
        # fetching only required columns
        warehousemaster1 = dict1['warehousemaster'][['warehousecode', 'warehousename', 'warehousetype', 'storagecapacity', 'statecode']]
        pwfreightmaster1 = dict1['pwfreightmaster'][['plantcode', 'warehousecode', 'transportercode', 'freightcost', 'handlingcost']]
        wdfreightmaster1 = dict1['wdfreightmaster'][['destinationcode', 'warehousecode', 'transportercode', 'freightcost', 'handlingcost']]
        plantmaster1 = dict1['plantmaster'][['statecode', 'plantcode', 'plantname']]
        costmaster1 = dict1['costmaster'][['plantcode', 'productcode', 'productioncost', 'packagingcost']]
        pdfreightmaster1 = dict1['pdfreightmaster'][['plantcode', 'destinationcode', 'transportercode', 'freightcost', 'handlingcost']]
        supplymaster1 = dict1['supplymaster'][['plantcode', 'productcode', 'capacity']]
        salespricemaster1 = dict1['salespricemaster'][['destinationcode', 'subproductcode', 'destributionchannelcode', 'incoterm', 'salesprice']]
        destinationmaster1 = dict1['destinationmaster'][['destinationcode', 'destinationname']]
        subproductmaster1 = dict1['subproductmaster'][['subproductcode', 'productcode', 'subproductname', 'sales', 'packtype']]
        demandmaster1 = dict1['demandmaster'][['destinationcode', 'subproductcode', 'distributionchannel', 'incoterm', 'demand', 'minshare']]

        #renaming few fields
        subproductmaster1 = subproductmaster1.rename({'sales': 'distributionchannel'})
        salespricemaster1 = salespricemaster1.rename({'destributionchannelcode': 'distributionchannel'})

        #fetch only the unique data based on given column combination
        warehousemaster1 = warehousemaster1.unique(subset=['warehousecode', 'warehousename', 'warehousetype'])
        pwfreightmaster1 = pwfreightmaster1.unique(subset=['plantcode', 'warehousecode', 'transportercode'])
        wdfreightmaster1 = wdfreightmaster1.unique(subset=['destinationcode', 'warehousecode', 'transportercode'])
        plantmaster1 = plantmaster1.unique(subset=['plantcode'])
        costmaster1 = costmaster1.unique(subset=['plantcode', 'productcode'])
        pdfreightmaster1 = pdfreightmaster1.unique(subset=['plantcode', 'destinationcode', 'transportercode'])
        supplymaster1 = supplymaster1.unique(subset=['plantcode', 'productcode'])
        salespricemaster1 = salespricemaster1.unique(subset=['destinationcode', 'subproductcode', 'distributionchannel', 'incoterm'])
        destinationmaster1 = destinationmaster1.unique(subset='destinationcode')
        subproductmaster1 = subproductmaster1.unique(subset=['subproductcode', 'productcode', 'distributionchannel', 'packtype'])
        demandmaster1 = demandmaster1.unique(subset=['destinationcode', 'subproductcode', 'distributionchannel', 'incoterm'])
        return (warehousemaster1,pwfreightmaster1,wdfreightmaster1,plantmaster1,costmaster1,pdfreightmaster1,
                supplymaster1,salespricemaster1,destinationmaster1,subproductmaster1,demandmaster1)

    except Exception as e:
        return e


def check_null_or_empty( row,column_list):
    error_message = ''
    for column, value in row.items():
        if value== None or value == '' and column in column_list:
            error_message += f"{column},"
    if error_message:
        error_message = error_message.rstrip(', ') + " is/are null or empty"
    return error_message


def join_operation(warehousemaster1,pwfreightmaster1,wdfreightmaster1,plantmaster1,costmaster1,pdfreightmaster1,
                   supplymaster1,salespricemaster1,destinationmaster1,subproductmaster1,demandmaster1):

    # join operation on demandmaster1 and destinationmaster1
    result_new = demandmaster1.join(destinationmaster1, on='destinationcode', how='left')
    result_new = result_new.with_columns(
        pl.when(pl.col('destinationname').is_null())
        .then(pl.col('destinationcode'))
        .otherwise(pl.col('destinationname'))
        .alias('destinationname')
    )
    result_new1 = result_new[['destinationcode', 'destinationname']].unique()
    result_new1 = result_new1.with_columns(pl.col("destinationname").is_duplicated().alias("is_duplicate"))
    result_new1 = result_new1.with_columns(
        pl.when(pl.col("is_duplicate"))
        .then(pl.col("destinationname") + "_" + pl.col("destinationcode").cast(str))
        .otherwise(pl.col("destinationname"))
        .alias("new_destinationname")
    )
    result_new = result_new.join(result_new1[['destinationcode', 'new_destinationname']], on='destinationcode',
                                 how='inner')
    result_new = result_new[['destinationcode', 'subproductcode', 'distributionchannel', 'incoterm',
                             'demand', 'minshare', 'new_destinationname']]
    result_new = result_new.rename({'new_destinationname': 'destinationname'})

    # join operation on result_new with other dataframe for plant to destination
    result = result_new.join(salespricemaster1[['destinationcode', 'subproductcode', 'incoterm', 'salesprice']],
                             on=['destinationcode', 'subproductcode', 'incoterm'], how='left')
    result = result.join(subproductmaster1[['productcode', 'subproductcode', 'subproductname', 'packtype']],
                         on='subproductcode', how='left')
    result = result.join(supplymaster1, on='productcode', how='left')
    result = result.join(pdfreightmaster1, on=['plantcode', 'destinationcode'], how='left')
    result = result.join(costmaster1, on=['plantcode', 'productcode'], how='left')
    result = result.join(plantmaster1, on='plantcode', how='left')
    result = result.with_columns(pl.lit('pd').alias('sourcetype'))
    result = result.rename({'transportercode': 'pdtransportercode', 'freightcost': 'pdfreightcost',
                            'handlingcost': 'pdhandlingcost'})

    # join operation on result_new with other dataframe for plant to warehouse to destination
    result1 = result_new.join(salespricemaster1[['destinationcode', 'subproductcode', 'incoterm', 'salesprice']],
                              on=['destinationcode', 'subproductcode', 'incoterm'], how='left')
    result1 = result1.join(subproductmaster1[['productcode', 'subproductcode', 'subproductname', 'packtype']],
                           on='subproductcode', how='left')
    result1 = result1.join(supplymaster1, on='productcode', how='left')
    result1 = result1.join(costmaster1, on=['plantcode', 'productcode'], how='left')
    result1 = result1.join(plantmaster1, on='plantcode', how='left')
    result1 = result1.join(wdfreightmaster1, on='destinationcode', how='left')
    result1 = result1.join(pwfreightmaster1, on=['plantcode', 'warehousecode'], how='left')
    result1 = result1.rename({'transportercode_right': 'pwtransportercode', 'freightcost_right': 'pwfreightcost',
                              'handlingcost_right': 'pwhandlingcost'})
    result1 = result1.join(warehousemaster1[['warehousecode', 'warehousename', 'storagecapacity', 'statecode']],
                           on='warehousecode', how='left')
    result2 = result1[
        ['destinationcode', 'subproductcode', 'distributionchannel', 'incoterm', 'demand', 'minshare',
         'destinationname', 'salesprice', 'productcode', 'subproductname', 'packtype', 'plantcode',
         'capacity', 'statecode', 'plantname', 'warehousecode', 'transportercode', 'freightcost',
         'handlingcost', 'pwtransportercode', 'pwfreightcost', 'pwhandlingcost', 'productioncost',
         'packagingcost', 'warehousename', 'storagecapacity']]
    result2 = result2.with_columns(pl.lit('pwd').alias('sourcetype'))
    result2 = result2.rename({'transportercode': 'wdtransportercode', 'freightcost': 'wdfreightcost',
                              'handlingcost': 'wdhandlingcost'})

    # null check in both df and assigned missing value in new column error
    column_list1 = ['plantcode','destinationcode','salesprice','productioncost','packagingcost','pdtransportercode',
                    'pdfreightcost','pdhandlingcost']
    result = result.with_columns(
        pl.struct(result.columns)
        .map_elements(lambda row: check_null_or_empty(row,column_list1))
        .alias('error')
    )
    column_list2 = ['plantcode','destinationcode','warehousecode','salesprice','productioncost','packagingcost',
                    'pwtransportercode','pwfreightcost','pwhandlingcost','wdtransportercode','wdfreightcost','wdhandlingcost']
    result3 = result2.with_columns(
        pl.struct(result2.columns)
        .map_elements(lambda row: check_null_or_empty(row,column_list2))
        .alias('error')
    )

    result_clone = result.clone()
    result2_clone = result3.clone()

    # assigning none to below field  as this field is not present for plant to destination dataframe
    for col_name, dtype in [("wdtransportercode", pl.Int64), ("wdfreightcost", pl.Int64),
                            ("wdhandlingcost", pl.Int64), ("pwtransportercode", pl.Int64),
                            ("pwfreightcost", pl.Int64), ("pwhandlingcost", pl.Int64),
                            ("warehousename", pl.Utf8), ("storagecapacity", pl.Int64),
                            ('warehousecode', pl.Int64)]:
        result_clone = result_clone.with_columns(pl.lit(None, dtype=dtype).alias(col_name))

    # assigning none to below field  as this field is not present for plant to warehouse to destination dataframe
    for col, dtype in [('pdtransportercode', pl.Int64), ('pdfreightcost', pl.Int64),
                       ('pdhandlingcost', pl.Int64)]:
        result2_clone = result2_clone.with_columns(pl.lit(None, dtype=dtype).alias(col))
    return result_clone,result2_clone


def optimize_profits(df_final1,unique_id,unique_name):
    df = df_final1.clone()
    #added destination_subproduct column and drop null value and made unique using multiple column then
    #the df is converted to dict and new dict is created for pulp input
    df = df.with_columns((df['destinationname']+'_'+df['subproductcode']).alias('destination_subproduct'))
    demands1 = df[['destination_subproduct','productcode','demand']].drop_nulls()
    demands2 = demands1.unique(subset=['destination_subproduct','productcode'])
    demands_dicts = demands2.to_dicts()
    demands = {(row['destination_subproduct'], row['productcode']): row['demand'] for row in demands_dicts}

    #plant to destination
    pd_inventorys1 = df[['plantname','productcode','pdtransportercode','actualcapacity']].drop_nulls()
    pd_inventorys2 = pd_inventorys1.unique(subset=['plantname','productcode','pdtransportercode'])
    pd_inventorys3 = pd_inventorys2.with_columns(pl.lit(None).alias('warehousename'))
    pd_inventorys4 = pd_inventorys3.rename({'pdtransportercode':'transportercode'})
    #plant to warehouse to destination
    pwd_inventorys1 = df[['plantname', 'warehousename','productcode','pwtransportercode','actualcapacity']].drop_nulls()
    pwd_inventorys2 = pwd_inventorys1.unique(subset=['plantname', 'warehousename','productcode','pwtransportercode'])
    pwd_inventorys3 = pwd_inventorys2.rename({'pwtransportercode':'transportercode'})
    pd_inventorys5 = pd_inventorys4.select(pwd_inventorys3.columns)   #ordering both dataframe column in same order
    inventorys = pl.concat([pd_inventorys5,pwd_inventorys3],how='vertical_relaxed')
    inventorys = inventorys.fill_null(value=' ')
    inventory_to_dict = inventorys.to_dicts()
    inventory_dict = {(row['plantname'], row['warehousename'], row['productcode'], row['transportercode']): row['actualcapacity'] for row in inventory_to_dict}
    pd_profits1 = df[['plantname','destination_subproduct','productcode','pdtransportercode','benefits']].drop_nulls()
    pd_profits2 = pd_profits1.unique(subset=['plantname','destination_subproduct','productcode','pdtransportercode'])
    pwd_profits1 = df[['plantname','warehousename','destination_subproduct','productcode','pwtransportercode','benefits']].drop_nulls()
    pwd_profits2 = pwd_profits1.unique(subset=['plantname','warehousename','destination_subproduct','productcode','pwtransportercode'])
    pd_profits3 = pd_profits2.with_columns(pl.lit(None).alias('warehousename'))
    pd_profits4 = pd_profits3.rename({'pdtransportercode':'transportercode'})
    pwd_profits3 = pwd_profits2.rename({'pwtransportercode':'transportercode'})
    pd_profits5 = pd_profits4.select(pwd_profits3.columns)
    profits = pl.concat([pd_profits5,pwd_profits3],how='vertical_relaxed')
    profits = profits.fill_null(value=" ")
    profits_to_dict = profits.to_dicts()
    profits_dict = {(row['plantname'], row['warehousename'], row['destination_subproduct'], row['productcode'], row['transportercode']): row['benefits'] for row in profits_to_dict}

    prob = LpProblem("Maximize_Profits", LpMaximize)  #requirement
    routes = LpVariable.dicts("Route", profits_dict.keys(), lowBound=0, cat='Continuous')  #setting decision variable
    prob += lpSum([routes[key] * profits_dict[key] for key in profits_dict.keys()])  #equation for finding max profit
    for key, inventory in inventory_dict.items():
        prob += lpSum([routes[route_key] for route_key in routes if tuple(list(route_key)[0:2] + list(route_key)[3:]) == key]) <= inventory, f"Supply_{key}"
    for key, demand in demands.items():
        prob += lpSum([routes[route_key] for route_key in routes if route_key[2:4] == key]) == demand, f"Demand_{key}"
    prob.solve()

    if LpStatus[prob.status] == 'Optimal':
        print("Optimal solution found.")
        data = []
        for variable in prob.variables():
            if variable.varValue > 0:
                route_data = variable.name.split('_', 1)
                route_data = route_data[1:6] + [variable.varValue]
                elements = [x.strip("(')_") for x in route_data[0].split(",")]
                elements.append(str(route_data[1]))
                data.append(elements)
        print('op1111111111111111',data)
        df = pd.DataFrame(data, columns=['plantname', 'warehousename', 'destinationname_subproductcode', 'productcode',
                                         'transportercode', 'quantity'])

        profits_dict_modified = {(k[0], '_' if k[1] == ' ' else k[1], k[2], k[3], k[4]): v for k, v in
                                 profits_dict.items()}
        df['demand'] = df.apply(
            lambda row: demands.get((row['destinationname_subproductcode'], row['productcode']), 0), axis=1)

        def get_benefits(row):
            plantname = row['plantname'].replace('_', ' ')
            warehousename = '_' if row['warehousename'] == '' else row['warehousename'].replace('_', ' ')
            key = (
            plantname, warehousename, row['destinationname_subproductcode'], row['productcode'], row['transportercode'])
            print(key)
            return profits_dict_modified.get(key, 0)

        df['benefits'] = df.apply(get_benefits, axis=1)
        df[['destinationname', 'subproductcode']] = df['destinationname_subproductcode'].str.split('_', n=1,
                                                                                                   expand=True)
        df = df.drop(columns=['destinationname_subproductcode'])
        df = df.drop(columns=['transportercode'])
        df["unique_id"] = unique_id
        df["unique_name"] = unique_name
        df = df.rename(columns={"quantity": "dispatchquantity"})
        return df
    return None


class MergedData(Resource):

    def get(self):
        try:
            dict1 = execute_queries()
            (warehousemaster1,pwfreightmaster1,wdfreightmaster1,plantmaster1,costmaster1,pdfreightmaster1,supplymaster1,
             salespricemaster1,destinationmaster1,subproductmaster1,demandmaster1) = preprocess_dataframes(dict1)

            result_clone,result2_clone = join_operation(warehousemaster1,pwfreightmaster1,wdfreightmaster1,
                                                        plantmaster1,costmaster1,pdfreightmaster1,supplymaster1,
                                                        salespricemaster1,destinationmaster1,subproductmaster1,demandmaster1)

            # aligning column of both dataframe in same order
            result_clone = result_clone.select(result2_clone.columns)
            result_final = pl.concat([result_clone, result2_clone], how='vertical_relaxed')

            result_final = result_final.fill_null(value=0)
            result_final = result_final.with_columns((result_final['salesprice'] - result_final['wdfreightcost'] -
                                                      result_final['wdhandlingcost'] - result_final['pwfreightcost'] -
                                                      result_final['pwhandlingcost'] - result_final['productioncost'] -
                                                      result_final['packagingcost'] - result_final['pdfreightcost'] -
                                                      result_final['pdhandlingcost']).alias('benefits'))
            result_final2 = result_final.select(
                ['error', 'sourcetype', 'plantcode', 'plantname', 'warehousecode', 'warehousename', 'destinationcode',
                 'destinationname', 'distributionchannel', 'subproductcode', 'subproductname', 'productcode','packtype',
                  'capacity', 'storagecapacity','demand', 'statecode', 'salesprice','productioncost', 'packagingcost',
                   'pdfreightcost','pwfreightcost', 'pwhandlingcost', 'pdhandlingcost', 'wdtransportercode', 'pwtransportercode',
                 'wdfreightcost', 'wdhandlingcost','pdtransportercode',
                 ])
                 
            result_final2 = result_final2.to_pandas()
            output = result_final2["error"].notnull() & (result_final2["error"] != "")
            if output.any():
            # if not result_final2["error"].null_count() < len(result_final2):
                return make_response({"error_attachment": result_final2.to_csv(index=False,header=True),"message":"Missing Values Found"}, 400)


            #adding new column actualcapacity in the dataframe and assigning value to it base on below condition
            df = result_final.clone()
            df = df.with_columns(pl.lit(0).alias("actualcapacity"))
            df = df.with_columns(
                pl.when((df["sourcetype"] == "pwd") & df["warehousecode"].is_not_null())
                .then(df["storagecapacity"])
                .otherwise(df["capacity"])
                .alias("actualcapacity")
            )
            df_final1 = df.select(
                ['plantcode', 'warehousecode', 'destinationcode', 'subproductcode', 'capacity','storagecapacity',
                  'actualcapacity', 'distributionchannel','incoterm','demand','minshare','destinationname',
                 'salesprice', 'productcode','subproductname','packtype', 'statecode','plantname', 'wdtransportercode',
                 'wdfreightcost','wdhandlingcost','pwtransportercode','pwfreightcost','pwhandlingcost','productioncost',
                 'packagingcost','warehousename', 'sourcetype','pdtransportercode','pdfreightcost','pdhandlingcost',
                 'benefits', ])
            print('s1111111111111111111111111111')
            unique_id, generated_date = generate_id_and_datetime()
            inspector = inspect(db.engine)
            print('s222222222222222222222222222')
            if not inspector.has_table(HistoryInfo.__tablename__):
                print('s33333333333333333333333')
                db.create_all()
                unique_name = "Run-1" + f"({generated_date})"
                new_data = HistoryInfo(unique_id=unique_id,unique_name=unique_name)
                db.session.add(new_data)
                db.session.commit()
            else:
                last_inserted_id = (HistoryInfo.query.order_by(HistoryInfo.id.desc()).first()).id + 1
                unique_name = "Run-" + str(last_inserted_id) + f"({generated_date})"
                new_data = HistoryInfo(unique_id=unique_id, unique_name=unique_name)
                db.session.add(new_data)
                db.session.commit()
            df_final1 = df_final1.with_columns(pl.lit(unique_id).alias('unique_id'))
            df_final1 = df_final1.with_columns(pl.lit(unique_name).alias('unique_name'))
            print('aaaaaaaaaaaaaa',df_final1[:1])
            # df_final1 = df_final

            metadata = MetaData()
            #check if table exist or not and then accordingly create and insert data
            if not inspector.has_table(MergeData.__tablename__):
                metadata.create_all(bind=db.engine, tables=[MergeData.__table__])
            df = df_final1.to_pandas()
            df.to_sql(MergeData.__tablename__, db.engine, if_exists='append', index=False)
            db.session.commit()

            optimize_data = optimize_profits(df_final1,unique_id,unique_name)
            if optimize_data is not None:
                if not optimize_data.empty:
                    if not inspector.has_table(OptimizeData.__tablename__):
                        metadata.create_all(bind=db.engine,tables=[OptimizeData.__table__])
                    optimize_data.to_sql(OptimizeData.__tablename__, db.engine, if_exists='append', index=False)
                    db.session.commit()
                    return make_response({"message": "Success", "unique_id": unique_id}, 200)
            return make_response({"message": "Optimal Solution Not Found For This Linear Problem"}, 400)
        except ValueError as ve:
            return make_response({"message": str(ve)}, 400)
        except Exception as e:
            return make_response({"message": f"Error occurred: {str(e)}"}, 400)