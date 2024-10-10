from flask import Flask,jsonify,request,Response
from flask_restful import Api,Resource
from flask_cors import CORS
from app.router import register_routes
from flask_sqlalchemy import SQLAlchemy
from urllib.parse import quote_plus


# flask_app = Flask(__name__)
#
# password = 'Test@1234'
# encoded_password = quote_plus(password)
#
# CORS(flask_app, resources={r"*": {"origins": ["*"]}})
# flask_app.config["CORS_HEADERS"] = "Content-Type"
# flask_app.config["CORS_ORIGINS"] = "*"
#
# flask_app.config['SQLALCHEMY_DATABASE_URI'] = f'mysql+pymysql://root:{encoded_password}@localhost:3306/llp'
# flask_app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
# db = SQLAlchemy(flask_app)
#
# api = Api(flask_app)
# register_routes(api)

from app.router import register_routes
from app.settings import flask_app
from flask_restful import Api,Resource

api = Api(flask_app)
register_routes(api)

if __name__ == "__main__":
    flask_app.run(debug=True,port=5000)
