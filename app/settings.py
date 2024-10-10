from flask import Flask
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from urllib.parse import quote_plus
from flask_marshmallow import Marshmallow

flask_app = Flask(__name__)

password = 'Test@1234'
encoded_password = quote_plus(password)

CORS(flask_app, resources={r"*": {"origins": ["*"]}})
flask_app.config["CORS_HEADERS"] = "Content-Type"
flask_app.config["CORS_ORIGINS"] = "*"

flask_app.config['SQLALCHEMY_DATABASE_URI'] = f'mysql+pymysql://root:{encoded_password}@localhost:3306/test2'
flask_app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(flask_app)
marsh = Marshmallow(flask_app)
