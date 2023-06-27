import pandas as pd
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS, cross_origin
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text

from utils.common import generate_token

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})
app.config['SQLALCHEMY_DATABASE_URI'] = 'mssql+pyodbc://sa:4568520sxgs.@43.143.116.236:1433/Drug?' \
                                        'driver=ODBC Driver 17 for SQL Server'
db = SQLAlchemy(app)


class Grug(db.Model):
    GrNo = db.Column(db.String(20), primary_key=True, nullable=False)
    GrName = db.Column(db.Unicode(20), nullable=False)
    GrType = db.Column(db.Unicode(20), nullable=False)
    GrPrice = db.Column(db.Float, nullable=False)
    GStodate = db.Column(db.Unicode(20))
    GrDate = db.Column(db.Date)


class Customer(db.Model):
    CusNo = db.Column(db.String(20), primary_key=True)
    CusName = db.Column(db.String(20), nullable=False)
    Cusage = db.Column(db.Integer)
    CusTel = db.Column(db.String(20))


@app.route('/get', methods=['get'])
def get():
    table = request.args.get('table')
    query = text(f"SELECT * FROM {table};")
    result = db.session.execute(query)
    column_names = result.keys()
    records = []
    for row in result:
        record = dict(zip(column_names, row))
        records.append(record)
    return jsonify(code=200, msg='success', data=records), 200


@app.route('/del', methods=['get'])
def delete():
    table = request.args.get('table')
    no = request.args.get('no')
    col = request.args.get('col')
    query = text(f"delete FROM {table} where {col} = '{no}';")
    db.session.execute(query)
    return jsonify(code=200, msg='success'), 200


@app.route('/login', methods=['get'])
@cross_origin(supports_credentials=True)
def login():
    work_id = request.args.get('work_id')
    passwd = request.args.get('password')
    sql = text(f"select privilege,password from [User] where username = '{work_id}'")
    result = db.session.execute(sql)
    column_names = result.keys()
    records = []
    for row in result:
        record = dict(zip(column_names, row))
        records.append(record)
    df = pd.DataFrame(records)
    if df.empty:
        return jsonify(code=404, msg='用户不存在')
    res = df.to_dict('records')[0]
    passwd_db = res['password']
    if passwd == passwd_db:
        return jsonify(code=200, msg='success', token=generate_token(work_id),
                       privilege=res['privilege'])
    else:
        return jsonify(code=401, msg='密码不正确')


if __name__ == '__main__':
    app.run(debug=True)
