import json

import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin

from lib.db import UseSQLServer
from utils.common import generate_token

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})


@app.route('/get', methods=['get'])
def get():
    table = request.args.get('table')
    con = UseSQLServer()
    query = f"SELECT * FROM {table};"
    result = con.get_mssql_data(query)
    return jsonify(code=200, msg='success', data=result.fillna('').to_dict('records')), 200


@app.route('/del', methods=['get'])
def delete():
    table = request.args.get('table')
    no = request.args.get('no')
    col = request.args.get('col')
    query = f"delete FROM {table} where {col} = '{no}'"
    con = UseSQLServer()
    con.update_mssql_data(query)
    return jsonify(code=200, msg='success'), 200


@app.route('/add', methods=['post'])
def add():
    val = json.loads(request.get_data())
    table = val['table']
    df = pd.DataFrame(val['data'])
    con = UseSQLServer()
    con.write_table(tb_name=table, df=df)
    return jsonify(code=200, msg='success'), 200


@app.route('/update', methods=['post'])
def update():
    val = json.loads(request.get_data())
    table = val['table']
    cols = val['col']
    df = pd.DataFrame(val['data'])
    con = UseSQLServer()
    sql = f"select {','.join(df.columns.values)} from {table} where {cols}='{str(df.iloc[0][cols])}'"
    df1 = con.get_mssql_data(sql)
    if not df1.empty:
        sql = f"update {table} set "
        tag = False
        for col, value in df.iteritems():
            if value[0] != df1.iloc[0][col]:
                tag = True
                sql = sql + f"{col}='{df.iloc[0][col]}',"
        if tag:
            sql = sql[:-1] + f" where {cols}='{str(df.iloc[0][cols])}'"
            con.update_mssql_data(sql)
    return jsonify(code=200, msg='success'), 200


@app.route('/login', methods=['get'])
@cross_origin(supports_credentials=True)
def login():
    work_id = request.args.get('work_id')
    passwd = request.args.get('password')
    sql = f"select privilege,password from [User] where username = '{work_id}'"
    con = UseSQLServer()
    df = con.get_mssql_data(sql)
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
    app.run(port=5001,debug=True)
