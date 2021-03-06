from flask import Flask, redirect, url_for, request, render_template, jsonify
import json
from sqlalchemy import create_engine
from sourceconnector.SourceDetails import SourceDetails
import data_util
import profiler
from exceptions.InvalidUsage import InvalidUsage
from resources.utilities.ExceptionLog import ExceptionLog
from ProfileDQWrapper import ProfileDQWrapper

conn_type = 'postgresql'
user = 'postgres'
password = 'password'
host = 'localhost'
db = 'postgres'
port = '5432'
postgre_conn = data_util.SQLAlchemy(conn_type, user, password, host, db)
app = Flask(__name__)

@app.route('/success/<name>')
def success(name):
   return 'welcome %s' % name

@app.route('/get_tables',methods = ['POST', 'GET'])
def get_tables():
   if request.method == 'POST':
      json = request.get_json()
      try:
         print(json)
         response = SourceDetails(json, postgre_conn.engine).fetchalltables()
         print(response)
         return response
      except:
         ExceptionLog().log()
         raise InvalidUsage('This view is gone', status_code=410)

      #titles = ['Table1']
      #result = jsonify(data.to_json())

      #put_config_table(database, host, user, password)
      #exec_profiler('snowflake', user, password, host, database)
      #print(data.T.to_dict().values())
      #return data.T.to_dict().values()

@app.route('/exec_profili',methods = ['POST', 'GET'])
def exec_profiling():
   if request.method == 'POST':
      json = request.get_json()
   batch_id = json['batch_id']
   files = json['files']



@app.route('/profile_dq', methods = ['POST', 'GET'])
def profile_dq():
   print(request)
   if request.method == 'POST':
      try:
         data = request.get_json()
         batchid = ProfileDQWrapper(data, postgre_conn).scheduleprofiledq()
         return jsonify(batchid)
      except:
         ExceptionLog().log()
         raise InvalidUsage('oops something went wrong at server side', status_code=410)
         
@app.route('/get_overview',methods = ['POST'])
def overvewsummary():
   import pandas as pd
   json = request.get_json()
   job_id = json['job_id']
   table_name = json['table_name']
   sql = "select * from table_level_profile where batch_d='{}' and source_name = '{}'".format(job_id, table_name)
   print(sql)
   data_from_table = pd.read_sql_query(sql,conn.engine)
   print(data_from_table)
   uniqness = 100 - data_from_table['p_duplicates']
   completeness = 100 - data_from_table['p_cells_missing']

   print(completeness[0])
   return jsonify(completeness[0],uniqness[0])

@app.route('/get_overvewDetailed',methods = ['POST'])
def overvewsummaryDetailed():
   import pandas as pd
   json = request.get_json()
   job_id = json['job_id']
   table_name = json['table_name']
   sql = "select * from table_level_profile where batch_d='{}' and source_name = '{}'".format(job_id, table_name)
   print(sql)
   data_from_table = pd.read_sql_query(sql,conn.engine)
   return jsonify(data_from_table.to_json())


@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


@app.route('/')
def login():
   return render_template("login.html")


if __name__ == '__main__':
   app.run(host='0.0.0.0')
   #app.run()
