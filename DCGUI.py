import time
from datetime import datetime
import datetime
from Config import Config
from func import func 

from flask import Flask, render_template, redirect, url_for, request
app = Flask(__name__)

@app.route("/")
def hello_world():
    return render_template('index.html')   

@app.route('/see/<int:Pid>/<int:AN>/<string:start_date>/<string:end_date>/<int:size>')
def see(Pid, AN, start_date, end_date, size):    

    pipeline_id = int(Pid)
    acc_no = AN
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    chunk_size = size

    account_name = Config.account_nos[acc_no]
    account = Config.creds[acc_no]['account']
    api_key = Config.creds[acc_no]['api_key']

    result = {
                "a" : acc_no,
                "b" : pipeline_id,
                "c" : start_date,
                "d" : end_date,
                "e" : chunk_size,
                "f" : account_name,
                "g" : account,
                "h" : api_key
            }
    while start_date < end_date:
      
      dummy_date = start_date + datetime.timedelta(days=chunk_size)
      if dummy_date > end_date :
        dummy_date = end_date
      response = func.backfill(pipeline_id,account,api_key,start_date,dummy_date)
      result = response.json()

      pipeline_run_id = result['data']['pipeline_run_id']
      
      while True:
        response1 = func.pipelineStatus(account,api_key,pipeline_id,pipeline_run_id)
        result1 = response1.json()

        pipeline_run_status = result1['data']['pipeline_run']['status']

        if(pipeline_run_status != 'Running' and pipeline_run_status != 'Queued'):
          flag = pipeline_run_status
          break
        else:
          time.sleep(10)
        
      chunk_list = [start_date, dummy_date, pipeline_run_status]
      start_date = dummy_date

    return render_template('checkdata.html', result=result)


 


@app.route('/submit', methods = ['POST', 'GET']) 
def submit():
    if request.method == 'POST':
            AN = int(request.form['AN'])
            Pid = int(request.form['Pid'])  
            StartDate = str(request.form['StartDate'])
            EndDate = str(request.form['EndDate'])
            Size = int(request.form['Size'])
    res = 'see'
    return  redirect(url_for(res,Pid=Pid, AN=AN, start_date=StartDate , end_date=EndDate, size=Size  ))
 
app.run(debug = True)