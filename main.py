import time
import datetime
from Config import Config
from Func import func

while True:
  func.info()
  acc_no = int(input("\nPlease enter account number : "))

  account_name = Config.account_nos[acc_no]
  account = Config.creds[acc_no]['account']
  api_key = Config.creds[acc_no]['api_key']

  pipeline_id = int(input("Please enter Pipeline ID : "))
  sd = str(input("\nPlease enter Start Date (YYYY-MM-DD) : "))
  ed = str(input("Please enter End Date (YYYY-MM-DD)   : "))
  chunk_size = int(input("Please enter Chunk Size (in days)    : "))

  start_date = datetime.datetime.strptime(sd, '%Y-%m-%d')
  end_date = datetime.datetime.strptime(ed, '%Y-%m-%d')

  func.check(acc_no, pipeline_id, start_date, end_date, chunk_size)

  proceed = input("\nDo you wish to proceed? (y or n) : ")
  flag = 'NULL'
  status = []

  if(proceed == 'y'): 
    while start_date < end_date:
      dummy_date = start_date + datetime.timedelta(days=chunk_size)
      if dummy_date > end_date :
        dummy_date = end_date
      response = func.backfill(pipeline_id,account,api_key,start_date,dummy_date)
      result = response.json()
      print(result)
      print(start_date)
      print(dummy_date)

      pipeline_run_id = result['data']['pipeline_run_id']
      
      while True:
        response1 = func.pipelineStatus(account,api_key,pipeline_id,pipeline_run_id)
        result1 = response1.json()
        print(result1)
        pipeline_run_status = result1['data']['pipeline_run']['status']

        if(pipeline_run_status != 'Running' and pipeline_run_status != 'Queued'):
          flag = pipeline_run_status
          break
        else:
          time.sleep(60)
        
      chunk_list = [start_date, dummy_date, pipeline_run_status]
      start_date = dummy_date

      status.append(chunk_list)  
  
  elif(proceed == "n"):
    finish = input("Are you sure you want to EXIT? (y/n) : ")

    if(finish == 'n'):
      continue
    else:
      break

  func.success(account_name, pipeline_id, status)
  break