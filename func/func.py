import requests
import json
import smtplib, ssl
from Config import Config
from func import func 

def backfill(pipeline_id,account,api_key,start_date,end_date):
    url = f"https://api.datachannel.co/api/v1/pipelines/{pipeline_id}/runs/backfill"

    headers = {
    'X-ACCOUNT-SLUG': account,
    'Content-Type': 'application/json',
    'X-API-KEY': api_key
    }

    v1 = start_date.strftime('%Y-%m-%d')
    v2 = end_date.strftime('%Y-%m-%d')

    payload = json.dumps({
        "from_date": v1,
        "to_date": v2
    })

    print("backfill_url :",url)
    print("headers :",headers)
    print("payload :",payload)
    response = requests.request("POST", url, headers=headers, data=payload)
    return response

def pipelineStatus(account,api_key,pipeline_id,pipeline_run_id):
    url = f"https://api.datachannel.co/api/v1/pipelines/{pipeline_id}/runs/{pipeline_run_id}"
    payload = {}
    headers = {
      'X-ACCOUNT-SLUG': account,
      'X-API-KEY': api_key
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    return response

def success(account_name, pipeline_id, status):
    msg = EmailMessage()
    msg['Subject'] = 'Backfill Successfull'
    msg['From'] = 'ansh.gupta@decision-tree.com'
    msg['To'] = 'aman.mishra@decision-tree.com'

    content = f"Backfill Status - \nAccount Name : {account_name} \nPipeline ID : {pipeline_id}"
    for i in status:
        content += f"\nChunk Start Date : {i[0]} "
        content += f"Chunk End Date : {i[1]} "
        content += f"Chunk Run Status : {i[2]}"
    
    msg.set_content(content)

    server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
    server.login("ansh.gupta@decision-tree.com", "xnxmuxdtgotgykjh")
    server.send_message(msg)
    server.quit()

    print("Email sent successfully") 

def info():
    print("\nPlease select the account : ")
    print("1.  BetterBeing")
    print("2.  CoffeeBean")
    print("3.  Therabody")   
    print("4.  BlueRoot")
    print("5.  Astral Beauty")
    print("6.  ModernGourmet")
    print("7.  VitaCup")
    print("8.  Boisson")
    print("9.  MGPCaliperCovers")
    print("10. UrbanRed")
    print("11. LonoLife")

def check(acc_no, pipeline_id, start_date, end_date, chunk_size):
    print("\nPlease confirm if the following information is correct\n")
    print(f"Account chosen : {Config.account_nos[acc_no]}")
    print(f"Pipeline ID    : {pipeline_id}")
    print(f"Start Date     : {start_date}")
    print(f"End Date       : {end_date}")
    print(f"Chunk Size     : {chunk_size}")
