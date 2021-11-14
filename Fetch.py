from websocket import create_connection
import json
import pandas as pd
import numpy as np
from IPython.display import display
from Calculate import getOperationRatios, selectOptimalWaterFlow
import websocket
import time
import _thread
from log import *  
from global_const import *

http_addr = "wss://2021-utd-hackathon.azurewebsites.net"
file_path = "./doc.txt"

# def startConnection(ws):
#     def run(*args):
#         time.sleep(1)
#         ws.send("hello")
#         time.sleep(1)
#         ws.close()
#     _thread.start_new_thread(run, ())


def closeConnection(ws, close_status_code, close_msg):
    print('Websocket closing...')

def on_error(ws, error):
    print(error)  

def fetch_data(msg):
    try:
        return json.loads(msg)
    except:
        return msg

def send_data(ws, msg):
    ws.send(msg)

def sort_data(client_data):
    """temperate function that returns the required data for future operation 

    Parameter
    ----------
    client_data : json_object
        json object that is required to extract information from 
        
    Return 
    -------
    flowRateIn : int
        amount of the avabiliable water
    factories : json_object
        factory information
    revenue_vs_flow : pandas.DataFrame
        revenue vs flow sorted in ascending order
        format: [{id:id},{flowPerDay:flowPerDay},{ratio:ratio}]
        example: ["id":"narrowly_preventation_mayma", FLOWPERDAY:INTERVAL, "ratio":ratio]
    """

    flowRateIn = client_data['flowRateIn']
    factories = client_data['operations']
    revenue_vs_flow = pd.DataFrame(columns=['id', 'FLOWPERDAY', 'ratio' "mark"])
    for iter, factory in enumerate(factories):
        id = factory['id']
        ratio = getOperationRatios(factory['revenueStructure'])
        flowPerDay = [flow*INTERVAL for flow in range(1, len(ratio)+1)]
        id = [id]*len(ratio)
        mark = [0]*len(ratio)
        
        arr = np.array([id, flowPerDay, ratio, mark])
        df = pd.DataFrame(arr.transpose(), columns=['id', FLOWPERDAY, 'ratio', 'mark'])
        df[FLOWPERDAY] = df[FLOWPERDAY].astype(int)
        df['ratio'] = df['ratio'].astype(float)
        df['mark'] = df['ratio'].astype(int)
        # arr = np.array([id, flowPerDay, ratio])
        # df = pd.DataFrame(arr.transpose(), columns=['id', 'FLOWPERDAY, 'ratio'])
        # df['FLOWPERDAY] = df['FLOWPERDAY].astype(int)
        # df['ratio'] = df['ratio'].astype(float)

        revenue_vs_flow = pd.concat([revenue_vs_flow, df], ignore_index=True)

    revenue_vs_flow = revenue_vs_flow.sort_values(by=['ratio'], ascending=False)
    revenue_vs_flow.to_csv('/Users/frankgao/HACKUTDVIII_Project/file.csv')
    # ID, X, Ratio. 3
    
    return flowRateIn, factories, revenue_vs_flow

def helper(ws, msg):
    client_data = fetch_data(msg)
    if type(client_data) == type("string"):
        print(msg)
        return -1, msg
    if client_data['type'] == "CURRENT_STATE":
      flowRateIn, factories, revenue_vs_flow = sort_data(client_data)
      res = selectOptimalWaterFlow(flowRateIn, factories, revenue_vs_flow)
      ans = pd.DataFrame(res)
      ans = ans.drop(columns=['ratio'])
      ans = ans.rename(columns={FLOWPERDAY: "flowRate", "id": "operationId"})
      display(ans)
      ws.send(ans.to_json(orient='records'))
      
      return 0, ans
    elif client_data['type'] == "OPTIMATION_RESULT":
       # response
       return 1, client_data


def mapResults(ws, msg):
    status, res = helper(ws, msg)
    if status == 0:
        print("Respond computed and sent to server")
    elif status == 1:
        print(res)
        print("Server responded")


if __name__ == "__main__":
    websocket.enableTrace(True)
    init_log(file_path)
    ws = websocket.WebSocketApp(
        http_addr, on_message=mapResults, on_error=on_error, on_close=closeConnection)
    ws.run_forever()
