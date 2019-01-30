execute sp_execute_external_script 
@language = N'Python',
@script = N'
import time
import pyodbc
import json
import pandas as pd
import requests
from sqlalchemy import create_engine
import urllib

myRequest=requests.get("http://bpm-hml.interfile.com.br/engine-rest/task")
myList = json.loads(myRequest.content.decode("utf-8"))

myDf = pd.DataFrame.from_dict(myList)

server = ""
database = ""
username = ""
password = ""

params = urllib.parse.quote_plus(r"DRIVER={ODBC Driver 13 for SQL Server};SERVER="+server+";DATABASE="+database+";UID="+username+";PWD="+ password)
conn_str = "mssql+pyodbc:///?odbc_connect={}".format(params)
engine = create_engine(conn_str)
myDf.to_sql(name="PORTO_CONTAS_ATIVIDADE",con=engine, if_exists="append",index=False)
';







