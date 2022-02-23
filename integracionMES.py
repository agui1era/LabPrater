import requests
import datetime
import math
import psycopg2
import logging
from dateutil.relativedelta import relativedelta
from array import array

url = "https://mesdev.igromi.com:9999/sensor"
urlProd="https://labprater.igromi.com:9999/sensor"




varName ="contador"
token="c29mdHdhcmVNRVM6M0hVWkJhZlVWV0YzNmtVZQ=="
logFile="mes.log"
user="postgres"
password="imagina12"
host="iot.igromi.com"
port= "5432"
database = "thingsboard"
prefijo="MES_LBP"

def getDB(sql_query):
    list_records=[]
    try:       
        connection = psycopg2.connect(  user = user ,
                                        password = password,
                                        host = host,
                                        port = port,
                                        database = database)
        cursor = connection.cursor()
        postgreSQL_select_Query = sql_query

        cursor.execute(postgreSQL_select_Query)
        bd_records = cursor.fetchall()
        for row in bd_records:
            list_records.append(row)

    except (Exception, psycopg2.Error) as error:
        print("Error fetching data from PostgreSQL table", error)
    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
    try:
       
       out_query=list_records

    except (Exception, psycopg2.Error) as error:
       out_query=0

    return out_query

def date_to_milis(date_string):

    #convert date to timestamp
    obj_date = datetime.datetime.strptime(date_string,"%d/%m/%Y %H:%M:%S")

    return str(math.trunc(obj_date.timestamp() * 1000))

def write_log(cadena):

    logging.basicConfig(filename=logFile,level=logging.DEBUG)

    end_date = datetime.datetime.now()
    str_date=end_date.strftime("%d/%m/%Y %H:%M:%S")
    logging.info(str_date+": "+cadena)
    print(str_date+": "+cadena)

    return 0


#Tiemnpos de producción del dia
end_date = datetime.datetime.now()
str_end_date=end_date.strftime("%d/%m/%Y %H:%M:%S")
str_begin_date=end_date.strftime("%d/%m/%Y 0:00:00")
print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
print("Tiempos de la producción diaria")
print(str_begin_date)
print(str_end_date)



sql_str_det="select name from device where name like '"+prefijo+"%'"
print(sql_str_det)
result_det=getDB(sql_str_det)


for row in result_det:

  cadena=str(row)
  cadena=cadena.replace("('","")
  cadena=cadena.replace("',)","")
  write_log("Resultado: "+cadena)

  sensorName=cadena
  write_log("")
  write_log(sensorName)
  write_log("")

  if sensorName == "MES_LBP_contador004_marconi12":
      varName="contador3"


  sql_str_det="SELECT ts FROM ts_kv WHERE  key=(select key_id from ts_kv_dictionary where key ='"+varName+"') AND  entity_id = (select id from device where name='"+sensorName+"') order by ts desc limit 1"
  write_log(sql_str_det)
  result_det=getDB(sql_str_det)
  cadena=str(result_det[0])
  cadena=cadena.replace("(","")
  cadena=cadena.replace(",)","")
  write_log("ts: "+cadena)
  ts=cadena

  sql_str_det="SELECT dbl_v FROM ts_kv WHERE  ts="+ts+" AND "+  "key=(select key_id from ts_kv_dictionary where key ='"+varName+"') AND  entity_id = (select id from device where name='"+sensorName+"')"
  write_log(sql_str_det)
  result_det=getDB(sql_str_det)
  cadena=str(result_det[0])
  cadena=cadena.replace("(","")
  cadena=cadena.replace(",)","")
  write_log("Produccion: " +cadena)
  produccion=cadena

  sql_str_det="SELECT SUM(dbl_v) FROM ts_kv WHERE ts >= "+ date_to_milis(str_begin_date)+ " AND ts <="+date_to_milis(str_end_date) + " AND "+  "key=(select key_id from ts_kv_dictionary where key='"+varName+"') AND  entity_id = (select id from device where name='"+sensorName+"')"
  print(sql_str_det)
  result_det=getDB(sql_str_det)
  cadena=str(result_det[0])
  cadena=cadena.replace("(","")
  cadena=cadena.replace(",)","")
  write_log("Acumulado: " +cadena)
  acumulado=cadena

  headers = {"Authorization": "Basic "+token, "Content-Type":"application/json"}
  x = {
    "data": [
      {"id":sensorName,"produccion":produccion,"acumulado":acumulado,"ts":ts}
    ]
  }

  response = requests.post(url, headers=headers, json=x)
  write_log("Status Code"+ str(response.status_code))
  response = requests.post(urlProd, headers=headers, json=x)
  write_log("Status Code"+ str(response.status_code))



