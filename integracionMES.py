import requests
import datetime
import math
import psycopg2
import logging
from dateutil.relativedelta import relativedelta

url = "http://mes.igromi.com:33331/sensor"
urlProd="http://labprater.igromi.com:33331/sensor"
varName ='contador'
token="c29mdHdhcmVNRVM6M0hVWkJhZlVWV0YzNmtVZQ=="
logFile='mes.log'

def getDB(sql_query):
    try:       
        connection = psycopg2.connect(user = "postgres",
                                        password = "imagina12",
                                        host = "iot.igromi.com",
                                        port = "5432",
                                        database = "thingsboard")
        cursor = connection.cursor()
        postgreSQL_select_Query = sql_query

        cursor.execute(postgreSQL_select_Query)
        bd_records = cursor.fetchall()
        for row in bd_records:
            i=1

    except (Exception, psycopg2.Error) as error:
        print("Error fetching data from PostgreSQL table", error)
    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
    try:
       
       out_query=row[0]

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


sensorName='Labprater1'
write_log('')
write_log(sensorName)
write_log('')


sql_str_det="SELECT ts FROM ts_kv WHERE  key=(select key_id from ts_kv_dictionary where key ='"+varName+"') AND  entity_id = (select id from device where name='"+sensorName+"') order by ts desc limit 1"
write_log(sql_str_det)
result_det=str(getDB(sql_str_det))
write_log("Resultado: "+result_det)
ts=result_det

sql_str_det="SELECT dbl_v FROM ts_kv WHERE  ts="+ts+" AND "+  "key=(select key_id from ts_kv_dictionary where key ='"+varName+"') AND  entity_id = (select id from device where name='"+sensorName+"')"
write_log(sql_str_det)
result_det=str(getDB(sql_str_det))
write_log("Resultado velocidad: "+result_det)
produccion=result_det

sql_str_det="SELECT SUM(dbl_v) FROM ts_kv WHERE ts >= "+ date_to_milis(str_begin_date)+ " AND ts <="+date_to_milis(str_end_date) + " AND "+  "key=(select key_id from ts_kv_dictionary where key ='"+varName+"') AND  entity_id = (select id from device where name='"+sensorName+"')"
print(sql_str_det)
result_det=str(getDB(sql_str_det))
write_log("Total acumulado: "+result_det)
acumulado=result_det

headers = {"Authorization": "Basic "+token, "Content-Type":"application/json"}
x = {
  "data": [
    {"id": "marconi12","produccion":produccion,"acumulado":acumulado,"ts":ts}
  ]
}

response = requests.post(url, headers=headers, json=x)
write_log("Status Code"+ str(response.status_code))
response = requests.post(urlProd, headers=headers, json=x)
write_log("Status Code"+ str(response.status_code))


sensorName='bridge001'
write_log('')
write_log(sensorName)
write_log('')


sql_str_det="SELECT ts FROM ts_kv WHERE  key=(select key_id from ts_kv_dictionary where key ='"+varName+"') AND  entity_id = (select id from device where name='"+sensorName+"') order by ts desc limit 1"
write_log(sql_str_det)
result_det=str(getDB(sql_str_det))
write_log("Resultado: "+result_det)
ts=result_det

sql_str_det="SELECT dbl_v FROM ts_kv WHERE  ts="+ts+" AND "+ "key=(select key_id from ts_kv_dictionary where key ='"+varName+"') AND  entity_id = (select id from device where name='"+sensorName+"')"
write_log(sql_str_det)
result_det=str(getDB(sql_str_det))
write_log("Resultado velocidad: "+result_det)
produccion=result_det

sql_str_det="SELECT SUM(dbl_v) FROM ts_kv WHERE ts >= "+ date_to_milis(str_begin_date)+ " AND ts <="+date_to_milis(str_end_date) + " AND "+  "key=(select key_id from ts_kv_dictionary where key ='"+varName+"') AND  entity_id = (select id from device where name='"+sensorName+"')"
print(sql_str_det)
result_det=str(getDB(sql_str_det))
write_log("Total acumulado: "+result_det)
acumulado=result_det

x = {
  "data": [
    {"id": "marconi3","produccion":produccion,"acumulado":acumulado,"ts":ts}
  ]
}
response = requests.post(url, headers=headers, json=x)
write_log("Status Code"+ str(response.status_code))
response = requests.post(urlProd, headers=headers, json=x)
write_log("Status Code"+ str(response.status_code))


sensorName='Labprater3'
write_log('')
write_log(sensorName)
write_log('')

sql_str_det="SELECT ts FROM ts_kv WHERE  key=(select key_id from ts_kv_dictionary where key ='"+varName+"') AND  entity_id = (select id from device where name='"+sensorName+"') order by ts desc limit 1"
write_log(sql_str_det)
result_det=str(getDB(sql_str_det))
write_log("Resultado: "+result_det)
ts=result_det

sql_str_det="SELECT dbl_v FROM ts_kv WHERE  ts="+ts+" AND "+  "key=(select key_id from ts_kv_dictionary where key ='"+varName+"') AND  entity_id = (select id from device where name='"+sensorName+"')"
write_log(sql_str_det)
result_det=str(getDB(sql_str_det))
write_log("Resultado velocidad: "+result_det)
produccion=result_det

sql_str_det="SELECT SUM(dbl_v) FROM ts_kv WHERE ts >= "+ date_to_milis(str_begin_date)+ " AND ts <="+date_to_milis(str_end_date) + " AND "+  "key=(select key_id from ts_kv_dictionary where key ='"+varName+"') AND  entity_id = (select id from device where name='"+sensorName+"')"
print(sql_str_det)
result_det=str(getDB(sql_str_det))
write_log("Total acumulado: "+result_det)
acumulado=result_det

x = {
  "data": [
    {"id": "envasadora","produccion":produccion,"acumulado":acumulado,"ts":ts}
  ]
}
response = requests.post(url, headers=headers, json=x)
write_log("Status Code"+ str(response.status_code))
response = requests.post(urlProd, headers=headers, json=x)
write_log("Status Code"+ str(response.status_code))