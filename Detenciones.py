
import os
import datetime
import math
import psycopg2
from dateutil.relativedelta import relativedelta

#cada sensor tiene un dispositvo mqtt y uno de indicadores
device_id_mqtt_1="aef16d20-fba7-11eb-ba16-e1db05e491fe"
device_id_mqtt_2="9beb3190-034d-11ec-9cb2-33b63abc84ef"
device_id_mqtt_3="61e99670-0353-11ec-9cb2-33b63abc84ef"

key_var_productos='161'


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



#Detenciones
end_date = datetime.datetime.now()
str_end_date=end_date.strftime("%d/%m/%Y %H:%M:%S")
begin_date = end_date  - relativedelta(seconds=600)
str_begin_date=begin_date.strftime("%d/%m/%Y %H:%M:%S")
print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
print("Tiempos detector detenciones")
print(str_begin_date)
print(str_end_date)


sql_str_det="SELECT SUM(dbl_v) FROM ts_kv WHERE ts >= "+ date_to_milis(str_begin_date)+ " AND ts <="+date_to_milis(str_end_date)+ " AND key="+key_var_productos+" AND  entity_id='"+device_id_mqtt_1+"'"
print(sql_str_det)
result_det=str(getDB(sql_str_det))
print("Resultado: ")
print(result_det)
print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
if result_det == 'None':
  os.system('curl -v -X POST -d "{\"alarma_detencion\":1}" iot.igromi.com:8080/api/v1/labprater_det/telemetry --header "Content-Type:application/json"')
else:
 os.system('curl -v -X POST -d "{\"alarma_detencion\":0}" iot.igromi.com:8080/api/v1/labprater_det/telemetry --header "Content-Type:application/json"')


sql_str_det="SELECT SUM(dbl_v) FROM ts_kv WHERE ts >= "+ date_to_milis(str_begin_date)+ " AND ts <="+date_to_milis(str_end_date)+ " AND key="+key_var_productos+" AND  entity_id='"+device_id_mqtt_2+"'"
print(sql_str_det)
result_det=str(getDB(sql_str_det))
print("Resultado: ")
print(result_det)
print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
if result_det == 'None':
  os.system('curl -v -X POST -d "{\"alarma_detencion2\":1}" iot.igromi.com:8080/api/v1/labprater_det/telemetry --header "Content-Type:application/json"')
else:
 os.system('curl -v -X POST -d "{\"alarma_detencion2\":0}" iot.igromi.com:8080/api/v1/labprater_det/telemetry --header "Content-Type:application/json"')


sql_str_det="SELECT SUM(dbl_v) FROM ts_kv WHERE ts >= "+ date_to_milis(str_begin_date)+ " AND ts <="+date_to_milis(str_end_date)+ " AND key="+key_var_productos+" AND  entity_id='"+device_id_mqtt_3+"'"
print(sql_str_det)
result_det=str(getDB(sql_str_det))
print("Resultado: ")
print(result_det)
print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
if result_det == 'None':
  os.system('curl -v -X POST -d "{\"alarma_detencion3\":1}" iot.igromi.com:8080/api/v1/labprater_det/telemetry --header "Content-Type:application/json"')
else:
 os.system('curl -v -X POST -d "{\"alarma_detencion3\":0}" iot.igromi.com:8080/api/v1/labprater_det/telemetry --header "Content-Type:application/json"')

