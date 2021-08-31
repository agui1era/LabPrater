
import os
import datetime
import math
import psycopg2
from dateutil.relativedelta import relativedelta

#cada sensor tiene un dispositvo mqtt y uno de indicadores
device_id_mqtt="aef16d20-fba7-11eb-ba16-e1db05e491fe"
device_id="b3a33100-006b-11ec-ba16-e1db05e491fe"


key_var_productos='161'
key_var_detenciones='166'

#productos esperados x turno y el tiempo del turno en minutos
productos_totales_objetivo=5000
tiempo_esperado=540

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



#Productos totales del dia anterior

end_date = datetime.datetime.now()
begin_date = end_date - relativedelta(days=1)
end_date=begin_date
str_end_date=end_date.strftime("%d/%m/%Y 23:59:59")
str_begin_date=begin_date.strftime("%d/%m/%Y 00:00:00")
print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
print("Fechas para total productos día")
print(str_begin_date)
print(str_end_date)
sql_str_det="SELECT SUM(long_v) FROM ts_kv WHERE ts >= "+ date_to_milis(str_begin_date)+ " AND ts <="+date_to_milis(str_end_date)+ " AND key="+key_var_productos+" AND  entity_id='"+device_id_mqtt+"'"
print(sql_str_det)
result_det=str(getDB(sql_str_det))
print("cantidad de productos del dia")
print(result_det)

if result_det == 'None':
   result_det=0

productos_totales=result_det

#Detencion total dia anterior

end_date = datetime.datetime.now()
begin_date = end_date - relativedelta(days=1)
end_date=begin_date
str_end_date=end_date.strftime("%d/%m/%Y 23:59:59")
str_begin_date=begin_date.strftime("%d/%m/%Y 00:00:00")
print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
print("Fechas detenciones totales día")
print(str_begin_date)
print(str_end_date)
sql_str_det="SELECT SUM(long_v) FROM ts_kv WHERE ts >= "+ date_to_milis(str_begin_date)+ " AND ts <="+date_to_milis(str_end_date)+ " AND key="+key_var_detenciones+" AND  entity_id='"+device_id+"'"
print(sql_str_det)
result_det=str(getDB(sql_str_det))
print("Detencion en minutos")
print(result_det)

if result_det == 'None':
   result_det=0

detenciones_totales=result_det

rendimiento=int(productos_totales)/productos_totales_objetivo
disponibilidad=(tiempo_esperado-int(detenciones_totales))/tiempo_esperado

print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
print("Rendimiento: "+str(rendimiento))
print("Disponibilidad: "+str(disponibilidad))
oee=rendimiento*disponibilidad
print("OEE: "+str(oee))
print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")

os.system('curl -v -X POST -d "{\"oee\":'+str(oee)+'}" iot.igromi.com:8080/api/v1/imagina13/telemetry --header "Content-Type:application/json"')
os.system('curl -v -X POST -d "{\"rendimiento\":'+str(rendimiento)+'}" iot.igromi.com:8080/api/v1/imagina13/telemetry --header "Content-Type:application/json"')
os.system('curl -v -X POST -d "{\"disponibilidad\":'+str(disponibilidad)+'}" iot.igromi.com:8080/api/v1/imagina13/telemetry --header "Content-Type:application/json"')
