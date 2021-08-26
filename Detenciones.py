
import os
import datetime
import math
import psycopg2
from dateutil.relativedelta import relativedelta


#cada sensor tiene un dispositvo mqtt y uno de indicadores
device_id_mqtt="b3a33100-006b-11ec-ba16-e1db05e491fe"
device_id="b3a33100-006b-11ec-ba16-e1db05e491fe"
key_var_productos='163'

#definicion del horario del turno 
hora_inicio="8:00:00"
hora_termino="17:00:00"
#el tiempo esperado se usa para realizar el grafico de torta
tiempo_esperado=1000

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

hora_inicio_turno = datetime.datetime.now()
str_hora_inicio_turno=hora_inicio_turno.strftime("%d/%m/%Y "+hora_inicio)
hora_fin_turno = hora_inicio_turno
str_hora_fin_turno=hora_fin_turno.strftime("%d/%m/%Y "+hora_termino)
print(str_hora_inicio_turno)
print(str_hora_fin_turno)

#Productos totales del dia

end_date = datetime.datetime.now()
str_end_date=end_date.strftime("%d/%m/%Y %H:%M:%S")
begin_date = end_date
str_begin_date=begin_date.strftime("%d/%m/%Y 00:00:00")
print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
print("Tiempos productos por dia")
print(str_begin_date)
print(str_end_date)
sql_str_det="SELECT SUM(long_v) FROM ts_kv WHERE ts >= "+ date_to_milis(str_begin_date)+ " AND ts <="+date_to_milis(str_end_date)+ " AND ts >="+date_to_milis(str_hora_inicio_turno)+" AND ts <="+date_to_milis(str_hora_fin_turno)+" AND key="+key_var_productos+" AND  entity_id='"+device_id_mqtt+"'"
print(sql_str_det)
result_det=str(getDB(sql_str_det))
print("Resultado: ")
print(result_det)
print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
os.system('curl -v -X POST -d "{\"productos_dia\":'+result_det+'}" iot.igromi.com:8080/api/v1/imagina13/telemetry --header "Content-Type:application/json"')

#Productos x hora

end_date = datetime.datetime.now()
str_end_date=end_date.strftime("%d/%m/%Y %H:%M:%S")
begin_date = end_date  - relativedelta(seconds=3600)
str_begin_date=begin_date.strftime("%d/%m/%Y %H:%M:%S")
print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
print("Tiempos productos por hora")
print(str_begin_date)
print(str_end_date)
sql_str_det="SELECT SUM(long_v) FROM ts_kv WHERE ts >= "+ date_to_milis(str_begin_date)+ " AND ts <="+date_to_milis(str_end_date)+ " AND ts >="+date_to_milis(str_hora_inicio_turno)+" AND ts <="+date_to_milis(str_hora_fin_turno)+" AND key="+key_var_productos+" AND  entity_id='"+device_id_mqtt+"'"
print(sql_str_det)
result_det=str(getDB(sql_str_det))
print("Resultado: ")
print(result_det)
print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
os.system('curl -v -X POST -d "{\"productos_hora\":'+result_det+'}" iot.igromi.com:8080/api/v1/imagina13/telemetry --header "Content-Type:application/json"')

#Detenciones

end_date = datetime.datetime.now()
str_end_date=end_date.strftime("%d/%m/%Y %H:%M:%S")
begin_date = end_date  - relativedelta(seconds=300)
str_begin_date=begin_date.strftime("%d/%m/%Y %H:%M:%S")
print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
print("Tiempos detector detenciones")
print(str_begin_date)
print(str_end_date)
sql_str_det="SELECT SUM(long_v) FROM ts_kv WHERE ts >= "+ date_to_milis(str_begin_date)+ " AND ts <="+date_to_milis(str_end_date)+ " AND ts >="+date_to_milis(str_hora_inicio_turno)+" AND ts <="+date_to_milis(str_hora_fin_turno)+" AND key="+key_var_productos+" AND  entity_id='"+device_id_mqtt+"'"
print(sql_str_det)
result_det=str(getDB(sql_str_det))
print("Resultado: ")
print(result_det)
print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
if result_det == 'None':
  os.system('curl -v -X POST -d "{\"detencion\":5}" iot.igromi.com:8080/api/v1/imagina13/telemetry --header "Content-Type:application/json"')
  os.system('curl -v -X POST -d "{\"alarma_detencion\":1}" iot.igromi.com:8080/api/v1/imagina13/telemetry --header "Content-Type:application/json"')
else:
 os.system('curl -v -X POST -d "{\"alarma_detencion\":0}" iot.igromi.com:8080/api/v1/imagina13/telemetry --header "Content-Type:application/json"')


#Detenciones del dia

end_date = datetime.datetime.now()
str_end_date=end_date.strftime("%d/%m/%Y %H:%M:%S")
begin_date = end_date
str_begin_date=begin_date.strftime("%d/%m/%Y 00:00:00")
print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
print("Tiempos detenciones del día")
print(str_begin_date)
print(str_end_date)
sql_str_det="SELECT SUM(long_v) FROM ts_kv WHERE ts >= "+ date_to_milis(str_begin_date)+ " AND ts <="+date_to_milis(str_end_date)+ " AND ts >="+date_to_milis(str_hora_inicio_turno)+" AND ts <="+date_to_milis(str_hora_fin_turno)+" AND key="+key_var_productos+" AND  entity_id='"+device_id+"'"
print(sql_str_det)
result_det=str(getDB(sql_str_det))
print("Resultado: ")
print(result_det)
print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
os.system('curl -v -X POST -d "{\"detencion_dia\":'+result_det+'}" iot.igromi.com:8080/api/v1/imagina13/telemetry --header "Content-Type:application/json"')
os.system('curl -v -X POST -d "{\"tiempo_produccion\":'+str(tiempo_esperado-int(result_det))+'}" iot.igromi.com:8080/api/v1/imagina13/telemetry --header "Content-Type:application/json"')
 