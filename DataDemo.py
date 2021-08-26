
import os
import time
import random
      
while (1):
    os.system('curl -v -X POST -d "{\"contador_productos\":25}" iot.igromi.com:8080/api/v1/imagina13/telemetry --header "Content-Type:application/json')
    time.sleep(random.randint(60, 600))



    