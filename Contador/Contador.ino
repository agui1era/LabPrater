

#include <esp_task_wdt.h>
//20 seconds WDT
#define WDT_TIMEOUT 120
#define limite_conteo 25
#define pin 12
#define RXD2 16
#define TXD2 17

int contador=0;

void setup() {
  Serial.begin(115200);
  Serial2.begin(115200, SERIAL_8N1, RXD2, TXD2);
  esp_task_wdt_init(WDT_TIMEOUT, true); //enable panic so ESP32 restarts
  esp_task_wdt_add(NULL); //add current thread to WDT watch 
  pinMode(pin, INPUT_PULLUP);
}

void loop() {

  Serial.println("Iniciando conteo ");
  while (digitalRead(pin)==0)
  {
     delay(50);
     esp_task_wdt_reset();  
  };

   while (digitalRead(pin)==1)
  {
     delay(50);
     esp_task_wdt_reset();  
  };
  Serial.print("Pin activado ");
  contador=contador+1;

  if (contador >= limite_conteo){
      Serial.println("Se envia a THB");
      Serial2.println(limite_conteo);
      contador=0;
  } 
}
