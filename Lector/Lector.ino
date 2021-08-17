#include <esp_task_wdt.h>
//20 seconds WDT
#define WDT_TIMEOUT 120
#include <ArduinoJson.h>
#include <WiFi.h>
#include <PubSubClient.h>
#define QoS 1

//Serial para conectar con arduino
#define RXD2 16
#define TXD2 17

//Definición red wifi
const char* ssid = "WWW_IGROMI_COM";
const char* password = "wifiiGromi12";

//Definicion MQTT
const char* mqtt_server = "iot.igromi.com";
const char* mqtt_id = "labprater";
const char* mqtt_user = "labprater";
const char* mqtt_pass = "imagina12";

//Variables
WiFiClient espClient;
PubSubClient client(espClient);
String str;
char payload[100];
int contador;

void setup() {
  Serial.begin(115200);
  Serial2.begin(115200, SERIAL_8N1, RXD2, TXD2);
  esp_task_wdt_init(WDT_TIMEOUT, true); //enable panic so ESP32 restarts
  esp_task_wdt_add(NULL); //add current thread to WDT watch 
  setup_wifi();
  client.setServer(mqtt_server, 1883);
  client.subscribe("v1/devices/me/telemetry",QoS);
}

//Conexión wifi
void setup_wifi() {
  delay(10);
  Serial.println();
  Serial.print("Conectando a ");
  Serial.println(ssid);
  WiFi.begin(ssid, password);
  while (WiFi.status() != WL_CONNECTED) {
    delay(200);
    Serial.print(".");
  }
  Serial.println("");
  Serial.println("WiFi conectado");
  Serial.println("IP : ");
  Serial.println(WiFi.localIP());
  
};

//Reconección MQTT
void reconnect() 
{
  // Loop de reconexion MQTT
  while (!client.connected()) {
    Serial.print("Conectando a MQTT...");
    // Intentado conectar
    if (client.connect(mqtt_id,mqtt_user,mqtt_pass)) {
      Serial.println("conectado al server");
      //Topico
      client.subscribe("v1/devices/me/telemetry",QoS);
      
    } else {
      Serial.print("RC-");
      Serial.println(client.state());
      Serial.println("Reintentando....");
      delay(500);
        
  }
 }
};

//Rutina para el envio de datos por MQTT
void EnvioMQTT(float Data,String ID) {
      //Se genera estructura de thingsboard
      str= "{\""+ID+"\":\""+String(Data)+"\"}";
      str.toCharArray(payload,100);
      Serial.println(payload);
      client.publish("v1/devices/me/telemetry",payload);
}
void loop() {
  if (!client.connected()) {
    reconnect();
  };
  while (Serial2.available()) {
    contador=Serial2.readString().toInt();
    EnvioMQTT(contador,"contador");
  };
  esp_task_wdt_reset();
 }
