"""
Ejercicio 6. Encadenamientos
Productor
"""

from multiprocessing import Process
from paho.mqtt.client import Client
import sys
import math
import time
import pickle


"""
Esquema:
    El cliente recibe mensajes del topic de los numeros
    Si son enteros, crea un proceso que se encarga de verificar si el numero es primo
    Si el numero es primo, se publica una alarma para que otro proceso cree un temporizador,
    además, se suscribe al topic de la temperatura y la analiza siguiendo el esquema del ejercicio 3 
    hasta que se acabe el temporizador. Cuando acaba el temporizador, se publica en el topic de las
    alarmas el mensaje "fin". Cuando este se lea, se desuscribe de la temperatura y de la humedad y deja de analizarlas
    



"""
def esPrimo(n, client):
    aux, d = (n%2!=0 and n>1), 3
    while aux and d < math.sqrt(n):
        aux = aux and (n%d!=0)
        d +=2
    if aux or n==2 :
        m = [60, "clients/alarma", "fin"]
        client.publish(topicAlarma, pickle.dumps(m))
        client.userdata["contador_alarmas"] += 1
        if not(topicTemp in client.userdata["topics"]):
            client.userdata["topics"].append(topicTemp)
            client.subscribe(topicTemp)



def esperarPublicar(client,t,topic,mensaje):
    time.sleep(float(t))
    client.userdata["contador_alarmas"] -= 1
    client.publish(topic.decode(), mensaje.decode())






def on_subscribe(client, userdata, mid, granted_qos):
    print(f'Conexion con {userdata["topics"][-1]} completada')

def on_unsubscribe(client, userdata, mid):
    print(f'Desuscribiendose de {userdata["topics"][-1]}')

    




       
def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        print(topic, msg.payload.decode())
        if topic == topicNum:
            n = float(msg.payload)
            if int(n) == n:
                p = Process(target= esPrimo , args= (n,))
                p.start()
                
        elif topic == topicAlarma:
            m = pickle.loads(msg.payload)
            if m == "fin" and client.userdata["contador_alarmas"] == 0:
                if topicHum in client.userdata["topics"]:
                    client.unsubscribe(topicHum)
                    userdata["topics"].delete(topicHum)
                if topicTemp in client.userdata["topics"]:
                    client.unsubscribe(topicTemp)
                    userdata["topics"].delete(topicTemp)
            else:
                t, topic, mensaje = m
                p = Process(target = esperarPublicar, args= (client,t,topic,mensaje))
                p.start()
        
        elif topic == topicTemp:
            if float(msg.payload) > k0 and not( topicHum in client.userdata["topics"] ):
                client.userdata["topics"].append(topicHum)
                client.subscribe(topicHum)
            elif float(msg.payload) < k0 and topicHum in client.userdata["topics"]:
                client.unsubscribe(topicHum)
                userdata["topics"].delete(topicHum)
        
        elif topic == topicHum:
            if float(msg.payload) > k1:
                client.unsubscribe(topicHum)
                userdata["topics"].delete(topicHum)
    except ValueError:
        pass






def main():
    userdata = {"topics" : [], "contador_alarmas" : 0}
    client = Client(userdata=userdata)
    client.on_message = on_message
    client.on_subscribe = on_subscribe
    client.on_unsubscribe = on_unsubscribe

    client.connect(broker)
    userdata["topics"].append(topicNum)
    client.subscribe(topicNum)
    userdata["topics"].append(topicAlarma)
    client.subscribe(topicAlarma)
    
    client.loop_forever()

if __name__ == '__main__':
    broker = "simba.fdi.ucm.es"
    if len(sys.argv) > 1:
        broker = sys.argv[1]
    topicNum = "numbers"
    topicAlarma = "clients/alarma"
    topicTemp = "clients/temperature/kitchen/temp1"
    topicHum = topicTemp.replace("temperature" , "humidity").replace("temp", "hump")
    k0 , k1 = 50, 50
    main()

    