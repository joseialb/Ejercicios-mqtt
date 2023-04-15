"""
Ejercicio 2. Clasificación de números simultáneamente a recibir los mensajes
"""

from multiprocessing import Process
from paho.mqtt.client import Client
import sys
import math

def esPrimo(n):
    aux, d = (n%2!=0 and n>1), 3
    while aux and d < math.sqrt(n):
        aux = aux and (n%d!=0)
        d +=2
    if aux or n==2 : print(f"El numero {n} es primo")
    else: print(f"El numero {n} no es primo") 

def frec(userdata):
    for n in userdata['int']:
        print(f"La frecuencia de {n} es {userdata['frec'][n]/userdata['frec']['total']}")
    for n in userdata['float']:
        print(f"La frecuencia de {n} es {userdata['frec'][n]/userdata['frec']['total']}")
        
def on_message(client, userdata, msg):
    try:
        if msg.payload.decode() == "frec":
            frec(userdata)
        else:
            n = float(msg.payload)
            #Separamos enteros y reales
            if int(n) == n:
                userdata['int'].append(n)
                # Como puede tardar mucho, para que no interfiera con el resto del programa, lo llamamos en un proceso a parte
                p = Process(target= esPrimo , args= (n,))
                p.start()
            else:
                userdata['float'].append(n)
                
            #Calculamos la frecuencia de los números que aparecen
            userdata['frec']['total'] += 1
            if n in userdata['frec'].keys():
                userdata['frec'][n] += 1
            else:
                userdata['frec'][n] = 1
    except ValueError:
        pass

def main():
    userdata = {'int': [], 'float': [], 'frec' : {'total' : 0}}
    client = Client(userdata=userdata)
    client.on_message = on_message

    print(f'Conectando al broker {broker}/{topic}')
    client.connect(broker)
    client.subscribe(topic)
    
    client.loop_forever()

if __name__ == '__main__':
    broker = "simba.fdi.ucm.es"
    if len(sys.argv) > 1:
        broker = sys.argv[1]
    topic = "numbers"
    if len(sys.argv) > 2:
        topic = sys.argv[2]
    # topic = 'clients/mi_tema/mi_subtema' # numbers no estaba produciendo numeros
    main()

    