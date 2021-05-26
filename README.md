# Implementación de DHT con Chord

Se implementó una distributed hash table (DHT) usando el protocolo *Chord* mediante el uso de remote procedure calls (RPC) con la biblioteca **Pyro4.**  

## Uso

Realizar los siguientes pasos en consolas diferentes, estos comandos poseen otros parámetros, pero para el funcionamiento local basta con correrlos tal cual:

1. Pyro utiliza un name server para guardar las direcciones reales bajo un alias. Es necesario iniciarlo para que el sistema funcione y pueda resolver los nombres asociados:  

>> $ python3 ch_ns.py

2. Para insertar a los nodos en la DHT se creó un sistema sencillo que lleva los nodos activos. Es necesario iniciarlo para que el sistema funcione y los nodos puedan recibir el nodo inicial necesario para incorporarse a la DHT:  

>> $ python3 ch_coord.py -b HASH_BITS

3. Crear tantos nodos chord como se necesite y agregarlos al sistema para formar la DHT.

>> $ python3 ch_init.py

4. Para probar el sistema se creó un pequeño cliente con funciones básicas.

>> $ python3 ch_client.py

