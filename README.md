# Implementación de Chord

Se implementó el protocolo Chord mediante el uso de remote procedure calls (RPC) usando la biblioteca Pyro4.  

## Uso

Realizar los siguientes pasosen consolas diferentes, estos comandos poseen otros parámetros, pero para el funcionamiento local basta con correrlos tal cual:

1. Pyro utiliza un name server para guardar las direcciones reales bajo un alias. Es necesario iniciarlo para que el sistema funcione:  

>> python3 ch_ns.py

2. Para insertar a los nodos en la DHT se creó un sistema sencillo que lleva los nodos activos. Es necesario iniciarlo para que el sistema funcione:  

>> python3 ch_coord.py -b HASH_BITS

3. Crear tantos nodos chord coo se necesite y agregarlos al sistema para formar la DHT.

>> python3 ch_init.py

4. Para probar el sistema se creó un pequeño cliente con funciones básicas.

>> python3 ch_client.py

