# Trabajo Práctico - Coordinación

En este trabajo se busca familiarizar a los estudiantes con los desafíos de la coordinación del trabajo y el control de la complejidad en sistemas distribuidos. Para tal fin se provee un esqueleto de un sistema de control de stock de una verdulería y un conjunto de escenarios de creciente grado de complejidad y distribución que demandarán mayor sofisticación en la comunicación de las partes involucradas.

## Ejecución

`make up` : Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

`make down`: Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make test`: Inicia los contenedores del sistema, espera a que los clientes finalicen, compara los resultados con una ejecución serial y detiene los contenederes.

`make switch`: Permite alternar rápidamente entre los archivos de docker compose de los distintos escenarios provistos.

## Elementos del sistema objetivo

![ ](./imgs/diagrama_de_robustez.jpg "Diagrama de Robustez")
_Fig. 1: Diagrama de Robustez_

### Client

Lee un archivo de entrada y envía por TCP/IP pares (fruta, cantidad) al sistema.
Cuando finaliza el envío de datos, aguarda un top de pares (fruta, cantidad) y vuelca el resultado en un archivo de salida csv.
El criterio y tamaño del top dependen de la configuración del sistema. Por defecto se trata de un top 3 de frutas de acuerdo a la cantidad total almacenada.

### Gateway

Es el punto de entrada y salida del sistema. Intercambia mensajes con los clientes y las colas internas utilizando distintos protocolos.

### Sum

Recibe pares (fruta, cantidad) y aplica la función Suma de la clase `FruitItem`. Por defecto esa suma es la canónica para los números enteros, ej:

`("manzana", 5) + ("manzana", 8) = ("manzana", 13)`

Pero su implementación podría modificarse.
Cuando se detecta el final de la ingesta de datos envía los pares (fruta, cantidad) totales a los Aggregators.

### Aggregator

Consolida los datos de las distintas instancias de Sum.
Cuando se detecta el final de la ingesta, se calcula un top parcial y se envía esa información al Joiner.

### Joiner

Recibe tops parciales de las instancias del Aggregator.
Cuando se detecta el final de la ingesta, se envía el top final hacia el gateway para ser entregado al cliente.

## Limitaciones del esqueleto provisto

La implementación base respeta la división de responsabilidades de los distintos controles y hace uso de la clase `FruitItem` como un elemento opaco, sin asumir la implementación de las funciones de Suma y Comparación.

No obstante, esta implementación no cubre los objetivos buscados tal y como es presentada. Entre sus falencias puede destactarse que:

- No se implementa la interfaz del middleware.
- No se dividen los flujos de datos de los clientes más allá del Gateway, por lo que no se es capaz de resolver múltiples consultas concurrentemente.
- No se implementan mecanismos de sincronización que permitan escalar los controles Sum y Aggregator. En particular:
  - Las instancias de Sum se dividen el trabajo, pero solo una de ellas recibe la notificación de finalización en la ingesta de datos.
  - Las instancias de Sum realizan _broadcast_ a todas las instancias de Aggregator, en lugar de agrupar los datos por algún criterio y evitar procesamiento redundante.
- No se maneja la señal SIGTERM, con la salvedad de los clientes y el Gateway.

## Condiciones de Entrega

El código de este repositorio se agrupa en dos carpetas, una para Python y otra para Golang. Los estudiantes deberán elegir **sólo uno** de estos lenguajes y realizar una implementación que funcione correctamente ante cambios en la multiplicidad de los controles (archivo de docker compose), los archivos de entrada y las implementaciones de las funciones de Suma y Comparación del `FruitItem`.

![ ](./imgs/mutabilidad.jpg "Mutabilidad de Elementos")
_Fig. 2: Elementos mutables e inmutables_

A modo de referencia, en la _Figura 2_ se marcan en tonos oscuros los elementos que los estudiantes no deben alterar y en tonos claros aquellos sobre los que tienen libertad de decisión.
Al momento de la evaluación y ejecución de las pruebas se **descartarán** o **reemplazarán** :

- Los archivos de entrada de la carpeta `datasets`.
- El archivo docker compose principal y los de la carpeta `scenarios`.
- Todos los archivos Dockerfile.
- Todo el código del cliente.
- Todo el código del gateway, salvo `message_handler`.
- La implementación del protocolo de comunicación externo y `FruitItem`.

Redactar un breve informe explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes y a la cantidad de controles.

## Informe

Las instancias de Sum consumen datos que llegan desde el Gateway desde una misma cola compartida. Como el `EOF` que envía el Gateway también llega a una única instancia, hace falta un mecanismo para que el conjunto decida cuándo se terminaron de procesar los datos de un cliente antes de entregar los resultados a los Aggregators.

Para esto se agregó un `exchange de control` al que cada instancia de Sum está bindeada con su propia cola, de modo que publicar al exchange equivale a un broadcast hacia todas las instancias, incluyéndose a si misma. Sobre ese canal se intercambian dos tipos de mensajes:

1. PRE-FLUSH: anuncia el fin del ingreso de datos para un cliente, incluyendo el total de `FruitItem` que el Gateway envió.
2. COUNT: mediante el cual cada instancia reporta periódicamente cuántos `FruitItem` procesó localmente.

Cada instancia acumula los reportes del resto y, cuando tiene un reporte de cada una y la suma coincide con el total anunciado por el Gateway, dispara el flush de su porción parcial hacia los Aggregators. Como pueden llegar mensajes después del `EOF`, cada nuevo record procesado envia un mensaje con el total actualizado, de modo que la suma global llegue a converger al total esperado sin perder nada.

Para escuchar el `exchange de control` hace falta un segundo hilo. El hilo principal se queda bloqueado consumiendo la cola de datos del Gateway, mientras que otro consume el canal de control. Ambos comparten el estado del filtro (conteos locales, reportes recibidos, totales acumulados por fruta) así que el acceso se serializa con un único lock. El broadcast al `exchange de control` también se hace dentro de ese lock.

Del lado de los Aggregators no hace falta un protocolo tan elaborado. Cada Sum particiona su salida aplicando un hash sobre el nombre de la fruta, de manera que cada fruta queda siempre asignada al mismo Aggregator.

En cuanto a escalabilidad, todos los mensajes internos viajan con el identificador del cliente que los originó, y todo el estado en los controles está indexado por cliente. Esto permite que múltiples clientes ejecuten consultas en paralelo sobre el mismo pipeline sin interferir entre sí.
