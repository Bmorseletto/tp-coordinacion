# Informe de entrega

En este trabajo se implementaron mecanismos para permitiría la coordinacion y sincronizacion de distintos trabajadores del sistema distribuido

## División de flujo de datos de los clientes
Para lograr un buen flujo de datos en el sistema para que los resultados de cada cliente no se mezclen se les asignó un uuid a cada cliente cuando se crea la instancia message handler encargada de manejar los mensajes de ese cliente

En los workers se utilizan diccionarios con el uuid como key para separa los datos relacionados a los clientes que se van recibiendo durante la ejecución

## Sincronización de múltiples Workers Sum
Dentro del sistema una vez que el cliente termina de mandar su información se manda un mensaje representativo de eof, dentro de este sistema por el uso de queues de Rabbitmq solo 1 worker sum recibe este mensaje, para resolver este problema se creó un exchange de rabbitmq, sum_intercomm, donde el primer worker que recibe el mensaje de eof lo reenvía por el exchange para avisarle a los workers,el worker que recibió el eof incluido, que el cliente terminó de enviar información.ademas se forwadean todos los mensajes recibidos por la input queue a la queue de exchange utilizando el id del sum worker que lo recibio como routing key para poder manetener todos los mensajes en elmismo chanel y evitar una race condition.

Otra sincronización que vale denotar es que mientras se hacía este trabajo se encontró que se podían causar race conditions donde algún trabajador tarda en conectarse al intercomunicador y se estanca el sistema así que se utilizó una barrera en la que el trabajador sum no empieza a interactuar con el gateway del sistema hasta que todos los sum workers estén conectados al intercomunicador

## Sincronización de múltiples aggregators
Originalmente en el sistema los sum workers hacían un broadcast de sus resultados a los aggregator workers, actualmentes los sum workers generan un hash utilizando el nombre de la  fruta y md5 luego a ese hash en base 10 se lo divide por la cantidad de aggregators y se utiliza el resto de esa división para definir, de esta manera siempre el mismo aggregator recibe los valores de esa fruta específica. No se utilizó la función nativa de hash de python ya que se utiliza una seed específica para cada proceso de python y podría producir problemas al hashear alguna fruta a otro valor.
Para avisar que el cliente terminó los sum workers mandan un broadcast a los aggregators y dichos aggregators no empiezan a procesar los resultados del cliente hasta que reciben el eof de todos los sum workers.
En el caso del worker de join para poder sincronizarlo a múltiples aggregator workers el worker join guarda los tops parciales de los clientes en un diccionario y cuando detecta que ya recibió los resultados de todos los workers en relación a un cliente el join worker procesa los tops parciales para definir el top final y mandarlo de vuelta el gateway para que se los indique al cliente.

## Notas de autor
- En el foro se mencionó que usar algo como sticky router para resolver la sincronía entre sum y aggregation podría resultar en no cumplir con hacer un buen aprovechamiento de los recursos, así que yo opte por usar la fruta en cambio para definir una manera consistente de comunicarse con los aggregators. De esta manera mientras que el cliente tenga cierto grado de variación en las frutas se aprovecharía de una buena manera la mayoría de aggregator workers.
