# Informe de entrega

En este trabajo se implementaron mecanismos para permitiría la coordinacion y sincronizacion de distintos trabajadores del sistema distribuido

## División de flujo de datos de los clientes
Para lograr un buen flujo de datos en el sistema para que los resultados de cada cliente no se mezclen se les asignó un uuid a cada cliente cuando se crea la instancia message handler encargada de manejar los mensajes de ese cliente

en los workers se utilizan diccionarios con el uuid como key para separa los datos relacionados a los clientes que se van recibiendo durante la ejecución

## Sincronización de múltiples Workers Sum
Dentro del sistema una vez que el cliente termina de mandar su información se manda un mensaje representativo de eof, dentro de este sistema por el uso de queues de Rabbitmq solo 1 worker sum recibe este mensaje, para resolver este problema se creó un exchange de rabbitmq, sum_intercomm, donde el primer worker que recibe el mensaje de eof lo reenvía por el exchange para avisarle a los workers,el worker que recibió el eof incluido, que el cliente termine de enviar información.
Otra sincronización que vale denotar es que mientras se hacía este trabajo se encontró que se podían causar race conditions donde algún trabajador tarda en conectarse al intercomunicador y se estanca el sistema así que se utilizó una barrera en la que el trabajador sum no empieza a interactuar con el gateway del sistema hasta que todos los sum workers estén conectados al intercomunicador

## Sincronización de múltiples aggregators
Originalmente en el sistema los sum workers hacían un broadcast de sus resultados a los aggregator workers, actualmentes los sum workers iteran los valores de el diccionario que contiene las frutas del cliente y mandan a un cliente una de esas frutas de manera serial, con esto quiero decir que por ejemplo si en en sum worker la primera fruta en los values de su diccionario del cliente x es: (mango, 3) entonces el worker le manda esa información a el aggregator 0 y si la segunda fruta es por ejemplo: (kiwi, 7) le manda esa información al aggregator 1 y así sucesivamente. En el caso de que haya más tipos de frutas que aggregators simplemente se empieza devuelta por el primer aggregator. Un problema con este sistema sería en el caso de que haya más aggregators que tipos de fruta ya que en ese caso si, por ejemplo, hay 3 tipos de fruta y 4 aggregators, el cuarto aggregator nunca recibiría trabajo.
Para avisar que el cliente terminó los sum workers mandan un broadcast a los aggregators y dichos aggregators no empiezan a procesar los resultados del cliente hasta que reciben el eof de todos los sum workers.

En el caso del worker de join para poder sincronizarlo a múltiples aggregator workers el worker join guarda los tops parciales de los clientes en un diccionario y cuando detecta que ya recibió los resultados de todos los workers en relación a un cliente el join worker procesa los tops parciales para definir el top y mandarlo de vuelta el gateway para que se los indique al cliente.


