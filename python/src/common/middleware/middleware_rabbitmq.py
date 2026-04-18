import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange,MessageMiddlewareDisconnectedError, MessageMiddlewareCloseError,MessageMiddlewareMessageError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    QUEUE_ARGUMENTS = {'x-queue-type': 'quorum'}
    def __init__(self, host, queue_name):
        self._conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        self._channel =  self._conn.channel()
        self._channel.queue_declare(queue=queue_name, durable=True)
        self._queue_name = queue_name
        self._delivery_tag = None
        self._consumer_tag = None
    def send(self,message):
        try:
            self._channel.basic_publish(exchange='',
                        routing_key=self._queue_name,
                        body=message)
        except pika.exceptions.AMQPConnectionError as e:
            self.close()
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            self.close()
            raise MessageMiddlewareMessageError(e)
    def close(self):
        try:
            _close(self)
        except Exception as e:
            raise MessageMiddlewareCloseError(e)
    def start_consuming(self, on_message_callback):
        try:
           _start_consuming(self, on_message_callback=on_message_callback)
        except pika.exceptions.AMQPConnectionError as e:
            self.close()
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            self.close()
            raise MessageMiddlewareMessageError(e)  
    def stop_consuming(self):
        try:
            self._channel.stop_consuming(self._consumer_tag)
        except pika.exceptions.AMQPConnectionError as e:
            self.close()
            raise MessageMiddlewareDisconnectedError(e)
    def ack(self):
        self._channel.basic_ack(delivery_tag=self._delivery_tag)
    def set_delivery_tag(self, delivery_tag):
        self._delivery_tag = delivery_tag
    def set_consumer_tag(self, consumer_tag):
        self._consumer_tag = consumer_tag

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    EXCHANGE_TYPE = "topic"
    def __init__(self, host, exchange_name, routing_keys, exchange_type = "topic"):
        self._conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        self._channel =  self._conn.channel()
        self._exchange_name = exchange_name
        self._channel.exchange_declare(exchange= self._exchange_name,exchange_type=exchange_type)
        result = self._channel.queue_declare(queue="")
        self._queue_name = result.method.queue
        for key in routing_keys:
            self._channel.queue_bind(exchange=self._exchange_name,queue=self._queue_name,  routing_key=key)  
        self._routing_keys = routing_keys
        self._delivery_tag = None
        self._consumer_tag = None

    def send(self,message):
        try:
            keys = ".".join(self._routing_keys)
            self._channel.basic_publish(exchange=self._exchange_name,
                        routing_key=keys,
                        body=message)
        except pika.exceptions.AMQPConnectionError as e:
            self.close()
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            self.close()
            raise MessageMiddlewareMessageError(e)
    def send_by_key(self,message, key):
        if key not in self._routing_keys:
            raise KeyError(f"{key} not in routing keys")
        try:
            self._channel.basic_publish(exchange=self._exchange_name,
                        routing_key=key,
                        body=message)
        except pika.exceptions.AMQPConnectionError as e:
            self.close()
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            self.close()
            raise MessageMiddlewareMessageError(e)
    def close(self):
        try:
            _close(self)
        except Exception as e:
            raise MessageMiddlewareCloseError(e)
    def start_consuming(self, on_message_callback):
        try:
           _start_consuming(self, on_message_callback=on_message_callback)
        except pika.exceptions.AMQPConnectionError as e:
            self.close()
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            self.close()
            raise MessageMiddlewareMessageError(e)
    def stop_consuming(self):
        try:
            self._channel.stop_consuming(self._consumer_tag)
        except pika.exceptions.AMQPConnectionError as e:
            self.close()
            raise MessageMiddlewareDisconnectedError(e)
    def ack(self):
        self._channel.basic_ack(delivery_tag=self._delivery_tag)
    def set_delivery_tag(self, delivery_tag):
        self._delivery_tag = delivery_tag
    def set_consumer_tag(self, consumer_tag):
        self._consumer_tag = consumer_tag
    def bind(self, routing_keys=[]):
        for key in routing_keys:
            self._channel.queue_bind(exchange=self._exchange_name,queue=self._queue_name,  routing_key=key) 
            self._routing_keys.append(key)

### Auxiliary function ###
def _start_consuming(message_middleware, on_message_callback):
    def callback(ch, method, properties, body):
        def ack():
            ch.basic_ack(delivery_tag=method.delivery_tag)
        message_middleware.set_delivery_tag(method.delivery_tag) 
        on_message_callback(body,ack, ch.basic_nack)
    consumer_tag = message_middleware._channel.basic_consume(queue=message_middleware._queue_name,on_message_callback= callback)
    message_middleware.set_consumer_tag(consumer_tag)
    message_middleware._channel.start_consuming()

def _close(message_middleware):
    if message_middleware._channel.is_open:
        if message_middleware._consumer_tag != None:
            message_middleware.stop_consuming()
        #message_middleware._channel.queue_delete(message_middleware._queue_name)
        message_middleware._channel.close()
    if message_middleware._conn.is_open:
        message_middleware._conn.close()