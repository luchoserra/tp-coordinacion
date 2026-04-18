import pika
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareCloseError,
)


class _MessageMiddlewareRabbitMQ:

    def _make_callback(self, on_message_callback):
        """Wraps the user callback to expose ack/nack functions per message."""

        def callback(_ch, method, _properties, body):
            def ack():
                self.channel.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                self.channel.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body, ack, nack)

        return callback

    def _start_consuming(self, queue_name):
        """Starts consumption from queue_name. Returns when stop_consuming is called."""
        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(
                queue=queue_name, on_message_callback=self._callback
            )
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    def stop_consuming(self):
        """Stops consuming messages after the current message."""
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))

    def close(self):
        """Closes the connection to the broker."""
        try:
            self.connection.close()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareCloseError(str(e))


class MessageMiddlewareQueueRabbitMQ(
    _MessageMiddlewareRabbitMQ, MessageMiddlewareQueue
):

    def __init__(self, host, queue_name):
        """Connects to the broker and declares a queue."""
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.confirm_delivery()
        self.channel.queue_declare(queue=queue_name, durable=True)

    def start_consuming(self, on_message_callback):
        """Starts consuming from the queue. Blocks until stop_consuming is called."""
        self._callback = self._make_callback(on_message_callback)
        self._start_consuming(self.queue_name)

    def send(self, message):
        """Publishes a message to the queue."""
        try:
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                ),
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))


class MessageMiddlewareExchangeRabbitMQ(
    _MessageMiddlewareRabbitMQ, MessageMiddlewareExchange
):

    def __init__(self, host, exchange_name, routing_keys):
        """Connects to the broker and declares a direct exchange."""
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.confirm_delivery()
        self.channel.exchange_declare(
            exchange=exchange_name, exchange_type="direct", durable=True
        )

    def start_consuming(self, on_message_callback):
        """Declares a temporary exclusive queue, binds all routing keys, and starts consuming."""
        self._callback = self._make_callback(on_message_callback)
        try:
            result = self.channel.queue_declare(queue="", exclusive=True)
            queue_name = result.method.queue
            for routing_key in self.routing_keys:
                self.channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=queue_name,
                    routing_key=routing_key,
                )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        self._start_consuming(queue_name)

    def send(self, message):
        """Publishes a message to each routing key."""
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=routing_key,
                    body=message,
                    properties=pika.BasicProperties(
                        delivery_mode=pika.DeliveryMode.Persistent
                    ),
                )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))
