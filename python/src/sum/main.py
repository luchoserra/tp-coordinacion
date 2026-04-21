import os
import logging
import signal
import hashlib
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


class SumFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.eof_broadcast = None
        self.eof_input = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_CONTROL_EXCHANGE}_{ID}"]
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            self.data_output_exchanges.append(
                middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
                )
            )

        self.fruit_amounts_by_client = {}
        self.lock = threading.Lock()

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info("SIGTERM received")
        self.input_queue.stop_consuming()
        self.eof_input.stop_consuming()

    def _get_aggregator_index(self, fruit):
        return int(hashlib.md5(fruit.encode()).hexdigest(), 16) % AGGREGATION_AMOUNT

    def _process_data(self, client_id, fruit, amount):
        with self.lock:
            client_fruits = self.fruit_amounts_by_client.setdefault(client_id, {})
            client_fruits[fruit] = client_fruits.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

    def _flush_client(self, client_id):
        with self.lock:
            fruit_amounts = self.fruit_amounts_by_client.pop(client_id, {})

        logging.info(f"Flushing results for client {client_id}")
        for fi in fruit_amounts.values():
            idx = self._get_aggregator_index(fi.fruit)
            self.data_output_exchanges[idx].send(
                message_protocol.internal.serialize([client_id, fi.fruit, fi.amount])
            )
        for exchange in self.data_output_exchanges:
            exchange.send(message_protocol.internal.serialize([client_id]))

    def process_data_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == 3:
                self._process_data(*fields)
            else:
                client_id = fields[0]
                self.eof_broadcast.send(
                    message_protocol.internal.serialize([client_id])
                )
            ack()
        except Exception:
            nack()
            raise

    def process_eof_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            client_id = fields[0]
            self._flush_client(client_id)
            ack()
        except Exception:
            nack()
            raise

    def _data_thread(self):
        self.eof_broadcast = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            [f"{SUM_CONTROL_EXCHANGE}_{i}" for i in range(SUM_AMOUNT)],
        )
        self.input_queue.start_consuming(self.process_data_message)
        self.input_queue.close()
        self.eof_broadcast.close()

    def start(self):
        t = threading.Thread(target=self._data_thread)
        t.start()
        self.eof_input.start_consuming(self.process_eof_message)
        self.eof_input.close()
        t.join()
        for exchange in self.data_output_exchanges:
            exchange.close()


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
