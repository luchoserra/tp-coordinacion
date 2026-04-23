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

PREFLUSH = "PREFLUSH"
COUNT = "COUNT"


class SumFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.control_input = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_CONTROL_EXCHANGE}_{ID}"]
        )
        self.control_output = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            [f"{SUM_CONTROL_EXCHANGE}_{i}" for i in range(SUM_AMOUNT)],
        )
        self.aggregator_outputs = [
            middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            for i in range(AGGREGATION_AMOUNT)
        ]

        self.partial_sums = {}
        self.record_count = {}
        self.expected_count = {}
        self.reported = {}

        self.lock = threading.Lock()

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        """Stops all consumers gracefully on SIGTERM."""
        logging.info("SIGTERM received")
        self.input_queue.stop_consuming()
        self.control_input.stop_consuming()

    def _aggregator_for(self, fruit):
        """Returns the aggregator index responsible for this fruit name."""
        return int(hashlib.md5(fruit.encode()).hexdigest(), 16) % AGGREGATION_AMOUNT

    def _broadcast(self, payload):
        """Sends a control message to all sum instances via the control exchange."""
        self.control_output.send(message_protocol.internal.serialize(payload))

    def _record_fruit(self, client_id, fruit, amount):
        """Accumulates amount into the running total for this client and fruit."""
        client_fruits = self.partial_sums.setdefault(client_id, {})
        client_fruits[fruit] = client_fruits.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))
        self.record_count[client_id] = self.record_count.get(client_id, 0) + 1

    def _flush(self, client_id):
        """Sends accumulated totals to the aggregators and clears all state for the client."""
        fruits = self.partial_sums.pop(client_id, {})
        self.record_count.pop(client_id, None)
        self.expected_count.pop(client_id, None)
        self.reported.pop(client_id, None)

        logging.info(f"Flushing results for client {client_id}")
        for fi in fruits.values():
            self.aggregator_outputs[self._aggregator_for(fi.fruit)].send(
                message_protocol.internal.serialize([client_id, fi.fruit, fi.amount])
            )
        for exchange in self.aggregator_outputs:
            exchange.send(message_protocol.internal.serialize([client_id]))

    def _try_flush(self, client_id):
        """Flushes the client if all instances have reported and counts are consistent."""
        if client_id not in self.expected_count:
            return
        reported = self.reported.get(client_id, {})
        if len(reported) < SUM_AMOUNT:
            return
        if sum(reported.values()) != self.expected_count[client_id]:
            return
        self._flush(client_id)

    def process_data_message(self, message, ack, nack):
        """Handles an incoming data message: records a fruit item or initiates a pre-flush."""
        try:
            fields = message_protocol.internal.deserialize(message)
            with self.lock:
                if len(fields) == 3:
                    client_id, fruit, amount = fields
                    self._record_fruit(client_id, fruit, amount)
                    if client_id in self.expected_count:
                        self._broadcast(
                            [COUNT, client_id, ID, self.record_count[client_id]]
                        )
                else:
                    client_id, total = fields
                    self._broadcast([PREFLUSH, client_id, total])
            ack()
        except Exception:
            nack()
            raise

    def process_control_message(self, message, ack, nack):
        """Handles a control message: coordinates count reporting and triggers flush when ready."""
        try:
            fields = message_protocol.internal.deserialize(message)
            tag = fields[0]
            with self.lock:
                if tag == PREFLUSH:
                    _, client_id, total = fields
                    self.expected_count[client_id] = total
                    self._broadcast(
                        [COUNT, client_id, ID, self.record_count.get(client_id, 0)]
                    )
                elif tag == COUNT:
                    _, client_id, sender_id, count = fields
                    reported = self.reported.setdefault(client_id, {})
                    reported[sender_id] = max(reported.get(sender_id, 0), count)
                    self._try_flush(client_id)
            ack()
        except Exception:
            nack()
            raise

    def start(self):
        """Starts data and control consumers in parallel, then closes all connections."""
        data_thread = threading.Thread(
            target=self.input_queue.start_consuming,
            args=(self.process_data_message,),
        )
        data_thread.start()
        self.control_input.start_consuming(self.process_control_message)
        data_thread.join()

        self.input_queue.close()
        self.control_input.close()
        self.control_output.close()
        for exchange in self.aggregator_outputs:
            exchange.close()


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
