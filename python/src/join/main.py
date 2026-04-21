import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.clients_top_fruit = {}

        self.clients_top_count = {}

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        client_id, partial_fruit_top = message_protocol.internal.deserialize(message)
        self.clients_top_fruit[client_id] = (
            self.clients_top_fruit.get(client_id, []) + partial_fruit_top
        )
        self.clients_top_count[client_id] = self.clients_top_count.get(client_id, 0) + 1
        try:
            if self.clients_top_count[client_id] == AGGREGATION_AMOUNT:
                all_fruits = self.clients_top_fruit[client_id]
                all_fruits.sort(key=lambda x: x[1], reverse=True)
                final_top = all_fruits[:TOP_SIZE]
                self.output_queue.send(
                    message_protocol.internal.serialize([client_id, final_top])
                )
                del self.clients_top_fruit[client_id]
                del self.clients_top_count[client_id]
            ack()
        except Exception:
            nack()
            raise

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM")
        self.input_queue.stop_consuming()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)
        self.input_queue.close()
        self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
