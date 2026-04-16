import os
import logging
import bisect

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])
DONE = True
WORKING = False

class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.client_fruit_top = {}
        self.client_worker_relation = {}

    def _process_data(self, fruit, amount, client_id, worker_id):
        if client_id not in  self.client_fruit_top.keys():
            self.client_fruit_top[client_id] = []
        if client_id not in  self.client_worker_relation.keys():
            self.client_worker_relation[client_id] = set()
        fruit_top = self.client_fruit_top[client_id]
        for i in range(len(fruit_top)):
            if fruit_top[i].fruit == fruit:
                fruit_top[i] = fruit_top[i] + fruit_item.FruitItem(
                    fruit, amount
                )
                return
        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id, worker_id):
        if client_id not in  self.client_worker_relation.keys():
            self.client_worker_relation[client_id] = set()
        self.client_worker_relation[client_id].add(worker_id)
        logging.info(f"Worker {worker_id}, finished with {client_id}")
        if len(self.client_worker_relation[client_id]) < SUM_AMOUNT:
            return
        logging.info(f"Received all EOF for {client_id},{self.client_worker_relation[client_id]}")
        self.client_fruit_top[client_id].sort()
        fruit_chunk = list(self.client_fruit_top[client_id][-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        logging.info(f"fruits of client {client_id}, {worker_id} {list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                self.client_fruit_top[client_id],
            )
        )}")
        self.output_queue.send(message_protocol.internal.serialize(fruit_top))
        del self.client_fruit_top[client_id]

    def process_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 4:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
