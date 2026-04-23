import os
import logging
import bisect
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
PROCESS_DATA_PARAMETERS = 3
FRUIT_AMOUNT_INDEX = 1
FRUIT_NAME_INDEX  = 0

class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.client_tops = {}
        self.worker_finished_with_client = {}

    def _process_data(self, new_fruit_top, client_id, worker_id):
        if client_id not in self.worker_finished_with_client.keys():
            self.worker_finished_with_client[client_id] = set() 
        if client_id not in self.client_tops.keys():
            self.client_tops[client_id] = {}
        if new_fruit_top is not None:
            logging.info(f"full dict: {self.client_tops}")
            client_dict = self.client_tops[client_id]
            for fruit, amount in new_fruit_top:
                client_dict[fruit] = client_dict.get(fruit, 0) + amount
        self.worker_finished_with_client[client_id].add(worker_id)
        if len(self.worker_finished_with_client[client_id]) == AGGREGATION_AMOUNT:
            fruits = []
            for fruit, value in self.client_tops[client_id].items():
                fruits.append([fruit, value])
            fruits.sort( reverse = True, key=lambda x: x[FRUIT_AMOUNT_INDEX])
            logging.info(f"client fruits: {fruits}")
            fruit_chunk = list(fruits[:TOP_SIZE])
            fruit_top = list(
                map(
                    lambda fruit_item: (fruit_item[FRUIT_NAME_INDEX], fruit_item[FRUIT_AMOUNT_INDEX]),
                    fruit_chunk,
                )
            )
            logging.info(f"sending top: {fruit_top} of client {client_id}:")
            self.output_queue.send(message_protocol.internal.serialize([fruit_top, client_id]))
            del self.client_tops[client_id]
            del self.worker_finished_with_client[client_id]


    def process_messsage(self, message, ack, nack):
        fruit_top = message_protocol.internal.deserialize(message)
        if len(fruit_top) == PROCESS_DATA_PARAMETERS:
            self._process_data(*fruit_top)
        else:
            logging.info(f"message does not comply with required format: {fruit_top}")
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)

    def stop(self):
        self.input_queue.stop_consuming()
    def close(self):
        self.input_queue.close()
        self.output_queue.close()
def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: join_filter.stop(),
    )
    join_filter.start()
    join_filter.close()
    return 0


if __name__ == "__main__":
    main()
