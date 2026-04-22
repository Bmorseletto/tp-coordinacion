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
        self.worker_finished_with_client[client_id].add(worker_id)
        if new_fruit_top != None:
            if client_id not in self.client_tops.keys():
                self.client_tops[client_id] = new_fruit_top
            else:
                current_fruit_top = self.client_tops[client_id]
                logging.info(f"current fruit top: {current_fruit_top}")
                for fruit_item in new_fruit_top:
                    fruit_found = False
                    for i in range(len(current_fruit_top)):
                        if current_fruit_top[i][0] == fruit_item[0]:
                            current_fruit_top[i][1] += fruit_item[1]
                            fruit_found = True
                            break
                    if not fruit_found:
                        bisect.insort(current_fruit_top, fruit_item)
        if len(self.worker_finished_with_client[client_id]) == AGGREGATION_AMOUNT:
            self.client_tops[client_id].sort( reverse = True, key=lambda x: x[1])
            logging.info(f"client_tops[client_id]: {self.client_tops[client_id]}")
            fruit_chunk = list(self.client_tops[client_id][TOP_SIZE:])
            fruit_top = list(
                map(
                    lambda fruit_item: (fruit_item[0], fruit_item[1]),
                    fruit_chunk,
                )
            )
            logging.info(f"sending top: {fruit_top} of client {client_id}:")
            self.output_queue.send(message_protocol.internal.serialize(fruit_top))


    def process_messsage(self, message, ack, nack):
        logging.info(f"Received top {message}")
        fruit_top = message_protocol.internal.deserialize(message)
        self._process_data(*fruit_top)
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
