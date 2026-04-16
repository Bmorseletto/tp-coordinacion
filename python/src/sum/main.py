import os
import logging
import multiprocessing
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
WORKING = True
DONE = False

class SumFilter:
    def __init__(self):
        manager = multiprocessing.Manager()
        self.data_output_exchanges = []
        self.amount_by_fruit =manager.dict()
        self.client_status =manager.dict()
        self._lock = manager.Lock()

    def _process_data(self, fruit, amount, client_id):
        with self._lock:
            if client_id not in self.amount_by_fruit.keys():
                logging.info(f"recived first message from client: {client_id}")
                self.amount_by_fruit[client_id] ={}
                self.client_status[client_id] = WORKING
            logging.info(f"recived message from client: {client_id}")
            client_dict = self.amount_by_fruit[client_id]
            client_dict[fruit] = client_dict.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))
            self.amount_by_fruit[client_id] = client_dict

    def _process_eof(self, client_id):
        logging.info(f"Broadcasting to other jobs")
        self.sum_intercomm.send(message_protocol.internal.serialize([client_id]))

    def send_to_data_outptut(self, client_id):
        with self._lock:   
            logging.info(f"Broadcasting data messages")
            logging.info(f"amount by fruit: {self.amount_by_fruit}")
            if client_id in self.amount_by_fruit.keys():
                for final_fruit_item in self.amount_by_fruit[client_id].values():
                    for data_output_exchange in self.data_output_exchanges:
                        data_output_exchange.send(
                            message_protocol.internal.serialize(
                                [final_fruit_item.fruit, final_fruit_item.amount, client_id, ID]
                            )
                        )
            logging.info(f"Broadcasting EOF message")
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(message_protocol.internal.serialize([client_id, ID]))

    def process_intercomm_message(self, message, ack, nack):
        client_id = message_protocol.internal.deserialize(message)[0]
        self.send_to_data_outptut(client_id) 
        ack()

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def start_inter_comm(self):
        logging.basicConfig(level=logging.INFO)
        sum_intercomm=middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE])
        def handle_sigterm(sum_intercomm, data_output_exchanges):
            sum_intercomm.stop_consuming()
            for exchange in data_output_exchange:
                exchange.close()
        signal.signal(
            signal.SIGTERM,
            lambda signum, frame:handle_sigterm(sum_intercomm, self.data_output_exchanges),
        )
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
        self.data_output_exchanges.append(data_output_exchange)
        try:
            sum_intercomm.start_consuming(self.process_intercomm_message)
        finally:
            sum_intercomm.close()

    def start_input_manager(self):
        logging.basicConfig(level=logging.INFO)
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.sum_intercomm=middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE])
        def handle_sigterm(input_queue):
            input_queue.stop_consuming()
        signal.signal(
            signal.SIGTERM,
            lambda signum, frame:handle_sigterm(self.input_queue),
        )
        try:
            self.input_queue.start_consuming(self.process_data_messsage)
        finally:
            self.input_queue.close()

    def start(self):
        process_input_queue = multiprocessing.Process(target=self.start_input_manager)
        process_sum_intercomm = multiprocessing.Process(target=self.start_inter_comm)
        process_input_queue.start()
        process_sum_intercomm.start()
        process_input_queue.join()
        process_sum_intercomm.join()
    

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
