import os
import logging
import multiprocessing
import signal
import hashlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
PROCESS_DATA_PARAMETERS = 3
PROCESS_EOF_PARAMETERS = 1
SUM_INTERCOMM_PARAMETERS = [MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE, str(ID)]]

class SumFilter:
    def __init__(self):
        manager = multiprocessing.Manager()
        self.data_output_exchanges = []
        self.amount_by_fruit =manager.dict()
        self._lock = manager.Lock()
        self._connected_sums = manager.list()
        self._barrier_condition=multiprocessing.Condition()

    def _process_data(self, fruit, amount, client_id):
        self.sum_intercomm.send_by_key(message_protocol.internal.serialize([fruit, amount, client_id]), str(ID))
        return


    def _process_eof(self, client_id):
        logging.info(f"Broadcasting to other jobs the eof of {client_id}")
        self.sum_intercomm.send_by_key(message_protocol.internal.serialize([client_id]), SUM_CONTROL_EXCHANGE)

    def send_to_data_outptut(self, client_id):
        with self._lock:   
            logging.info(f"Broadcasting data messages")
            if client_id in self.amount_by_fruit.keys():
                for final_fruit_item in sorted(self.amount_by_fruit[client_id].values()):
                    aggregator =int(hashlib.md5(final_fruit_item.fruit.encode()).hexdigest(), 16) % AGGREGATION_AMOUNT
                    self.data_output_exchange.send_by_key(
                        message_protocol.internal.serialize(
                            [final_fruit_item.fruit, final_fruit_item.amount, client_id, ID]
                        ),
                        str(aggregator)
                    )
            logging.info(f"Broadcasting EOF message of client {client_id}")
            self.data_output_exchange.send_by_key(message_protocol.internal.serialize([client_id, ID]), AGGREGATION_PREFIX)

    def process_fruit_data(self,fruit, amount, client_id):
        with self._lock:
            if client_id not in self.amount_by_fruit.keys():
                logging.info(f"recived first message from client: {client_id}")
                self.amount_by_fruit[client_id] ={}
            client_dict = self.amount_by_fruit[client_id]
            client_dict[fruit] = client_dict.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))
            self.amount_by_fruit[client_id] = client_dict

    def process_intercomm_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == PROCESS_DATA_PARAMETERS:
            self.process_fruit_data(*fields)
        elif len(fields) == PROCESS_EOF_PARAMETERS:
            id = fields[0]
            if isinstance(id, int):
                if id != ID and id not in self._connected_sums and len(self._connected_sums) <= SUM_AMOUNT:
                    self._connected_sums.append(id)
                    self.sum_intercomm.send(message_protocol.internal.serialize([ID]))
                if len(self._connected_sums) >= SUM_AMOUNT:
                    logging.info("all workers connected to intercomm")
                    with self._barrier_condition:
                        self._barrier_condition.notify_all()
            else:
                self.send_to_data_outptut(id) 
        ack()

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == PROCESS_DATA_PARAMETERS:
            self._process_data(*fields)
        elif len(fields) == PROCESS_EOF_PARAMETERS:
            self._process_eof(*fields)
        else:
            logging.info(f"message does not comply with required format: {fields}")
        ack()

    def start_inter_comm(self):
        logging.basicConfig(level=logging.INFO)
        self.sum_intercomm=middleware.MessageMiddlewareExchangeRabbitMQ(*SUM_INTERCOMM_PARAMETERS)
        def handle_sigterm(sum_intercomm):
            sum_intercomm.stop_consuming()
        signal.signal(
            signal.SIGTERM,
            lambda signum, frame:handle_sigterm(self.sum_intercomm),
        )
        routing_keys = [AGGREGATION_PREFIX] + [str(i) for i in range(AGGREGATION_AMOUNT)]
        self.data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, routing_keys
        )
        try:
            self._connected_sums.append(ID)
            self.sum_intercomm.send_by_key(message_protocol.internal.serialize([ID]), SUM_CONTROL_EXCHANGE)
            self.sum_intercomm.start_consuming(self.process_intercomm_message)
        finally:
            self.sum_intercomm.close()
            self.data_output_exchange.close()

    def start_input_manager(self):
        with self._barrier_condition:
            self._barrier_condition.wait_for(lambda: len(self._connected_sums) >= SUM_AMOUNT)
        logging.basicConfig(level=logging.INFO)
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.sum_intercomm=middleware.MessageMiddlewareExchangeRabbitMQ(*SUM_INTERCOMM_PARAMETERS)
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
        def handle_sigterm():
            process_input_queue.terminate()
            process_sum_intercomm.terminate()
        signal.signal(
            signal.SIGTERM,
            lambda signum, frame:handle_sigterm(),
        )
        process_sum_intercomm.start()
        process_input_queue.start()
        process_sum_intercomm.join()
        process_input_queue.join()
    

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
