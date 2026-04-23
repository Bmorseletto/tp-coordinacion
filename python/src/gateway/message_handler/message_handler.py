from common import message_protocol
import uuid

class MessageHandler:

    def __init__(self):
        self._curret_uuid = str(uuid.uuid4())
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([fruit, amount, self._curret_uuid])

    def serialize_eof_message(self, message):
        return_message = message_protocol.internal.serialize([self._curret_uuid])
        return return_message

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        client_id=fields[1]
        if client_id != self._curret_uuid:
            return None
        return fields[0]
