from common import message_protocol
import uuid


class MessageHandler:

    def __init__(self):
        """Assigns a uuid to each client and initialises the record counter."""
        self.client_id = uuid.uuid4().hex
        self.record_count = 0

    def serialize_data_message(self, message):
        """Serialises a (fruit, amount) pair into an internal data message tagged with the client ID."""
        [fruit, amount] = message
        self.record_count += 1
        return message_protocol.internal.serialize([self.client_id, fruit, amount])

    def serialize_eof_message(self, message):
        """Serialises an EOF message carrying the total record count for this client."""
        return message_protocol.internal.serialize([self.client_id, self.record_count])

    def deserialize_result_message(self, message):
        """Deserialises a result message and returns the fruit top only if it belongs to this client."""
        client_id, fruit_top = message_protocol.internal.deserialize(message)
        return fruit_top if client_id == self.client_id else None
