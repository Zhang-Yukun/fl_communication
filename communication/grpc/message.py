import json
import pickle
import base64
import numpy as np
import gRPC_communicator_pb2
from datetime import datetime
from pympler import asizeof


def b64serializer(x):
    return base64.b64encode(pickle.dumps(x))


class Message:
    """
    The data exchanged during an FL course are abstracted as 'Message'.
    A message object includes:
        message_type: The type of message, which is used to trigger the
        corresponding handlers of server/client
        sender: The sender's ID
        receiver: The receiver's ID
        communication_round: The training round of the message, which is determined by
        the sender and used to filter out the outdated messages.
    """
    def __init__(
            self,
            message_type: int,
            sender: str,
            receiver: str,
            content: str,
            communication_round: int = 0
    ):
        self._message_type = message_type
        self._sender = sender
        self._receiver = receiver
        self._content = content
        self._communication_round = communication_round
        self._timestamp = datetime.now().timestamp()
        self.param_serializer = b64serializer

    @property
    def message_type(self):
        return self._message_type

    @message_type.setter
    def message_type(self, value):
        self._message_type = value

