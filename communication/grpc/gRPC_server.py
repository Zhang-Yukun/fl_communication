
from collections import deque
import gRPC_communicator_pb2
import gRPC_communicator_pb2_grpc


class gRPCComServeFunc(gRPC_communicator_pb2_grpc.gRPCComServeFuncServicer):
    def __init__(self):
        self.message_queue = deque()

    def sendMessage(self, request, context):
        self.message_queue.append(request)

        return gRPC_communicator_pb2.MessageResponse(msg='ACK')

    def receive(self):
        while len(self.message_queue) == 0:
            continue
        message = self.message_queue.popleft()
        return message
