
from communication.communicator import gRPCCommunicationManager
from communication.message import Message
import argparse
from utils.serialization import SerializationTool
from utils.logger import Logger
from model.mlp import MLP
from transformers import BertModel


class Server:
    def __init__(
            self,
            ip,
            port,
            client_num,
            model,
            gRPC_config=None
    ):
        self.ip = ip
        self.port = port
        self.client_num = client_num
        self.gRPC_config = gRPC_config
        self.logger = Logger(
            log_name="server",
            log_file="/Applications/LANGUAGE/PYTHON/fl_communication/log/server.log"
        )
        self.model = model
        self.comm_manager = gRPCCommunicationManager(
            ip=ip,
            port=port,
            max_connection_num=client_num,
            gRPC_config=gRPC_config
        )

    def join_in(self):
        current_client_num = 0
        while len(self.comm_manager.communicators) < self.client_num:
            message = self.comm_manager.receive()
            print(message.content["port"])
            if message.message_type == 100:
                current_client_num += 1
                sender, ip, port = message.sender, message.content['ip'], message.content['port']
                self.logger.info(f"Client {sender} ({ip}:{port}) joined in.")
                self.comm_manager.add_communicators(
                    message.sender, f"{message.content['ip']}:{message.content['port']}"
                )
        # self.comm_manager.terminate_server()

    def local_process(self):
        r = 0
        while r < 2:
            self.logger.info("Start a new round.")
            model_parameters = SerializationTool.serialize_model(self.model)
            print(len(model_parameters))
            self.comm_manager.send(
                Message(
                    message_type=201,
                    sender="0",
                    content={
                        'model': model_parameters
                    }
                )
            )
            self.logger.info("Model sent to all clients.")
            num = 0
            while num < self.client_num:
                msg = self.comm_manager.receive()
                if msg.message_type == 201:
                    num += 1
                    self.logger.info(f"Round {r}: Client {msg.sender} updated the model.")
            r += 1
        self.comm_manager.send(
            Message(
                message_type=101,
                sender="0",
                content=""
            )
        )
        self.comm_manager.terminate_server()


parser = argparse.ArgumentParser()
parser.add_argument('--ip', type=str, default='127.0.0.1')
parser.add_argument('--port', type=str, default='50051')
parser.add_argument('--client_num', type=int, default=2)
if __name__ == '__main__':
    # model = MLP(784, 10, 10)
    model = BertModel.from_pretrained("./model/")
    args = parser.parse_args()
    server = Server(
        ip=args.ip,
        port=args.port,
        client_num=args.client_num,
        model=model,
        gRPC_config={
            "grpc_max_send_message_length": 1000 * 1024 * 1024,
            "grpc_max_receive_message_length": 1000 * 1024 * 1024,
            "grpc_enable_http_proxy": False,
            "grpc_compression": "gzip"
        }
    )
    server.join_in()
    server.local_process()
