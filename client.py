
from communication.communicator import gRPCCommunicationManager
from communication.message import Message
import argparse
import torch
from utils.logger import Logger
from utils.serialization import SerializationTool
from model.mlp import MLP
from transformers import BertModel


class Client:
    def __init__(
            self,
            ip,
            port,
            client_id,
            server_ip,
            server_port,
            model,
            gRPC_config=None
    ):
        self.ip = ip
        self.port = port
        self.client_id = client_id
        self.server_ip = server_ip
        self.server_port = server_port
        self.model = model
        self.gRPC_config = gRPC_config
        self.logger = Logger(
            log_name=f"client{client_id}",
            log_file=f"/Applications/LANGUAGE/PYTHON/fl_communication/log/client{client_id}.log"
        )
        self.comm_manager = gRPCCommunicationManager(
            ip=ip,
            port=port,
            max_connection_num=1,
            gRPC_config=gRPC_config
        )
        self.comm_manager.add_communicators("0", f"{server_ip}:{server_port}")

    def join_in(self):
        self.comm_manager.send(
            Message(
                message_type=100,
                sender=self.client_id,
                receiver="0",
                content={
                    "ip": self.ip,
                    "port": self.port
                }
            ),
            receiver="0"
        )

    def local_process(self):
        while True:
            msg = self.comm_manager.receive()
            if msg.message_type == 101:
                self.logger.info("Terminating the client.")
                break
            elif msg.message_type == 201:
                SerializationTool.deserialize_model(self.model, msg.content['model'])
                self.logger.info("Model received.")
            self.comm_manager.send(
                Message(
                    message_type=201,
                    sender=self.client_id,
                    receiver="0",
                    content={
                        'model': SerializationTool.serialize_model(self.model)
                    }
                ),
                receiver="0"
            )
            self.logger.info("Model sent.")


parser = argparse.ArgumentParser()
parser.add_argument('--ip', type=str, default='127.0.0.1')
parser.add_argument('--port', type=str, default="50052")
parser.add_argument('--client_id', type=str, default="1")
if __name__ == "__main__":
    # model = MLP(784, 10, 10)
    model = BertModel.from_pretrained("./model/")
    args = parser.parse_args()
    client = Client(
        ip=args.ip,
        port=args.port,
        client_id=args.client_id,
        server_ip="127.0.0.1",
        server_port="50051",
        model=model,
        gRPC_config={
            "grpc_max_send_message_length": 1000 * 1024 * 1024,
            "grpc_max_receive_message_length": 1000 * 1024 * 1024,
            "grpc_enable_http_proxy": False,
            "grpc_compression": "gzip"
        }
    )
    client.join_in()
    client.local_process()
    client.comm_manager.terminate_server()
