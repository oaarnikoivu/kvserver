import argparse
import socket
import time
import uuid
import xmlrpc.client
from enum import Enum, auto

from loguru import logger


class Operation(Enum):
    GET = auto()
    PUT = auto()
    APPEND = auto()


class Client:
    def __init__(self, server_port: int) -> None:
        self._server = xmlrpc.client.ServerProxy(f"http://localhost:{server_port}")
        self._timeout = 5  # seconds

    def get(self, key: str) -> str:
        return self._send_with_retry(operation=Operation.GET, key=key)

    def put(self, key: str, value: str) -> str:
        return self._send_with_retry(operation=Operation.PUT, key=key, value=value)

    def append(self, key: str, value: str) -> str:
        return self._send_with_retry(operation=Operation.APPEND, key=key, value=value)

    def _send_with_retry(
        self,
        operation: Operation,
        key: str,
        value: str | None = None,
        request_id: str | None = None,
    ) -> str:
        if request_id is None:
            request_id = uuid.uuid4().hex

        socket.setdefaulttimeout(self._timeout)
        while True:
            try:
                match operation:
                    case Operation.GET:
                        result = self._server.get(key)
                        return result
                    case Operation.PUT:
                        result = self._server.put(key, value, request_id)
                        return result
                    case Operation.APPEND:
                        result = self._server.append(key, value, request_id)
                        return result
                    case _:
                        raise NotImplementedError
            except Exception:
                logger.info("Retrying request")
                time.sleep(3)
                self._send_with_retry(
                    operation=operation, key=key, value=value, request_id=request_id
                )
            finally:
                socket.setdefaulttimeout(None)


def run_client() -> None:
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--sport", type=int, required=True, help="Server port.")
    args = parser.parse_args()

    client = Client(server_port=args.sport)
    for key in ["test", "test1", "test2"]:
        client.put(key=key, value=f"{key}-123")

    for key in ["test", "test1", "test2"]:
        print(client.get(key=key))

    for key in ["test", "test1"]:
        client.append(key=key, value="456")

    for key in ["test", "test1", "test2"]:
        print(client.get(key=key))


if __name__ == "__main__":
    run_client()
