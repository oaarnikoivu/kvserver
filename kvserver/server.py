import argparse
import hashlib
import threading
from collections import defaultdict
from xmlrpc.server import SimpleXMLRPCServer

from loguru import logger


class Server:
    def __init__(self):
        self._db = defaultdict(str)
        self._cache = defaultdict(str)
        self._lock = threading.Lock()

    def get(self, key: str) -> str:
        with self._lock:
            return self._db.get(key, "")

    def put(self, key: str, value: str, request_id: str) -> str:
        cache_key = self._make_cache_key(request_id, key, value)

        with self._lock:
            if self._is_duplicate_request(cache_key=cache_key):
                logger.info("Duplicate PUT request, returning result from cache")
                return self._cache[cache_key]

            current = self._db.get(key)
            if current != value:
                self._db[key] = value
                result = value
            else:
                result = current

            self._cache[cache_key] = result

        return result

    def append(self, key: str, value: str, request_id: str):
        cache_key = self._make_cache_key(request_id, key, value)

        with self._lock:
            if self._is_duplicate_request(cache_key=cache_key):
                logger.info("Duplicate APPEND request, returning result from cache")
                return self._cache[cache_key]

            current = self._db.get(key)
            if current is None:
                self._db[key] = value
                result = value
            else:
                self._db[key] = current + value
                result = current + value

            self._cache[cache_key] = result

        return result

    def _is_duplicate_request(self, cache_key: str) -> bool:
        return cache_key in self._cache

    @staticmethod
    def _make_cache_key(*args) -> str:
        return hashlib.sha256("_".join(args).encode()).hexdigest()


def run_server() -> None:
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--port", type=int, required=True, help="Server port.")
    args = parser.parse_args()

    port = args.port

    server = SimpleXMLRPCServer(("localhost", port), allow_none=True)
    rpc_server = Server()
    server.register_instance(rpc_server)

    logger.info(f"Server listening on port {port}...")
    server.serve_forever()


if __name__ == "__main__":
    run_server()
