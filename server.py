from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer

from collections import namedtuple
from io import BytesIO
from socket import error as socket_error

Error = namedtuple("Error", ("message",))


class CommandError(Exception):
    pass


class DisconnectError(Exception):
    pass


class ProtocolHandler:
    def handle_request(self, socket_file):
        # Parse an incoming request
        pass

    def write_response(self, socket_file):
        # Serialize and send response
        pass


class Server:
    def __init__(self, host="127.0.0.1", port=6379, max_clients=64):
        self._pool = Pool(max_clients)
        self._server = StreamServer(
            (host, port), self.connection_handler, spawn=self._pool
        )
        self._protocol = ProtocolHandler()
        self._kv = {}

    def connection_handler(self, connection, address):
        # Convert `connection`, a socket object into a file-like
        socket_file = connection.makefile("rwb")

        # Process requests until client disconnects

        while True:
            try:
                data = self._protocol.handle_request(socket_file)
            except DisconnectError:
                break

    def get_response(self, data):
        # Unpack data sent by the client, execute the command and return response
        pass

    def run(self):
        self._server.serve_forever()
