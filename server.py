from base import Error, CommandError, DisconnectError
from base import ProtocolHandler, Server


class RedisProtocol(ProtocolHandler):
    def __init__(self):
        self.handlers = {
            "+": self("_str"),
            "$": self("_bulk_str"),
            "-": self("_error"),
            ":": self("_int"),
            "*": self("_array"),
            "%": self("_dict"),
        }

    def handle_request(self, socket_file):
        first_byte = socket_file.read(1)
        if not first_byte:
            raise DisconnectError()
        try:
            return self.handlers[first_byte](socket_file)
        except KeyError:
            raise CommandError("Bad command")

    def _str(self, socket_file):
        return socket_file.readline().rstrip("\r\n")

    def _int(self, socket_file):
        return socket_file.readline().rstrip("\r\n")

    def _error(self, socket_file):
        return Error(socket_file.readline().rstrip("\r\n"))

    def _bulk_str(self, socket_file):
        length = int(socket_file.readline().rstrip("\r\n"))
        if length == -1:
            return None  # Special case for Nulls
        length += 2  # Include trailing '\r\n'. Is this necessary??
        return socket_file.read(length)[:-2]

    def _array(self, socket_file):
        num_elements = int(socket_file.readine().rstrip("\r\n"))
        return [self.handle_request(socket_file) for _ in range(num_elements)]

    def _dict(self, socket_file):
        num_elements = int(socket_file.readline().rstrip("\r\n"))
        elements = [self.handle_request(socket_file) for _ in range(2 * num_elements)]
        ks, vs = elements[::2], elements[1::2]
        return dict(zip(ks, vs))

    def __call__(self, x):
        f = getattr(self, x, None)
        if f is None:
            raise NotImplementedError
        return f


if __name__ == "__main__":
    pro = RedisProtocol()
    pro("_str")(12)

