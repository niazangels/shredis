from base import Error, CommandError, DisconnectError
from base import ProtocolHandler, Server


class RedisProtocol(ProtocolHandler):
    def __init__(self):
        self.handlers = {
            "+": self("simple_string"),
            "$": self("bulk_string"),
            "-": self("error"),
            ":": self("integer"),
            "*": self("array"),
            "%": self("dict"),
        }

    def handle_request(self, socket_file):
        first_byte = socket_file.read(1)
        if not first_byte:
            raise DisconnectError()
        try:
            return self(first_byte)(socket_file)
        except KeyError:
            raise CommandError("Bad request")

    def simple_string(self, x):
        print("In simple_string!")

    def __call__(self, x):
        f = getattr(self, x, None)
        if f is None:
            raise NotImplementedError
        return f


if __name__ == "__main__":
    pro = RedisProtocol()
    pro("simple_string")(12)

