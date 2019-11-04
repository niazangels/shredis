from base import Error, CommandError, DisconnectError
from base import ProtocolHandler, Server
from io import BytesIO

eos = "\r\n"


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

    def write_response(self, socket_file, data):
        buf = BytesIO()
        self.write(buf, data)
        buf.seek(0)
        socket_file.write(buf.getvalue())
        socket_file.flush()

    def write(self, buf, data):
        if isinstance(data, str):
            data = data.encode("utf-8")

        if isinstance(data, bytes):
            buf.write(f"${len(data)}{eos}{data}{eos}")
        elif isinstance(data, int):
            buf.write(f":{data}{eos}")
        elif isinstance(data, Error):
            buf.write(f"-{data.message}{eos}")
        elif isinstance(data, (list, tuple)):
            buf.write(f"*{len(data)}{eos}")
            for item in data:
                self.write(buf, item)
        elif isinstance(data, dict):
            buf.write(f"%{len(data)}{eos}")
            for key in data:
                self.write(buf, key)
                self.write(buf, data[key])
        elif data is None:
            buf.write(f"$-1{eos}")
        else:
            raise CommandError(f"Unrecognized type: {type(data)}")

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


class RedisServer(Server):
    def __init__(self, host="127.0.0.1", port=6379, max_clients=64):
        super().__init__()
        self._commands = self.get_commands()

    def get_commands(self):
        return {
            "GET": self._get,
            "SET": self._set,
            "DELETE": self._delete,
            "FLUSH": self._flush,
            "MGET": self._mget,
            "MSET": self._mset,
        }

    def get_response(self, data):
        if not isinstance(data, list):
            try:
                data = data.split()
            except:
                raise CommandError("Request must be list or simple str")

        if not data:
            raise CommandError("Missing command")

        command = data[0].upper()
        if command not in self._commands:
            raise CommandError(f"Unrecognized comamnd: {command}")

        return self._commands[command](*data[1:])

    def _get(self, k):
        return self._kv.get(key)

    def _set(self, k, v):
        self._kv[k] = v
        return 1

    def _delete(self, k):
        if k in self._kv:
            self._kv.pop(k)
            return 1
        return 0

    def _flush(self):
        size = len(self._kv)
        self._kv.clear()
        return size

    def _mget(self, *keys):
        return [self._kv.get(k) for k in keys]

    def _mset(self, *items):
        data = dict(zip(items[::2], items[1::2]))
        self._kv.update(data)
        return len(data)


if __name__ == "__main__":
    from gevent import monkey

    monkey.patch_all()
    RedisServer().run()
