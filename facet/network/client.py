import struct
import pickle
import socket
import inspect
from ..helpers import parse_address


__all__ = ['SocketClient']


class SocketClient:
    """Socket client for communications with RPC-like server.

    Args:
        address (Tuple[str, int]): Host/port pair for socket address.

        target_class (object): Class to be served by the server. Only
            public, non-property methods will be binded.

        protocol (int): Version number of the protocol used to pickle/unpickle
            objects. Necessary to be set if and only if server and client are
            running on different Python versions.
    """

    def __init__(
        self,
        address,
        *,
        target_class,
        protocol=pickle.HIGHEST_PROTOCOL,
    ):
        self._address = None
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._protocol = protocol

        # Resolve socket address
        host, port = (address[0], None) if len(address) == 1 else address
        self._address = parse_address(host, port)

        # Bind public methods of 'target_class'
        for method_name, method in inspect.getmembers(
            target_class,
            predicate=inspect.isfunction,
        ):
            if method_name.startswith('_'):
                continue
            setattr(self, method_name, self._func_req_wrapper(method_name))

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, traceback):
        self.disconnect()

    def disconnect(self):
        if self._sock is not None:
            self._sock.close()
            self._sock = None

    def connect(self):
        self._sock.connect(self._address)

    def _func_req_wrapper(self, method_name):
        """Create a method with the same method name to communicate
        with the server."""

        def func_request(*args, **kwargs):
            """Send the request to the server."""
            if self._sock is None:
                self.connect()

            # SEND
            data = pickle.dumps(
                (method_name, args, kwargs),
                protocol=self._protocol,
            )
            slen = struct.pack('>L', len(data))
            self._sock.send(slen)
            self._sock.sendall(data)

            # RECEIVE
            data = self._sock.recv(4)
            # Handle malformed messages under the assumption that messages
            # should be of the form <4 bytes for message size><message>.
            # Also, this check stops service loop when connection is closed.
            if len(data) < 4:
                raise Exception('malformed message, too small')

            slen = struct.unpack('>L', data)[0]

            # Receive all data from client (works for TCP socket only)
            response = self._sock.recv(slen, socket.MSG_WAITALL)
            if len(response) < slen:
                raise Exception('received less data than expected')

            data = pickle.loads(response)

            # Raise an exception if an exception was raised by the
            # served object while the server was executing method named
            # method_name.
            if isinstance(data, Exception):
                raise data

            return data

        return func_request
