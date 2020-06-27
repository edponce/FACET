import struct
import pickle
import socket
import inspect


__all__ = ['SocketClient']


class SocketClient:
    """Socket client to provide communication with the server.

    Args:
        target_class (object): Class to be served by the server. Only
            public methods will be binded.

        host (str): Host address of the server.

        port (int): Port the server is listening to.

        buffersize (int): Size of the buffer used for communication.
            Must be the same for both the server and the client.

        pickle_protocol (int, None): Version number of the protocol used
            to pickle/unpickle objects. Necessary to be set if and only
            if server and client are running on different Python versions.
    """

    def __init__(self, target_class, *, host='localhost', port=4444):
        self._host = None
        self._port = None
        self._sock = None

        # Bind public methods of 'target_class'
        for method_name, method in inspect.getmembers(
            target_class,
            predicate=inspect.isfunction,
        ):
            if method_name.startswith('_'):
                continue
            setattr(self, method_name, self._func_req_wrapper(method_name))

        self.connect(host, port)

    def disconnect(self):
        self._sock.close()
        self._sock = None

    def connect(self, host=None, port=None):
        if self._sock:
            self.disconnect()
        if host is not None:
            self._host = host
        if port is not None:
            self._port = port
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self._sock.connect((self._host, self._port))
        except ConnectionRefusedError:
            self._sock = None

    def _func_req_wrapper(self, method_name):
        """create a method with the same method_name that communicate
        with the server"""

        def func_request(*args, **kwargs):
            """Send the request to the server"""
            if self._sock is None:
                self.connect()

            # SEND
            data = pickle.dumps((method_name, args, kwargs))
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
