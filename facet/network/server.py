import struct
import pickle
import socket
import socketserver
import selectors
from ..helpers import parse_address


__all__ = [
    'SocketServer',
    'SocketServerHandler',
]


class SocketServerHandler(socketserver.StreamRequestHandler):
    """Handler for a RPC-like request.

    Handle multiple requests - each expected to be a 4-byte length,
    followed by the target method name, args, and kwargs in pickle format.
    """
    # socketserver.StreamRequestHandler
    timeout = None  # timeout to apply to socket (connection)

    def handle(self):
        while True:
            data = self.connection.recv(4)
            # Handle malformed messages under the assumption that messages
            # should be of the form <4 bytes for message size><message>.
            # Also, this check stops service loop when connection is closed.
            if len(data) < 4:
                break

            slen = struct.unpack('>L', data)[0]

            # Receive all data from client (works for TCP socket only)
            data = self.connection.recv(slen, socket.MSG_WAITALL)
            if len(data) < slen:
                continue

            method_name, args, kwargs = pickle.loads(data)

            # Try executing the method from the server object, if it
            # fails, pass the error as response (the client will raise
            # the expection).
            try:
                response = getattr(
                    self.server.served_object,
                    method_name
                )(*args, **kwargs)
            except Exception as ex:
                # NOTE: Should we extend exception message with server info?
                response = ex

            data = pickle.dumps(response, protocol=self.server.protocol)
            slen = struct.pack('>L', len(data))
            self.connection.send(slen)
            self.connection.sendall(data)

        print('Deactivating thread handler for {}:{}'.format(
            *self.connection.getpeername(),
        ))


class SocketServer(socketserver.ThreadingTCPServer):
    """Socket server with RPC-like support.

    For proper resource release, use in 'with' statement.
    This is because the server socket is closed by BaseServer in __exit__.

    This class has been modified to support connections without timeouts.

    Args:
        address (Tuple[str, int]): Host/port pair for socket address.

        handler_class (RequestHandlerClass): Class to handle client requests.

        served_object (obj): Instance with methods for RPC calls from clients.

        protocol (int): Version number of the protocol used to pickle/unpickle
            objects. Necessary to be set if and only if server and client are
            running on different Python versions.
    """
    # socketserver.BaseServer
    timeout = None  # wait for requests, used in handle_request()
    # socketserver.TCPServer
    address_family = socket.AF_INET  # AF_INET{,6}, AF_UNIT (TCP/UNIX sockets)
    socket_type = socket.SOCK_STREAM  # TCP
    # socket_type = socket.SOCK_STREAM | socket.SOCK_CLOEXEC  # TCP
    allow_reuse_address = True  # NOTE: shoule be False for security measures
    # Number of unaccepted connections for socket.listen() before system
    # refuses connections
    request_queue_size = 10
    # socketserver.ThreadingMixIn
    daemon_threads = False  # setting for request threads

    _min_bytes_control_commands = 8  # min bytes for server control commands
    _max_bytes_control_commands = 8  # max bytes for server control commands

    def __init__(
        self,
        address,
        handler_class,
        *,
        served_object,
        protocol=pickle.HIGHEST_PROTOCOL,
    ):
        # Resolve socket address
        host, port = (address[0], None) if len(address) == 1 else address
        address = parse_address(host, port)

        super().__init__(address, handler_class)

        self._handlers_connections = []
        self.served_object = served_object
        self.protocol = protocol

    def serve_forever(self, poll_interval=None):
        """Handle one request at a time until shutdown.

        Polls for shutdown every poll_interval seconds. Ignores
        self.timeout. If you need to do periodic tasks, do them in
        another thread.
        """
        self._BaseServer__is_shut_down.clear()
        # NOTE: The try-finally breaks if error occurs in the service loop,
        # for example, closing the server socket by an external thread/process.
        try:
            with socketserver._ServerSelector() as selector:
                selector.register(self, selectors.EVENT_READ)

                while not self._BaseServer__shutdown_request:
                    ready = selector.select(poll_interval)
                    # Exit if shutdown() called during select()
                    if self._BaseServer__shutdown_request:
                        break
                    if ready:
                        self._handle_request_noblock()
                    # Exit if 'shutdown' command or shutdown() call
                    if self._BaseServer__shutdown_request:
                        break

                    self.service_actions()
        finally:
            # Terminate request handler connections (breaks service loop)
            self.shutdown_connection(self._handlers_connections)
            self.close_connection(self._handlers_connections)

            # Terminate server connection
            self.shutdown_connection(self.socket)
            self.close_connection(self.socket)

            self._BaseServer__is_shut_down.set()

            # Reset server resources so it can be restarted
            self._BaseServer__shutdown_request = False
            self._handlers_connections = []

    def service_actions(self):
        # Remove closed/disconnected handler connections.
        # Not truly necessary because during shutdown, handlers are validated
        # but this allows saving resources if number of connections is large.
        self._handlers_connections = [
            conn for conn in self._handlers_connections if conn.fileno() != -1
        ]

    def verify_request(self, request, client_address):
        # Request is a socket (connection) passed to handler
        # Server peeks at socket to check if a server control command was sent.
        # Reasons for using this approach:
        #   1. Allow long or no timeout from server socket
        #   2. Support server shutdown from an external client
        data = request.recv(self._max_bytes_control_commands, socket.MSG_PEEK)

        # Consider it may be a control command, if message size conforms
        if len(data) >= self._min_bytes_control_commands:
            data = data.lower()
            if data == b'shutdown':
                # We do not shutdown/close current request because this
                # method's return value triggers TCPServer to call
                # shutdown_request(request)

                # Shutdown server based on serve_forever() from BaseServer
                self._BaseServer__shutdown_request = True
                return False

        # To close handler threads gracefully during shutdown, server maintains
        # a list of request sockets.
        self._handlers_connections.append(request)
        return True

    @staticmethod
    def shutdown_connection(connection, mode=socket.SHUT_RDWR):
        # NOTE: socket.SHUT_RD|WR|RDWR make corresponding socket.recv|send()
        # calls to return immediately with 0 bytes.
        # SHUT_RD* is necessary to break handler threads from service loop.
        try:
            iter(connection)
        except TypeError:
            connection = [connection]
        for conn in connection:
            try:
                conn.shutdown(mode)
            except OSError:
                pass

    @staticmethod
    def close_connection(connection):
        try:
            iter(connection)
        except TypeError:
            connection = [connection]
        for conn in connection:
            try:
                conn.close()
            except OSError:
                pass

    def shutdown(self):
        """Stops the server from service loop.

        Blocks until the loop has finished. This must be called while
        serve_forever() is running in another thread, or it will
        deadlock.

        Only one thread at a time should call shutdown() due to possible
        race condition with internal shutdown_request flag.
        """
        self._BaseServer__shutdown_request = True
        self.shutdown_connection(self.socket)
        self.server_close()
        self._BaseServer__is_shut_down.wait()
