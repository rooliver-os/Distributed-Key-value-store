import socket
import json
import threading

class JsonRpcServer:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.methods = {}

    def register(self, name, user_function):
        self.methods[name] = user_function

    def handle_request(self, request_json):
        method = request_json.get("method")
        parameters = request_json.get("parameters", [])
        call_id = request_json.get("call_id")

        if method not in self.methods:
            return json.dumps({"jsonrpc": "2.0", "error": f"Method '{method}' not found", "call_id": call_id})

        try:
            result = self.methods[method](*parameters)
            return json.dumps({"jsonrpc": "2.0", "result": result, "call_id": call_id})
        except Exception as e:
            return json.dumps({"jsonrpc": "2.0", "error": str(e), "call_id": call_id})

    def start(self):
        # HM: In production, use getaddrinfo() to get an IPv6 + IPv4 socket
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        server_socket.bind((self.host, self.port))
        server_socket.listen()
        print(f"[Server] Listening on {self.host}:{self.port}")

        while True:
            caller_connection, caller_address = server_socket.accept()

            # very easy to create and initiate these new threads in python
            # (the "daemon" flag means that when the main program exits, the "daemon" threads also exit)
            thread = threading.Thread(
                target=self.handle_client,
                args=(caller_connection, caller_address),
                daemon=True
            )
            thread.start()
        
        server_socket.close()

    def handle_client(self, caller_connection, caller_address):
        print(f"[Server] New connection from {caller_address}")

        # Buffer to store up to 4K of client requests
        buffer = b''

        while len(buffer) < 4096:
            data = caller_connection.recv(4096)

            if not data:
                break
            
            buffer += data

        try:
            request = json.loads(buffer.decode())
            response = self.handle_request(request)
            caller_connection.sendall(response.encode())
        except Exception as e:
            error = json.dumps({"jsonrpc": "2.0", "error": str(e), "call_id": None})
            caller_connection.sendall(error.encode())
        
        caller_connection.close()
