import socket
import json

class JsonRpcClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.call_id_counter = 1

    def call(self, method, parameters=None):
        request = {
            "jsonrpc": "2.0",
            "method": method,
            "parameters": parameters or [],
            "call_id": self.call_id_counter
        }

        self.call_id_counter += 1

        rpc_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        rpc_server.connect((self.host, self.port))
        rpc_server.sendall(json.dumps(request).encode())

        # TCP half-close
        rpc_server.shutdown(socket.SHUT_WR)

        response = rpc_server.recv(4096)
        data = json.loads(response.decode())

        # TCP complte close
        rpc_server.close()

        if "error" in data:
            raise Exception(f"RPC Error: {data['error']}")

        return data.get("result")

    # HM: Now, here's where the magic happens. When you access an attribute or method in Python
    # it falls back to this function if it cannot find it. See more at (https://www.pythonmorsels.com/getattr-vs-getattribute/)
    def __getattr__(self, method_name):
        # HM: Returns a newly made method that accepts a variable-length sequence of parameters
        # This method delegates the task to call()
        def method(*args):
            return self.call(method_name, list(args))

        return method