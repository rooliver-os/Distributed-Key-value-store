from jsonrpc_server import JsonRpcServer
from jsonrpc_client import JsonRpcClient
import threading
import random

# Creates client server on a random port.
client_port = random.randint(6000, 7000)
client_address = {"host": "127.0.0.1", "port": client_port}
rpc = JsonRpcServer(client_address["host"], client_address["port"])

# The Manager’s address to get a random peer.
manager = JsonRpcClient("127.0.0.1", 4000)

def submitAnswerGet(key, value):
    print(f"{key} -> {value}")

def submitAnswerPut(key, previousValue):
    status = "replaced" if previousValue else "inserted"
    print(f"{key} {status}")

rpc.register("submitAnswerGet", submitAnswerGet)
rpc.register("submitAnswerPut", submitAnswerPut)

thread = threading.Thread(target=rpc.start, daemon=True)
thread.start()

def prompt_key():
    return input("Type key: ").strip() or "0"

def prompt_value():
    return input("Type value: ")

def main():
    while True:
        operation = input('Say "put", "get", or "exit": ').strip()
        
        if operation == "put":
            key = prompt_key()
            value = prompt_value()
            # Ask Manager for a random peer, then send the put request.
            random_peer = manager.get_random()
            if random_peer is None:
                print("No peer available!")
            else:
                peer_client = JsonRpcClient(random_peer["host"], random_peer["port"])
                peer_client.put(key, value, client_address)
                
        elif operation == "get":
            key = prompt_key()
            # Ask Manager for a random peer, then send the get request there.
            random_peer = manager.get_random()
            if random_peer is None:
                print("No peer available!")
            else:
                peer_client = JsonRpcClient(random_peer["host"], random_peer["port"])
                peer_client.get(key, client_address)
                
        elif operation == "exit":
            print("Exiting")
            break

if __name__ == "__main__":
    main()
