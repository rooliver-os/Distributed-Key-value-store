import threading
import random
from jsonrpc_server import JsonRpcServer
from jsonrpc_client import JsonRpcClient

# Manager's own address and JSON-RPC server setup
my_address = {"host": "127.0.0.1", "port": 4000}
rpc = JsonRpcServer(my_address["host"], my_address["port"])

# Global peer registry and the accompanying lock
peers = {}
peer_lock = threading.RLock()

def put(key, value, client_address):
    """
    Receives a put request from a client, chooses the authoritative peer based
    on consistent hashing, and forwards the put request there.
    """
    print(f"[Manager] Put {key} -> {value}")
    with peer_lock:
        peer = find_peer(key)
        if peer:
            try:
                peer_rpc = JsonRpcClient(peer["host"], peer["port"])
                peer_rpc.put(key, value, client_address)
            except Exception as e:
                print(f"[Manager] Error contacting peer: {e}")
        else:
            print("[Manager] No peer available to forward put request.")

def get(key, client_address):
    """
    Receives a get request from a client, finds the authoritative peer, and
    forwards the request there.
    """
    print(f"[Manager] Get {key}")
    with peer_lock:
        peer = find_peer(key)
        if peer:
            try:
                peer_rpc = JsonRpcClient(peer["host"], peer["port"])
                peer_rpc.get(key, client_address)
            except Exception as e:
                print(f"[Manager] Error contacting peer: {e}")
        else:
            print("[Manager] No peer available to forward get request.")

def register(peer):
    """
    Registers a new peer using consistent hashing. After registration, it
    identifies the new peer's predecessor and instructs that peer to reallocate
    its keys that now belong to the new peer.
    """
    # Compute peer ID using absolute hash modulo 2^16.
    peer_id = abs(hash(f'{peer["host"]}:{peer["port"]}')) % (2**16)
    
    peer_lock.acquire()
    try:
        peers[peer_id] = peer
        print(f"[Manager] Registered peer {peer} with ID {peer_id}")

        # Obtaining a sorted list of the registered peer IDs
        sorted_ids = sorted(peers.keys())
        new_index = sorted_ids.index(peer_id)
        
        # Identify the predecessor; wrap-around if needed.
        predecessor_id = sorted_ids[new_index - 1] if new_index > 0 else sorted_ids[-1]
        source_peer = peers[predecessor_id]

        print(f"[Manager] Reallocating keys in interval ({predecessor_id}, {peer_id}] from peer {source_peer} to {peer}")
        try:
            # Instruct the predecessor to move keys to the new peer.
            source_client = JsonRpcClient(source_peer["host"], source_peer["port"])
            source_client.move(predecessor_id, peer_id, peer)
        except Exception as e:
            print(f"[Manager] Error reallocating keys from {source_peer} to {peer}: {e}")
        
        return peer_id
    finally:
        peer_lock.release()

def unregister(peer):
    """
    Unregisters a peer from the manager's peer registry.
    """
    peer_id = abs(hash(f'{peer["host"]}:{peer["port"]}')) % (2**16)
    peer_lock.acquire()
    try:
        if peer_id in peers:
            del peers[peer_id]
            print(f"[Manager] Unregistered peer {peer}")
    finally:
        peer_lock.release()

def get_current_peers():
    """
    Returns the list of currently registered peers.
    """
    peer_lock.acquire()
    try:
        return list(peers.values())
    finally:
        peer_lock.release()

def find_peer(key):
    """
    Finds the authoritative peer for a given key using consistent hashing.
    The key is hashed (absolute value modulo 2^16). The function returns the
    first peer with an ID greater than or equal to the key hash, or wraps-around.
    """
    hashed_key = abs(hash(key)) % (2**16)
    peer_lock.acquire()
    try:
        if not peers:
            return None

        # Create a sorted list of peer IDs.
        sorted_peer_ids = sorted(peers.keys())

        # Find the first peer id that is greater than or equal to the hashed_key.
        for pid in sorted_peer_ids:
            if pid >= hashed_key:
                found_peer = peers[pid]
                print(f"[Manager] Found authoritative peer {found_peer} for key {key} (hash: {hashed_key})")
                return found_peer

        # If none found (wrap-around), return the first peer.
        found_peer = peers[sorted_peer_ids[0]]
        print(f"[Manager] Found authoritative peer {found_peer} for key {key} (hash: {hashed_key}) [wrap-around]")
        return found_peer
    finally:
        peer_lock.release()

def get_random():
    """
    Returns a random peer from the currently registered peers.
    """
    with peer_lock:
        if not peers:
            return None
        peer_selected = random.choice(get_current_peers())
        print(f"[Manager] Get random returning {peer_selected}")
        return peer_selected

def check_heartbeat():
    """
    Checks the health of each registered peer by invoking their heartbeat method.
    """
    with peer_lock:
        for peer in list(peers.values()):
            try:
                peer_rpc = JsonRpcClient(peer["host"], peer["port"])
                peer_rpc.heartbeat()
            except Exception as e:
                print(f"[Manager] Heartbeat failed for {peer['host']}:{peer['port']} — {e}")

# Register RPC methods.
rpc.register("put", put)
rpc.register("get", get)
rpc.register("register", register)
rpc.register("unregister", unregister)
rpc.register("get_current_peers", get_current_peers)
rpc.register("get_random", get_random)
rpc.register("check_heartbeat", check_heartbeat)

print("[Manager] Running...")
rpc.start()
