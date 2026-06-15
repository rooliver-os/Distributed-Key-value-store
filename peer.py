import sys
import random
import threading
import time

from jsonrpc_server import JsonRpcServer
from jsonrpc_client import JsonRpcClient

if len(sys.argv) != 2:
    print("Usage: peer.py <port>")
    sys.exit(1)

my_port = int(sys.argv[1])
my_address = {"host": "127.0.0.1", "port": my_port}
rpc = JsonRpcServer(my_address["host"], my_address["port"])

manager_address = {"host": "127.0.0.1", "port": 4000}
manager = JsonRpcClient(manager_address["host"], manager_address["port"])

data_table = {}

# Distributed membership maintained locally at each peer.
myPeers = {}

def compute_peer_id(peer):
    return abs(hash(f'{peer["host"]}:{peer["port"]}')) % (2**16)


# Helper function for getting the Replication Group for a Key
def get_replication_group(key):
    """
    Using the membership list (myPeers), compute the replication group for a given key.
    Returns a tuple (primary, replica1, replica2), where:
      - primary    is the node that should be authoritative,
      - replica1   is the next node (clockwise) in the ring,
      - replica2   is the following node.
    """
    key_hash = abs(hash(key)) % (2**16)
    if not myPeers:
        return (my_address, my_address, my_address)
    sorted_ids = sorted(myPeers.keys())
    primary = None
    index = None
    for i, pid in enumerate(sorted_ids):
        if pid >= key_hash:
            primary = myPeers[pid]
            index = i
            break
    if primary is None:
        primary = myPeers[sorted_ids[0]]
        index = 0
    replica1 = myPeers[ sorted_ids[(index + 1) % len(sorted_ids)] ]
    replica2 = myPeers[ sorted_ids[(index + 2) % len(sorted_ids)] ]
    return (primary, replica1, replica2)

# --------------------------
# QUERY ROUTING W FAULT TOLERANCE (Step 6)
# --------------------------
def put(key, value, client_address):
    """
    Updated put: If this peer is authoritative, store locally.
    Otherwise, try to forward the request to the authoritative peer.
    If that fails, try the first replica and then the second replica.
    """
    primary, rep1, rep2 = get_replication_group(key)
    # Check if this peer is primary.
    if primary["host"] == my_address["host"] and primary["port"] == my_address["port"]:
        print(f"[Peer {my_port}] (Authoritative) Storing key '{key}' -> '{value}'")
        old_value = data_table.get(key)
        data_table[key] = value
        if client_address is not None:
            client = JsonRpcClient(client_address["host"], client_address["port"])
            client.submitAnswerPut(key, old_value)
    else:
        # Try primary first.
        try:
            print(f"[Peer {my_port}] Forwarding put({key}, {value}) to PRIMARY {primary}")
            fwd_client = JsonRpcClient(primary["host"], primary["port"])
            fwd_client.put(key, value, client_address)
            return
        except Exception as e:
            print(f"[Peer {my_port}] PRIMARY {primary} did not respond: {e}")
        # Try first replica.
        try:
            print(f"[Peer {my_port}] Trying FIRST REPLICA {rep1} for put({key}, {value})")
            fwd_client = JsonRpcClient(rep1["host"], rep1["port"])
            fwd_client.put(key, value, client_address)
            return
        except Exception as e:
            print(f"[Peer {my_port}] FIRST REPLICA {rep1} did not respond: {e}")
        # Try second replica.
        try:
            print(f"[Peer {my_port}] Trying SECOND REPLICA {rep2} for put({key}, {value})")
            fwd_client = JsonRpcClient(rep2["host"], rep2["port"])
            fwd_client.put(key, value, client_address)
            return
        except Exception as e:
            print(f"[Peer {my_port}] SECOND REPLICA {rep2} did not respond: {e}")
        print(f"[Peer {my_port}] All replicas failed for put({key}, {value})")

def get(key, client_address):
    """
    Updated get: If this peer is authoritative, retrieve the key.
    Otherwise, try the primary authoritative peer. On failure, try replica1 then replica2.
    """
    primary, rep1, rep2 = get_replication_group(key)
    if primary["host"] == my_address["host"] and primary["port"] == my_address["port"]:
        print(f"[Peer {my_port}] (Authoritative) Retrieving key '{key}'")
        value = data_table.get(key)
        client = JsonRpcClient(client_address["host"], client_address["port"])
        client.submitAnswerGet(key, value)
    else:
        try:
            print(f"[Peer {my_port}] Forwarding get({key}) to PRIMARY {primary}")
            fwd_client = JsonRpcClient(primary["host"], primary["port"])
            fwd_client.get(key, client_address)
            return
        except Exception as e:
            print(f"[Peer {my_port}] PRIMARY {primary} did not respond: {e}")
        try:
            print(f"[Peer {my_port}] Trying FIRST REPLICA {rep1} for get({key})")
            fwd_client = JsonRpcClient(rep1["host"], rep1["port"])
            fwd_client.get(key, client_address)
            return
        except Exception as e:
            print(f"[Peer {my_port}] FIRST REPLICA {rep1} did not respond: {e}")
        try:
            print(f"[Peer {my_port}] Trying SECOND REPLICA {rep2} for get({key})")
            fwd_client = JsonRpcClient(rep2["host"], rep2["port"])
            fwd_client.get(key, client_address)
            return
        except Exception as e:
            print(f"[Peer {my_port}] SECOND REPLICA {rep2} did not respond: {e}")
        print(f"[Peer {my_port}] All replicas failed for get({key})")

# --------------------------
# STEPS 3-5
# --------------------------
def ping():
    print(f"[Peer {my_port}] Tum-Tum")
    return True

def move(begin, end, destination):
    global data_table
    keys_to_move = []
    for key in list(data_table.keys()):
        key_hash = abs(hash(key)) % (2**16)
        if begin < end:
            if key_hash > begin and key_hash <= end:
                keys_to_move.append(key)
        else:
            if key_hash > begin or key_hash <= end:
                keys_to_move.append(key)
    for key in keys_to_move:
        value = data_table[key]
        print(f"[Peer {my_port}] Moving key '{key}' (hash: {abs(hash(key)) % (2**16)}) to {destination}")
        try:
            dest_client = JsonRpcClient(destination["host"], destination["port"])
            dest_client.put(key, value, None)
            del data_table[key]
        except Exception as e:
            print(f"[Peer {my_port}] Error moving key '{key}' to {destination}: {e}")

def getPeers():
    return list(myPeers.values())

def addPeer(new_peer):
    new_id = compute_peer_id(new_peer)
    myPeers[new_id] = new_peer
    print(f"[Peer {my_port}] Added new peer: {new_peer}. Current membership: {myPeers}")
    return True

def join():
    manager.register(my_address)
    random_peer = manager.get_random()
    if random_peer is not None:
        try:
            rand_client = JsonRpcClient(random_peer["host"], random_peer["port"])
            members = rand_client.getPeers()
            for p in members:
                myPeers[compute_peer_id(p)] = p
            print(f"[Peer {my_port}] Fetched membership from {random_peer}: {myPeers}")
        except Exception as e:
            print(f"[Peer {my_port}] Error fetching membership: {e}")
    else:
        myPeers[compute_peer_id(my_address)] = my_address
        print(f"[Peer {my_port}] I am the first peer. Membership: {myPeers}")
    for peer in list(myPeers.values()):
        if peer != my_address:
            try:
                peer_client = JsonRpcClient(peer["host"], peer["port"])
                peer_client.addPeer(my_address)
            except Exception as e:
                print(f"[Peer {my_port}] Error notifying peer {peer}: {e}")

# --------------------------
# Replication and Fault-Tolerance (from Step 5)
# --------------------------
def copyAll(destination):
    for key, value in data_table.items():
        try:
            dest_client = JsonRpcClient(destination["host"], destination["port"])
            dest_client.put(key, value, None)
        except Exception as e:
            print(f"[Peer {my_port}] Error copying key '{key}' to {destination}: {e}")
    return True

def flushData():
    global data_table
    keys_to_flush = []
    for key in list(data_table.keys()):
        owner = get_replication_group(key)[0]
        if not myPeers:
            continue
        sorted_ids = sorted(myPeers.keys())
        owner_id = compute_peer_id(owner)
        try:
            idx = sorted_ids.index(owner_id)
        except ValueError:
            continue
        pred1 = myPeers[sorted_ids[(idx - 1) % len(sorted_ids)]]
        pred2 = myPeers[sorted_ids[(idx - 2) % len(sorted_ids)]]
        replicas = {f"{owner['host']}:{owner['port']}",
                    f"{pred1['host']}:{pred1['port']}",
                    f"{pred2['host']}:{pred2['port']}"}
        my_id = f"{my_address['host']}:{my_address['port']}"
        if my_id not in replicas:
            keys_to_flush.append(key)
    for key in keys_to_flush:
        print(f"[Peer {my_port}] Flushing key '{key}' (no longer in replication group)")
        del data_table[key]

def replication_thread():
    while True:
        try:
            if len(myPeers) >= 3:
                sorted_ids = sorted(myPeers.keys())
                my_id_val = compute_peer_id(my_address)
                if my_id_val in sorted_ids:
                    idx = sorted_ids.index(my_id_val)
                    pred1 = myPeers[sorted_ids[(idx - 1) % len(sorted_ids)]]
                    pred2 = myPeers[sorted_ids[(idx - 2) % len(sorted_ids)]]
                    print(f"[Peer {my_port}] Replication: copying all data from predecessors {pred1} and {pred2}")
                    pred1_client = JsonRpcClient(pred1["host"], pred1["port"])
                    pred1_client.copyAll(my_address)
                    pred2_client = JsonRpcClient(pred2["host"], pred2["port"])
                    pred2_client.copyAll(my_address)
            flushData()
        except Exception as e:
            print(f"[Peer {my_port}] Replication thread error: {e}")
        time.sleep(10)

# --------------------------
# RPC Registration and Startup
# --------------------------

rpc.register("put", put)
rpc.register("get", get)
rpc.register("ping", ping)
rpc.register("move", move)
rpc.register("getPeers", getPeers)
rpc.register("addPeer", addPeer)
rpc.register("copyAll", copyAll)

join_timer = threading.Timer(2.0, join)
join_timer.start()

rep_thread = threading.Thread(target=replication_thread, daemon=True)
rep_thread.start()

print(f"[Peer {my_port}] Running on port {my_port}")
rpc.start()
