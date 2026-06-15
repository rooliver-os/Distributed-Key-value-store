# Distributed-Key-value-store

**Features**
- Distributed key-value storage
- JSON-RPC communication over TCP sockets
- Consistent hashing for data partitioning
- Data replication across multiple peers
- Fault-tolerant request routing
- Dynamic peer management
- Client-server architecture
- Concurrent request handling

**Prerequisites**
- Python 3.10+
- No external dependencies required beyond the Python standard library

**Step 1: Start the Manager**
The manager maintains peer membership information and assists with request routing.
python manager.py

**Step 2: Start Peer Nodes**
Launch multiple peer nodes in separate terminal windows.
      Example:
        python peer.py 5001
        python peer.py 5002
        python peer.py 5003
Each peer joins the distributed system and becomes responsible for a portion of the hash ring.

**Step 3: Start the Client**
Open a new terminal and launch the client.
python client.py

**Example Operations**
Store a key-value pair:
put("name", "Robert")

Retrieve a value:
get("name")

Expected output:
Robert
