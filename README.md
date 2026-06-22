# AeroDB (Distributed Key-Value Store)

[![Go Version](https://img.shields.io/badge/Go-1.24+-00ADD8?style=flat&logo=go)](https://golang.org)
[![Protocol](https://img.shields.io/badge/Protocol-Redis%20RESP-red)](https://redis.io/)
[![Architecture](https://img.shields.io/badge/Architecture-Masterless-blueviolet)](#)

**AeroDB** is a lightweight, masterless, and secure distributed in-memory key-value database written purely in Go. Inspired by the architectural principles of Cassandra and DynamoDB, AeroDB prioritizes high availability, decentralized routing, and local concurrency without the heavy footprint of traditional databases.

---

## Key Features

- **Masterless Architecture:** No single point of failure. Every node is an equal peer and can act as a coordinator for client requests.
- **Consistent Hashing:** Uses a Hash Ring with virtual nodes (3 replicas) for efficient data distribution and minimal rehashing when cluster topology changes.
- **Decentralized Gossip Protocol:** UDP-based peer discovery and failure detection. Nodes continuously exchange health states to maintain cluster awareness.
- **Data Consistency via Lamport Clocks:** Employs Lamport Logical Clocks to track record versions, ensuring conflict resolution and proper ordering of asynchronous replica updates.
- **High Concurrency (Lock Striping):** In-memory storage is partitioned into 16 separate shards, allowing massive concurrent reads and writes without heavy global mutex contention.
- **Durability & LRU Eviction:** - **WAL:** Write-Ahead Logging ensures zero data loss upon crash recovery.
  - **Snapshots:** Background snapshotting automatically triggers based on a dirty-operation threshold.
  - **Memory Limits:** Built-in Least Recently Used (LRU) eviction kicks in when shard capacities are reached.
- **Security & Access Control:**
  - **HMAC-SHA256 Signatures:** UDP Gossip packets are cryptographically signed to prevent network spoofing and malicious node injections.
  - **RBAC (Role-Based Access Control):** Granular `admin` and `reader` permissions managed via authentication.
- **Redis Protocol Compatible:** Communicates via the standard RESP (Redis Serialization Protocol), meaning you can natively query AeroDB using the standard `redis-cli`.

---

## Architecture Overview

AeroDB strictly separates cluster topology management from data storage:

1. **Topology & Health:** Nodes run a background UDP routine, securely gossiping their state. Unresponsive nodes are flagged as "suspects" and eventually removed.
2. **Routing:** Client requests are mapped using `CRC32` checksums onto a distributed Hash Ring. If a node receives a request for data it doesn't own, it automatically forwards the command to the correct replica.
3. **Storage Engine:** The underlying memory engine relies on a sharded linked-list and map combination (LRU Cache), persisting operations immediately to a `.wal` file before safely returning success to the client.

---

## Benchmarks & Performance

AeroDB is designed to handle concurrent operations efficiently using lock-striping and asynchronous replication. The performance was evaluated using the standard `redis-benchmark` tool on a **3-Node local cluster** with authentication enabled, simulating real-world network routing and replication overhead.

**Test Methodology:** To rigorously test the Consistent Hash Ring distribution and concurrent load handling, the benchmark was configured to process a total of **100,000 requests** across **100,000 randomized keys** using 100 concurrent client connections.

**Results:**

| Operation | Throughput (Req/Sec) | p50 Latency |
| --- | --- | --- |
| **SET** | ~ 1,478 ops/sec | 60.63 msec |
| **GET** | ~ 199 ops/sec | 23.29 msec |

*Architectural Note:* You might notice that `SET` operations yield higher throughput than `GET` operations. This is a deliberate design choice in AeroDB's masterless architecture:

* **Writes (SET)** are replicated *asynchronously*. The coordinator node writes locally and returns `OK` to the client immediately, while background goroutines securely sync the data to replica nodes.
* **Reads (GET)** are strictly routed. If a client queries a node that doesn't own the requested data (based on the Hash Ring), the node *synchronously* forwards the request to the correct owner over a new TCP connection and waits for the response before replying to the client.

---

## Getting Started

### 1. Configuration (`gokv.yaml`)

Create a `gokv.yaml` file in the root directory to define the server, cluster secret, storage capacity, and user roles:

```yaml
server:
  host: "127.0.0.1"
  port: "8000"
cluster:
  secret: "SuperSecretClusterKey123!@#"
  seed: "" # Leave empty for the first node
storage:
  capacity: 2048
users:
  - username: "boss"
    password: "admin_password123"
    role: "admin"
  - username: "guest"
    password: "readonly_password123"
    role: "reader"

```

### 2. Running a Cluster

You can launch multiple nodes on the same machine by overriding ports and seed addresses via Environment Variables.

**Terminal 1 (Seed Node):**

```bash
go run ./cmd/server/main.go

```

**Terminal 2 (Node 2):**

```bash
GOKV_PORT=8001 GOKV_SEED=127.0.0.1:8000 go run ./cmd/server/main.go

```

**Terminal 3 (Node 3):**

```bash
GOKV_PORT=8002 GOKV_SEED=127.0.0.1:8000 go run ./cmd/server/main.go

```

---

## Usage (Client Interaction)

Because AeroDB speaks RESP, you don't need a custom client. Simply use `redis-cli`:

```bash
$ redis-cli -p 8000

# Authenticate as an admin
127.0.0.1:8000> AUTH boss admin_password123
OK

# Write data (Replicated automatically to the Hash Ring)
127.0.0.1:8000> SET user:100 "Ali"
OK

# Read data
127.0.0.1:8000> GET user:100
"Ali"
```