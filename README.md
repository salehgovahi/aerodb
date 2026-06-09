# AeroDB (Distributed Key-Value Store)

[![Go Version](https://img.shields.io/badge/Go-1.20+-00ADD8?style=flat&logo=go)](https://golang.org)
[![Protocol](https://img.shields.io/badge/Protocol-Redis%20RESP-red)](https://redis.io/)
[![Architecture](https://img.shields.io/badge/Architecture-Masterless-blueviolet)]()

**AeroDB** is a lightweight, masterless, and secure distributed in-memory key-value database written purely in Go. It implements an architecture inspired by Cassandra and DynamoDB, prioritizing high availability, decentralized routing, and strict security without the heavy footprint.

---

##  Key Features

- **Masterless Architecture:** No single point of failure. Every node is equal and can act as a coordinator.
- **Consistent Hashing:** Efficient data distribution and routing with minimal rehashing when nodes join or leave.
- **Gossip Protocol:** UDP-based decentralized peer discovery and failure detection.
- **Enterprise-Grade Security:**
  - **TLS/SSL Encryption:** All TCP client and inter-node replication traffic is securely tunneled.
  - **HMAC-SHA256 Signatures:** UDP Gossip packets are cryptographically signed to prevent network spoofing.
  - **RBAC (Role-Based Access Control):** Granular `Admin` and `Reader` permissions.
- **Distributed Lock Manager (DLM):** Owner-based distributed locking to prevent *Lost Updates* and concurrent write conflicts.
- **Durability & Concurrency:** Write-Ahead Logging (WAL), background Snapshots, and Lock Striping for massive local concurrency.
- **Redis Compatible:** Communicates via the standard RESP (Redis Serialization Protocol), allowing you to use the standard `redis-cli`.

---

## Architecture Overview

AeroDB separates cluster topology management from data storage and access control. 

1. **Topology:** Nodes continuously exchange states using a secure Gossip protocol.
2. **Routing:** Client requests are routed using a Hash Ring (`HashRing(3)`).
3. **Consistency:** Writes are coordinated via a Distributed Lock Manager (DLM) before updating replicas asynchronously.

---

## Getting Started

### 1. Generate TLS Certificates
AeroDB requires TLS certificates for secure TCP communication. Generate a self-signed certificate in the project root:
```bash
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes -subj "/CN=localhost"
```

### 2. Configuration (gokv.yaml)
Create a `gokv.yaml` file in the root directory:
```yaml
server:
  host: "127.0.0.1"
  port: "8000"
  tls:
    enabled: true
    cert_file: "cert.pem"
    key_file: "key.pem"
cluster:
  seed: ""
  secret: "SuperSecretClusterKey123!@#"
storage:
  capacity: 2048
users:
  - username: "boss"
    password: "admin_password123"
    role: "admin"
```

### 3. Running a Cluster
You can launch multiple nodes on the same machine by overriding ports via Environment Variables.

**Terminal 1 (Seed Node):**
```bash
go run ./cmd/server/main.go
```

**Terminal 2 (Node 2):**
```bash
GOKV_PORT=8001 GOKV_SEED=127.0.0.1:8000 go run ./cmd/server/main.go
```

---

## Usage (Client Interaction)
Since AeroDB uses RESP, you can interact with it using the standard `redis-cli`:
```bash
$ redis-cli --tls --insecure -p 8000

127.0.0.1:8000> AUTH boss admin_password123
OK
127.0.0.1:8000> SET user:100 "Ali"
OK
```