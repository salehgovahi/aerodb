package main

import (
	"bufio"
	"crypto/subtle"
	"fmt"
	"memorydb/internal/cluster"
	"memorydb/internal/dlock"
	"memorydb/internal/protocol"
	"memorydb/internal/routing"
	"memorydb/internal/storage"
	"net"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type UserConf struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Role     string `yaml:"role"`
}

type Config struct {
	Server struct {
		Host string `yaml:"host"`
		Port string `yaml:"port"`
	} `yaml:"server"`
	Cluster struct {
		Seed   string `yaml:"seed"`
		Secret string `yaml:"secret"`
	} `yaml:"cluster"`
	Storage struct {
		Capacity int `yaml:"capacity"`
	} `yaml:"storage"`
	Users []UserConf `yaml:"users"`
}

func main() {
	configFile := "gokv.yaml"
	if len(os.Args) > 1 {
		configFile = os.Args[1]
	}

	data, err := os.ReadFile(configFile)
	if err != nil {
		panic(err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		panic(err)
	}

	if envPort := os.Getenv("GOKV_PORT"); envPort != "" {
		cfg.Server.Port = envPort
	}
	if envSeed := os.Getenv("GOKV_SEED"); envSeed != "" {
		cfg.Cluster.Seed = envSeed
	}

	myAddr := net.JoinHostPort(cfg.Server.Host, cfg.Server.Port)

	fmt.Printf("[INIT] Starting node at %s\n", myAddr)
	if cfg.Cluster.Seed != "" {
		fmt.Printf("[INIT] Joining cluster via seed: %s\n", cfg.Cluster.Seed)
	}

	store := storage.NewMemoryStorage(fmt.Sprintf("dump_%s.bin", cfg.Server.Port), cfg.Storage.Capacity)
	hashRing := routing.NewHashRing(3)
	hashRing.Add(myAddr)

	c := cluster.NewCluster(myAddr, cfg.Cluster.Seed, cfg.Cluster.Secret)

	dlm := dlock.NewLockManager()

	go c.StartListening(cfg.Server.Port)
	go c.StartGossiping()

	go syncRingWithCluster(c, hashRing)

	startTCPServer(cfg.Server.Port, hashRing, store, myAddr, cfg.Users, dlm)
}

func syncRingWithCluster(c *cluster.Cluster, ring *routing.HashRing) {
	ticker := time.NewTicker(2 * time.Second)
	knownNodes := make(map[string]bool)
	knownNodes[c.Me] = true

	for range ticker.C {
		c.Mu.RLock()
		activePeers := make(map[string]bool)
		for peer := range c.Peers {
			if !c.Suspects[peer] {
				activePeers[peer] = true
			}
		}
		c.Mu.RUnlock()

		for peer := range activePeers {
			if !knownNodes[peer] {
				fmt.Printf("[RING] New node discovered! Adding to HashRing: %s\n", peer)
				ring.Add(peer)
				knownNodes[peer] = true
			}
		}

		for peer := range knownNodes {
			if peer != c.Me && !activePeers[peer] {
				fmt.Printf("[RING] Node is dead. Removing from HashRing: %s\n", peer)
				ring.Remove(peer)
				delete(knownNodes, peer)
			}
		}
	}
}

func startTCPServer(port string, ring *routing.HashRing, store storage.Storage, myAddr string, users []UserConf, dlm *dlock.LockManager) {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Database ready! TCP Client listening on port %s\n", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		go handleClient(conn, ring, store, myAddr, users, dlm)
	}
}

func handleClient(conn net.Conn, ring *routing.HashRing, store storage.Storage, myAddr string, users []UserConf, dlm *dlock.LockManager) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	currentRole := ""
	if len(users) == 0 {
		currentRole = "admin"
	}

	for {
		parts, err := protocol.ReadCommand(reader)
		if err != nil {
			return
		}
		if len(parts) < 1 {
			continue
		}
		command := strings.ToUpper(parts[0])

		if command == "AUTH" {
			if len(parts) < 3 {
				protocol.WriteError(conn, "ERR AUTH requires username and password")
				continue
			}
			userParam := parts[1]
			passParam := parts[2]

			matched := false
			for _, u := range users {
				if u.Username == userParam {
					if subtle.ConstantTimeCompare([]byte(u.Password), []byte(passParam)) == 1 {
						currentRole = u.Role
						matched = true
						break
					}
				}
			}

			if matched {
				protocol.WriteSimpleString(conn, "OK")
			} else {
				protocol.WriteError(conn, "WRONGPASS invalid username or password")
			}
			continue
		}

		if currentRole == "" {
			protocol.WriteError(conn, "NOAUTH Authentication required.")
			continue
		}

		if len(parts) < 2 {
			protocol.WriteError(conn, "wrong number of arguments")
			continue
		}
		key := parts[1]

		if command == "_LOCK" {
			if dlm.TryLock(key, 5*time.Second) {
				protocol.WriteSimpleString(conn, "OK")
			} else {
				protocol.WriteError(conn, "LOCKED")
			}
			continue
		}

		if command == "_UNLOCK" {
			dlm.Unlock(key)
			protocol.WriteSimpleString(conn, "OK")
			continue
		}

		if command == "_SYNC_SET" || command == "_SYNC_DEL" {
			if currentRole != "admin" {
				protocol.WriteError(conn, "ERR unauthorized for cluster sync")
				continue
			}
			if command == "_SYNC_SET" && len(parts) >= 3 {
				store.Set(key, parts[2])
			} else if command == "_SYNC_DEL" {
				store.Delete(key)
			}
			protocol.WriteSimpleString(conn, "OK")
			continue
		}

		if command == "SET" || command == "DEL" {
			if currentRole != "admin" {
				protocol.WriteError(conn, "ERR unauthorized operation")
				continue
			}
		}

		replicas := ring.GetReplicas(key, 2)
		if len(replicas) == 0 {
			protocol.WriteError(conn, "Cluster is empty")
			continue
		}

		if command == "GET" {
			answered := false
			for _, node := range replicas {
				if node == myAddr {
					val, err := store.Get(key)
					if err == nil {
						protocol.WriteBulkString(conn, val)
						answered = true
						break
					}
				} else {
					resp, err := forwardCommandToNode(node, parts, users)
					if err == nil {
						conn.Write(resp)
						answered = true
						break
					}
				}
			}
			if !answered {
				protocol.WriteNull(conn)
			}
			continue
		}

		if command == "SET" || command == "DEL" {
			owner := replicas[0]
			lockAcquired := false

			if owner == myAddr {
				lockAcquired = dlm.TryLock(key, 5*time.Second)
			} else {
				lockResp, err := forwardCommandToNode(owner, []string{"_LOCK", key}, users)
				if err == nil && string(lockResp) == "+OK\r\n" {
					lockAcquired = true
				}
			}

			if !lockAcquired {
				protocol.WriteError(conn, "ERR resource is temporarily locked by another transaction")
				continue
			}

			for _, node := range replicas {
				if node == myAddr {
					if command == "SET" && len(parts) >= 3 {
						store.Set(key, parts[2])
					} else if command == "DEL" {
						store.Delete(key)
					}
				} else {
					syncParts := make([]string, len(parts))
					copy(syncParts, parts)
					if command == "SET" {
						syncParts[0] = "_SYNC_SET"
					} else if command == "DEL" {
						syncParts[0] = "_SYNC_DEL"
					}
					go forwardCommandToNode(node, syncParts, users)
					fmt.Printf("[REPLICATION] Sent backup of '%s' to %s\n", key, node)
				}
			}

			if owner == myAddr {
				dlm.Unlock(key)
			} else {
				go forwardCommandToNode(owner, []string{"_UNLOCK", key}, users)
			}

			protocol.WriteSimpleString(conn, "OK")
		}
	}
}

func forwardCommandToNode(node string, parts []string, users []UserConf) ([]byte, error) {
	conn, err := net.DialTimeout("tcp", node, 2*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var adminUser, adminPass string
	for _, u := range users {
		if u.Role == "admin" {
			adminUser = u.Username
			adminPass = u.Password
			break
		}
	}

	if adminUser != "" {
		authCmd := fmt.Sprintf("*3\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(adminUser), adminUser, len(adminPass), adminPass)
		conn.Write([]byte(authCmd))
		buf := make([]byte, 128)
		conn.Read(buf)
	}

	conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(parts))))
	for _, p := range parts {
		conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(p), p)))
	}

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}
