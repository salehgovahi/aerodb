package cluster

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"
)

type Cluster struct {
	Mu       sync.RWMutex
	Me       string
	Secret   []byte
	Peers    map[string]time.Time
	Suspects map[string]bool
}

func NewCluster(myAddr string, seedNode string, secret string) *Cluster {
	c := &Cluster{
		Me:       myAddr,
		Secret:   []byte(secret),
		Peers:    make(map[string]time.Time),
		Suspects: make(map[string]bool),
	}
	if seedNode != "" {
		c.Peers[seedNode] = time.Now()
	}
	return c
}

func (c *Cluster) StartListening(port string) {
	addr, _ := net.ResolveUDPAddr("udp", ":"+port)
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	fmt.Println("Gossip listener started on UDP port", port)
	buffer := make([]byte, 2048)

	for {
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil || n <= 32 {
			continue
		}

		signature := buffer[:32]
		payload := buffer[32:n]

		mac := hmac.New(sha256.New, c.Secret)
		mac.Write(payload)
		expectedSignature := mac.Sum(nil)

		if !hmac.Equal(signature, expectedSignature) {
			fmt.Println("⚠️ [SECURITY] Dropped spoofed gossip packet!")
			continue
		}

		var incomingPeers map[string]time.Time
		err = json.Unmarshal(payload, &incomingPeers)
		if err == nil {
			c.mergePeers(incomingPeers)
		}
	}
}

func (c *Cluster) sendGossip(targetAddr string, data map[string]time.Time) {
	addr, err := net.ResolveUDPAddr("udp", targetAddr)
	if err != nil {
		return
	}

	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return
	}
	defer conn.Close()

	payload, _ := json.Marshal(data)

	mac := hmac.New(sha256.New, c.Secret)
	mac.Write(payload)
	signature := mac.Sum(nil)

	finalPacket := append(signature, payload...)

	conn.Write(finalPacket)
}

func (c *Cluster) mergePeers(incoming map[string]time.Time) {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	for peerAddr := range incoming {
		if peerAddr == c.Me {
			continue
		}
		c.Peers[peerAddr] = time.Now()
	}
}

func (c *Cluster) StartGossiping() {
	ticker := time.NewTicker(1 * time.Second)

	for range ticker.C {
		c.Mu.RLock()
		peerCount := len(c.Peers)

		if peerCount == 0 {
			c.Mu.RUnlock()
			continue
		}

		peerList := make([]string, 0, peerCount)
		for p := range c.Peers {
			peerList = append(peerList, p)
		}

		randomPeer := peerList[rand.Intn(peerCount)]

		dataToSend := make(map[string]time.Time)
		for k, v := range c.Peers {
			dataToSend[k] = v
		}
		dataToSend[c.Me] = time.Now()
		c.Mu.RUnlock()

		c.sendGossip(randomPeer, dataToSend)
		c.CheckNodeHealth()
	}
}

func (c *Cluster) CheckNodeHealth() {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	now := time.Now()

	for addr, lastSeen := range c.Peers {
		duration := now.Sub(lastSeen)
		if duration > 30*time.Second {
			fmt.Printf("[DEAD] Node %s is dead. Removing from cluster.\n", addr)
			delete(c.Peers, addr)
			delete(c.Suspects, addr)
		} else if duration > 10*time.Second {
			if !c.Suspects[addr] {
				fmt.Printf("[SUSPECT] Node %s hasn't responded in 10s. Marking as suspect...\n", addr)
				c.Suspects[addr] = true
			}
		} else {
			if c.Suspects[addr] {
				fmt.Printf("[RECOVERED] Node %s is back online!\n", addr)
				c.Suspects[addr] = false
			}
		}
	}
}
