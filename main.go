package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Item represents a cached value with an optional expiration timestamp.
type Item struct {
	Value     string
	ExpiresAt time.Time
}

// IsExpired checks if the item had a TTL and has passed its expiration time.
func (item Item) IsExpired() bool {
	if item.ExpiresAt.IsZero() {
		return false
	}
	return time.Now().After(item.ExpiresAt)
}

// CacheStore is a thread-safe in-memory key-value store.
type CacheStore struct {
	mu   sync.RWMutex
	data map[string]Item
}

func NewCacheStore() *CacheStore {
	return &CacheStore{
		data: make(map[string]Item),
	}
}

// Set stores a key-value pair, with an optional TTL.
func (s *CacheStore) Set(key, value string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	s.data[key] = Item{
		Value:     value,
		ExpiresAt: expiresAt,
	}
}

// Get retrieves a value by key. Returns false if the key doesn't exist or expired.
func (s *CacheStore) Get(key string) (string, bool) {
	s.mu.RLock()
	item, exists := s.data[key]
	s.mu.RUnlock()

	if !exists {
		return "", false
	}

	// Passive expiration check: delete if expired
	if item.IsExpired() {
		s.Delete(key)
		return "", false
	}

	return item.Value, true
}

// Delete removes a key from the store. Returns true if the key existed.
func (s *CacheStore) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.data[key]
	if exists {
		delete(s.data, key)
		return true
	}
	return false
}

// Expire sets a TTL (in seconds) on an existing key.
func (s *CacheStore) Expire(key string, seconds int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, exists := s.data[key]
	if !exists || item.IsExpired() {
		return false
	}

	item.ExpiresAt = time.Now().Add(time.Duration(seconds) * time.Second)
	s.data[key] = item
	return true
}

// TTL returns the remaining seconds until expiration:
// -1 if the key exists but has no expiration.
// -2 if the key does not exist or expired.
func (s *CacheStore) TTL(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	item, exists := s.data[key]
	if !exists || item.IsExpired() {
		return -2
	}

	if item.ExpiresAt.IsZero() {
		return -1
	}

	remaining := int(time.Until(item.ExpiresAt).Seconds())
	if remaining < 0 {
		return -2
	}
	return remaining
}

// StartJanitor runs a background Goroutine that cleans expired keys every interval.
func (s *CacheStore) StartJanitor(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			s.mu.Lock()
			now := time.Now()
			for k, item := range s.data {
				if !item.ExpiresAt.IsZero() && now.After(item.ExpiresAt) {
					delete(s.data, k)
				}
			}
			s.mu.Unlock()
		}
	}()
}

// handleConnection processes commands for a single connected TCP client.
func handleConnection(conn net.Conn, store *CacheStore) {
	defer conn.Close()

	clientAddr := conn.RemoteAddr().String()
	log.Printf("[+] Client connected: %s\n", clientAddr)
	defer log.Printf("[-] Client disconnected: %s\n", clientAddr)

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("Connection error (%s): %v\n", clientAddr, err)
			}
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "PING":
			writer.WriteString("+PONG\r\n")

		case "SET":
			if len(parts) < 3 {
				writer.WriteString("-ERR wrong number of arguments for 'set'\r\n")
			} else {
				key := parts[1]
				val := parts[2]
				var ttl time.Duration

				// Supports: SET key val EX 10
				if len(parts) >= 5 && strings.ToUpper(parts[3]) == "EX" {
					sec, err := strconv.Atoi(parts[4])
					if err == nil && sec > 0 {
						ttl = time.Duration(sec) * time.Second
					}
				}

				store.Set(key, val, ttl)
				writer.WriteString("+OK\r\n")
			}

		case "GET":
			if len(parts) != 2 {
				writer.WriteString("-ERR wrong number of arguments for 'get'\r\n")
			} else {
				val, found := store.Get(parts[1])
				if !found {
					writer.WriteString("$-1\r\n") // RESP format for nil
				} else {
					writer.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))
				}
			}

		case "DEL":
			if len(parts) != 2 {
				writer.WriteString("-ERR wrong number of arguments for 'del'\r\n")
			} else {
				if store.Delete(parts[1]) {
					writer.WriteString(":1\r\n")
				} else {
					writer.WriteString(":0\r\n")
				}
			}

		case "EXPIRE":
			if len(parts) != 3 {
				writer.WriteString("-ERR wrong number of arguments for 'expire'\r\n")
			} else {
				sec, err := strconv.Atoi(parts[2])
				if err != nil {
					writer.WriteString("-ERR value is not an integer\r\n")
				} else {
					if store.Expire(parts[1], sec) {
						writer.WriteString(":1\r\n")
					} else {
						writer.WriteString(":0\r\n")
					}
				}
			}

		case "TTL":
			if len(parts) != 2 {
				writer.WriteString("-ERR wrong number of arguments for 'ttl'\r\n")
			} else {
				remaining := store.TTL(parts[1])
				writer.WriteString(fmt.Sprintf(":%d\r\n", remaining))
			}

		case "QUIT":
			writer.WriteString("+OK\r\n")
			writer.Flush()
			return

		default:
			writer.WriteString(fmt.Sprintf("-ERR unknown command '%s'\r\n", cmd))
		}

		writer.Flush()
	}
}

func main() {
	port := ":6379"
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to bind port %s: %v\n", port, err)
	}
	defer listener.Close()

	store := NewCacheStore()
	// Background Janitor Goroutine: cleans expired keys every 5 seconds
	store.StartJanitor(5 * time.Second)

	fmt.Println("==================================================")
	fmt.Println("  MINI-REDIS (Golang In-Memory Key-Value Server)  ")
	fmt.Printf("  Listening on TCP port %s                     \n", port)
	fmt.Println("  Supported commands: PING, SET, GET, DEL,        ")
	fmt.Println("                      EXPIRE, TTL, QUIT          ")
	fmt.Println("==================================================")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept failed: %v\n", err)
			continue
		}

		// Spawn a lightweight Goroutine for each connected client
		go handleConnection(conn, store)
	}
}
