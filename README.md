# Mini-Redis (Golang)

A lightweight, concurrent, in-memory key-value cache server built from scratch in Go, modeled after Redis. Features custom TCP connection handling, thread-safe memory storage, Redis Serialization Protocol (RESP) formatting, and dual-layer TTL expiration.

---

## 🚀 Features

- **Concurrent TCP Server:** Listens on `:6379` (default Redis port) and spawns an independent Goroutine for every connected client.
- **Thread-Safe Storage:** Uses `sync.RWMutex` to guarantee safe concurrent reads (`RLock`) and writes (`Lock`).
- **Dual Expiration Engine (TTL):**
  - **Passive Eviction:** Automatically verifies expiration on key retrieval (`GET`) and evicts expired keys.
  - **Active Janitor Routine:** A background Goroutine runs on a 5-second ticker to sweep the keyspace and clean up expired keys to prevent memory leaks.
- **RESP Wire Protocol:** Supports Redis Serialization Protocol response conventions (`+OK`, `+PONG`, `$-1`, bulk strings, integer responses `:1`, and `-ERR`).
- **Interactive CLI Client:** Includes a built-in terminal client with a REPL prompt (`miniredis> `) and pretty-printed RESP outputs.

---

## 🛠️ Architecture

```
[ Client 1 ] ---> (TCP Connection) --+
[ Client 2 ] ---> (TCP Connection) --+--> [ Goroutines per Conn ]
                                                    |
                                            (RESP Parser / Exec)
                                                    |
                                          +-------------------+
                                          |    CacheStore     |
                                          |  (sync.RWMutex)   |
                                          | map[string]Item   |
                                          +-------------------+
                                                    ^
                                                    |
                                        [ Background Janitor ]
                                        (Ticker: every 5 sec)
```

---

## 📋 Supported Commands

| Command | Syntax | Description | Example |
| :--- | :--- | :--- | :--- |
| `PING` | `PING` | Health check / connectivity probe | `PING` &rarr; `+PONG` |
| `SET` | `SET <key> <val> [EX <sec>]` | Store a key-value pair with optional TTL | `SET user Gurshan EX 10` &rarr; `+OK` |
| `GET` | `GET <key>` | Retrieve value by key (or `(nil)` if expired/missing) | `GET user` &rarr; `"Gurshan"` |
| `DEL` | `DEL <key>` | Delete a key from the store | `DEL user` &rarr; `(integer) 1` |
| `EXPIRE` | `EXPIRE <key> <seconds>` | Set an expiration timeout on an existing key | `EXPIRE session 30` &rarr; `(integer) 1` |
| `TTL` | `TTL <key>` | Get remaining time to live in seconds | `TTL session` &rarr; `(integer) 28` |
| `QUIT` | `QUIT` | Gracefully close client connection | `QUIT` &rarr; `Bye!` |

---

## 🏁 Quickstart

### 1. Clone the repository
```bash
git clone https://github.com/<your-username>/mini-redis.git
cd mini-redis
```

### 2. Start the Mini-Redis Server
In your first terminal:
```bash
go run main.go
```
*The server will start listening on `localhost:6379`.*

### 3. Connect with the Interactive Client
In a second terminal window:
```bash
go run ./client
```

You will see:
```text
Connected! Type commands (e.g. SET name Alice, GET name, TTL name, QUIT ,DEL name):
-------------------------------------------------------------------------
miniredis> SET greeting Hello
OK
miniredis> GET greeting
"Hello"
miniredis> SET token 12345 EX 5
OK
miniredis> TTL token
(integer) 4
miniredis> GET token
(nil)
```

---

## 📄 License
This project is open-source and available under the [MIT License](LICENSE).
