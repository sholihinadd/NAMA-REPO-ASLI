package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type SignalMessage struct {
	Type      string      `json:"type"`
	ClientID  string      `json:"client_id"`
	TargetID  string      `json:"target_id,omitempty"`
	SDP       interface{} `json:"sdp,omitempty"`
	Candidate interface{} `json:"candidate,omitempty"`
}

type Router struct {
	mu      sync.RWMutex
	clients map[string]*websocket.Conn
}

func main() {
	router := &Router{clients: make(map[string]*websocket.Conn)}

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("Upgrade error:", err)
			return
		}

		var currentClientID string

		defer func() {
			if currentClientID != "" {
				router.mu.Lock()
				delete(router.clients, currentClientID)
				router.mu.Unlock()
				log.Printf("[Signaling] Client %s terputus.\n", currentClientID)
			}
			conn.Close()
		}()

		for {
			_, msgBytes, err := conn.ReadMessage()
			if err != nil {
				break
			}

			var msg SignalMessage
			if err := json.Unmarshal(msgBytes, &msg); err != nil {
				continue
			}

			// Registrasi Client ID pertama kali terhubung
			if msg.Type == "register" {
				currentClientID = msg.ClientID
				router.mu.Lock()
				router.clients[currentClientID] = conn
				router.mu.Unlock()
				log.Printf("[Signaling] Client terdaftar: %s\n", currentClientID)
				continue
			}

			// Routing pesan ke TargetID spesifik
			if msg.TargetID != "" {
				router.mu.RLock()
				targetConn, exists := router.clients[msg.TargetID]
				router.mu.RUnlock()

				if exists {
					_ = targetConn.WriteMessage(websocket.TextMessage, msgBytes)
				} else {
					log.Printf("[Signaling] Target ID %s tidak ditemukan untuk pesan dari %s\n", msg.TargetID, msg.ClientID)
				}
			}
		}
	})

	log.Println("[Signaling] Router berjalan di ws://172.28.57.180:8080/ws")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
