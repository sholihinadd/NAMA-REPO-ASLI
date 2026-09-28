package main

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
)

const ServerID = "server-jakarta"

type SignalMessage struct {
	Type      string                     `json:"type"`
	ClientID  string                     `json:"client_id"`
	TargetID  string                     `json:"target_id,omitempty"`
	SDP       *webrtc.SessionDescription `json:"sdp,omitempty"`
	Candidate *webrtc.ICECandidateInit   `json:"candidate,omitempty"`
}

type MultiReceiver struct {
	mu          sync.Mutex
	connections map[string]*webrtc.PeerConnection
	ws          *websocket.Conn
}

func main() {
	ws, _, err := websocket.DefaultDialer.Dial("ws://172.28.57.180:8080/ws", nil)
	if err != nil {
		log.Fatal("Gagal terhubung ke signaling:", err)
	}
	defer ws.Close()

	mr := &MultiReceiver{
		connections: make(map[string]*webrtc.PeerConnection),
		ws:          ws,
	}

	// 1. Registrasikan Server ID ke Signaling
	regMsg, _ := json.Marshal(SignalMessage{Type: "register", ClientID: ServerID})
	ws.WriteMessage(websocket.TextMessage, regMsg)
	log.Printf("[%s] Terdaftar di signaling server, siap menerima multiple sender...\n", ServerID)

	// 2. Loop Utama Menerima Pesan dari Berbagai Sender
	for {
		_, message, err := ws.ReadMessage()
		if err != nil {
			log.Fatal("Koneksi signaling terputus:", err)
		}

		var sig SignalMessage
		if err := json.Unmarshal(message, &sig); err != nil {
			continue
		}

		switch sig.Type {
		case "offer":
			log.Printf("[%s] Menerima SDP Offer dari Sender: %s\n", ServerID, sig.ClientID)
			go mr.handleOffer(sig.ClientID, sig.SDP)

		case "candidate":
			if sig.Candidate != nil {
				mr.mu.Lock()
				pc, exists := mr.connections[sig.ClientID]
				mr.mu.Unlock()

				if exists {
					_ = pc.AddICECandidate(*sig.Candidate)
				}
			}
		}
	}
}

func (mr *MultiReceiver) handleOffer(senderID string, remoteSDP *webrtc.SessionDescription) {
	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{URLs: []string{"stun:stun.l.google.com:19302"}},
		},
	}

	peerConnection, err := webrtc.NewPeerConnection(config)
	if err != nil {
		log.Printf("[%s] Gagal membuat PeerConnection untuk %s: %v\n", ServerID, senderID, err)
		return
	}

	// Simpan koneksi berdasarkan senderID
	mr.mu.Lock()
	if oldPC, exists := mr.connections[senderID]; exists {
		oldPC.Close() // Tutup koneksi lama jika ada reconnection dari ID yang sama
	}
	mr.connections[senderID] = peerConnection
	mr.mu.Unlock()

	// Handler saat audio track diterima dari sender ini
	peerConnection.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		log.Printf("[%s] [STREAM MASUK] Stream Audio aktif dari Sender: %s (Codec: %s)\n", ServerID, senderID, track.Codec().MimeType)

		for {
			packet, _, readErr := track.ReadRTP()
			if readErr != nil {
				log.Printf("[%s] Stream audio dari %s terhenti: %v\n", ServerID, senderID, readErr)
				return
			}

			// Pemrosesan paket audio per senderID
			// packet.Payload mengandung Opus frame dari lokasi senderID terkait
			_ = packet.Payload
		}
	})

	// ICE Candidate Handling per Sender
	peerConnection.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			candidate := c.ToJSON()
			msg, _ := json.Marshal(SignalMessage{
				Type:      "candidate",
				ClientID:  ServerID,
				TargetID:  senderID,
				Candidate: &candidate,
			})
			mr.ws.WriteMessage(websocket.TextMessage, msg)
		}
	})

	// Set Remote SDP Offer
	if err := peerConnection.SetRemoteDescription(*remoteSDP); err != nil {
		log.Printf("[%s] SetRemoteDescription Error (%s): %v\n", ServerID, senderID, err)
		return
	}

	// Buat SDP Answer
	answer, err := peerConnection.CreateAnswer(nil)
	if err != nil {
		log.Printf("[%s] CreateAnswer Error (%s): %v\n", ServerID, senderID, err)
		return
	}

	if err = peerConnection.SetLocalDescription(answer); err != nil {
		log.Printf("[%s] SetLocalDescription Error (%s): %v\n", ServerID, senderID, err)
		return
	}

	// Kirim SDP Answer kembali ke senderID spesifik
	answerMsg, _ := json.Marshal(SignalMessage{
		Type:     "answer",
		ClientID: ServerID,
		TargetID: senderID,
		SDP:      &answer,
	})
	mr.ws.WriteMessage(websocket.TextMessage, answerMsg)
	log.Printf("[%s] Mengirimkan SDP Answer ke Sender: %s\n", ServerID, senderID)
}
