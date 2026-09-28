package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/gordonklaus/portaudio"
	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"gopkg.in/hraban/opus.v2"
)

const (
	TargetServerID  = "server-jakarta"
	sampleRate      = 48000
	channels        = 1
	frameSizeMs     = 20
	samplesPerFrame = sampleRate * frameSizeMs / 1000 // 960 samples
)

type SignalMessage struct {
	Type      string                     `json:"type"`
	ClientID  string                     `json:"client_id"`
	TargetID  string                     `json:"target_id,omitempty"`
	SDP       *webrtc.SessionDescription `json:"sdp,omitempty"`
	Candidate *webrtc.ICECandidateInit   `json:"candidate,omitempty"`
}

// ----------------------------------------------------
// 1. FUNGSI UTAMA (WebRTC + Signaling)
// ----------------------------------------------------
func main() {
	clientID := flag.String("id", "bali-01", "ID unik untuk pengirim ini")
	flag.Parse()

	log.Printf("[Sender %s] Memulai inisialisasi pengirim audio...\n", *clientID)

	m := &webrtc.MediaEngine{}
	opusCodec := webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeOpus,
			ClockRate:   48000,
			Channels:    1,
			SDPFmtpLine: "minptime=10;useinbandfec=1;usedtx=0",
		},
		PayloadType: 111,
	}

	if err := m.RegisterCodec(opusCodec, webrtc.RTPCodecTypeAudio); err != nil {
		log.Fatal(err)
	}

	api := webrtc.NewAPI(webrtc.WithMediaEngine(m))

	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{URLs: []string{"stun:stun.l.google.com:19302"}},
		},
	}

	peerConnection, err := api.NewPeerConnection(config)
	if err != nil {
		log.Fatal("Gagal membuat PeerConnection:", err)
	}
	defer peerConnection.Close()

	audioTrack, err := webrtc.NewTrackLocalStaticSample(
		opusCodec.RTPCodecCapability, "audio", "pion-"+*clientID,
	)
	if err != nil {
		log.Fatal(err)
	}

	if _, err = peerConnection.AddTrack(audioTrack); err != nil {
		log.Fatal(err)
	}

	ws, _, err := websocket.DefaultDialer.Dial("ws://172.28.57.180:8080/ws", nil)
	if err != nil {
		log.Fatal("Gagal terhubung ke signaling:", err)
	}
	defer ws.Close()

	regMsg, _ := json.Marshal(SignalMessage{Type: "register", ClientID: *clientID})
	ws.WriteMessage(websocket.TextMessage, regMsg)

	peerConnection.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			candidate := c.ToJSON()
			msg, _ := json.Marshal(SignalMessage{
				Type:      "candidate",
				ClientID:  *clientID,
				TargetID:  TargetServerID,
				Candidate: &candidate,
			})
			ws.WriteMessage(websocket.TextMessage, msg)
		}
	})

	go func() {
		for {
			_, message, err := ws.ReadMessage()
			if err != nil {
				return
			}

			var sig SignalMessage
			if err := json.Unmarshal(message, &sig); err != nil {
				continue
			}

			if sig.Type == "answer" && sig.SDP != nil {
				log.Printf("[Sender %s] Menerima SDP Answer dari Server Jakarta\n", *clientID)
				_ = peerConnection.SetRemoteDescription(*sig.SDP)
			} else if sig.Type == "candidate" && sig.Candidate != nil {
				_ = peerConnection.AddICECandidate(*sig.Candidate)
			}
		}
	}()

	offer, err := peerConnection.CreateOffer(nil)
	if err != nil {
		log.Fatal(err)
	}
	if err = peerConnection.SetLocalDescription(offer); err != nil {
		log.Fatal(err)
	}

	offerMsg, _ := json.Marshal(SignalMessage{
		Type:     "offer",
		ClientID: *clientID,
		TargetID: TargetServerID,
		SDP:      &offer,
	})
	ws.WriteMessage(websocket.TextMessage, offerMsg)
	log.Printf("[Sender %s] Mengirimkan SDP Offer ke %s...\n", *clientID, TargetServerID)

	// Memanggil fungsi mikrofon
	err = StartMicrophoneStream(audioTrack)
	if err != nil {
		log.Fatal("Gagal memulai streaming mikrofon:", err)
	}

	select {}
}

// ----------------------------------------------------
// 2. FUNGSI CAPTURE MIKROFON (Kode yang Anda tanyakan)
// ----------------------------------------------------
func StartMicrophoneStream(audioTrack *webrtc.TrackLocalStaticSample) error {
	if err := portaudio.Initialize(); err != nil {
		return fmt.Errorf("gagal inisialisasi PortAudio: %w", err)
	}

	encoder, err := opus.NewEncoder(sampleRate, channels, opus.Application(opus.AppVoIP))
	if err != nil {
		return fmt.Errorf("gagal membuat Opus Encoder: %w", err)
	}

	_ = encoder.SetInBandFEC(true)
	_ = encoder.SetDTX(false)
	_ = encoder.SetBitrate(48000)

	pcmBuffer := make([]int16, samplesPerFrame)

	stream, err := portaudio.OpenDefaultStream(channels, 0, float64(sampleRate), len(pcmBuffer), pcmBuffer)
	if err != nil {
		return fmt.Errorf("gagal membuka stream mikrofon: %w", err)
	}

	if err := stream.Start(); err != nil {
		return fmt.Errorf("gagal menjalankan stream mikrofon: %w", err)
	}

	log.Println("[Audio Capture] Mikrofon fisik aktif! Mengirimkan audio real-time...")

	opusBuffer := make([]byte, 1024)

	go func() {
		defer func() {
			_ = stream.Stop()
			_ = stream.Close()
			_ = portaudio.Terminate()
		}()

		for {
			if err := stream.Read(); err != nil {
				log.Println("[Audio Capture] Error membaca dari mikrofon:", err)
				continue
			}

			n, err := encoder.Encode(pcmBuffer, opusBuffer)
			if err != nil {
				log.Println("[Audio Capture] Error encoding Opus:", err)
				continue
			}

			err = audioTrack.WriteSample(media.Sample{
				Data:     opusBuffer[:n],
				Duration: 20 * time.Millisecond,
			})
			if err != nil {
				log.Println("[Audio Capture] Error mengirimkan sample WebRTC:", err)
				return
			}
		}
	}()

	return nil
}
