package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	messages            = map[int]struct{}{}
	mu                  sync.Mutex
	neighbors           = []string{}
	broadcastRetryCount = 25
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("topology", topologyHandler(n))
	n.Handle("read", readHandler(n))
	n.Handle("broadcast", broadcastHandler(n))

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type (
	topologyRequest struct {
		Type  string              `json:"type"`
		MsgID any                 `json:"msg_id"`
		Topo  map[string][]string `json:"topology"`
	}

	topologyResponse struct {
		Type  string `json:"type"`
		MsgID any    `json:"msg_id"`
	}
)

func topologyHandler(n *maelstrom.Node) func(maelstrom.Message) error {
	return func(msg maelstrom.Message) error {
		var body topologyRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Topology tells us our neighbors
		// e.g., body.Topo["n1"] = ["n2", "n3"]
		neighbors = body.Topo[n.ID()]

		log.Printf("Received topology: %+v", body.Topo)

		return n.Reply(msg, topologyResponse{
			Type:  "topology_ok",
			MsgID: body.MsgID,
		})
	}
}

type (
	readRequest struct {
		Type  string `json:"type"`
		MsgID any    `json:"msg_id"`
	}

	readResponse struct {
		Type     string `json:"type"`
		MsgID    any    `json:"msg_id"`
		Messages []int  `json:"messages"`
	}
)

func readHandler(n *maelstrom.Node) func(maelstrom.Message) error {
	return func(msg maelstrom.Message) error {
		var body readRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		messagesCopy := make([]int, 0, len(messages))
		for msg := range messages {
			messagesCopy = append(messagesCopy, msg)
		}
		mu.Unlock()

		return n.Reply(msg, readResponse{
			Type:     "read_ok",
			MsgID:    body.MsgID,
			Messages: messagesCopy,
		})
	}
}

type (
	broadcastBody struct {
		Type    string  `json:"type"`
		Message float64 `json:"message"`
		ID      int     `json:"msg_id"`
	}

	broadcastResponse struct {
		Type  string `json:"type"`
		MsgID int    `json:"msg_id"`
	}
)

func broadcastHandler(n *maelstrom.Node) func(maelstrom.Message) error {
	return func(msg maelstrom.Message) error {
		var body broadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		messages[int(body.Message)] = struct{}{}
		mu.Unlock()

		// Forward to neighbors
		// Skip sending to direct neighbor that sent the message to avoid loops
		for _, id := range neighbors {
			if id == msg.Src {
				continue
			}

			// This function is just a simple retry mechanism without backoff
			// in a real system we would want to implement exponential backoff and jitter
			// to avoid overwhelming the network. Here we keep simple given context.
			go func(retryCount int) {
				for range retryCount {
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					defer cancel()

					msg, err := n.SyncRPC(ctx, id, body)
					if err == nil || msg.Type() == "broadcast_ok" {
						break
					}
				}
			}(broadcastRetryCount)
		}

		return n.Reply(msg, broadcastResponse{
			Type:  "broadcast_ok",
			MsgID: body.ID,
		})
	}
}
