package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var messages = []int{}
var mu sync.Mutex

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
		Type  string         `json:"type"`
		MsgID any            `json:"msg_id"`
		Topo  map[string]any `json:"topology"`
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
		messagesCopy := make([]int, len(messages))
		copy(messagesCopy, messages)
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
		messages = append(messages, int(body.Message))
		mu.Unlock()

		for _, id := range n.NodeIDs() {
			if n.ID() == id {
				continue
			}

			err := n.Send(id, body)
			if err != nil {
				log.Fatalf(err.Error())
			}
		}

		return n.Reply(msg, broadcastResponse{
			Type:  "broadcast_ok",
			MsgID: body.ID,
		})
	}
}
