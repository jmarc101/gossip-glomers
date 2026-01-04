package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type (
	request struct {
		Source string `json:"src"`
		Dest   string `json:"dest"`
		Body   Body   `json:"body"`
	}

	Body struct {
		Msg_type string `json:"type"`
		MsgId    int    `json:"msg_id"`
		Echo     string `json:"echo"`
	}
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("echo", func(msg maelstrom.Message) error {
		var body Body
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body.Msg_type = "echo_ok"
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
