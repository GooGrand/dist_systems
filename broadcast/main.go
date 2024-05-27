package main

import (
	"encoding/json"
	"log"

	"github.com/google/uuid"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type TopologyJson struct {
	topology map[string][]string `json:"topology"`
}

var topology map[string][]string

func main() {
	n := maelstrom.NewNode()
	var values []float64

	values = make([]float64, 100)

	n.Handle("echo", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "echo_ok"
		return n.Reply(msg, body)
	})

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		id := uuid.NewString()

		body["type"] = "generate_ok"
		body["id"] = id
		return n.Reply(msg, body)
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		var response map[string]any
		response = make(map[string]any)

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		msg_id := len(values)
		values = append(values, body["message"].(float64))
		response["type"] = "broadcast_ok"
		response["msg_id"] = msg_id
		return n.Reply(msg, response)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		body = make(map[string]any)
		body["type"] = "read_ok"
		body["messages"] = make([]int, 100)
		body["messages"] = values
		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var response map[string]any
		response = make(map[string]any)
		body := new(TopologyJson)
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		topology = make(map[string][]string)
		topology = body.topology
		response["type"] = "topology_ok"
		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
