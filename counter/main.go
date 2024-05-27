package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"github.com/google/uuid"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type TopologyJson struct {
	topology map[string][]string `json:"topology"`
}

var topology map[string][]string

const key = "counter"

var ctx = context.Background()

func main() {
	n := maelstrom.NewNode()
	var values []float64

	values = make([]float64, 100)
	kv := maelstrom.NewSeqKV(n)

	n.Handle("init", func(msg maelstrom.Message) error {
		err := kv.Write(ctx, key, 0)
		if err != nil {
			return err
		}
		return nil
	})

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

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		var response map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		response = make(map[string]any)
		value, err := kv.ReadInt(ctx, key)
		if err != nil {
			return err
		}
		value = int(body["delta"].(float64)) + value
		err = kv.CompareAndSwap(ctx, key, value, value, true)
		if err != nil {
			return err
		}
		response["type"] = "add_ok"
		return n.Reply(msg, response)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		body = make(map[string]any)
		body["type"] = "read_ok"
		value, err := kv.ReadInt(ctx, key)
		if err != nil {
			return err
		}
		body["value"] = value
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

func isKeyNotExists(err error) bool {
	var rpcErr *maelstrom.RPCError
	if !errors.As(err, &rpcErr) {
		return false
	}
	return rpcErr.Code == maelstrom.KeyDoesNotExist
}
