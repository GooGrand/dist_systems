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
	logs := make(map[string][]int)
	commited_logs := make(map[string][]string)
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

	n.Handle("send", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		key := body["key"].(string)
		old := logs[key]
		logs[key] = append(old, body["msg"].(int))
		body["type"] = "send_ok"
		body["offset"] = len(logs[key])
		return n.Reply(msg, body)
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		offsets := body["offsets"].(map[string]int)
		msgs := make(map[string][][2]int)
		for key, val := range offsets {
			logs := logs[key][val:]
			var inner [][2]int
			for id, log := range logs {
				inner = append(inner, [2]int{val + id, log})
			}
			msgs[key] = inner
		}
		body["type"] = "poll_ok"
		body["msgs"] = msgs
		return n.Reply(msg, body)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		offsets := body["offsets"].(map[string]int)
		msgs := make(map[string][][2]int)
		for key, val := range offsets {
			logs := logs[key][val:]
			var inner [][2]int
			for id, log := range logs {
				inner = append(inner, [2]int{val + id, log})
			}
			msgs[key] = inner
		}
		body["type"] = "poll_ok"
		body["msgs"] = msgs
		return n.Reply(msg, body)
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
