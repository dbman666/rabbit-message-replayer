package main

import (
	"bytes"
	"fmt"
)

const (
	defaultMethod = "Process"
)

// RabbitMessage represents a message that must be stored into RabbitMQ
type RabbitMessage struct {
	Queue            string
	Method           string
	Data             []byte
	Length, Position int
}

// IsPush determines if the current messsage comes from PushAPI (Coveo related)
func (msg *RabbitMessage) IsPush() bool { return msg.Data[0] != 'i' }

// GetQueueName retrieve the name of the queue that should be used
func (msg *RabbitMessage) GetQueueName(data []byte) (string, error) {
	blob := RabbitBlob{data: data[msg.Position:]}
	if blob.pos = bytes.Index(blob.data, []byte("exchange")); blob.pos >= 0 {
		blob.pos += 9
		len := blob.ReadUInt32()
		if len == 0 {
			blob.pos += 6
			len = blob.ReadUInt32()
		}

		return string(blob.ReadBytes(int(len))), nil
	}
	return "", fmt.Errorf("Unable to find queuename at position %d", msg.Position)
}

// GetMethod retrieve the method that should be used, defaults to "Process"
func (msg *RabbitMessage) GetMethod(data []byte) string {
	if !msg.IsPush() {
		return defaultMethod
	}

	blob := RabbitBlob{data: data[msg.Position:]}
	if blob.pos = bytes.Index(blob.data, []byte("method:")); blob.pos >= 0 {
		blob.pos += 7
		if len := bytes.Index(blob.data[blob.pos:], []byte(",")); len != -1 {
			return string(blob.ReadBytes(len))
		}
	}
	return defaultMethod
}
