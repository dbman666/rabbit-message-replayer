package main

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/coveo/gotemplate/errors"
)

const (
	rabbitHeaderBytes = "rabbit_framing_amqp_0_9_1"
	lenHeader         = len(rabbitHeaderBytes)
)

// RabbitBlob is a structure representing the data of a rabbit Index or persistent store file
type RabbitBlob struct {
	data   []byte
	pos    int
	name   string
	no     int
	useLen bool
}

// Name returns the name of the current blob
func (rb *RabbitBlob) Name() string { return fmt.Sprintf("%s:%d", rb.name, rb.no) }

// ProcessMessages scan a blob to extract all messages
func (rb *RabbitBlob) ProcessMessages(handler func(*RabbitMessage)) {
	defer func() {
		if err := errors.Trap(nil, recover()); err != nil {
			errors.Raise("Error %v while processing %s", err, rb.name)
		}
	}()

	for rb.pos < len(rb.data) {
		msg := RabbitMessage{Position: rb.pos}
		var blob *RabbitBlob
		if rb.useLen {
			msg.Length = int(rb.ReadUInt64())
			blob = &RabbitBlob{
				data: rb.ReadBytes(msg.Length),
				name: rb.name,
			}
			rb.AssertByte(0xff)
		} else {
			blob = rb
		}
		msgPos := bytes.Index(blob.data[blob.pos:], []byte(rabbitHeaderBytes))
		if msgPos == -1 {
			break
		}
		blob.pos += msgPos + lenHeader

		blob.AssertByte('l')
		nbBlocks := int(blob.ReadUInt32())
		switch nbBlocks {
		case 1:
			blob.AssertByte('m')
			msg.Length = int(blob.ReadUInt32())
			msg.Data = blob.ReadBytes(msg.Length)
		default:
			if !rb.useLen {
				errors.Raise("Expected only one blob when reading from an index file.")
			}
			msg.Data = make([]byte, 0, msg.Length)
			blocks := make([][]byte, nbBlocks)
			for i := range blocks {
				blob.AssertByte('m')
				blobLen := int(blob.ReadUInt32())
				blocks[i] = blob.ReadBytes(blobLen)
			}
			// We have to join blocks in reverse order
			for i := range blocks {
				msg.Data = append(msg.Data, blocks[nbBlocks-i-1]...)
			}
		}

		msg.Queue = must(msg.GetQueueName(rb.data)).(string)

		if handler != nil {
			handler(&msg)
		}
	}
}

// ReadUInt32 extract an uint32 from the current file
func (rb *RabbitBlob) ReadUInt32() (result uint32) {
	result = binary.BigEndian.Uint32(rb.data[rb.pos : rb.pos+8])
	rb.pos += 4
	return
}

// ReadUInt64 extract an uint64 from the current file
func (rb *RabbitBlob) ReadUInt64() (result uint64) {
	result = binary.BigEndian.Uint64(rb.data[rb.pos : rb.pos+8])
	rb.pos += 8
	return
}

// ReadBytes extract an array of bytes from the current file
func (rb *RabbitBlob) ReadBytes(len int) (result []byte) {
	result = rb.data[rb.pos : rb.pos+len]
	rb.pos += len
	return
}

// AssertByte extract a byte from the current file
func (rb *RabbitBlob) AssertByte(mustBe byte) {
	if rb.ReadBytes(1)[0] != mustBe {
		rb.pos--
		format := "%[1]X('%[1]c')"
		expected := fmt.Sprintf(format, mustBe)
		got := fmt.Sprintf(format, rb.data[rb.pos])
		errors.Raise("Expected %s but got %s at %d in %s", expected, got, rb.pos, rb.name)
	}
}
