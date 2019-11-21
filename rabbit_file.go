package main

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/coveooss/multilogger/errors"
)

// ReadRabbitFile load a RabbitMQ index or persistent store file in RAM
func ReadRabbitFile(fileName string, reMatch *regexp.Regexp) (result RabbitFile, err error) {
	defer func() {
		if err = errors.Trap(err, recover()); err != nil {
			err = fmt.Errorf("Error %v while processing %s", err, fileName)
		}
	}()
	data, err := ioutil.ReadFile(fileName)
	return RabbitFile{
		blob: RabbitBlob{
			data:   data,
			name:   fileName,
			useLen: strings.HasSuffix(fileName, ".rdq"),
		},
		match: reMatch,
		Stat:  Statistic{Name: fileName},
	}, err
}

// RabbitFile is a structure representing the data of a rabbit Index or persistent store file
type RabbitFile struct {
	blob     RabbitBlob
	Messages []*RabbitMessage
	Stat     Statistic
	Queues   Statistics
	match    *regexp.Regexp
}

// Name returns the name of the current file
func (rf *RabbitFile) Name() string { return rf.blob.name }

// Type returns the type of the file
func (rf *RabbitFile) Type() string { return strings.TrimPrefix(filepath.Ext(rf.Name()), ".") }

// Count returns the number of messages in the file
func (rf *RabbitFile) Count() int { return len(rf.Messages) }

// Size returns the total size of messages in the file
func (rf *RabbitFile) Size() float64 { return rf.Stat.Sum() }

// ProcessMessages scan a file to extract all messages
func (rf *RabbitFile) ProcessMessages(handler func(*RabbitMessage)) {
	rf.blob.ProcessMessages(func(msg *RabbitMessage) {
		if rf.match != nil {
			if !rf.match.MatchString(msg.Queue) {
				return
			}
		}
		rf.Messages = append(rf.Messages, msg)
		rf.Stat.Add(msg.Length)
		rf.Queues.Add(msg.Queue, msg.Length)
		if handler != nil {
			handler(msg)
		}
	})
}
