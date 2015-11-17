// The MIT License (MIT)
//
// Copyright (c) 2015 Seagate Technology
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//
// @author = ["Ignacio Corderi"]

package kinetic

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"net"

	"github.com/y-okubo/kinetic-go/kinetic/network"
	kproto "github.com/y-okubo/kinetic-go/kinetic/proto"
)

// Default credentials
var (
	USER_ID       = int64(1)
	CLIENT_SECRET = []byte("asdfasdf")
)

func calculate_hmac(secret []byte, bytes []byte) []byte {
	mac := hmac.New(sha1.New, secret)

	ln := make([]byte, 4)
	binary.BigEndian.PutUint32(ln, uint32(len(bytes)))

	mac.Write(ln)
	mac.Write(bytes)

	return mac.Sum(nil)
}

type RemoteError struct {
	status kproto.Command_Status
}

func (e RemoteError) Error() string {
	return fmt.Sprintf("%v: %v", e.status.Code, *e.status.StatusMessage)
}

type Client interface {
	Put(key []byte, value []byte) (<-chan interface{}, <-chan error, error)
	PutFrom(key []byte, length int, queue <-chan []byte) (<-chan error, error)
	Delete(key []byte) (<-chan interface{}, <-chan error, error)
	Get(key []byte) (<-chan interface{}, <-chan error, error)
	GetVersion(key []byte) (<-chan interface{}, <-chan error, error)
	GetNextKey(key []byte) (<-chan interface{}, <-chan error, error)
	GetPreviousKey(key []byte) (<-chan interface{}, <-chan error, error)
	GetKeyRange(startKey []byte, endKey []byte) (<-chan interface{}, <-chan error, error)

	Close()
}

type PendingOperation struct {
	sequence int64
	error    chan error
	value    chan interface{}
}

type NetworkClient struct {
	connectionId int64
	userId       int64
	secret       []byte
	sequence     int64
	conn         net.Conn
	closed       bool
	error        error
	notifier     chan<- PendingOperation
}

func Connect(target string) (Client, error) {
	conn, err := net.Dial("tcp", target)
	if err != nil {
		return nil, err
	}
	// hanshake
	_, cmd, _, err := network.Receive(conn)
	if err != nil {
		return nil, err
	}

	ch := make(chan PendingOperation)

	c := &NetworkClient{connectionId: *cmd.Header.ConnectionID,
		userId:   USER_ID,
		secret:   CLIENT_SECRET,
		sequence: 1,
		conn:     conn,
		closed:   false,
		error:    nil,
		notifier: ch}

	go c.listen(ch)
	return c, nil
}

func (self *NetworkClient) listen(notifications <-chan PendingOperation) {
	pending := make(map[int64]PendingOperation) // pendings
	for {
		_, cmd, value, err := network.Receive(self.conn)
		if err != nil {
			if !self.closed {
				self.error = err
				// TODO: try closing socket
			}
			break
		}

		var response error
		if *cmd.Status.Code != kproto.Command_Status_SUCCESS {
			response = RemoteError{status: *cmd.Status}
		}

		// Notify caller
		// Seems more complicated than it should, but we are optimizing
		// for when we receive in order
		for {
			op := <-notifications
			// Chances are, it's in order
			if op.sequence == *cmd.Header.AckSequence {
				switch *cmd.Header.MessageType {
				case kproto.Command_PUT_RESPONSE:
					// PUT
					op.value <- *cmd.Status.Code
					op.error <- response
				case kproto.Command_DELETE_RESPONSE:
					// DELETE
					op.value <- *cmd.Status.Code
					op.error <- response
				case kproto.Command_GET_RESPONSE:
					// GET
					op.value <- value
					op.error <- response
				case kproto.Command_GETVERSION_RESPONSE:
					// Get Version
					op.value <- cmd.Body.KeyValue.DbVersion
					op.error <- response
				case kproto.Command_GETNEXT_RESPONSE:
					// Get Next
					op.value <- cmd.Body.KeyValue.Key
					op.error <- response
				case kproto.Command_GETPREVIOUS_RESPONSE:
					// Get Previous
					op.value <- cmd.Body.KeyValue.Key
					op.error <- response
				case kproto.Command_GETKEYRANGE_RESPONSE:
					// Get Key Range
					op.value <- cmd.Body.Range.Keys
					op.error <- response
				default:
					// Others
					op.value <- *cmd.Status.Code
					op.error <- response
				}
				break
			} else {
				// Either we missed it or it hasnt arrived yet.
				pending[op.sequence] = op
				op, ok := pending[*cmd.Header.AckSequence]
				if ok { // this is the case where we missed it
					op.value <- value
					op.error <- response
					delete(pending, op.sequence)
					break
				}
			}
		}
	}

	// Notify all pendings that we are closed for business
}

// Client implementation

func (self *NetworkClient) Put(key []byte, value []byte) (<-chan interface{}, <-chan error, error) {
	cmd := &kproto.Command{
		Header: &kproto.Command_Header{
			ConnectionID: proto.Int64(self.connectionId),
			Sequence:     proto.Int64(self.sequence),
			MessageType:  kproto.Command_PUT.Enum(),
		},
		Body: &kproto.Command_Body{
			KeyValue: &kproto.Command_KeyValue{
				Key:             key,
				Algorithm:       kproto.Command_SHA1.Enum(),
				Tag:             make([]byte, 0),
				Synchronization: kproto.Command_WRITEBACK.Enum(),
			},
		},
	}

	cmd_bytes, err := proto.Marshal(cmd)
	if err != nil {
		return nil, nil, err
	}

	msg := &kproto.Message{
		AuthType: kproto.Message_HMACAUTH.Enum(),
		HmacAuth: &kproto.Message_HMACauth{
			Identity: proto.Int64(self.userId),
			Hmac:     calculate_hmac(self.secret, cmd_bytes),
		},
		CommandBytes: cmd_bytes,
	}

	err = network.Send(self.conn, msg, value)
	if err != nil {
		return nil, nil, err
	}

	errChan := make(chan error, 1)
	valChan := make(chan interface{}, 1)
	pending := PendingOperation{sequence: self.sequence, error: errChan, value: valChan}

	self.notifier <- pending

	self.sequence += 1

	return valChan, errChan, nil
}

// TOOD: needs refactor
func (self *NetworkClient) PutFrom(key []byte, length int, queue <-chan []byte) (<-chan error, error) {
	cmd := &kproto.Command{
		Header: &kproto.Command_Header{
			ConnectionID: proto.Int64(self.connectionId),
			Sequence:     proto.Int64(self.sequence),
			MessageType:  kproto.Command_PUT.Enum(),
		},
		Body: &kproto.Command_Body{
			KeyValue: &kproto.Command_KeyValue{
				Key:             key,
				Algorithm:       kproto.Command_SHA1.Enum(),
				Tag:             make([]byte, 0),
				Synchronization: kproto.Command_WRITEBACK.Enum(),
			},
		},
	}

	cmd_bytes, err := proto.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	msg := &kproto.Message{
		AuthType: kproto.Message_HMACAUTH.Enum(),
		HmacAuth: &kproto.Message_HMACauth{
			Identity: proto.Int64(self.userId),
			Hmac:     calculate_hmac(self.secret, cmd_bytes),
		},
		CommandBytes: cmd_bytes,
	}

	err = network.SendFrom(self.conn, msg, length, queue)
	if err != nil {
		return nil, err
	}

	rx := make(chan error, 1)
	pending := PendingOperation{sequence: self.sequence, error: rx}

	self.notifier <- pending

	self.sequence += 1

	return rx, nil
}

func (self *NetworkClient) Delete(key []byte) (<-chan interface{}, <-chan error, error) {
	cmd := &kproto.Command{
		Header: &kproto.Command_Header{
			ConnectionID: proto.Int64(self.connectionId),
			Sequence:     proto.Int64(self.sequence),
			MessageType:  kproto.Command_DELETE.Enum(),
		},
		Body: &kproto.Command_Body{
			KeyValue: &kproto.Command_KeyValue{
				Key:             key,
				Algorithm:       kproto.Command_SHA1.Enum(),
				Tag:             make([]byte, 0),
				Synchronization: kproto.Command_WRITEBACK.Enum(),
			},
		},
	}

	cmd_bytes, err := proto.Marshal(cmd)
	if err != nil {
		return nil, nil, err
	}

	msg := &kproto.Message{
		AuthType: kproto.Message_HMACAUTH.Enum(),
		HmacAuth: &kproto.Message_HMACauth{
			Identity: proto.Int64(self.userId),
			Hmac:     calculate_hmac(self.secret, cmd_bytes),
		},
		CommandBytes: cmd_bytes,
	}

	err = network.Send(self.conn, msg, nil)
	if err != nil {
		return nil, nil, err
	}

	error := make(chan error, 1)
	value := make(chan interface{}, 1)
	pending := PendingOperation{sequence: self.sequence, error: error, value: value}

	self.notifier <- pending

	self.sequence += 1

	return value, error, nil
}

// Method GET
func (self *NetworkClient) Get(key []byte) (<-chan interface{}, <-chan error, error) {
	cmd := &kproto.Command{
		Header: &kproto.Command_Header{
			ConnectionID: proto.Int64(self.connectionId),
			Sequence:     proto.Int64(self.sequence),
			MessageType:  kproto.Command_GET.Enum(),
		},
		Body: &kproto.Command_Body{
			KeyValue: &kproto.Command_KeyValue{
				Key:             key,
				Algorithm:       kproto.Command_SHA1.Enum(),
				Tag:             make([]byte, 0),
				Synchronization: kproto.Command_WRITEBACK.Enum(),
			},
		},
	}

	cmd_bytes, err := proto.Marshal(cmd)
	if err != nil {
		return nil, nil, err
	}

	msg := &kproto.Message{
		AuthType: kproto.Message_HMACAUTH.Enum(),
		HmacAuth: &kproto.Message_HMACauth{
			Identity: proto.Int64(self.userId),
			Hmac:     calculate_hmac(self.secret, cmd_bytes),
		},
		CommandBytes: cmd_bytes,
	}

	err = network.Send(self.conn, msg, nil)
	if err != nil {
		return nil, nil, err
	}

	errChan := make(chan error, 1)
	valChan := make(chan interface{}, 1)
	pending := PendingOperation{sequence: self.sequence, error: errChan, value: valChan}

	self.notifier <- pending

	self.sequence += 1

	return valChan, errChan, nil
}

// Method Get Version
func (self *NetworkClient) GetVersion(key []byte) (<-chan interface{}, <-chan error, error) {
	metadataOnly := true

	cmd := &kproto.Command{
		Header: &kproto.Command_Header{
			ConnectionID: proto.Int64(self.connectionId),
			Sequence:     proto.Int64(self.sequence),
			MessageType:  kproto.Command_GETVERSION.Enum(),
		},
		Body: &kproto.Command_Body{
			KeyValue: &kproto.Command_KeyValue{
				Key:          key,
				MetadataOnly: &metadataOnly,
			},
		},
	}

	cmd_bytes, err := proto.Marshal(cmd)
	if err != nil {
		return nil, nil, err
	}

	msg := &kproto.Message{
		AuthType: kproto.Message_HMACAUTH.Enum(),
		HmacAuth: &kproto.Message_HMACauth{
			Identity: proto.Int64(self.userId),
			Hmac:     calculate_hmac(self.secret, cmd_bytes),
		},
		CommandBytes: cmd_bytes,
	}

	err = network.Send(self.conn, msg, nil)
	if err != nil {
		return nil, nil, err
	}

	errChan := make(chan error, 1)
	valChan := make(chan interface{}, 1)
	pending := PendingOperation{sequence: self.sequence, error: errChan, value: valChan}

	self.notifier <- pending

	self.sequence += 1

	return valChan, errChan, nil
}

// Method Get Next
func (self *NetworkClient) GetNextKey(key []byte) (<-chan interface{}, <-chan error, error) {
	metadataOnly := true

	cmd := &kproto.Command{
		Header: &kproto.Command_Header{
			ConnectionID: proto.Int64(self.connectionId),
			Sequence:     proto.Int64(self.sequence),
			MessageType:  kproto.Command_GETNEXT.Enum(),
		},
		Body: &kproto.Command_Body{
			KeyValue: &kproto.Command_KeyValue{
				Key:          key,
				MetadataOnly: &metadataOnly,
			},
		},
	}

	cmd_bytes, err := proto.Marshal(cmd)
	if err != nil {
		return nil, nil, err
	}

	msg := &kproto.Message{
		AuthType: kproto.Message_HMACAUTH.Enum(),
		HmacAuth: &kproto.Message_HMACauth{
			Identity: proto.Int64(self.userId),
			Hmac:     calculate_hmac(self.secret, cmd_bytes),
		},
		CommandBytes: cmd_bytes,
	}

	err = network.Send(self.conn, msg, nil)
	if err != nil {
		return nil, nil, err
	}

	errChan := make(chan error, 1)
	valChan := make(chan interface{}, 1)
	pending := PendingOperation{sequence: self.sequence, error: errChan, value: valChan}

	self.notifier <- pending

	self.sequence += 1

	return valChan, errChan, nil
}

// Method Get Previous
func (self *NetworkClient) GetPreviousKey(key []byte) (<-chan interface{}, <-chan error, error) {
	metadataOnly := true

	cmd := &kproto.Command{
		Header: &kproto.Command_Header{
			ConnectionID: proto.Int64(self.connectionId),
			Sequence:     proto.Int64(self.sequence),
			MessageType:  kproto.Command_GETPREVIOUS.Enum(),
		},
		Body: &kproto.Command_Body{
			KeyValue: &kproto.Command_KeyValue{
				Key:          key,
				MetadataOnly: &metadataOnly,
			},
		},
	}

	cmd_bytes, err := proto.Marshal(cmd)
	if err != nil {
		return nil, nil, err
	}

	msg := &kproto.Message{
		AuthType: kproto.Message_HMACAUTH.Enum(),
		HmacAuth: &kproto.Message_HMACauth{
			Identity: proto.Int64(self.userId),
			Hmac:     calculate_hmac(self.secret, cmd_bytes),
		},
		CommandBytes: cmd_bytes,
	}

	err = network.Send(self.conn, msg, nil)
	if err != nil {
		return nil, nil, err
	}

	errChan := make(chan error, 1)
	valChan := make(chan interface{}, 1)
	pending := PendingOperation{sequence: self.sequence, error: errChan, value: valChan}

	self.notifier <- pending

	self.sequence += 1

	return valChan, errChan, nil
}

// Method Get Key Range
func (self *NetworkClient) GetKeyRange(startKey []byte, endKey []byte) (<-chan interface{}, <-chan error, error) {
	var startKeyInclusive bool = true
	var endKeyInclusive bool = true
	var maxReturned int32 = 200
	var reverse bool = false

	cmd := &kproto.Command{
		Header: &kproto.Command_Header{
			ConnectionID: proto.Int64(self.connectionId),
			Sequence:     proto.Int64(self.sequence),
			MessageType:  kproto.Command_GETKEYRANGE.Enum(),
		},
		Body: &kproto.Command_Body{
			Range: &kproto.Command_Range{
				StartKey:          startKey,
				StartKeyInclusive: &startKeyInclusive,
				EndKey:            endKey,
				EndKeyInclusive:   &endKeyInclusive,
				MaxReturned:       &maxReturned,
				Reverse:           &reverse,
			},
		},
	}

	cmd_bytes, err := proto.Marshal(cmd)
	if err != nil {
		return nil, nil, err
	}

	msg := &kproto.Message{
		AuthType: kproto.Message_HMACAUTH.Enum(),
		HmacAuth: &kproto.Message_HMACauth{
			Identity: proto.Int64(self.userId),
			Hmac:     calculate_hmac(self.secret, cmd_bytes),
		},
		CommandBytes: cmd_bytes,
	}

	err = network.Send(self.conn, msg, nil)
	if err != nil {
		return nil, nil, err
	}

	errChan := make(chan error, 1)
	valChan := make(chan interface{}, 1)
	pending := PendingOperation{sequence: self.sequence, error: errChan, value: valChan}

	self.notifier <- pending

	self.sequence += 1

	return valChan, errChan, nil
}

func (self *NetworkClient) Close() {
	self.closed = true
	self.conn.Close()
}
