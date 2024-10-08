/*
Copyright [2014] - [2024] The Last.Backend authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package rabbitmq

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/lastbackend/toolkit/pkg/runtime"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ack struct{}
type reject struct{}

type CallHandler func(ctx context.Context, payload []byte)

type brokerOptions struct {
	Endpoint        string
	TLSVerify       bool
	TLSCert         string
	TLSKey          string
	PrefetchCount   int
	PrefetchGlobal  bool
	DefaultExchange *Exchange
}

type broker struct {
	mtx            sync.Mutex
	runtime        runtime.Runtime
	conn           *amqpConn
	opts           Config
	endpoints      []string
	prefetchCount  int
	prefetchGlobal bool
	exchange       Exchange

	handlers map[string][]CallHandler

	wg sync.WaitGroup
}

type message struct {
	Event   string `json:"event"`
	Payload string `json:"payload"`
}

func newBroker(runtime runtime.Runtime, opts Config) *broker {

	exchange := DefaultExchange
	if opts.DefaultExchange != nil {
		exchange = *opts.DefaultExchange
	}

	return &broker{
		runtime:   runtime,
		endpoints: []string{opts.DSN},
		opts:      opts,
		exchange:  exchange,
		handlers:  make(map[string][]CallHandler, 0),
	}
}

func (r *broker) Ack(ctx context.Context) error {
	fn, ok := ctx.Value(ack{}).(func(bool) error)
	if !ok {
		return errors.New("no acknowledged")
	}
	return fn(false)
}

func (r *broker) Reject(ctx context.Context) error {
	fn, ok := ctx.Value(reject{}).(func(bool) error)
	if !ok {
		return errors.New("no rejected")
	}
	return fn(false)
}

func (r *broker) RejectAndRequeue(ctx context.Context) error {
	fn, ok := ctx.Value(reject{}).(func(bool) error)
	if !ok {
		return errors.New("no rejected")
	}
	return fn(true)
}

func (r *broker) Publish(ctx context.Context, exchange, event string, payload []byte, opts *PublishOptions) error {

	e := message{
		Event:   event,
		Payload: string(payload),
	}

	body, err := json.Marshal(e)
	if err != nil {
		return err
	}

	if opts == nil {
		opts = new(PublishOptions)
	}

	m := amqp.Publishing{
		Body:    body,
		Type:    fmt.Sprintf("%s:%s", exchange, event),
		Headers: amqp.Table{},
	}

	if opts.Headers != nil {
		for k, v := range opts.Headers {
			m.Headers[k] = v
		}
	}

	if r.conn == nil {
		return errors.New("connection is nil")
	}

	return r.conn.Publish(ctx, exchange, "*", m)
}

func (r *broker) Subscribe(exchange, queue, event string, handler CallHandler, opts *SubscribeOptions) (Subscriber, error) {
	if r.conn == nil {
		return nil, errors.New("not connected")
	}

	var handlerIndex = -1

	r.mtx.Lock()
	key := fmt.Sprintf("%s:%s", exchange, event)
	if _, ok := r.handlers[key]; !ok {
		r.handlers[key] = make([]CallHandler, 0)
	}

	handlerIndex = len(r.handlers[key])
	r.handlers[key] = append(r.handlers[key], handler)
	r.mtx.Unlock()

	if opts == nil {
		opts = new(SubscribeOptions)
	}

	c := consumer{
		runtime:      r.runtime,
		exchange:     exchange,
		queue:        queue,
		key:          "*",
		autoAck:      true,
		broker:       r,
		headers:      opts.Headers,
		durableQueue: opts.DurableQueue,
		fn: func(msg amqp.Delivery) {
			ctx := context.Background()
			e := message{}
			json.Unmarshal(msg.Body, &e)

			var (
				handlers []CallHandler
				ok       bool
			)

			r.mtx.Lock()
			handlers, ok = r.handlers[msg.Type]
			if !ok {
				return
			}
			r.mtx.Unlock()

			headers := make(map[string]string)
			for k, v := range msg.Headers {
				headers[k], _ = v.(string)
			}

			ctx = context.WithValue(ctx, "headers", headers)

			for _, h := range handlers {
				h(ctx, []byte(e.Payload))
			}
		},
	}

	c.unsubscribe = func() {
		if handlerIndex > -1 {
			r.mtx.Lock()
			r.handlers[key] = append(r.handlers[key][:handlerIndex], r.handlers[key][handlerIndex+1:]...)
			r.mtx.Unlock()
			handlerIndex = -1
		}
	}

	go c.consume()

	return &c, nil
}

func (r *broker) Connected() error {
	return r.conn.Connected()
}

func (r *broker) Channel() (*amqp.Channel, error) {
	return r.conn.Channel()
}

func (r *broker) Connect() error {
	if r.conn == nil {
		r.conn = newConnection(r.runtime, r.exchange, r.endpoints, r.opts.PrefetchCount, r.opts.PrefetchGlobal)
	}

	conf := defaultAmqpConfig

	if r.opts.TLSVerify {
		cer, err := tls.X509KeyPair([]byte(r.opts.TLSCert), []byte(r.opts.TLSKey))
		if err != nil {
			return err
		}
		conf.TLSClientConfig = &tls.Config{Certificates: []tls.Certificate{cer}}
	}

	conf.Properties = amqp.Table{
		"connection_name": r.runtime.Meta().GetName(),
	}

	return r.conn.Connect(r.opts.TLSVerify, &conf)
}

func (r *broker) Disconnect() error {
	if r.conn == nil {
		return errors.New("not connected")
	}

	err := r.conn.Close()

	r.wg.Wait()

	return err
}
