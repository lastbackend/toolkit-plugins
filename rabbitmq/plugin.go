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
	"fmt"
	"sync"
	"time"

	"github.com/lastbackend/toolkit/pkg/runtime"
	"github.com/lastbackend/toolkit/pkg/runtime/logger"
	"github.com/lastbackend/toolkit/pkg/tools/probes"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	defaultPrefix = "rabbitmq"
)

const (
	errNotReceiveExpectedResponse = "did not receive expected response"
	errTimedOutWaiting            = "timed out waiting for a response"
)

type Plugin interface {
	Publish(ctx context.Context, event string, payload []byte, opts *PublishOptions) error
	Subscribe(service, event string, handler CallHandler, opts *SubscribeOptions) (Subscriber, error)
	Channel() (*amqp.Channel, error)
}

type Options struct {
	Name string
}

type Config struct {
	DSN      string `env:"DSN" envDefault:"" comment:"DSN = complete connection string (amqp://guest:guest@127.0.0.1:5672)"`
	Host     string `env:"HOST" envDefault:"127.0.0.1"  comment:"The host to connect to (required)"`
	Port     int32  `env:"PORT" envDefault:"5672" comment:"The port to bind to (default: 5672)"`
	Vhost    string `env:"VHOST" envDefault:"/" comment:"The default host connect to (default: / )"`
	Username string `env:"USERNAME" comment:"The username to connect with (not required, guest by default)"`
	Password string `env:"PASSWORD" comment:"The password to connect with(not required, guest by default) "`

	TLSVerify bool   `env:"TLS_VERIFY" comment:"Use SSL in rabbitmq connection"`
	TLSCA     string `env:"TLS_CA" comment:"TLS CA file content used in connection"`
	TLSCert   string `env:"TLS_CERT" comment:"TLS Cert file content used in connection"`
	TLSKey    string `env:"TLS_KEY"  comment:"TLS Key file content used in connection"`

	PrefetchCount  int  `env:"PREFETCH_COUNT"  comment:"Limit the number of unacknowledged messages on a channel (or connection) when consuming"`
	PrefetchGlobal bool `env:"PREFETCH_GLOBAL"  comment:"Set prefetch limit number globally"`

	DefaultExchange *Exchange
}

type plugin struct {
	sync.RWMutex

	log     logger.Logger
	runtime runtime.Runtime

	ctx    context.Context
	cancel context.CancelFunc

	prefix  string
	service string

	opts Config

	broker *broker
}

func NewPlugin(runtime runtime.Runtime, opts *Options) Plugin {
	p := new(plugin)

	p.runtime = runtime
	p.log = runtime.Log()
	p.service = p.runtime.Meta().GetName()
	p.prefix = opts.Name
	if p.prefix == "" {
		p.prefix = defaultPrefix
	}

	if err := runtime.Config().Parse(&p.opts, p.prefix); err != nil {
		return nil
	}

	return p
}

type TestConfig struct {
	Config

	RunContainer   bool
	ContainerImage string
	ContainerName  string
}

func NewTestPlugin(ctx context.Context, cfg TestConfig) (Plugin, error) {

	opts := cfg

	if opts.DSN == "" {
		if opts.Host == "" {
			return nil, fmt.Errorf("DSN or Host environment variable required but not set")
		}
		opts.DSN = fmt.Sprintf("amqp://%s:%s@%s:%d%s",
			opts.Username, opts.Password, opts.Host, opts.Port, opts.Vhost)
	}

	if opts.RunContainer {
		if opts.ContainerImage == "" {
			opts.ContainerImage = "rabbitmq:management-alpine"
		}
		if opts.ContainerName == "" {
			opts.ContainerName = "rabbitmq-test-container"
		}

		strategy := wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(5 * time.Second)

		req := testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Name:         opts.ContainerName,
				Image:        opts.ContainerImage,
				ExposedPorts: []string{"5672/tcp", "15672/tcp"},
				Env: map[string]string{
					"RABBITMQ_DEFAULT_USER": "user",
					"RABBITMQ_DEFAULT_PASS": "pass",
				},
				WaitingFor: strategy,
			},
			Reuse: true,
		}

		container, err := testcontainers.GenericContainer(ctx, req)
		if err != nil {
			return nil, err
		}

		host, err := container.Host(ctx)
		if err != nil {
			return nil, err
		}
		realPort, err := container.MappedPort(ctx, "5672")
		if err != nil {
			return nil, err
		}

		opts.DSN = fmt.Sprintf("amqp://user:pass@%s:%s%s", host, realPort.Port(), opts.Vhost)
	}

	p := new(plugin)
	p.opts = opts.Config
	p.opts.DefaultExchange = &Exchange{
		Name:    p.service,
		Durable: true,
	}

	p.broker = newBroker(p.runtime, p.opts)

	if err := p.broker.Connect(); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *plugin) PreStart(ctx context.Context) error {

	p.ctx, p.cancel = context.WithCancel(ctx)

	if p.opts.DSN == "" {
		if p.opts.Host == "" {
			return fmt.Errorf("%s_DSN or %s_Host environment variable required but not set",
				p.prefix, p.prefix)
		}
		p.opts.DSN = fmt.Sprintf("amqp://%s:%s@%s:%d%s",
			p.opts.Username, p.opts.Password, p.opts.Host, p.opts.Port, p.opts.Vhost)
	}

	p.opts.DefaultExchange = &Exchange{
		Name:    p.service,
		Durable: true,
	}

	p.broker = newBroker(p.runtime, p.opts)

	if err := p.broker.Connect(); err != nil {
		return err
	}

	p.runtime.Tools().Probes().RegisterCheck(p.prefix, probes.ReadinessProbe, checkRabbitMQ(p.broker, 1*time.Second))
	p.runtime.Tools().Probes().RegisterCheck(p.prefix, probes.LivenessProbe, checkRabbitMQ(p.broker, 1*time.Second))

	return nil
}

func (p *plugin) OnStop(context.Context) error {
	return p.broker.Disconnect()
}

func (p *plugin) Publish(ctx context.Context, event string, payload []byte, opts *PublishOptions) error {
	return p.broker.Publish(ctx, p.service, event, payload, opts)
}

func (p *plugin) Subscribe(service, event string, handler CallHandler, opts *SubscribeOptions) (Subscriber, error) {
	queue := fmt.Sprintf("%s:events", service)
	return p.broker.Subscribe(service, queue, event, handler, opts)
}

func (p *plugin) Channel() (*amqp.Channel, error) {
	return p.broker.Channel()
}

func checkRabbitMQ(broker *broker, timeout time.Duration) probes.HandleFunc {
	return func() error {
		correlationID := "readiness_check"

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if broker == nil {
			return fmt.Errorf("connection is nil")
		}

		channel, err := broker.Channel()
		if err != nil {
			return err
		}
		defer channel.Close()

		queue, err := channel.QueueDeclare(
			"",    // an empty string creates a temporary queue with a unique name
			false, // durable
			true,  // delete when unused (очередь будет удалена при закрытии соединения)
			true,  // exclusive
			false, // no-wait
			nil,   // arguments
		)
		if err != nil {
			return fmt.Errorf("failed to declare a queue: %v", err)
		}

		msgs, err := channel.Consume(
			queue.Name, // queue
			"",         // consumer
			true,       // auto-ack
			false,      // exclusive
			false,      // no-local
			false,      // no-wait
			nil,        // args
		)
		if err != nil {
			return fmt.Errorf("failed to register a consumer: %v", err)
		}

		err = channel.PublishWithContext(
			ctx,
			"",         // exchange
			queue.Name, // routing key
			false,      // mandatory
			false,      // immediate
			amqp.Publishing{
				ContentType:   "text/plain",
				Body:          []byte("readiness probe"),
				CorrelationId: correlationID,
				ReplyTo:       queue.Name,
			})
		if err != nil {
			return fmt.Errorf("failed to publish a message: %v", err)
		}

		defer func() {
			channel.QueueDelete(
				queue.Name, // queue name
				false,      // ifUnused
				false,      // ifEmpty
				false,      // no-wait
			)
		}()

		select {
		case <-ctx.Done():
			return fmt.Errorf(errTimedOutWaiting)
		case msg := <-msgs:
			if msg.CorrelationId == correlationID {
				return nil
			}
		case <-time.After(timeout):
			return fmt.Errorf(errTimedOutWaiting)
		}

		return fmt.Errorf(errNotReceiveExpectedResponse)
	}
}
