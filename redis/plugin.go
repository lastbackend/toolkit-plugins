/*
Copyright [2014] - [2025] The Last.Backend authors.

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

package redis

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/lastbackend/toolkit/pkg/runtime"
	"github.com/lastbackend/toolkit/pkg/tools/probes"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	redistestcontainers "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	defaultPrefix        = "REDIS"
	defaultEndpoint      = ":6379"
	defaultImage         = "redis:7-alpine"
	defaultContainerName = "redis-test-container"
)

type Config struct {
	Endpoint string `env:"ENDPOINT" envDefault:":6379" comment:"Endpoint = host:port,host:port addresses of ring shards."`

	Cluster bool `env:"CLUSTER" comment:"Cluster = enable cluster mode"`

	Database int `env:"DATABASE" required:"true" comment:"Database to be selected after connecting to the server."`

	Username string `env:"USERNAME" comment:"Use the specified Username to authenticate the current connection with one of the connections defined in the ACL list when connecting to a Redis 6.0 instance, or greater, that is using the Redis ACL system."`

	Password string `env:"PASSWORD" comment:"Optional password. Must match the password specified in the requirepass server configuration option (if connecting to a Redis 5.0 instance, or lower), or the User Password when connecting to a Redis 6.0 instance, or greater, that is using the Redis ACL system."`

	HeartbeatFrequency time.Duration `env:"HEARNEAT_FREQUENCY" comment:"Frequency of PING commands sent to check shards availability. Shard is considered down after 3 subsequent failed checks."`

	MaxRetries int `env:"MAX_RETRIES" comment:"Maximum number of retries before giving up. Default is 3 retries; -1 (not 0) disables retries."`

	MinRetryBackoff time.Duration `env:"MIN_RETRY_BACKOFF" comment:"Minimum backoff between each retry. Default is 8 milliseconds; -1 disables backoff."`

	MaxRetryBackoff time.Duration `env:"MAX_RETRY_BACKOFF" comment:"Maximum backoff between each retry. Default is 512 milliseconds; -1 disables backoff."`

	DialTimeout time.Duration `env:"DIAL_TIMEOUT" comment:"Dial timeout for establishing new connections. Default is 5 seconds."`

	ReadTimeout time.Duration `env:"READ_TIMEOUT" comment:"Timeout for socket reads. If reached, commands will fail with a timeout instead of blocking. Use value -1 for no timeout and 0 for default. Default is 3 seconds."`

	WriteTimeout time.Duration `env:"WRITE_TIMEOUT" comment:"Timeout for socket writes. If reached, commands will fail with a timeout instead of blocking. Default is Read Timeout."`

	PoolSize int `env:"POOL_SIZE" comment:"Maximum number of socket connections. Default is 10 connections per every CPU as reported by runtime.NumCPU."`

	MinIdleConns int `env:"MIN_IDLE_CONNS" comment:"Minimum number of idle connections which is useful when establishing new connection is slow."`

	PoolTimeout time.Duration `env:"POOL_TIMEOUT" comment:"Amount of time client waits for connection if all connections are busy before returning an error. Default is ReadTimeout + 1 second."`

	// TODO: need to implement the ability to install tls
	// TLS Config to use. When set TLS will be negotiated.
	TLSConfig *tls.Config
}

// TestConfig extends Config with additional testing-specific options
type TestConfig struct {
	Config

	// RunContainer indicates whether to start a test container
	RunContainer bool
	// ContainerImage specifies custom Redis image for test container
	ContainerImage string
	// ContainerName sets custom name for test container
	ContainerName string
	// ConnAttempts sets number of connection attempts
	ConnAttempts int
	// ConnTimeout sets connection timeout
	ConnTimeout time.Duration
}

// RedisContainer defines interface for Redis container management
type RedisContainer interface {
	GetConnectionString(ctx context.Context) (string, error)
	Close(ctx context.Context) error
}

type Plugin interface {
	DB() *redis.Client
	ClusterDB() *redis.ClusterClient
	Client() redis.Cmdable // Add this method for compatibility with caching system
	Print()
}

type Options struct {
	Name string
}

type plugin struct {
	prefix  string
	runtime runtime.Runtime

	opts Config
	db   *redis.Client
	cdb  *redis.ClusterClient

	//probe toolkit.Probe
}

func NewPlugin(runtime runtime.Runtime, opts *Options) Plugin {

	p := new(plugin)

	p.runtime = runtime
	p.prefix = opts.Name
	if p.prefix == "" {
		p.prefix = defaultPrefix
	}

	if err := runtime.Config().Parse(&p.opts, p.prefix); err != nil {
		return nil
	}

	return p
}

// NewTestPlugin creates a plugin instance configured for testing
func NewTestPlugin(ctx context.Context, cfg TestConfig) (Plugin, error) {
	// Set default connection attempts and timeout if not specified
	if cfg.ConnAttempts == 0 {
		cfg.ConnAttempts = 10
	}
	if cfg.ConnTimeout == 0 {
		cfg.ConnTimeout = 15 * time.Second
	}

	// Set default database if not specified
	if cfg.Database == 0 {
		cfg.Database = 0 // Redis default database
	}

	if cfg.RunContainer {
		endpoint, err := runRedisContainer(ctx, cfg)
		if err != nil {
			return nil, err
		}
		cfg.Endpoint = endpoint
	}

	p := &plugin{opts: cfg.Config}
	if err := p.initPlugin(ctx); err != nil {
		return nil, err
	}

	return p, nil
}

// runRedisContainer starts a Redis container for testing
func runRedisContainer(ctx context.Context, cfg TestConfig) (string, error) {
	image := getImage(cfg.ContainerImage)
	containerName := getContainerName(cfg.ContainerName)

	containerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Name:         containerName,
			Image:        image,
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor: wait.ForLog("Ready to accept connections").
				WithStartupTimeout(30 * time.Second),
		},
		Reuse: true,
	}

	// Add password configuration if specified
	if cfg.Password != "" {
		containerReq.Cmd = []string{"redis-server", "--requirepass", cfg.Password}
	}

	container, err := redistestcontainers.RunContainer(ctx,
		testcontainers.CustomizeRequest(containerReq),
	)
	if err != nil {
		return "", fmt.Errorf("failed to start redis container: %w", err)
	}

	cleanup := func() {
		if err := container.Terminate(context.Background()); err != nil {
			log.Printf("failed to terminate container: %v", err)
		}
	}

	// Handle cleanup on context cancellation
	go func() {
		<-ctx.Done()
		cleanup()
	}()

	// Get connection string and convert from redis:// format to host:port
	endpoint, err := container.ConnectionString(ctx)
	if err != nil {
		cleanup()
		return "", fmt.Errorf("failed to get connection string: %w", err)
	}

	// Convert redis://localhost:port to localhost:port
	if strings.HasPrefix(endpoint, "redis://") {
		endpoint = strings.TrimPrefix(endpoint, "redis://")
	}

	return endpoint, nil
}

func (p *plugin) DB() *redis.Client {
	return p.db
}

func (p *plugin) ClusterDB() *redis.ClusterClient {
	return p.cdb
}

// Client returns the appropriate Redis client interface
func (p *plugin) Client() redis.Cmdable {
	if p.opts.Cluster && p.cdb != nil {
		return p.cdb
	}
	return p.db
}

func (p *plugin) PreStart(ctx context.Context) (err error) {
	if err := p.initPlugin(ctx); err != nil {
		return err
	}

	// Register health check probes
	if p.runtime != nil {
		if p.opts.Cluster && p.cdb != nil {
			p.runtime.Tools().Probes().RegisterCheck(p.prefix, probes.ReadinessProbe, redisClusterPingChecker(p.cdb, 1*time.Second))
			p.runtime.Tools().Probes().RegisterCheck(p.prefix, probes.LivenessProbe, redisClusterPingChecker(p.cdb, 1*time.Second))
		} else if p.db != nil {
			p.runtime.Tools().Probes().RegisterCheck(p.prefix, probes.ReadinessProbe, redisPingChecker(p.db, 1*time.Second))
			p.runtime.Tools().Probes().RegisterCheck(p.prefix, probes.LivenessProbe, redisPingChecker(p.db, 1*time.Second))
		}
	}

	return nil
}

func (p *plugin) initPlugin(ctx context.Context) error {
	if p.opts.Cluster {
		client := redis.NewClusterClient(p.prepareClusterOptions(p.opts))
		p.cdb = client
	} else {
		client := redis.NewClient(p.prepareOptions(p.opts))
		p.db = client
	}

	return nil
}

func (p *plugin) OnStop(context.Context) error {
	if p.db != nil {
		return p.db.Close()
	}
	if p.cdb != nil {
		return p.cdb.Close()
	}
	return nil
}

func (p *plugin) prepareOptions(opts Config) *redis.Options {

	addr := defaultEndpoint
	if len(opts.Endpoint) > 0 {
		opts.Endpoint = strings.Replace(opts.Endpoint, " ", "", -1)
		addr = strings.Split(opts.Endpoint, ",")[0]
	}

	return &redis.Options{
		Addr:            addr,
		Username:        opts.Username,
		Password:        opts.Password,
		DB:              opts.Database,
		MaxRetries:      opts.MaxRetries,
		MinRetryBackoff: opts.MinRetryBackoff,
		MaxRetryBackoff: opts.MaxRetryBackoff,
		DialTimeout:     opts.DialTimeout,
		ReadTimeout:     opts.ReadTimeout,
		WriteTimeout:    opts.WriteTimeout,
		PoolSize:        opts.PoolSize,
		MinIdleConns:    opts.MinIdleConns,
		PoolTimeout:     opts.PoolTimeout,
		TLSConfig:       opts.TLSConfig,
	}
}

func (p *plugin) prepareClusterOptions(opts Config) *redis.ClusterOptions {

	addrs := []string{defaultEndpoint}
	if len(opts.Endpoint) > 0 {
		opts.Endpoint = strings.Replace(opts.Endpoint, " ", "", -1)
		addrs = strings.Split(opts.Endpoint, ",")
	}

	return &redis.ClusterOptions{
		Addrs:           addrs,
		Username:        opts.Username,
		Password:        opts.Password,
		MaxRetries:      opts.MaxRetries,
		MinRetryBackoff: opts.MinRetryBackoff,
		MaxRetryBackoff: opts.MaxRetryBackoff,
		DialTimeout:     opts.DialTimeout,
		ReadTimeout:     opts.ReadTimeout,
		WriteTimeout:    opts.WriteTimeout,
		PoolSize:        opts.PoolSize,
		MinIdleConns:    opts.MinIdleConns,
		PoolTimeout:     opts.PoolTimeout,
		TLSConfig:       opts.TLSConfig,
	}
}

func (p *plugin) Print() {
	if p.runtime != nil {
		p.runtime.Config().Print(p.opts, p.prefix)
	}
}

// Helper functions
func getImage(image string) string {
	if image == "" {
		return defaultImage
	}
	return image
}

func getContainerName(name string) string {
	if name == "" {
		return defaultContainerName
	}
	return name
}

func redisClusterPingChecker(client *redis.ClusterClient, timeout time.Duration) probes.HandleFunc {
	return func() error {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return client.Ping(ctx).Err()
	}
}

func redisPingChecker(client *redis.Client, timeout time.Duration) probes.HandleFunc {
	return func() error {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return client.Ping(ctx).Err()
	}
}
