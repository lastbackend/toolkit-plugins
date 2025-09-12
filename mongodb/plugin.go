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

package mongodb

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"time"

	"github.com/lastbackend/toolkit/pkg/runtime"
	"github.com/lastbackend/toolkit/pkg/runtime/logger"
	"github.com/lastbackend/toolkit/pkg/tools/probes"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	defaultPrefix        = "MONGODB"
	defaultEndpoint      = "mongodb://localhost:27017"
	defaultImage         = "mongo:7"
	defaultContainerName = "mongodb-test-container"
	defaultDatabase      = "test"
)

type Config struct {
	URI      string `env:"URI" envDefault:"mongodb://localhost:27017" comment:"URI = complete MongoDB connection string"`
	Host     string `env:"HOST" envDefault:"localhost" comment:"The host to connect to (required)"`
	Port     int32  `env:"PORT" envDefault:"27017" comment:"The port to bind to (default: 27017)"`
	Database string `env:"DATABASE" envDefault:"test" comment:"Database name to connect to"`
	Username string `env:"USERNAME" comment:"The username to connect with"`
	Password string `env:"PASSWORD" comment:"The password to connect with"`

	// Connection options
	MaxPoolSize              uint64        `env:"MAX_POOL_SIZE" comment:"Maximum number of connections in the connection pool"`
	MinPoolSize              uint64        `env:"MIN_POOL_SIZE" comment:"Minimum number of connections in the connection pool"`
	MaxIdleTimeMS            time.Duration `env:"MAX_IDLE_TIME_MS" comment:"Maximum time connections can remain idle in the pool"`
	ConnectTimeoutMS         time.Duration `env:"CONNECT_TIMEOUT_MS" comment:"Timeout for connection attempts"`
	ServerSelectionTimeoutMS time.Duration `env:"SERVER_SELECTION_TIMEOUT_MS" comment:"Timeout for server selection"`

	// TLS configuration
	TLSEnable             bool   `env:"TLS_ENABLE" comment:"Enable TLS connection"`
	TLSInsecureSkipVerify bool   `env:"TLS_INSECURE_SKIP_VERIFY" comment:"Skip TLS certificate verification"`
	TLSCAFile             string `env:"TLS_CA_FILE" comment:"Path to TLS CA file"`
	TLSCertFile           string `env:"TLS_CERT_FILE" comment:"Path to TLS certificate file"`
	TLSKeyFile            string `env:"TLS_KEY_FILE" comment:"Path to TLS key file"`

	// Authentication
	AuthSource string `env:"AUTH_SOURCE" comment:"Authentication source database"`
	ReplicaSet string `env:"REPLICA_SET" comment:"Replica set name"`

	// Additional options
	Compressors []string `env:"COMPRESSORS" comment:"Network compression algorithms (snappy, zlib, zstd)"`
}

// TestConfig extends Config with additional testing-specific options
type TestConfig struct {
	Config

	// RunContainer indicates whether to start a test container
	RunContainer bool
	// ContainerImage specifies custom MongoDB image for test container
	ContainerImage string
	// ContainerName sets custom name for test container
	ContainerName string
	// ConnAttempts sets number of connection attempts
	ConnAttempts int
	// ConnTimeout sets connection timeout
	ConnTimeout time.Duration
}

// MongoContainer defines interface for MongoDB container management
type MongoContainer interface {
	GetConnectionString(ctx context.Context) (string, error)
	Close(ctx context.Context) error
}

type Plugin interface {
	Client() *mongo.Client
	Database() *mongo.Database
	Collection(name string) *mongo.Collection
	Print()
}

type Options struct {
	Name string
}

type plugin struct {
	log     logger.Logger
	runtime runtime.Runtime

	prefix string
	opts   Config
	client *mongo.Client
	db     *mongo.Database
}

func NewPlugin(runtime runtime.Runtime, opts *Options) Plugin {
	p := new(plugin)

	p.runtime = runtime
	p.log = runtime.Log()
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
	if cfg.Database == "" {
		cfg.Database = defaultDatabase
	}

	if cfg.RunContainer {
		uri, err := runMongoContainer(ctx, cfg)
		if err != nil {
			return nil, err
		}
		cfg.URI = uri
	}

	p := &plugin{opts: cfg.Config}
	if err := p.initPlugin(ctx); err != nil {
		return nil, err
	}

	return p, nil
}

// runMongoContainer starts a MongoDB container for testing
func runMongoContainer(ctx context.Context, cfg TestConfig) (string, error) {
	image := getImage(cfg.ContainerImage)
	containerName := getContainerName(cfg.ContainerName)

	container, err := mongodb.RunContainer(ctx,
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Name:         containerName,
				Image:        image,
				ExposedPorts: []string{"27017/tcp"},
				Env: map[string]string{
					"MONGO_INITDB_ROOT_USERNAME": cfg.Username,
					"MONGO_INITDB_ROOT_PASSWORD": cfg.Password,
					"MONGO_INITDB_DATABASE":      cfg.Database,
				},
				WaitingFor: wait.ForLog("Waiting for connections").
					WithStartupTimeout(30 * time.Second),
			},
			Reuse: true,
		}),
	)
	if err != nil {
		return "", fmt.Errorf("failed to start mongodb container: %w", err)
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

	// Get connection string
	uri, err := container.ConnectionString(ctx)
	if err != nil {
		cleanup()
		return "", fmt.Errorf("failed to get connection string: %w", err)
	}

	return uri, nil
}

func (p *plugin) Client() *mongo.Client {
	return p.client
}

func (p *plugin) Database() *mongo.Database {
	return p.db
}

func (p *plugin) Collection(name string) *mongo.Collection {
	return p.db.Collection(name)
}

func (p *plugin) PreStart(ctx context.Context) error {
	if err := p.initPlugin(ctx); err != nil {
		return err
	}

	// Register health check probes
	if p.runtime != nil && p.client != nil {
		p.runtime.Tools().Probes().RegisterCheck(p.prefix, probes.ReadinessProbe, mongoPingChecker(p.client, 1*time.Second))
		p.runtime.Tools().Probes().RegisterCheck(p.prefix, probes.LivenessProbe, mongoPingChecker(p.client, 1*time.Second))
	}

	return nil
}

func (p *plugin) initPlugin(ctx context.Context) error {
	clientOptions := p.prepareClientOptions(p.opts)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Test the connection
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	p.client = client
	p.db = client.Database(p.opts.Database)

	return nil
}

func (p *plugin) OnStop(ctx context.Context) error {
	if p.client != nil {
		return p.client.Disconnect(ctx)
	}
	return nil
}

func (p *plugin) prepareClientOptions(opts Config) *options.ClientOptions {
	// Start with URI if provided, otherwise build from components
	clientOpts := options.Client()

	if opts.URI != "" {
		clientOpts.ApplyURI(opts.URI)
	} else {
		// Build URI from components
		uri := fmt.Sprintf("mongodb://%s:%d", opts.Host, opts.Port)
		if opts.Username != "" && opts.Password != "" {
			uri = fmt.Sprintf("mongodb://%s:%s@%s:%d", opts.Username, opts.Password, opts.Host, opts.Port)
		}
		if opts.AuthSource != "" {
			uri += "?authSource=" + opts.AuthSource
		}
		clientOpts.ApplyURI(uri)
	}

	// Set connection pool options
	if opts.MaxPoolSize > 0 {
		clientOpts.SetMaxPoolSize(opts.MaxPoolSize)
	}
	if opts.MinPoolSize > 0 {
		clientOpts.SetMinPoolSize(opts.MinPoolSize)
	}
	if opts.MaxIdleTimeMS > 0 {
		clientOpts.SetMaxConnIdleTime(opts.MaxIdleTimeMS)
	}
	if opts.ConnectTimeoutMS > 0 {
		clientOpts.SetConnectTimeout(opts.ConnectTimeoutMS)
	}
	if opts.ServerSelectionTimeoutMS > 0 {
		clientOpts.SetServerSelectionTimeout(opts.ServerSelectionTimeoutMS)
	}

	// Set authentication source
	if opts.AuthSource != "" {
		auth := options.Credential{
			AuthSource: opts.AuthSource,
			Username:   opts.Username,
			Password:   opts.Password,
		}
		clientOpts.SetAuth(auth)
	}

	// Set replica set
	if opts.ReplicaSet != "" {
		clientOpts.SetReplicaSet(opts.ReplicaSet)
	}

	// Set compressors
	if len(opts.Compressors) > 0 {
		clientOpts.SetCompressors(opts.Compressors)
	}

	// Configure TLS
	if opts.TLSEnable {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: opts.TLSInsecureSkipVerify,
		}

		// Load certificates if provided
		if opts.TLSCertFile != "" && opts.TLSKeyFile != "" {
			cert, err := tls.LoadX509KeyPair(opts.TLSCertFile, opts.TLSKeyFile)
			if err == nil {
				tlsConfig.Certificates = []tls.Certificate{cert}
			}
		}

		clientOpts.SetTLSConfig(tlsConfig)
	}

	return clientOpts
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

func mongoPingChecker(client *mongo.Client, timeout time.Duration) probes.HandleFunc {
	return func() error {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		if client == nil {
			return fmt.Errorf("client is nil")
		}

		return client.Ping(ctx, readpref.Primary())
	}
}
