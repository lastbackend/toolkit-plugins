package postgres_pgx

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4"
	mpgx "github.com/golang-migrate/migrate/v4/database/pgx"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lastbackend/toolkit/pkg/runtime"
	"github.com/lastbackend/toolkit/pkg/tools/probes"
	"github.com/pkg/errors"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/lib/pq"
)

const (
	defaultPrefix        = "psql"
	driverName           = "postgres"
	errMissingConnString = "missing connection string"
	defaultImage         = "postgres:15.2"
	defaultContainerName = "postgres-test-container"
)

// Plugin represents a PostgreSQL database plugin interface
type Plugin interface {
	// DB returns the database connection pool
	DB() *pgxpool.Pool
	// RunMigration executes database migrations from the configured directory
	RunMigration() error
}

// Options contains plugin initialization options
type Options struct {
	// Name is the prefix used for configuration variables
	Name string
}

// Config defines the database configuration parameters
type Config struct {
	DSN               string        `env:"DSN" envDefault:"" comment:"DSN = postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...] complete connection string"`
	Host              string        `env:"HOST" envDefault:"127.0.0.1" comment:"The host to connect to"`
	Port              int32         `env:"PORT" envDefault:"5432" comment:"The port to connect to"`
	Database          string        `env:"DATABASE" envDefault:"postgres" comment:"Database name"`
	Username          string        `env:"USERNAME" envDefault:"postgres" comment:"The username to connect with"`
	Password          string        `env:"PASSWORD" envDefault:"" comment:"The password to connect with"`
	SSLMode           string        `env:"SSLMODE" envDefault:"disable" comment:"SSL mode (disable, allow, prefer, require, verify-ca, verify-full)"`
	MaxPoolSize       int           `env:"MAX_POOL_SIZE" envDefault:"50" comment:"Max pool size"`
	MinPoolSize       int           `env:"MIN_POOL_SIZE" envDefault:"10" comment:"Min pool size"`
	MaxConnLifetime   time.Duration `env:"MAX_CONN_LIFETIME" envDefault:"1h" comment:"Maximum connection lifetime"`
	MaxConnIdleTime   time.Duration `env:"MAX_CONN_IDLE_TIME" envDefault:"30m" comment:"Maximum connection idle time"`
	HealthCheckPeriod time.Duration `env:"HEALTH_CHECK_PERIOD" envDefault:"1m" comment:"Health check period"`
	ConnAttempts      int           `env:"CONN_ATTEMPTS" envDefault:"3" comment:"Connection attempts"`
	ConnTimeout       time.Duration `env:"CONN_TIMEOUT" envDefault:"5s" comment:"Connection timeout"`
	MigrationsDir     string        `env:"MIGRATIONS_DIR" comment:"Migrations directory"`
}

// TestConfig extends Config with additional testing-specific options
type TestConfig struct {
	Config

	// RunContainer indicates whether to start a test container
	RunContainer bool
	// ContainerImage specifies custom PostgreSQL image for test container
	ContainerImage string
	// ContainerName sets custom name for test container
	ContainerName string
	// MacConnections limits maximum number of connections
	MacConnections int
}

// PostgresContainer defines interface for database container management
type PostgresContainer interface {
	GetDSN(ctx context.Context) (string, error)
	Close(ctx context.Context) error
}

// connConfig holds database connection parameters
type connConfig struct {
	host     string
	port     string
	user     string
	password string
	dbName   string
	sslMode  string
}

func (c *connConfig) toDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		c.user, c.password, c.host, c.port, c.dbName, c.sslMode)
}

type plugin struct {
	runtime runtime.Runtime
	opts    Config
	pool    *pgxpool.Pool
	prefix  string
}

// NewPlugin creates a new instance of the PostgreSQL plugin
func NewPlugin(runtime runtime.Runtime, opts *Options) Plugin {
	p := &plugin{
		runtime: runtime,
		prefix:  opts.Name,
	}

	if p.prefix == "" {
		p.prefix = defaultPrefix
	}

	if err := runtime.Config().Parse(&p.opts, p.prefix); err != nil {
		log.Fatalf("failed to parse config: %v", err)
	}

	return p
}

// NewTestPlugin creates a plugin instance configured for testing
func NewTestPlugin(ctx context.Context, cfg TestConfig) (Plugin, error) {
	if cfg.DSN == "" && !cfg.RunContainer {
		if cfg.Host == "" {
			return nil, fmt.Errorf("DSN or Host is required but not set")
		}
		cfg.DSN = fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
			cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Database, cfg.SSLMode)
	}

	if cfg.MaxPoolSize == 0 {
		cfg.MaxPoolSize = 1
	}

	// Set default connection attempts and timeout if not specified
	if cfg.ConnAttempts == 0 {
		cfg.ConnAttempts = 10
	}
	if cfg.ConnTimeout == 0 {
		cfg.ConnTimeout = 15 * time.Second
	}

	if cfg.RunContainer {
		dbURL, err := runPostgresContainer(ctx, cfg)
		if err != nil {
			return nil, err
		}
		cfg.DSN = dbURL
	}

	p := &plugin{opts: cfg.Config}
	if err := p.initPlugin(ctx); err != nil {
		return nil, err
	}

	return p, nil
}

// runPostgresContainer starts a PostgreSQL container for testing
func runPostgresContainer(ctx context.Context, cfg TestConfig) (string, error) {
	image := getImage(cfg.ContainerImage)
	containerName := getContainerName(cfg.ContainerName)

	containerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Name:         containerName,
			Image:        image,
			ExposedPorts: []string{"5432/tcp"},
			Cmd:          []string{fmt.Sprintf("-N %d", getMaxConnections(cfg.MacConnections))},
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5 * time.Second),
		},
		Reuse: true,
	}

	container, err := postgres.RunContainer(ctx,
		testcontainers.CustomizeRequest(containerReq),
		postgres.WithDatabase("postgres"),
		postgres.WithUsername("user"),
		postgres.WithPassword("pass"),
	)
	if err != nil {
		return "", fmt.Errorf("failed to start postgres container: %w", err)
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

	connCfg, err := getConnectionConfig(ctx, container, cfg.Database)
	if err != nil {
		cleanup()
		return "", fmt.Errorf("failed to get connection config: %w", err)
	}

	if err := ensureDatabaseExists(ctx, connCfg.toDSN(), cfg.Database); err != nil {
		cleanup()
		return "", fmt.Errorf("failed to ensure database exists: %w", err)
	}

	return connCfg.toDSN(), nil
}

// getConnectionConfig retrieves connection parameters from container
func getConnectionConfig(ctx context.Context, container *postgres.PostgresContainer, dbName string) (*connConfig, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get container host: %w", err)
	}

	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		return nil, fmt.Errorf("failed to get container port: %w", err)
	}

	return &connConfig{
		host:     host,
		port:     port.Port(),
		user:     "user",
		password: "pass",
		dbName:   dbName,
		sslMode:  "disable",
	}, nil
}

// ensureDatabaseExists checks if database exists and creates it if necessary
func ensureDatabaseExists(ctx context.Context, dbURL, dbName string) error {
	// Connect to default postgres database first
	host, port := getHostPort(dbURL)
	postgresURL := fmt.Sprintf("postgres://user:pass@%s:%s/postgres?sslmode=disable", host, port)

	pool, err := pgxpool.New(ctx, postgresURL)
	if err != nil {
		return fmt.Errorf("failed to connect to postgres database: %w", err)
	}
	defer pool.Close()

	exists, err := databaseExists(ctx, pool, dbName)
	if err != nil {
		return fmt.Errorf("failed to check if database exists: %w", err)
	}

	if !exists {
		// Create database if it doesn't exist
		_, err = pool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName))
		if err != nil {
			return fmt.Errorf("failed to create database: %w", err)
		}
		fmt.Printf("Database %s created successfully\n", dbName)
	} else {
		fmt.Printf("Database %s already exists\n", dbName)
	}

	return nil
}

// Helper function to extract host and port from DSN
func getHostPort(dsn string) (host, port string) {
	// Parse DSN to extract host and port
	parts := strings.Split(dsn, "@")
	if len(parts) != 2 {
		return "", ""
	}
	hostPart := strings.Split(parts[1], "/")[0]
	hostPortParts := strings.Split(hostPart, ":")
	if len(hostPortParts) != 2 {
		return "", ""
	}
	return hostPortParts[0], hostPortParts[1]
}

// databaseExists checks if a database with given name exists
func databaseExists(ctx context.Context, pool *pgxpool.Pool, dbName string) (bool, error) {
	var exists bool
	err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)", dbName).Scan(&exists)
	return exists, err
}

func (p *plugin) DB() *pgxpool.Pool {
	return p.pool
}

// PreStart prepares the plugin before application start
func (p *plugin) PreStart(ctx context.Context) error {
	if p.opts.DSN == "" {
		p.opts.DSN = fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
			p.opts.Username, p.opts.Password, p.opts.Host, p.opts.Port, p.opts.Database, p.opts.SSLMode)
	}

	if err := p.initPlugin(ctx); err != nil {
		return err
	}

	p.runtime.Tools().Probes().RegisterCheck(p.prefix, probes.ReadinessProbe, PostgresPingChecker(p.pool, 1*time.Second))
	p.runtime.Tools().Probes().RegisterCheck(p.prefix, probes.LivenessProbe, PostgresPingChecker(p.pool, 1*time.Second))

	return nil
}

// OnStop performs cleanup when application stops
func (p *plugin) OnStop(ctx context.Context) error {
	if p.pool != nil {
		p.pool.Close()
	}
	return nil
}

// PostgresPingChecker creates a health check function for the database
func PostgresPingChecker(pool *pgxpool.Pool, timeout time.Duration) probes.HandleFunc {
	return func() error {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return pool.Ping(ctx)
	}
}

func (p *plugin) RunMigration() error {
	if p.opts.MigrationsDir == "" {
		return fmt.Errorf("migrations directory is not set: %s", p.opts.MigrationsDir)
	}

	fmt.Printf("Running migration from dir: %s\n", p.opts.MigrationsDir)

	opts, err := pgxpool.ParseConfig(p.opts.DSN)
	if err != nil {
		return errors.New(errMissingConnString)
	}

	conn, err := sql.Open(driverName, opts.ConnString())
	if err != nil {
		return fmt.Errorf("failed to open connection: %v", err)
	}
	defer conn.Close()

	driver, err := mpgx.WithInstance(conn, &mpgx.Config{})
	if err != nil {
		return fmt.Errorf("failed to create migrate driver: %v", err)
	}

	m, err := migrate.NewWithDatabaseInstance(fmt.Sprintf("file://%s", p.opts.MigrationsDir), "postgres", driver)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %v", err)
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("failed to migrate: %v", err)
	}

	fmt.Println("Migrations ran successfully")
	return nil
}

// initPlugin initializes database connection pool with retries
func (p *plugin) initPlugin(ctx context.Context) error {
	if p.pool != nil {
		return nil
	}

	config, err := pgxpool.ParseConfig(p.opts.DSN)
	if err != nil {
		return fmt.Errorf("failed to parse connection string: %w", err)
	}

	// Set pool configuration with production-ready settings
	config.MaxConns = int32(p.opts.MaxPoolSize)
	config.MinConns = int32(p.opts.MinPoolSize)
	config.MaxConnLifetime = p.opts.MaxConnLifetime
	config.MaxConnIdleTime = p.opts.MaxConnIdleTime
	config.HealthCheckPeriod = p.opts.HealthCheckPeriod
	config.ConnConfig.ConnectTimeout = p.opts.ConnTimeout

	var lastErr error
	for attempt := 1; attempt <= p.opts.ConnAttempts; attempt++ {
		p.pool, err = pgxpool.NewWithConfig(ctx, config)
		if err == nil {
			return nil
		}

		lastErr = err
		log.Printf("Connection attempt %d/%d failed: %v", attempt, p.opts.ConnAttempts, err)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(p.opts.ConnTimeout):
			continue
		}
	}

	return fmt.Errorf("failed to connect after %d attempts: %w", p.opts.ConnAttempts, lastErr)
}

// getImage returns container image name with fallback to default
func getImage(image string) string {
	if image == "" {
		return defaultImage
	}
	return image
}

// getContainerName returns container name with fallback to default
func getContainerName(name string) string {
	if name == "" {
		return defaultContainerName
	}
	return name
}

// getMaxConnections returns max connections number with fallback
func getMaxConnections(connections int) int {
	if connections <= 0 {
		return 100
	}
	return connections
}
