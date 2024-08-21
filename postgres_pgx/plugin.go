package postgres_pgx

import (
	"context"
	"database/sql"
	"fmt"
	"log"
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

type Plugin interface {
	DB() *pgxpool.Pool
	RunMigration() error
}

type Options struct {
	Name string
}

type Config struct {
	DSN           string        `env:"DSN" envDefault:"" comment:"DSN = postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...] complete connection string"`
	Host          string        `env:"HOST" envDefault:"127.0.0.1" comment:"The host to connect to"`
	Port          int32         `env:"PORT" envDefault:"5432" comment:"The port to connect to"`
	Database      string        `env:"DATABASE" comment:"Database name"`
	Username      string        `env:"USERNAME" comment:"The username to connect with"`
	Password      string        `env:"PASSWORD" comment:"The password to connect with"`
	SSLMode       string        `env:"SSLMODE" comment:"SSL mode (disable, allow, prefer, require, verify-ca, verify-full)"`
	MaxPoolSize   int           `env:"MAX_POOL_SIZE" envDefault:"2" comment:"Max pool size"`
	ConnAttempts  int           `env:"CONN_ATTEMPTS" envDefault:"10" comment:"Connection attempts"`
	ConnTimeout   time.Duration `env:"CONN_TIMEOUT" envDefault:"15s" comment:"Connection timeout"`
	MigrationsDir string        `env:"MIGRATIONS_DIR" comment:"Migrations directory"`
}

type TestConfig struct {
	Config

	RunContainer   bool
	ContainerImage string
	ContainerName  string
	MacConnections int
}

type plugin struct {
	runtime runtime.Runtime
	opts    Config
	pool    *pgxpool.Pool
	prefix  string
}

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

func runPostgresContainer(ctx context.Context, cfg TestConfig) (string, error) {
	image := getImage(cfg.ContainerImage)
	containerName := getContainerName(cfg.ContainerName)
	cmd := fmt.Sprintf("-N %d", getMaxConnections(cfg.MacConnections))

	container, err := postgres.RunContainer(ctx,
		testcontainers.WithImage(image),
		postgres.WithDatabase("postgres"),
		postgres.WithUsername("user"),
		postgres.WithPassword("pass"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(5*time.Second)),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Name: containerName,
				Cmd:  []string{cmd},
			},
			Reuse: true,
		}),
	)
	if err != nil {
		return "", err
	}

	host, err := container.Host(ctx)
	if err != nil {
		return "", err
	}
	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		return "", err
	}

	dbURL := fmt.Sprintf("postgres://user:pass@%s:%s/postgres?sslmode=disable", host, port.Port())
	if err := ensureDatabaseExists(ctx, dbURL, cfg.Database); err != nil {
		return "", err
	}

	return fmt.Sprintf("postgres://user:pass@%s:%s/%s?sslmode=disable", host, port.Port(), cfg.Database), nil
}

func ensureDatabaseExists(ctx context.Context, dbURL, dbName string) error {
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	exists, err := databaseExists(ctx, pool, dbName)
	if err != nil {
		return err
	}

	if !exists {
		_, err = pool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName))
		if err != nil {
			return err
		}
		fmt.Printf("Database %s created successfully\n", dbName)
	} else {
		fmt.Printf("Database %s already exists\n", dbName)
	}

	return nil
}

func databaseExists(ctx context.Context, pool *pgxpool.Pool, dbName string) (bool, error) {
	var exists bool
	err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)", dbName).Scan(&exists)
	return exists, err
}

func (p *plugin) DB() *pgxpool.Pool {
	return p.pool
}

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

func (p *plugin) OnStop(ctx context.Context) error {
	if p.pool != nil {
		p.pool.Close()
	}
	return nil
}

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

func (p *plugin) initPlugin(ctx context.Context) error {
	if p.pool != nil {
		return nil
	}

	var err error
	for i := 0; i < p.opts.ConnAttempts; i++ {
		p.pool, err = pgxpool.New(ctx, p.opts.DSN)
		if err == nil {
			return nil
		}

		log.Printf("Connection attempt %d/%d failed: %v", i+1, p.opts.ConnAttempts, err)
		time.Sleep(p.opts.ConnTimeout)
	}

	return fmt.Errorf("failed to connect to the database after %d attempts: %v", p.opts.ConnAttempts, err)
}

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

func getMaxConnections(connections int) int {
	if connections <= 0 {
		return 100
	}
	return connections
}
