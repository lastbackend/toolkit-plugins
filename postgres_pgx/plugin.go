package postgres_pgx

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/golang-migrate/migrate/v4"
	mpgx "github.com/golang-migrate/migrate/v4/database/pgx"
	"github.com/jackc/pgx/v5"
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
	defaultPrefix = "psql"
	driverName    = "postgres"
)

const (
	errMissingConnectionString = "Missing connection string"
	errConnectAttempts         = "Connect attempts failed"
)

type Plugin interface {
	DB() *pgx.Conn
	RunMigration() error
}

type Options struct {
	Name string
}

type Config struct {
	DSN           string        `env:"DSN"  envDefault:"" comment:"DSN = postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...] complete connection string"`
	Host          string        `env:"HOST" envDefault:"127.0.0.1"  comment:"The host to connect to (required)"`
	Port          int32         `env:"PORT" envDefault:"5432" comment:"The port to bind to (default: 5432)"`
	Database      string        `env:"DATABASE" comment:"Database to be selected after connecting to the server."`
	Username      string        `env:"USERNAME" comment:"The username to connect with. Not required if using IntegratedSecurity"`
	Password      string        `env:"PASSWORD" comment:"The password to connect with. Not required if using IntegratedSecurity"`
	SSLMode       string        `env:"SSLMODE" comment:" Whether or not to use SSL mode (disable, allow, prefer, require, verify-ca, verify-full)"`
	TimeZone      string        `env:"TIMEZONE" comment:"Sets the session timezone"`
	MaxPoolSize   int           `env:"MAX_POOL_SIZE" envDefault:"2" comment:"Sets the max pool size"`
	ConnAttempts  int           `env:"CONN_ATTEMPTS" envDefault:"10" comment:"Sets the connection attempts"`
	ConnTimeout   time.Duration `env:"CONN_TIMEOUT" envDefault:"15s" comment:"Sets the connection timeout"`
	MigrationsDir string        `env:"MIGRATIONS_DIR" comment:"Migrations directory to run migration when plugin is started"`
}

type plugin struct {
	runtime runtime.Runtime

	prefix     string
	envPrefix  string
	connection string
	opts       Config

	conn *pgx.Conn
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

type TestConfig struct {
	Config

	RunContainer   bool
	ContainerImage string
	ContainerName  string
	MacConnections int
}

func NewTestPlugin(ctx context.Context, cfg TestConfig) (Plugin, error) {

	opts := cfg

	if opts.DSN == "" && !opts.RunContainer {
		if opts.Host == "" {
			return nil, fmt.Errorf("DSN or Host environment variable required but not set")
		}
		opts.DSN = fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
			opts.Username, opts.Password, opts.Host, opts.Port, opts.Database, opts.SSLMode)
	}

	if opts.Config.MaxPoolSize == 0 {
		opts.Config.MaxPoolSize = 1
	}

	if opts.RunContainer {
		if opts.ContainerImage == "" {
			opts.ContainerImage = "postgres:15.2"
		}
		if opts.ContainerName == "" {
			opts.ContainerName = "postgres-test-container"
		}

		strategy := wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(5 * time.Second)

		cmd := "-N 500"
		if cfg.MacConnections > 0 {
			cmd = fmt.Sprintf("-N %d", cfg.MacConnections)
		}

		container, err := postgres.RunContainer(ctx,
			testcontainers.WithImage(opts.ContainerImage),
			postgres.WithDatabase("postgres"),
			postgres.WithUsername("user"),
			postgres.WithPassword("pass"),
			testcontainers.WithWaitStrategy(strategy),
			testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
				ContainerRequest: testcontainers.ContainerRequest{
					Name: opts.ContainerName,
					Cmd:  []string{cmd},
				},
				Reuse: true,
			}),
		)
		if err != nil {
			return nil, err
		}

		host, err := container.Host(ctx)
		if err != nil {
			return nil, err
		}
		realPort, err := container.MappedPort(ctx, "5432")
		if err != nil {
			return nil, err
		}

		dbURL := fmt.Sprintf("postgres://user:pass@%v:%v/postgres?sslmode=disable", host, realPort.Port())

		conn, err := pgx.Connect(ctx, dbURL)
		if err != nil {
			log.Fatalf("failed to connect to database: %v", err)
		}

		if opts.Database == "" {
			opts.Database = "postgres"
		}

		exists, err := databaseExists(ctx, conn, opts.Database)
		if err != nil {
			return nil, fmt.Errorf("failed to check if database exists: %v", err)
		}

		if !exists {
			createDBQuery := fmt.Sprintf("CREATE DATABASE %s", opts.Database)
			_, err = conn.Exec(ctx, createDBQuery)
			if err != nil {
				return nil, fmt.Errorf("failed to create database: %v", err)
			}
			fmt.Printf("Database %s created successfully\n", opts.Database)
		} else {
			fmt.Printf("Database %s already exists, continuing...\n", opts.Database)
		}
		_ = conn.Close(ctx)

		if opts.Database != "postgres" {
			opts.DSN = fmt.Sprintf("postgres://user:pass@%v:%v/%s?sslmode=disable", host, realPort.Port(), opts.Database)
		} else {
			opts.DSN = dbURL
		}
	}

	p := new(plugin)
	p.opts = opts.Config

	if err := p.initPlugin(ctx); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *plugin) DB() *pgx.Conn {
	return p.conn
}

func (p *plugin) PreStart(ctx context.Context) (err error) {

	if p.opts.DSN == "" {
		if p.opts.Host == "" {
			return fmt.Errorf("%s_DSN or %s_Host environment variable required but not set",
				p.prefix, p.prefix)
		}
		p.opts.DSN = fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
			p.opts.Username, p.opts.Password, p.opts.Host, p.opts.Port, p.opts.Database, p.opts.SSLMode)
	}

	if err := p.initPlugin(ctx); err != nil {
		return err
	}

	db, err := pgx.Connect(ctx, p.opts.DSN)
	if err != nil {
		return errors.New(errMissingConnectionString)
	}

	p.runtime.Tools().Probes().RegisterCheck(p.prefix, probes.ReadinessProbe, PostgresPingChecker(db, 1*time.Second))
	p.runtime.Tools().Probes().RegisterCheck(p.prefix, probes.LivenessProbe, PostgresPingChecker(db, 1*time.Second))

	return nil
}

func (p *plugin) OnStop(ctx context.Context) error {
	if p.conn != nil {
		return p.conn.Close(ctx)
	}
	return nil
}

func PostgresPingChecker(database *pgx.Conn, timeout time.Duration) probes.HandleFunc {
	return func() error {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if database == nil {
			return fmt.Errorf("connection is nil")
		}
		return database.Ping(ctx)
	}
}

func (p *plugin) RunMigration() error {

	if p.opts.MigrationsDir == "" {
		return fmt.Errorf("can not run migration: dir is not set: %s", p.opts.MigrationsDir)
	}

	fmt.Printf("\nRun migration from dir: %s", p.opts.MigrationsDir)

	opts, err := pgx.ParseConfig(p.opts.DSN)
	if err != nil {
		return errors.New(errMissingConnectionString)
	}

	conn, err := sql.Open(driverName, p.opts.DSN)
	if err != nil {
		return fmt.Errorf("failed to connection open: %v", err)
	}

	driver, err := mpgx.WithInstance(conn, &mpgx.Config{})
	if err != nil {
		return err
	}

	m, err := migrate.NewWithDatabaseInstance(fmt.Sprintf("file://%s", p.opts.MigrationsDir), opts.Database, driver)
	if err != nil {
		return err
	}

	version, dirty, err := m.Version()
	if err != nil && err != migrate.ErrNilVersion {
		return err
	}
	if dirty {
		if err := m.Force(int(version)); err != nil {
			return err
		}
		if err := m.Down(); err != nil {
			return err
		}
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return err
	}

	fmt.Printf("\nMigration completed!\n")

	return nil
}

func (p *plugin) initPlugin(ctx context.Context) error {

	connAttempts := p.opts.ConnAttempts
	if connAttempts == 0 {
		connAttempts = 1
	}

	var err error
	for connAttempts > 0 {
		p.conn, err = pgx.Connect(ctx, p.opts.DSN)
		if err == nil {
			break
		}

		log.Printf("Postgres is trying to connect, attempts left: %d", connAttempts)

		time.Sleep(p.opts.ConnTimeout)

		connAttempts--
	}
	if err != nil {
		return errors.New(errConnectAttempts)
	}

	p.connection = p.opts.DSN

	return nil
}

func databaseExists(ctx context.Context, conn *pgx.Conn, dbName string) (bool, error) {
	var exists bool
	query := fmt.Sprintf("SELECT EXISTS(SELECT datname FROM pg_catalog.pg_database WHERE datname = '%s')", dbName)
	err := conn.QueryRow(ctx, query).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}
