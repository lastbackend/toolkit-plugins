# Postgres PGX Plugin

A Go plugin for PostgreSQL database management using PGX driver with support for connection pooling, migrations, and test containers.

## Features

- Connection pooling with PGX
- Database migrations support
- Test containers integration
- Health checks (readiness and liveness probes)
- Configurable connection parameters
- Automatic database creation for tests
  
## Installation
```bash
go get github.com/lastbackend/toolkit-plugins/postgres_pgx
```

## Usage

### Basic Usage
```go
import "github.com/lastbackend/toolkit-plugins/postgres_pgx"

func main() {
    // Initialize runtime (from your toolkit)
    rt := runtime.New()

    // Create plugin
    pg := postgres_pgx.NewPlugin(rt, &postgres_pgx.Options{
        Name: "my-postgres",
    })

    // Get database connection pool
    db := pg.DB()

    // Use the database
    // ...
}
```

### Configuration

Configuration can be provided via environment variables:

```env
PSQL_DSN=postgresql://user:pass@localhost:5432/dbname
# or individual parameters
PSQL_HOST=localhost
PSQL_PORT=5432
PSQL_DATABASE=dbname
PSQL_USERNAME=user
PSQL_PASSWORD=pass
PSQL_SSLMODE=disable
PSQL_MAX_POOL_SIZE=2
PSQL_CONN_ATTEMPTS=10
PSQL_CONN_TIMEOUT=15s
PSQL_MIGRATIONS_DIR=/path/to/migrations
```

### Running Migrations
```go
if err := pg.RunMigration(); err != nil {
    log.Fatal(err)
}
```

### Testing with Containers
```go
ctx := context.Background()

pg, err := postgres_pgx.NewTestPlugin(ctx, postgres_pgx.TestConfig{
    Config: postgres_pgx.Config{
        Database: "testdb",
    },
    RunContainer: true,
    // Optional container configuration
    ContainerImage: "postgres:15.2",
    ContainerName:  "my-test-postgres",
    MacConnections: 100,
})
if err != nil {
    log.Fatal(err)
}
// Use pg for testing
```

## Configuration Options

### Plugin Options

| Option | Description |
|--------|-------------|
| Name | Plugin name prefix for configuration |

### Database Config

| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| DSN | PSQL_DSN | "" | Complete connection string |
| Host | PSQL_HOST | "127.0.0.1" | Database host |
| Port | PSQL_PORT | 5432 | Database port |
| Database | PSQL_DATABASE | "postgres" | Database name |
| Username | PSQL_USERNAME | "postgres" | Database user |
| Password | PSQL_PASSWORD | "" | Database password |
| SSLMode | PSQL_SSLMODE | "disable" | SSL mode |
| MaxPoolSize | PSQL_MAX_POOL_SIZE | 2 | Connection pool size |
| ConnAttempts | PSQL_CONN_ATTEMPTS | 10 | Connection retry attempts |
| ConnTimeout | PSQL_CONN_TIMEOUT | "15s" | Connection timeout |
| MigrationsDir | PSQL_MIGRATIONS_DIR | "" | Migrations directory path |

### Test Config

| Parameter | Description |
|-----------|-------------|
| RunContainer | Whether to run a test container |
| ContainerImage | Custom PostgreSQL image |
| ContainerName | Custom container name |
| MacConnections | Max connections limit |

