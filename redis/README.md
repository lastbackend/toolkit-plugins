# Redis Plugin for Last.Backend Toolkit

This plugin provides Redis database connectivity for Last.Backend applications with support for both standalone and cluster modes, as well as containerized testing.

## Features

- **Standalone Redis**: Single Redis instance connection
- **Cluster Mode**: Redis cluster support for distributed setups  
- **Container Support**: Automated Redis container for testing
- **Health Checks**: Built-in readiness and liveness probes
- **Configurable**: Environment-based configuration
- **TLS Support**: Optional TLS encryption

## Installation

```bash
go get github.com/lastbackend/toolkit-plugins/redis
```

## Basic Usage

### Production Setup

```go
package main

import (
    "context"
    "github.com/lastbackend/toolkit/pkg/runtime"
    redisplugin "github.com/lastbackend/toolkit-plugins/redis"
)

func main() {
    rt := runtime.New()
    
    // Create Redis plugin
    redis := redisplugin.NewPlugin(rt, &redisplugin.Options{
        Name: "REDIS", // Environment prefix
    })
    
    // Start runtime with plugin
    rt.WithPlugin(redis)
    rt.Start(context.Background())
    
    // Use Redis client
    client := redis.DB()
    client.Set(context.Background(), "key", "value", 0)
}
```

### Testing with Container

```go
package main

import (
    "context"
    "testing"
    redisplugin "github.com/lastbackend/toolkit-plugins/redis"
)

func TestWithRedisContainer(t *testing.T) {
    ctx := context.Background()
    
    // Configure test Redis with container
    cfg := redisplugin.TestConfig{
        Config: redisplugin.Config{
            Database: 0,
        },
        RunContainer:   true,
        ContainerImage: "redis:7-alpine",
        ContainerName:  "redis-test",
    }
    
    // Create test plugin (starts container automatically)
    plugin, err := redisplugin.NewTestPlugin(ctx, cfg)
    if err != nil {
        t.Fatal(err)
    }
    
    // Use Redis client for testing
    client := plugin.DB()
    err = client.Set(ctx, "test-key", "test-value", 0).Err()
    if err != nil {
        t.Fatal(err)
    }
    
    val, err := client.Get(ctx, "test-key").Result()
    if err != nil {
        t.Fatal(err)
    }
    
    if val != "test-value" {
        t.Fatalf("Expected 'test-value', got '%s'", val)
    }
}
```

## Configuration

### Environment Variables

Set environment variables with the specified prefix (default: `REDIS_`):

```bash
# Basic connection
REDIS_ENDPOINT=localhost:6379
REDIS_DATABASE=0
REDIS_PASSWORD=your-password

# Cluster mode
REDIS_CLUSTER=true
REDIS_ENDPOINT=node1:6379,node2:6379,node3:6379

# Connection tuning
REDIS_POOL_SIZE=10
REDIS_MIN_IDLE_CONNS=5
REDIS_DIAL_TIMEOUT=5s
REDIS_READ_TIMEOUT=3s
REDIS_WRITE_TIMEOUT=3s

# Retry configuration
REDIS_MAX_RETRIES=3
REDIS_MIN_RETRY_BACKOFF=8ms
REDIS_MAX_RETRY_BACKOFF=512ms
```

### Config Struct

```go
type Config struct {
    Endpoint           string        // Redis server address(es)
    Cluster            bool          // Enable cluster mode
    Database           int           // Database number (0-15)
    Username           string        // Redis 6.0+ username
    Password           string        // Redis password
    HeartbeatFrequency time.Duration // Cluster heartbeat
    MaxRetries         int           // Connection retries
    MinRetryBackoff    time.Duration // Min retry delay
    MaxRetryBackoff    time.Duration // Max retry delay
    DialTimeout        time.Duration // Connection timeout
    ReadTimeout        time.Duration // Read operation timeout
    WriteTimeout       time.Duration // Write operation timeout
    PoolSize           int           // Connection pool size
    MinIdleConns       int           // Minimum idle connections
    PoolTimeout        time.Duration // Pool wait timeout
    TLSConfig          *tls.Config   // TLS configuration
}
```

## Container Testing

### TestConfig Options

```go
type TestConfig struct {
    Config             // Embedded config
    RunContainer   bool   // Start test container
    ContainerImage string // Redis image (default: redis:7-alpine)
    ContainerName  string // Container name
    ConnAttempts   int    // Connection retry attempts
    ConnTimeout    time.Duration // Connection timeout
}
```

### Container Features

- **Automatic Setup**: Container starts automatically when `RunContainer=true`
- **Password Support**: Configures Redis authentication if password specified
- **Container Reuse**: Reuses existing containers for faster tests
- **Auto Cleanup**: Container cleanup on context cancellation
- **Custom Images**: Support for custom Redis images

## Health Checks

The plugin automatically registers health check probes for monitoring Redis connectivity:

- **Readiness Probe**: Checks if Redis is ready to accept connections
- **Liveness Probe**: Monitors ongoing Redis connectivity
- **Automatic Registration**: Probes are registered during plugin startup
- **Cluster Support**: Separate probes for cluster and standalone modes

The probes perform Redis PING operations with 1-second timeout and are automatically integrated with the toolkit's monitoring system.

## API Reference

### Plugin Interface

```go
type Plugin interface {
    DB() *redis.Client                // Get standalone client
    ClusterDB() *redis.ClusterClient  // Get cluster client
    Client() redis.Cmdable            // Get appropriate client interface
    Print()                           // Print configuration
}
```

### Compatibility

The `Client()` method returns `redis.Cmdable` interface, making it compatible with caching systems and other components that expect the standard go-redis interface.

## Examples

### Cluster Configuration

```go
cfg := redisplugin.Config{
    Cluster: true,
    Endpoint: "redis-node1:6379,redis-node2:6379,redis-node3:6379",
    Password: "cluster-password",
    PoolSize: 20,
}
```

### Testing with Authentication

```go
cfg := redisplugin.TestConfig{
    Config: redisplugin.Config{
        Database: 0,
        Password: "test-password",
    },
    RunContainer: true,
    ContainerImage: "redis:7-alpine",
}

plugin, err := redisplugin.NewTestPlugin(ctx, cfg)
```

### Production Deployment

```bash
# Environment configuration
export REDIS_CLUSTER=true
export REDIS_ENDPOINT=redis-cluster.production.com:6379
export REDIS_PASSWORD=production-secret
export REDIS_POOL_SIZE=50
export REDIS_MAX_RETRIES=5
```

## Requirements

- Go 1.21+
- Docker (for container testing)
- Redis 6.0+ (for username/password auth)

## License

Licensed under the Apache License, Version 2.0. 