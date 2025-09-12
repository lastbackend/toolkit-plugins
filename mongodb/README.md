# MongoDB Plugin для Toolkit

Плагин MongoDB для Last.Backend Toolkit обеспечивает простую интеграцию с базой данных MongoDB.

## Установка

```bash
go get github.com/lastbackend/toolkit-plugins/mongodb
```

## Конфигурация

Плагин поддерживает следующие переменные окружения (с префиксом `MONGODB_`):

### Основные параметры
- `MONGODB_URI` - полная строка подключения MongoDB (по умолчанию: `mongodb://localhost:27017`)
- `MONGODB_HOST` - хост для подключения (по умолчанию: `localhost`)
- `MONGODB_PORT` - порт для подключения (по умолчанию: `27017`)
- `MONGODB_DATABASE` - имя базы данных (по умолчанию: `test`)
- `MONGODB_USERNAME` - имя пользователя для аутентификации
- `MONGODB_PASSWORD` - пароль для аутентификации

### Параметры пула соединений
- `MONGODB_MAX_POOL_SIZE` - максимальное количество соединений в пуле
- `MONGODB_MIN_POOL_SIZE` - минимальное количество соединений в пуле
- `MONGODB_MAX_IDLE_TIME_MS` - максимальное время простоя соединения в пуле
- `MONGODB_CONNECT_TIMEOUT_MS` - таймаут для установления соединения
- `MONGODB_SERVER_SELECTION_TIMEOUT_MS` - таймаут для выбора сервера

### TLS конфигурация
- `MONGODB_TLS_ENABLE` - включить TLS соединение
- `MONGODB_TLS_INSECURE_SKIP_VERIFY` - пропустить проверку TLS сертификата
- `MONGODB_TLS_CA_FILE` - путь к файлу CA сертификата
- `MONGODB_TLS_CERT_FILE` - путь к файлу TLS сертификата
- `MONGODB_TLS_KEY_FILE` - путь к файлу TLS ключа

### Дополнительные параметры
- `MONGODB_AUTH_SOURCE` - база данных для аутентификации
- `MONGODB_REPLICA_SET` - имя набора реплик
- `MONGODB_COMPRESSORS` - алгоритмы сжатия сети (snappy, zlib, zstd)

## Использование

### Основное использование

```go
package main

import (
    "context"
    "log"

    "github.com/lastbackend/toolkit/pkg/runtime"
    "github.com/lastbackend/toolkit-plugins/mongodb"
    "go.mongodb.org/mongo-driver/bson"
)

func main() {
    // Создать runtime
    r, err := runtime.NewRuntime(context.Background())
    if err != nil {
        log.Fatal(err)
    }

    // Инициализировать MongoDB плагин
    mongoPlugin := mongodb.NewPlugin(r, &mongodb.Options{
        Name: "MONGODB", // Префикс для переменных окружения
    })

    // Получить клиент MongoDB
    client := mongoPlugin.Client()
    
    // Получить базу данных
    db := mongoPlugin.Database()
    
    // Получить коллекцию
    collection := mongoPlugin.Collection("users")

    // Выполнить операции с MongoDB
    ctx := context.Background()
    
    // Вставить документ
    _, err = collection.InsertOne(ctx, bson.M{
        "name":  "John Doe",
        "email": "john@example.com",
        "age":   30,
    })
    if err != nil {
        log.Fatal(err)
    }

    // Найти документ
    var result bson.M
    err = collection.FindOne(ctx, bson.M{"name": "John Doe"}).Decode(&result)
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("Найден пользователь: %+v", result)
}
```

### Использование с тестами

```go
package main

import (
    "context"
    "testing"

    "github.com/lastbackend/toolkit-plugins/mongodb"
    "go.mongodb.org/mongo-driver/bson"
)

func TestMongoDB(t *testing.T) {
    ctx := context.Background()

    // Создать тестовый плагин с контейнером
    mongoPlugin, err := mongodb.NewTestPlugin(ctx, mongodb.TestConfig{
        Config: mongodb.Config{
            Database: "test_db",
            Username: "admin",
            Password: "password",
        },
        RunContainer: true,  // Автоматически запустить MongoDB контейнер
    })
    if err != nil {
        t.Fatal(err)
    }

    // Использовать плагин в тестах
    collection := mongoPlugin.Collection("test_collection")
    
    // Вставить тестовые данные
    _, err = collection.InsertOne(ctx, bson.M{"test": "data"})
    if err != nil {
        t.Fatal(err)
    }

    // Проверить данные
    count, err := collection.CountDocuments(ctx, bson.M{})
    if err != nil {
        t.Fatal(err)
    }

    if count != 1 {
        t.Errorf("Ожидалось 1 документ, получено %d", count)
    }
}
```

### Конфигурация через переменные окружения

```bash
export MONGODB_HOST=localhost
export MONGODB_PORT=27017
export MONGODB_DATABASE=myapp
export MONGODB_USERNAME=user
export MONGODB_PASSWORD=password
export MONGODB_AUTH_SOURCE=admin
export MONGODB_MAX_POOL_SIZE=100
export MONGODB_MIN_POOL_SIZE=10
export MONGODB_TLS_ENABLE=true
```

### Конфигурация через URI

```bash
export MONGODB_URI="mongodb://user:password@localhost:27017/myapp?authSource=admin&ssl=true"
```

## Особенности

- **Автоматические проверки здоровья**: Плагин регистрирует readiness и liveness проверки
- **Пул соединений**: Настраиваемый пул соединений для оптимальной производительности
- **TLS поддержка**: Полная поддержка TLS шифрования
- **Тестирование**: Встроенная поддержка TestContainers для интеграционных тестов
- **Гибкая конфигурация**: Поддержка как URI строк, так и отдельных параметров

## API

### Plugin Interface

```go
type Plugin interface {
    Client() *mongo.Client           // Получить MongoDB клиент
    Database() *mongo.Database       // Получить экземпляр базы данных
    Collection(name string) *mongo.Collection // Получить коллекцию по имени
    Print()                         // Вывести конфигурацию
}
```

### Конструкторы

- `NewPlugin(runtime, opts)` - создать плагин для продакшна
- `NewTestPlugin(ctx, cfg)` - создать плагин для тестирования

### Проверки здоровья

Плагин автоматически регистрирует проверки здоровья, которые выполняют ping к MongoDB серверу для проверки доступности соединения.
