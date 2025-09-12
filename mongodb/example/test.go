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

package example

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/lastbackend/toolkit-plugins/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type User struct {
	ID    primitive.ObjectID `bson:"_id,omitempty"`
	Name  string             `bson:"name"`
	Email string             `bson:"email"`
	Age   int                `bson:"age"`
}

func ExamplePlugin_basic() {
	ctx := context.Background()

	// Создать тестовый плагин с контейнером
	mongoPlugin, err := mongodb.NewTestPlugin(ctx, mongodb.TestConfig{
		Config: mongodb.Config{
			Database: "example_db",
		},
		RunContainer: true,
		ConnTimeout:  30 * time.Second,
	})
	if err != nil {
		log.Fatal("Failed to create MongoDB plugin:", err)
	}

	// Получить коллекцию
	collection := mongoPlugin.Collection("users")

	// Создать пользователя
	user := User{
		Name:  "John Doe",
		Email: "john@example.com",
		Age:   30,
	}

	// Вставить пользователя
	result, err := collection.InsertOne(ctx, user)
	if err != nil {
		log.Fatal("Failed to insert user:", err)
	}

	log.Printf("Inserted user with ID: %v", result.InsertedID)

	// Найти пользователя
	var foundUser User
	err = collection.FindOne(ctx, bson.M{"name": "John Doe"}).Decode(&foundUser)
	if err != nil {
		log.Fatal("Failed to find user:", err)
	}

	log.Printf("Found user: %+v", foundUser)

	// Обновить пользователя
	_, err = collection.UpdateOne(
		ctx,
		bson.M{"_id": foundUser.ID},
		bson.M{"$set": bson.M{"age": 31}},
	)
	if err != nil {
		log.Fatal("Failed to update user:", err)
	}

	// Получить количество пользователей
	count, err := collection.CountDocuments(ctx, bson.M{})
	if err != nil {
		log.Fatal("Failed to count users:", err)
	}

	log.Printf("Total users: %d", count)

	// Удалить пользователя
	_, err = collection.DeleteOne(ctx, bson.M{"_id": foundUser.ID})
	if err != nil {
		log.Fatal("Failed to delete user:", err)
	}

	log.Println("User deleted successfully")
}

func TestMongoDBPlugin(t *testing.T) {
	ctx := context.Background()

	// Создать тестовый плагин с контейнером
	mongoPlugin, err := mongodb.NewTestPlugin(ctx, mongodb.TestConfig{
		Config: mongodb.Config{
			Database: "test_db",
		},
		RunContainer: true,
		ConnTimeout:  30 * time.Second,
	})
	if err != nil {
		t.Fatal("Failed to create MongoDB plugin:", err)
	}

	// Тестировать основные операции
	t.Run("InsertAndFind", func(t *testing.T) {
		collection := mongoPlugin.Collection("test_users")

		// Очистить коллекцию
		_, err := collection.DeleteMany(ctx, bson.M{})
		if err != nil {
			t.Fatal("Failed to clear collection:", err)
		}

		// Вставить тестовые данные
		user := User{
			Name:  "Test User",
			Email: "test@example.com",
			Age:   25,
		}

		result, err := collection.InsertOne(ctx, user)
		if err != nil {
			t.Fatal("Failed to insert user:", err)
		}

		if result.InsertedID == nil {
			t.Fatal("Expected inserted ID to be set")
		}

		// Найти пользователя
		var foundUser User
		err = collection.FindOne(ctx, bson.M{"name": "Test User"}).Decode(&foundUser)
		if err != nil {
			t.Fatal("Failed to find user:", err)
		}

		if foundUser.Name != user.Name {
			t.Errorf("Expected name %s, got %s", user.Name, foundUser.Name)
		}
		if foundUser.Email != user.Email {
			t.Errorf("Expected email %s, got %s", user.Email, foundUser.Email)
		}
		if foundUser.Age != user.Age {
			t.Errorf("Expected age %d, got %d", user.Age, foundUser.Age)
		}
	})

	t.Run("Update", func(t *testing.T) {
		collection := mongoPlugin.Collection("test_users")

		// Найти существующего пользователя
		var user User
		err := collection.FindOne(ctx, bson.M{"name": "Test User"}).Decode(&user)
		if err != nil {
			t.Fatal("Failed to find user:", err)
		}

		// Обновить возраст
		newAge := 26
		_, err = collection.UpdateOne(
			ctx,
			bson.M{"_id": user.ID},
			bson.M{"$set": bson.M{"age": newAge}},
		)
		if err != nil {
			t.Fatal("Failed to update user:", err)
		}

		// Проверить обновление
		var updatedUser User
		err = collection.FindOne(ctx, bson.M{"_id": user.ID}).Decode(&updatedUser)
		if err != nil {
			t.Fatal("Failed to find updated user:", err)
		}

		if updatedUser.Age != newAge {
			t.Errorf("Expected age %d, got %d", newAge, updatedUser.Age)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		collection := mongoPlugin.Collection("test_users")

		// Подсчитать документы до удаления
		countBefore, err := collection.CountDocuments(ctx, bson.M{})
		if err != nil {
			t.Fatal("Failed to count documents:", err)
		}

		// Удалить всех пользователей
		deleteResult, err := collection.DeleteMany(ctx, bson.M{})
		if err != nil {
			t.Fatal("Failed to delete users:", err)
		}

		if deleteResult.DeletedCount != countBefore {
			t.Errorf("Expected to delete %d documents, deleted %d", countBefore, deleteResult.DeletedCount)
		}

		// Проверить что коллекция пуста
		countAfter, err := collection.CountDocuments(ctx, bson.M{})
		if err != nil {
			t.Fatal("Failed to count documents after delete:", err)
		}

		if countAfter != 0 {
			t.Errorf("Expected 0 documents after delete, got %d", countAfter)
		}
	})

	t.Run("MultipleOperations", func(t *testing.T) {
		collection := mongoPlugin.Collection("bulk_test")

		// Очистить коллекцию
		_, err := collection.DeleteMany(ctx, bson.M{})
		if err != nil {
			t.Fatal("Failed to clear collection:", err)
		}

		// Вставить несколько документов
		users := []interface{}{
			User{Name: "Alice", Email: "alice@example.com", Age: 28},
			User{Name: "Bob", Email: "bob@example.com", Age: 32},
			User{Name: "Charlie", Email: "charlie@example.com", Age: 24},
		}

		insertResult, err := collection.InsertMany(ctx, users)
		if err != nil {
			t.Fatal("Failed to insert multiple users:", err)
		}

		if len(insertResult.InsertedIDs) != 3 {
			t.Errorf("Expected 3 inserted IDs, got %d", len(insertResult.InsertedIDs))
		}

		// Найти пользователей старше 25
		cursor, err := collection.Find(ctx, bson.M{"age": bson.M{"$gt": 25}})
		if err != nil {
			t.Fatal("Failed to find users:", err)
		}
		defer cursor.Close(ctx)

		var foundUsers []User
		if err := cursor.All(ctx, &foundUsers); err != nil {
			t.Fatal("Failed to decode users:", err)
		}

		if len(foundUsers) != 2 {
			t.Errorf("Expected 2 users older than 25, got %d", len(foundUsers))
		}
	})
}

func TestMongoDBPluginHealthCheck(t *testing.T) {
	ctx := context.Background()

	// Создать тестовый плагин с контейнером
	mongoPlugin, err := mongodb.NewTestPlugin(ctx, mongodb.TestConfig{
		Config: mongodb.Config{
			Database: "health_test",
		},
		RunContainer: true,
		ConnTimeout:  30 * time.Second,
	})
	if err != nil {
		t.Fatal("Failed to create MongoDB plugin:", err)
	}

	// Проверить что клиент работает
	client := mongoPlugin.Client()
	if client == nil {
		t.Fatal("Expected client to be initialized")
	}

	// Проверить ping
	err = client.Ping(ctx, nil)
	if err != nil {
		t.Fatal("Failed to ping MongoDB:", err)
	}

	// Проверить базу данных
	db := mongoPlugin.Database()
	if db == nil {
		t.Fatal("Expected database to be initialized")
	}

	// Проверить имя базы данных
	if db.Name() != "health_test" {
		t.Errorf("Expected database name 'health_test', got '%s'", db.Name())
	}
}
