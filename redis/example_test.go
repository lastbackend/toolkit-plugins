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
	"fmt"
	"testing"
	"time"
)

// Example_containerUsage demonstrates how to use Redis plugin with containers
func Example_containerUsage() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Configure test Redis with container
	cfg := TestConfig{
		Config: Config{
			Database: 0,
		},
		RunContainer:   true,
		ContainerImage: "redis:7-alpine",
		ContainerName:  "redis-example-container",
	}

	// Create test plugin (starts container automatically)
	plugin, err := NewTestPlugin(ctx, cfg)
	if err != nil {
		fmt.Printf("Failed to create test plugin: %v\n", err)
		return
	}

	// Use Redis client for operations
	client := plugin.DB()

	// Set a value
	err = client.Set(ctx, "example-key", "example-value", time.Hour).Err()
	if err != nil {
		fmt.Printf("Failed to set key: %v\n", err)
		return
	}

	// Get the value
	val, err := client.Get(ctx, "example-key").Result()
	if err != nil {
		fmt.Printf("Failed to get key: %v\n", err)
		return
	}

	fmt.Printf("Retrieved value: %s\n", val)

	// Test Client() method for compatibility
	cmdableClient := plugin.Client()

	// Use through Cmdable interface
	err = cmdableClient.Set(ctx, "cmdable-key", "cmdable-value", time.Hour).Err()
	if err != nil {
		fmt.Printf("Failed to set key through Cmdable: %v\n", err)
		return
	}

	val2, err := cmdableClient.Get(ctx, "cmdable-key").Result()
	if err != nil {
		fmt.Printf("Failed to get key through Cmdable: %v\n", err)
		return
	}

	fmt.Printf("Retrieved through Cmdable: %s\n", val2)

	// Output:
	// Retrieved value: example-value
	// Retrieved through Cmdable: cmdable-value
}

// TestBasicContainerFunctionality tests basic container operations
func TestBasicContainerFunctionality(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping container test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cfg := TestConfig{
		Config: Config{
			Database: 0,
		},
		RunContainer:   true,
		ContainerImage: "redis:7-alpine",
		ContainerName:  "redis-basic-test",
	}

	plugin, err := NewTestPlugin(ctx, cfg)
	if err != nil {
		t.Skipf("Could not create test plugin (likely no Docker): %v", err)
		return
	}

	client := plugin.DB()
	if client == nil {
		t.Fatal("Redis client is nil")
	}

	// Test basic Redis operations
	testKey := "test:basic"
	testValue := "basic-value"

	err = client.Set(ctx, testKey, testValue, time.Hour).Err()
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}

	result, err := client.Get(ctx, testKey).Result()
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}

	if result != testValue {
		t.Fatalf("Expected %s, got %s", testValue, result)
	}

	t.Logf("Successfully tested Redis container functionality")
}

// TestHelperFunctionsExist tests that helper functions work correctly
func TestHelperFunctionsExist(t *testing.T) {
	// Test getImage function
	defaultImg := getImage("")
	if defaultImg != defaultImage {
		t.Errorf("Expected %s, got %s", defaultImage, defaultImg)
	}

	customImg := "redis:6-alpine"
	customImgResult := getImage(customImg)
	if customImgResult != customImg {
		t.Errorf("Expected %s, got %s", customImg, customImgResult)
	}

	// Test getContainerName function
	defaultName := getContainerName("")
	if defaultName != defaultContainerName {
		t.Errorf("Expected %s, got %s", defaultContainerName, defaultName)
	}

	customName := "my-redis-container"
	customNameResult := getContainerName(customName)
	if customNameResult != customName {
		t.Errorf("Expected %s, got %s", customName, customNameResult)
	}
}
