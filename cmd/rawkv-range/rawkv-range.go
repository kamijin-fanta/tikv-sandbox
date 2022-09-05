package main

import (
	"context"
	"fmt"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
	"log"
	"os"
	"strings"
)

func main() {
	addresses := []string{"127.0.0.1:2379"}
	{
		addrEnv := os.Getenv("PD_ADDR")
		if addrEnv != "" {
			addresses = strings.Split(addrEnv, ",")
		}
	}

	ctx := context.Background()
	cli, err := rawkv.NewClient(
		ctx,
		addresses,
		config.DefaultConfig().Security,
	)
	if err != nil {
		panic(err)
	}
	defer cli.Close()

	// Put multiple keys
	items := 10
	keys := make([][]byte, 0, items)
	values := make([][]byte, 0, items)
	for i := 0; i < items; i++ {
		key := fmt.Sprintf("range-test_%d", i)
		value := fmt.Sprintf("value-%d", i)

		keys = append(keys, []byte(key))
		values = append(values, []byte(value))
	}

	err = cli.BatchPut(ctx, keys, values)
	if err != nil {
		panic(err)
	}
	log.Printf("Successfully put %d items", items)

	// Get key from TiKV
	firstKey := []byte("range-test_1")
	val, err := cli.Get(ctx, firstKey)
	if err != nil {
		panic(err)
	}
	log.Printf("Found '%s'->'%s'", firstKey, val)

	// Scan key range from TiKV
	getLowerKey := []byte("range-test_2")
	getUpperKey := []byte("range-test_7")
	keysRes, valsRes, err := cli.Scan(ctx, getLowerKey, getUpperKey, 10)
	if err != nil {
		panic(err)
	}
	log.Printf("Scan %d keys '%s'~'%s'", len(keysRes), getLowerKey, getUpperKey)
	for i, key := range keysRes {
		log.Printf("  '%s'->'%s'", key, valsRes[i])
	}

	// Reverse scan key range from TiKV
	keysRes, valsRes, err = cli.ReverseScan(ctx, getUpperKey, getLowerKey, 10)
	if err != nil {
		panic(err)
	}
	log.Printf("Scan %d keys '%s'~'%s'", len(keysRes), getUpperKey, getLowerKey)
	for i, key := range keysRes {
		log.Printf("  '%s'->'%s'", key, valsRes[i])
	}

	// Delete key from TiKV
	delStartKey := []byte("range-test_")
	delStopKey := []byte("range-test_\xFF")
	err = cli.DeleteRange(ctx, delStartKey, delStopKey)
	if err != nil {
		panic(err)
	}
	log.Printf("Deleted keys")

	// Get key from TiKV
	val, err = cli.Get(ctx, firstKey)
	if err != nil {
		panic(err)
	}
	log.Printf("Found '%s'->'%s'", firstKey, val)
}
