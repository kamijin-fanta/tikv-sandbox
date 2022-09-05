package main

import (
	"context"
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

	log.Printf("Cluster ID: %d\n", cli.ClusterID())

	key := []byte("key-example")
	val := []byte("bytes-value")

	// Put key into TiKV
	err = cli.Put(ctx, key, val)
	if err != nil {
		panic(err)
	}
	log.Printf("Successfully put '%s'->'%s'", key, val)

	// Get key from TiKV
	val, err = cli.Get(ctx, key)
	if err != nil {
		panic(err)
	}
	log.Printf("Found '%s'->'%s'", key, val)

	// Delete key from TiKV
	err = cli.Delete(ctx, key)
	if err != nil {
		panic(err)
	}
	log.Printf("Deleted key: '%s'", key)

	// Get key again from TiKV
	val, err = cli.Get(ctx, key)
	if err != nil {
		panic(err)
	}
	log.Printf("Found '%s'->'%s'", key, val)
}
