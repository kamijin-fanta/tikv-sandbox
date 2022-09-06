package main

import (
	"context"
	"github.com/tikv/client-go/v2/txnkv"
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

	client, err := txnkv.NewClient(addresses)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Put keys with transaction
	tx, err := client.Begin()
	if err != nil {
		panic(err)
	}
	err = tx.Set([]byte("key-1"), []byte("value-1"))
	if err != nil {
		panic(err)
	}
	err = tx.Set([]byte("key-2"), []byte("value-2"))
	if err != nil {
		panic(err)
	}
	err = tx.Commit(ctx) // Commit
	if err != nil {
		panic(err)
	}
	log.Printf("Put 2 keys with transaction")

	// Get key from TiKV
	tx, err = client.Begin()
	if err != nil {
		panic(err)
	}
	v, err := tx.Get(ctx, []byte("key-1"))
	if err != nil {
		panic(err)
	}
	log.Printf("Get key from TiKV '%s'", v)

	// Scan key range from TiKV
	lowerKey := []byte("key-0")
	upperKey := []byte("key-9")

	it, err := tx.Iter(lowerKey, upperKey)
	if err != nil {
		panic(err)
	}
	log.Printf("Scan keys '%s'~'%s'", lowerKey, upperKey)
	for it.Valid() {
		log.Printf("  '%s'->'%s'", it.Key(), it.Value())

		err := it.Next()
		if err != nil {
			panic(err)
		}
	}

	// Delete key from TiKV
	tx, err = client.Begin()
	err = tx.Delete([]byte("key-1"))
	if err != nil {
		panic(err)
	}
	err = tx.Delete([]byte("key-2"))
	if err != nil {
		panic(err)
	}
	err = tx.Commit(ctx)
	if err != nil {
		panic(err)
	}
	log.Printf("Delete 2 keys")
}
