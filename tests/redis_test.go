package tests

import (
    "github.com/DGHeroin/store/store/StoreRedis"
    "github.com/go-redis/redis/v8"
    "testing"
)

func TestRedis(t *testing.T) {
    cli := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
    s := StoreRedis.New(cli)
    if s == nil {
        t.Error("redis init failed.")
        return
    }
    doTestStore(t, s)
}
