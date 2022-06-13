package tests

import (
    "github.com/DGHeroin/store/store/StoreMemoryLru"
    "testing"
)

func TestLRU(t *testing.T) {
    s := StoreMemoryLru.New(2, func(key string, value []byte) {
        t.Log("on lru evict:", key, value)
    })
    doTestStore(t, s)
}
