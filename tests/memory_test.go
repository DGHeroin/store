package tests

import (
    "github.com/DGHeroin/store/store/StoreMemory"
    "testing"
)

func TestMemory(t *testing.T) {
    s := StoreMemory.New()
    doTestStore(t, s)
}
