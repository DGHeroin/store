package tests

import (
    "github.com/DGHeroin/store"
    "github.com/DGHeroin/store/store/StoreBoltDB"
    "go.etcd.io/bbolt"
    "os"
    "path"
    "testing"
)

func TestChain(t *testing.T) {
    tmpDir, _ := os.MkdirTemp(os.TempDir(), "store_")
    t.Log("chain db ready in", tmpDir)

    db0, err := bbolt.Open(path.Join(tmpDir, "db0"), os.ModePerm, bbolt.DefaultOptions)
    if err != nil {
        panic(err)
    }
    defer db0.Close()
    s0 := StoreBoltDB.New(db0)

    db1, err := bbolt.Open(path.Join(tmpDir, "db1"), os.ModePerm, bbolt.DefaultOptions)
    if err != nil {
        panic(err)
    }
    defer db1.Close()

    s1 := StoreBoltDB.New(db1)

    s := store.NewChain(s0, s1)
    doTestStore(t, s)

}
