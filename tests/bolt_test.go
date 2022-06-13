package tests

import (
    "github.com/DGHeroin/store/store/StoreBoltDB"
    "go.etcd.io/bbolt"
    "os"
    "path"
    "testing"
)

func TestBolt(t *testing.T) {
    tmpDir, _ := os.MkdirTemp(os.TempDir(), "store_")
    db, err := bbolt.Open(path.Join(tmpDir, "db0"), os.ModePerm, bbolt.DefaultOptions)
    if err != nil {
        t.Error(err)
        return
    }
    s := StoreBoltDB.New(db)
    doTestStore(t, s)
}
