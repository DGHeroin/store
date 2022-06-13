package tests

import (
    "github.com/DGHeroin/store/store/StoreLeveldb"
    "github.com/syndtr/goleveldb/leveldb"
    "os"
    "testing"
)

func TestLvdb(t *testing.T) {
    tmpDir, _ := os.MkdirTemp(os.TempDir(), "store_")
    db, err := leveldb.OpenFile(tmpDir, nil)
    if err != nil {
        t.Error(err)
        return
    }
    s := StoreLeveldb.New(db)
    doTestStore(t, s)
}
