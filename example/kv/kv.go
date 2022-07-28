package main

import (
    "github.com/DGHeroin/store/store/StoreBoltDB"
    "go.etcd.io/bbolt"
    "log"
    "os"
    "path"
    "time"
)

func main() {
    tmpDir, _ := os.MkdirTemp(os.TempDir(), "store_")
    db, err := bbolt.Open(path.Join(tmpDir, "db0"), os.ModePerm, bbolt.DefaultOptions)
    if err != nil {
        log.Fatalln(err)
        return
    }
    s := StoreBoltDB.New(db)
    defer s.Close()

    key := "hello"
    s.Put(key, []byte{6, 6, 6})
    log.Println(s.Get(key))
    err = s.PutTTL(key, []byte{1, 2, 3}, time.Second*10)
    if err != nil {
        log.Fatalln("write err:", err)
        return
    }

    time.Sleep(time.Second)
    ttl, err := s.TTL(key)
    if err != nil {
        log.Fatalln("get ttl err:", err)
        return
    }
    log.Println("ttl:", ttl)
}
