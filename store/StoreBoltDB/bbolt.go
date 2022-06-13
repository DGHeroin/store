package StoreBoltDB

import (
    "bytes"
    "github.com/DGHeroin/store"
    "github.com/DGHeroin/store/utils"
    bolt "go.etcd.io/bbolt"
    "io"
    "io/ioutil"
    "os"
    "strings"
)

type (
    boltImpl struct {
        bucketName []byte
        db         *bolt.DB
    }
)

func (b boltImpl) RangeKeys(prefix, limit string, max int) (result store.KeysInfoSlice, err error) {
    db := b.db
    err = db.View(func(tx *bolt.Tx) error {
        b := tx.Bucket(b.bucketName)
        if b == nil {
            return store.Nil
        }
        prefixBytes := []byte(prefix)
        cur := b.Cursor()

        for k, v := cur.Seek(prefixBytes); k != nil; k, v = cur.Next() {
            key := string(k)
            if limit != "" && strings.HasPrefix(key, limit) {
                return nil
            }
            ok, _, value := utils.SplitData(v)
            if !ok {
                continue
            }
            result = append(result, store.KeysInfo{
                Key:  key,
                Size: int64(len(value)),
            })
            if len(result) >= max {
                return nil
            }
        }
        return nil
    })
    return
}

func (b boltImpl) Range(prefix, limit string, cb func(key string, value []byte) bool) error {
    db := b.db
    return db.View(func(tx *bolt.Tx) error {
        b := tx.Bucket(b.bucketName)
        if b == nil {
            return store.Nil
        }
        prefixBytes := []byte(prefix)
        cur := b.Cursor()
        for k, v := cur.Seek(prefixBytes); k != nil; k, v = cur.Next() {
            key := string(k)
            if limit != "" && strings.HasPrefix(key, limit) {
                return nil
            }
            ok, _, value := utils.SplitData(v)
            if !ok {
                continue
            }
            if !cb(key, value) {
                return nil
            }
        }
        return nil
    })
}

func (b boltImpl) RRange(prefix, limit string, cb func(key string, r io.Reader) bool) error {
    return b.Range(prefix, limit, func(key string, value []byte) bool {
        return cb(key, bytes.NewBuffer(value))
    })
}

func (b boltImpl) RPut(key string, r io.Reader, size int64) error {
    return b.RPutTTL(key, r, size, 0)
}

func (b boltImpl) RPutTTL(key string, r io.Reader, _ int64, ttl int64) error {
    value, err := ioutil.ReadAll(r)
    if err != nil {
        return err
    }
    return b.PutTTL(key, value, ttl)
}

func (b boltImpl) RGet(key string) (io.Reader, error) {
    value, err := b.Get(key)
    if err != nil {
        return nil, err
    }
    return bytes.NewBuffer(value), nil
}

func (b boltImpl) Put(key string, value []byte) error {
    return b.db.Update(func(tx *bolt.Tx) error {
        b, err := tx.CreateBucketIfNotExists(b.bucketName)
        if err != nil {
            return err
        }
        return b.Put([]byte(key), utils.CombineData(0, value))
    })
}

func (b boltImpl) PutTTL(key string, value []byte, ttl int64) error {
    return b.db.Update(func(tx *bolt.Tx) error {
        b, err := tx.CreateBucketIfNotExists(b.bucketName)
        if err != nil {
            return err
        }
        return b.Put([]byte(key), utils.CombineData(int(ttl), value))
    })
}

func (b boltImpl) Get(key string) (result []byte, err error) {
    err = b.db.View(func(tx *bolt.Tx) error {
        b := tx.Bucket(b.bucketName)
        if b == nil {
            return store.Nil
        }
        data := b.Get([]byte(key))
        ok, _, val := utils.SplitData(data)
        if !ok {
            go func() {
                _ = b.Delete([]byte(key))
            }()
            return store.Nil
        }
        result = utils.CopyBytes(val)
        return nil
    })
    return
}

func (b boltImpl) Exist(key string) (ok bool, err error) {
    err = b.db.View(func(tx *bolt.Tx) error {
        b := tx.Bucket(b.bucketName)
        if b == nil {
            return store.Nil
        }
        data := b.Get([]byte(key))
        ok, _, _ = utils.SplitData(data)
        return nil
    })
    return
}

func (b boltImpl) Delete(key string) error {
    return b.db.Update(func(tx *bolt.Tx) error {
        b, err := tx.CreateBucketIfNotExists(b.bucketName)
        if err != nil {
            return err
        }
        return b.Delete([]byte(key))
    })
}

func New(db *bolt.DB) store.Store {
    impl := &boltImpl{
        bucketName: []byte("default"),
        db:         db,
    }
    return impl
}
func FromEnv() store.Store {
    dbDir := os.Getenv("BBOLT_PATH")
    db, err := bolt.Open(dbDir, os.ModePerm, bolt.DefaultOptions)
    if err != nil {
        return nil
    }
    return New(db)
}

var _ = FromEnv