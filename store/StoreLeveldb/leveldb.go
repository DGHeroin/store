package StoreLeveldb

import (
    "bytes"
    "github.com/DGHeroin/store"
    "github.com/DGHeroin/store/utils"
    "github.com/syndtr/goleveldb/leveldb"
    "github.com/syndtr/goleveldb/leveldb/util"
    "io"
    "io/ioutil"
    "os"
    "time"
)

type (
    leveldbImpl struct {
        db *leveldb.DB
    }
)

func (l leveldbImpl) Close() error {
    return l.db.Close()
}

func (l leveldbImpl) TTL(key string) (time.Duration, error) {
    p, err := l.db.Get([]byte(key), nil)
    if err != nil {
        return 0, err
    }
    ok, ttl, _ := utils.SplitData(p)
    if !ok {
        return -1, nil
    }
    durationSec := int64(ttl) - utils.GetTimeNow().Unix()
    return time.Duration(durationSec) * time.Second, nil
}

func (l leveldbImpl) RangeKeys(prefix, limit string, max int) (result store.KeysInfoSlice, err error) {
    db := l.db
    it := db.NewIterator(&util.Range{
        Start: []byte(prefix),
        Limit: []byte(limit),
    }, nil)
    defer it.Release()
    for it.Next() {
        ok, _, value := utils.SplitData(it.Value())
        if !ok {
            continue
        }
        result = append(result, store.KeysInfo{
            Key:  string(it.Key()),
            Size: int64(len(value)),
        })
        if len(result) >= max {
            break
        }
    }
    return
}

func (l leveldbImpl) Range(prefix, limit string, cb func(key string, value []byte) bool) error {
    db := l.db
    it := db.NewIterator(&util.Range{
        Start: []byte(prefix),
        Limit: []byte(limit),
    }, nil)
    defer it.Release()
    for it.Next() {
        ok, _, value := utils.SplitData(it.Value())
        if !ok {
            continue
        }
        if !cb(string(it.Key()), utils.CopyBytes(value)) {
            break
        }
    }
    return nil
}

func (l leveldbImpl) RRange(prefix, limit string, cb func(key string, r io.Reader) bool) error {
    return l.Range(prefix, limit, func(key string, value []byte) bool {
        return cb(key, bytes.NewBuffer(value))
    })
}

func (l leveldbImpl) RPut(key string, r io.Reader, size int64) error {
    return l.RPutTTL(key, r, size, 0)
}

func (l leveldbImpl) RPutTTL(key string, r io.Reader, _ int64, ttl time.Duration) error {
    value, err := ioutil.ReadAll(r)
    if err != nil {
        return err
    }
    return l.PutTTL(key, value, ttl)
}

func (l leveldbImpl) RGet(key string) (io.Reader, error) {
    value, err := l.Get(key)
    if err != nil {
        return nil, err
    }
    return bytes.NewBuffer(value), nil
}

func (l leveldbImpl) Put(key string, value []byte) error {
    return l.db.Put([]byte(key), utils.CombineData(0, value), nil)
}

func (l leveldbImpl) PutTTL(key string, value []byte, ttl time.Duration) error {
    return l.db.Put([]byte(key), utils.CombineData(ttl, value), nil)
}

func (l leveldbImpl) Get(key string) ([]byte, error) {
    value, err := l.db.Get([]byte(key), nil)
    if err != nil {
        if err == leveldb.ErrNotFound {
            return nil, nil
        }
        return nil, err
    }
    if ok, _, data := utils.SplitData(value); ok {
        return utils.CopyBytes(data), nil
    } else {
        err = l.db.Delete([]byte(key), nil)
        return nil, err
    }
}

func (l leveldbImpl) Exist(key string) (bool, error) {
    data, err := l.Get(key)
    return data != nil, err
}

func (l leveldbImpl) Delete(key string) error {
    return l.db.Delete([]byte(key), nil)
}

func New(db *leveldb.DB) store.Store {
    p := &leveldbImpl{db: db}
    return p
}
func FromEnv() store.Store {
    dbDir := os.Getenv("LEVELDB_PATH")
    db, err := leveldb.OpenFile(dbDir, nil)
    if err != nil {
        return nil
    }
    return New(db)
}

var _ = FromEnv
