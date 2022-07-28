package store

import (
    "io"
    "time"
)

type (
    Store interface {
        Close() error
        Put(key string, value []byte) error
        PutTTL(key string, value []byte, ttl time.Duration) error
        Get(key string) ([]byte, error)

        TTL(key string) (time.Duration, error)

        RPut(key string, r io.Reader, size int64) error
        RPutTTL(key string, r io.Reader, size int64, ttl time.Duration) error
        RGet(key string) (io.Reader, error)

        Exist(key string) (bool, error)
        Delete(key string) error

        RangeKeys(prefix, limit string, max int) (KeysInfoSlice, error)
        Range(prefix, limit string, cb func(key string, value []byte) bool) error
        RRange(prefix, limit string, cb func(key string, r io.Reader) bool) error
    }
    KeysInfo struct {
        Key  string
        Size int64
    }
    KeysInfoSlice []KeysInfo
)

var (
    buckets = map[string]Store{}
)

func Get(bucket string) Store {
    return buckets[bucket]
}

func InitStore(bucket string, store Store) {
    buckets[bucket] = store
}

var (
    _ = Get
    _ = InitStore
)

func (s KeysInfoSlice) ToKeys() (result []string) {
    for _, info := range s {
        result = append(result, info.Key)
    }
    return
}
