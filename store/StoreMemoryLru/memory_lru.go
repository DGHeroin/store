package StoreMemoryLru

import (
    "bytes"
    "github.com/DGHeroin/store"
    "github.com/DGHeroin/store/utils"
    "io"
    "io/ioutil"
    "time"
)

type (
    implMemoryLRU struct {
        m *utils.LRU
    }
)

func (i *implMemoryLRU) Close() error {
    return nil
}

func (i *implMemoryLRU) TTL(key string) (time.Duration, error) {
    p, ok := i.m.Get(key)
    if !ok {
        return 0, nil
    }
    ok, ttl, _ := utils.SplitData(p.([]byte))
    if !ok {
        return -1, nil
    }
    durationSec := int64(ttl) - utils.GetTimeNow().Unix()
    return time.Duration(durationSec) * time.Second, nil
}

func (i *implMemoryLRU) RangeKeys(prefix, limit string, max int) (result store.KeysInfoSlice, err error) {
    keysPtr := i.m.Keys()
    var keys []string
    for _, k := range keysPtr {
        keys = append(keys, k.(string))
    }
    keys = utils.CutStringSlice(keys, prefix, limit)
    if len(keys) > max {
        keys = keys[:max]
    }
    for _, key := range keys {
        if p, ok := i.m.Get(key); ok {
            if ok, _, value := utils.SplitData(p.([]byte)); ok {
                result = append(result, store.KeysInfo{
                    Key:  key,
                    Size: int64(len(value)),
                })
            }
        }
    }
    return
}

func (i *implMemoryLRU) Put(key string, value []byte) error {
    return i.PutTTL(key, value, 0)
}

func (i *implMemoryLRU) PutTTL(key string, value []byte, ttl time.Duration) error {
    data := utils.CombineData(ttl, value)
    i.m.Add(key, data)
    return nil
}

func (i *implMemoryLRU) Get(key string) ([]byte, error) {
    if p, ok := i.m.Get(key); ok {
        ok, _, value := utils.SplitData(p.([]byte))
        if ok {
            return utils.CopyBytes(value), nil
        } else {
            go func() {
                _ = i.Delete(key)
            }()
        }
    }
    return nil, nil
}

func (i *implMemoryLRU) RPut(key string, r io.Reader, size int64) error {
    return i.RPutTTL(key, r, size, 0)
}

func (i *implMemoryLRU) RPutTTL(key string, r io.Reader, _ int64, ttl time.Duration) error {
    value, err := ioutil.ReadAll(r)
    if err != nil {
        return err
    }
    data := utils.CombineData(ttl, value)
    i.m.Add(key, data)
    return nil
}

func (i *implMemoryLRU) RGet(key string) (io.Reader, error) {
    data, err := i.Get(key)
    if err != nil {
        return nil, err
    }
    return bytes.NewBuffer(data), nil
}

func (i *implMemoryLRU) Exist(key string) (bool, error) {
    if _, err := i.Get(key); err != nil {
        return false, err
    }
    return true, nil
}

func (i *implMemoryLRU) Delete(key string) error {
    i.m.Remove(key)
    return nil
}

func (i *implMemoryLRU) Range(prefix, limit string, cb func(key string, value []byte) bool) error {
    if i.m.Len() == 0 {
        return nil
    }
    keys := make([]string, 0)
    var keysPtr = i.m.Keys()
    for _, k := range keysPtr {
        keys = append(keys, k.(string))
    }
    keys = utils.CutStringSlice(keys, prefix, limit)
    for _, k := range keys {
        value, err := i.Get(k)
        if err != nil {
            continue
        }
        if !cb(k, value) {
            break
        }
    }
    return nil
}

func (i *implMemoryLRU) RRange(prefix, limit string, cb func(key string, r io.Reader) bool) error {
    return i.Range(prefix, limit, func(key string, value []byte) bool {
        return cb(key, bytes.NewBuffer(value))
    })
}

func New(size int, cb func(key string, value []byte)) store.Store {
    m := &implMemoryLRU{
        m: utils.NewLRU(size, func(key interface{}, value interface{}) {
            k := key.(string)
            v := value.([]byte)
            if ok, _, data := utils.SplitData(v); ok {
                cb(k, data)
            }

        }),
    }
    return m
}

var _ = New
