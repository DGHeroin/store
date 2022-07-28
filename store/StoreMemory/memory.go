package StoreMemory

import (
    "bytes"
    "github.com/DGHeroin/store"
    "github.com/DGHeroin/store/utils"
    "io"
    "io/ioutil"
    "sync"
    "time"
)

type (
    implMemory struct {
        mu sync.RWMutex
        m  map[string][]byte
    }
)

func (i *implMemory) Close() error {
    return nil
}

func (i *implMemory) TTL(key string) (time.Duration, error) {
    i.mu.RLock()
    defer i.mu.RUnlock()
    p, ok := i.m[key]
    if !ok {
        return 0, nil
    }
    ok, ttl, _ := utils.SplitData(p)
    if !ok {
        return -1, nil
    }
    durationSec := int64(ttl) - utils.GetTimeNow().Unix()
    return time.Duration(durationSec) * time.Second, nil
}

func (i *implMemory) RangeKeys(prefix, limit string, max int) (result store.KeysInfoSlice, err error) {
    i.mu.RLock()
    defer i.mu.RUnlock()
    var (
        keys []string
        mm   = map[string]store.KeysInfo{}
    )
    for key, v := range i.m {
        ok, _, value := utils.SplitData(v)
        if ok {
            keys = append(keys, key)
            mm[key] = store.KeysInfo{
                Key:  key,
                Size: int64(len(value)),
            }
        } else {
            go func() {
                _ = i.Delete(key)
            }()
        }
    }
    keys = utils.CutStringSlice(keys, prefix, limit)
    if len(keys) > max {
        keys = keys[:max]
    }
    for _, key := range keys {
        v := i.m[key]
        _, _, value := utils.SplitData(v)
        result = append(result, store.KeysInfo{
            Key:  key,
            Size: int64(len(value)),
        })
    }
    return
}

func (i *implMemory) Put(key string, value []byte) error {
    return i.PutTTL(key, value, 0)
}

func (i *implMemory) PutTTL(key string, value []byte, ttl time.Duration) error {
    i.mu.Lock()
    defer i.mu.Unlock()
    data := utils.CombineData(ttl, value)
    i.m[key] = data
    return nil
}

func (i *implMemory) Get(key string) ([]byte, error) {
    i.mu.RLock()
    defer i.mu.RUnlock()
    if data, ok := i.m[key]; ok {
        ok, _, value := utils.SplitData(data)
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

func (i *implMemory) RPut(key string, r io.Reader, size int64) error {
    return i.RPutTTL(key, r, size, 0)
}

func (i *implMemory) RPutTTL(key string, r io.Reader, _ int64, ttl time.Duration) error {
    i.mu.Lock()
    defer i.mu.Unlock()
    value, err := ioutil.ReadAll(r)
    if err != nil {
        return err
    }
    data := utils.CombineData(ttl, value)
    i.m[key] = data
    return nil
}

func (i *implMemory) RGet(key string) (io.Reader, error) {
    data, err := i.Get(key)
    if err != nil {
        return nil, err
    }
    return bytes.NewBuffer(data), nil
}

func (i *implMemory) Exist(key string) (bool, error) {
    if _, err := i.Get(key); err != nil {
        return false, err
    }
    return true, nil
}

func (i *implMemory) Delete(key string) error {
    i.mu.Lock()
    defer i.mu.Unlock()
    delete(i.m, key)
    return nil
}

func (i *implMemory) Range(prefix, limit string, cb func(key string, value []byte) bool) error {
    i.mu.RLock()
    defer i.mu.RUnlock()
    if len(i.m) == 0 {
        return nil
    }
    keys := make([]string, 0)
    for k := range i.m {
        keys = append(keys, k)
    }
    keys = utils.CutStringSlice(keys, prefix, limit)
    for _, k := range keys {
        data := i.m[k]
        ok, _, value := utils.SplitData(data)
        if !ok {
            delete(i.m, k)
            continue
        }
        if !cb(k, value) {
            break
        }
    }
    return nil
}

func (i *implMemory) RRange(prefix, limit string, cb func(key string, r io.Reader) bool) error {
    return i.Range(prefix, limit, func(key string, value []byte) bool {
        return cb(key, bytes.NewBuffer(value))
    })
}

func New() store.Store {
    m := &implMemory{
        m: make(map[string][]byte),
    }
    return m
}

var _ = New
