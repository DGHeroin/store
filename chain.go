package store

import (
    "io"
    "time"
)

type (
    chain struct {
        list StSlice
    }
    StSlice []Store
)

func (c chain) Close() error {
    c.list.Range(func(store Store) bool {
        _ = store.Close()
        return true
    })
    return nil
}

func (c chain) TTL(key string) (r time.Duration, err error) {
    c.list.Range(func(store Store) bool {
        if r, err = store.TTL(key); err == nil {
            return false
        }
        return true
    })
    return
}

func (c chain) RangeKeys(prefix, limit string, max int) (result KeysInfoSlice, err error) {
    c.list.Range(func(store Store) bool {
        if result, err = store.RangeKeys(prefix, limit, max); err == nil && len(result) > 0 {
            return false
        }
        return true
    })
    return
}

func (c chain) Range(prefix, limit string, cb func(key string, value []byte) bool) (err error) {
    c.list.Range(func(v Store) bool {
        if err = v.Range(prefix, limit, cb); err != nil {
            return false
        }
        return true
    })
    return
}

func (c chain) RRange(prefix, limit string, cb func(key string, r io.Reader) bool) (err error) {
    c.list.Range(func(v Store) bool {
        if err := v.RRange(prefix, limit, cb); err != nil {
            return false
        }
        return true
    })
    return
}

func (c chain) RPut(key string, r io.Reader, size int64) error {
    return c.RPutTTL(key, r, size, 0)
}

func (c chain) RPutTTL(key string, r io.Reader, size int64, ttl time.Duration) (err error) {
    c.list.RevRange(func(v Store) bool {
        if err = v.RPutTTL(key, r, size, ttl); err != nil {
            return false
        }
        return true
    })
    return
}

func (c chain) RGet(key string) (r io.Reader, err error) {
    c.list.RevRange(func(v Store) bool {
        if r, err = v.RGet(key); err == nil {
            return false
        }
        return true
    })
    return
}

func (c chain) Put(key string, value []byte) error {
    for i := len(c.list) - 1; i >= 0; i-- {
        store := c.list[i]
        if err := store.Put(key, value); err != nil {
            return err
        }
    }
    return nil
}

func (c chain) PutTTL(key string, value []byte, ttl time.Duration) error {
    for i := len(c.list) - 1; i >= 0; i-- {
        store := c.list[i]
        if err := store.PutTTL(key, value, ttl); err != nil {
            return err
        }
    }
    return nil
}

func (c chain) Get(key string) ([]byte, error) {
    for _, store := range c.list {
        if data, err := store.Get(key); err == nil {
            return data, err
        }
    }
    return nil, nil
}

func (c chain) Exist(key string) (bool, error) {
    for _, store := range c.list {
        if ok, err := store.Exist(key); err == nil {
            if ok {
                return true, nil
            }
        }
    }
    return false, nil
}

func (c chain) Delete(key string) error {
    for _, store := range c.list {
        if err := store.Delete(key); err != nil {
            return err
        }
    }
    return nil
}

// NewChain 存储链
func NewChain(store ...Store) Store {
    c := chain{
        list: store,
    }
    return c
}

var _ = NewChain

func (s StSlice) Range(cb func(Store) bool) {
    for _, v := range s {
        if !cb(v) {
            break
        }
    }
}
func (s StSlice) RevRange(cb func(Store) bool) {
    for i := len(s) - 1; i >= 0; i-- {
        if !cb(s[i]) {
            break
        }
    }
}
