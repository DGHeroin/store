package tests

import (
    "github.com/DGHeroin/store"
    "io"
    "io/ioutil"
    "testing"
)

func doTestStore(t *testing.T, s store.Store) {
    tIfError(t, s.Put("key_09", []byte{9}))
    tIfError(t, s.Put("key_10", []byte{10}))
    tIfError(t, s.Put("key_11", []byte{11}))
    tIfError(t, s.Put("key_12", []byte{12}))
    tIfError(t, s.Put("key_13", []byte{13}))
    tIfError(t, s.Put("key_14", []byte{14}))

    lst, err := s.RangeKeys("key_1", "key_12", 2)
    tIfError(t, err)
    for _, info := range lst {
        t.Log("RangeKeys:", info.Key, info.Size)
    }

    _ = s.Delete("key_11")
    _ = s.RRange("key_1", "key_13", func(key string, r io.Reader) bool {
        data, _ := ioutil.ReadAll(r)
        t.Log("RRange:", key, data)
        return true
    })
    _ = s.Range("key_1", "key_13", func(key string, data []byte) bool {
        t.Log("Range:", key, data)
        return true
    })
}
func tIfError(t *testing.T, err error) {
    if err != nil {
        t.Error(err)
    }
}
