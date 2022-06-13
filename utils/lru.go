package utils

import (
    "container/list"
    "sync"
)

type (
    EvictCallback func(key interface{}, value interface{})
    LRU           struct {
        mu        sync.RWMutex
        size      int
        evictList *list.List
        items     map[interface{}]*list.Element
        onEvict   EvictCallback
    }
    entry struct {
        key   interface{}
        value interface{}
    }
)

const DefaultEvictedBufferSize = 16

func NewLRU(size int, onEvict EvictCallback) *LRU {
    if size <= 0 {
        size = DefaultEvictedBufferSize
    }
    c := &LRU{
        size:      size,
        evictList: list.New(),
        items:     make(map[interface{}]*list.Element),
        onEvict:   onEvict,
    }
    return c
}
func (c *LRU) Clear() {
    c.mu.Lock()
    c.mu.Unlock()
    for k, v := range c.items {
        if c.onEvict != nil {
            c.onEvict(k, v.Value.(*entry).value)
        }
        delete(c.items, k)
    }
    c.evictList.Init()
}
func (c *LRU) Add(key, value interface{}) (evicted bool) {
    c.mu.Lock()
    defer c.mu.Unlock()
    if ent, ok := c.items[key]; ok {
        c.evictList.MoveToFront(ent)
        ent.Value.(*entry).value = value
        return false
    }
    ent := &entry{key, value}
    entry := c.evictList.PushFront(ent)
    c.items[key] = entry

    evict := c.evictList.Len() > c.size
    if evict {
        c.removeOldest()
    }
    return evict
}
func (c *LRU) Get(key interface{}) (value interface{}, ok bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    if ent, ok := c.items[key]; ok {
        c.evictList.MoveToFront(ent)
        if ent.Value.(*entry) == nil {
            return nil, false
        }
        return ent.Value.(*entry).value, true
    }
    return
}
func (c *LRU) Contains(key interface{}) (ok bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    _, ok = c.items[key]
    return ok
}
func (c *LRU) Peek(key interface{}) (value interface{}, ok bool) {
    c.mu.Lock()
    defer c.mu.Unlock()
    var ent *list.Element
    if ent, ok = c.items[key]; ok {
        return ent.Value.(*entry).value, true
    }
    return nil, ok
}
func (c *LRU) Remove(key interface{}) (present bool) {
    c.mu.Lock()
    defer c.mu.Unlock()
    if ent, ok := c.items[key]; ok {
        c.removeElement(ent)
        return true
    }
    return false
}
func (c *LRU) RemoveOldest() (key, value interface{}, ok bool) {
    c.mu.Lock()
    defer c.mu.Unlock()

    ent := c.evictList.Back()
    if ent != nil {
        c.removeElement(ent)
        kv := ent.Value.(*entry)
        return kv.key, kv.value, true
    }
    return nil, nil, false
}
func (c *LRU) GetOldest() (key, value interface{}, ok bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    ent := c.evictList.Back()
    if ent != nil {
        kv := ent.Value.(*entry)
        return kv.key, kv.value, true
    }
    return nil, nil, false
}
func (c *LRU) Keys() []interface{} {
    c.mu.RLock()
    defer c.mu.RUnlock()
    keys := make([]interface{}, len(c.items))
    i := 0
    for ent := c.evictList.Back(); ent != nil; ent = ent.Prev() {
        keys[i] = ent.Value.(*entry).key
        i++
    }
    return keys
}
func (c *LRU) Len() int {
    c.mu.RLock()
    defer c.mu.RUnlock()
    return c.evictList.Len()
}

func (c *LRU) Resize(size int) (evicted int) {
    c.mu.Lock()
    defer c.mu.Unlock()
    diff := c.Len() - size
    if diff < 0 {
        diff = 0
    }
    for i := 0; i < diff; i++ {
        c.removeOldest()
    }
    c.size = size
    return diff
}
func (c *LRU) removeOldest() {
    ent := c.evictList.Back()
    if ent != nil {
        c.removeElement(ent)
    }
}
func (c *LRU) removeElement(e *list.Element) {
    c.evictList.Remove(e)
    kv := e.Value.(*entry)
    delete(c.items, kv.key)
    if c.onEvict != nil {
        c.onEvict(kv.key, kv.value)
    }
}
