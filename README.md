# store

```
易用的链式存储kv store
避免在缓存与持久化里沉沦, 减少心智负担
```

```
read
-> memory
    -> redis
        -> [kvdb/asw s3]

write
-> [kvdb/asw s3]
    -> redis
        -> memory
```
