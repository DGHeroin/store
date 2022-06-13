package StoreRedis

import (
    "bytes"
    "context"
    "crypto/tls"
    "github.com/DGHeroin/store"
    "github.com/DGHeroin/store/utils"
    "github.com/go-redis/redis/v8"
    "io"
    "io/ioutil"
    "os"
    "strconv"
    "strings"
    "time"
)

type (
    redisImpl struct {
        client *redis.Client
    }
)

func (s redisImpl) RangeKeys(prefix, limit string, max int) (result store.KeysInfoSlice, err error) {
    cli := s.client
    ctx := context.Background()

    var cursor uint64
    // var value []byte
    matchStr := prefix
    if !strings.HasSuffix(prefix, "*") {
        matchStr = prefix + "*"
    }

    // var n int
    for {
        var keys []string
        keys, cursor, err = cli.Scan(ctx, cursor, matchStr, 10000).Result()
        if err != nil {
            return
        }
        keys = utils.CutStringSlice(keys, prefix, limit)
        for _, key := range keys {
            result = append(result, store.KeysInfo{
                Key:  key,
                Size: -1,
            })
            if len(result) >= max {
                return
            }
            // if value, err = cli.Get(ctx, key).Bytes(); err == nil {
            //     result = append(result, store.KeysInfo{
            //         Key:  key,
            //         Size: int64(len(value)),
            //     })
            //     if len(result) >= max {
            //         return
            //     }
            // }
        }
        if cursor == 0 {
            break
        }
    }
    return
}

func (s redisImpl) Range(prefix, limit string, cb func(key string, value []byte) bool) error {
    cli := s.client
    ctx := context.Background()

    var cursor uint64
    // var n int
    for {
        var keys []string
        var err error
        keys, cursor, err = cli.Scan(ctx, cursor, prefix, 10000).Result()
        if err != nil {
            return err
        }
        keys = utils.CutStringSlice(keys, prefix, limit)
        for _, key := range keys {
            if limit != "" {
                if strings.HasPrefix(key, limit) {
                    return nil
                }
            }
            if value, err := cli.Get(ctx, key).Bytes(); err == nil {
                if !cb(key, value) {
                    return nil
                }
            }
        }
        if cursor == 0 {
            break
        }
    }
    return nil
}

func (s redisImpl) RRange(prefix, limit string, cb func(key string, r io.Reader) bool) error {
    return s.Range(prefix, limit, func(key string, value []byte) bool {
        return cb(key, bytes.NewBuffer(value))
    })
}

func (s redisImpl) RPut(key string, r io.Reader, size int64) error {
    return s.RPutTTL(key, r, size, 0)
}

func (s redisImpl) RPutTTL(key string, r io.Reader, _ int64, ttl int64) error {
    value, err := ioutil.ReadAll(r)
    if err != nil {
        return err
    }
    return s.PutTTL(key, value, ttl)
}

func (s redisImpl) RGet(key string) (io.Reader, error) {
    value, err := s.Get(key)
    if err != nil {
        return nil, err
    }
    return bytes.NewBuffer(value), nil
}

func (s redisImpl) Exist(key string) (bool, error) {
    val, err := s.client.Exists(context.Background(), key).Result()
    if err == redis.Nil {
        err = store.Nil
    }
    return val == 1, err
}

func (s redisImpl) Put(key string, value []byte) error {
    return s.PutTTL(key, value, 0)
}

func (s redisImpl) Get(key string) ([]byte, error) {
    r, err := s.client.Get(context.Background(), key).Bytes()
    if err == redis.Nil {
        err = store.Nil
    }
    return r, err
}

func (s redisImpl) PutTTL(key string, value []byte, ttl int64) error {
    return s.client.Set(context.Background(), key, value, time.Duration(ttl)).Err()
}
func (s redisImpl) Delete(key string) error {
    return s.client.Del(context.Background(), key).Err()
}

func New(client *redis.Client) store.Store {
    s := redisImpl{
        client: client,
    }
    return s
}
func FromEnv() store.Store {
    opt := &redis.Options{
        Addr:     os.Getenv("REDIS_ADDRESS"),
        Username: os.Getenv("REDIS_USERNAME"),
        Password: os.Getenv("REDIS_PASSWORD"),
    }
    if val, err := strconv.Atoi(os.Getenv("REDIS_DB")); err == nil {
        opt.DB = val
    }
    if val, err := strconv.Atoi(os.Getenv("REDIS_POOL_SIZE")); err == nil {
        opt.PoolSize = val
    }
    tlsCrt := os.Getenv("REDIS_TLS_CRT")
    tlsKey := os.Getenv("REDIS_TLS_KEY")
    if tlsKey != tlsCrt && tlsKey != "" {
        if cer, err := tls.LoadX509KeyPair(tlsCrt, tlsKey); err == nil {
            opt.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cer}}
        }
    }
    return New(redis.NewClient(opt))
}

var _ = FromEnv
