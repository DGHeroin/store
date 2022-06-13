package StoreS3

import (
    "bytes"
    "context"
    "github.com/DGHeroin/store"
    "github.com/DGHeroin/store/utils"
    "github.com/minio/minio-go/v7"
    "github.com/minio/minio-go/v7/pkg/credentials"
    "io"
    "io/ioutil"
    "math"
    "os"
    "strings"
)

type (
    s3Impl struct {
        bucketName string
        client     *minio.Client
    }
)

func (s s3Impl) RangeKeys(prefix, limit string, max int) (result store.KeysInfoSlice, err error) {
    ch := s.client.ListObjects(context.Background(), s.bucketName, minio.ListObjectsOptions{
        Prefix:    prefix,
        Recursive: true,
        MaxKeys:   max,
    })
    var keys []string
    var mm = map[string]store.KeysInfo{}
    for info := range ch {
        key := info.Key
        if strings.HasSuffix(key, "/") {
            continue
        }
        keys = append(keys, key)
        mm[key] = store.KeysInfo{
            Key:  key,
            Size: info.Size,
        }
    }
    keys = utils.CutStringSlice(keys, prefix, limit)
    for _, key := range keys {
        result = append(result, mm[key])
        if len(result) >= max {
            break
        }
    }
    return
}

func (s s3Impl) Range(prefix, limit string, cb func(key string, value []byte) bool) error {
    return s.RRange(prefix, limit, func(key string, r io.Reader) bool {
        if data, err := ioutil.ReadAll(r); err == nil {
            return cb(key, data)
        }
        return true
    })
}

func (s s3Impl) RRange(prefix, limit string, cb func(key string, r io.Reader) bool) error {
    arr, err := s.RangeKeys(prefix, limit, math.MaxInt64)
    if err != nil {
        return err
    }
    for _, info := range arr {
        key := info.Key
        if r, err := s.RGet(key); err == nil {
            if !cb(key, r) {
                return nil
            }
        }
    }
    return nil
}

func (s s3Impl) RPut(key string, r io.Reader, size int64) error {
    return s.RPutTTL(key, r, size, 0)
}

func (s s3Impl) RPutTTL(key string, r io.Reader, size, _ int64) error {
    _, err := s.client.PutObject(context.Background(), s.bucketName, key, r, size, minio.PutObjectOptions{})
    return err
}

func (s s3Impl) RGet(key string) (io.Reader, error) {
    return s.client.GetObject(context.Background(), s.bucketName, key, minio.GetObjectOptions{})
}

func (s s3Impl) Put(key string, value []byte) error {
    return s.PutTTL(key, value, 0)
}

func (s s3Impl) PutTTL(key string, value []byte, _ int64) error {
    return s.RPutTTL(key, bytes.NewBuffer(value), int64(len(value)), 0)
}

func (s s3Impl) Get(key string) ([]byte, error) {
    obj, err := s.RGet(key)
    if err != nil {
        return nil, err
    }
    return ioutil.ReadAll(obj)
}

func (s s3Impl) Exist(key string) (bool, error) {
    obj, err := s.client.StatObject(context.Background(), s.bucketName, key, minio.StatObjectOptions{})
    if err != nil {
        return false, err
    }
    return obj.Key != "", nil
}

func (s s3Impl) Delete(key string) error {
    return s.client.RemoveObject(context.Background(), s.bucketName, key, minio.RemoveObjectOptions{})
}

func New(bucketName, endpoint, accessKeyID, secretAccessKey string) store.Store {
    minioClient, err := minio.New(endpoint, &minio.Options{
        Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
        Secure: false,
    })
    if err != nil {
        return nil
    }
    s := &s3Impl{
        bucketName: bucketName,
        client:     minioClient,
    }
    if ok, err := minioClient.BucketExists(context.Background(), bucketName); err != nil {
        return nil
    } else {
        if !ok {
            err = minioClient.MakeBucket(context.Background(), bucketName, minio.MakeBucketOptions{})
            if err != nil {
                return nil
            }
        }
    }
    return s
}
func FromEnv() store.Store {
    bucketName := os.Getenv("bucketName")
    endpoint := os.Getenv("endpoint")
    accessKeyID := os.Getenv("accessKeyID")
    secretAccessKey := os.Getenv("secretAccessKey")

    return New(bucketName, endpoint, accessKeyID, secretAccessKey)
}

var _ = FromEnv
