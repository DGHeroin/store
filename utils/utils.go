package utils

import (
    "bytes"
    "encoding/binary"
    "time"
)

func SplitData(val []byte) (bool, int, []byte) {
    if len(val) == 0 {
        return false, 0, nil
    }
    ttl, data := val[:4], val[4:]
    sec := binary.BigEndian.Uint32(ttl)
    if sec > 0 && time.Unix(int64(sec), 0).Before(GetTimeNow()) {
        return false, int(sec), data
    }
    return true, int(sec), data
}
func CombineData(ttl time.Duration, val []byte) []byte {
    buf := bytes.Buffer{}
    ttlByte := make([]byte, 4)
    if ttl > 0 {
        sec := int64(ttl) / int64(time.Second)
        expireAt := uint32(GetTimeNow().Unix() + sec)
        binary.BigEndian.PutUint32(ttlByte, expireAt)
    }
    buf.Write(ttlByte)
    buf.Write(val)
    return buf.Bytes()
}
func CopyBytes(data []byte) []byte {
    result := make([]byte, len(data))
    copy(result, data)
    return result
}

func GetTimeNow() time.Time {
    return time.Now()
}
