package utils

import (
    "bytes"
    "encoding/binary"
    "time"
)

func SplitData(val []byte) (bool, int, []byte) {
    ttl, data := val[:4], val[4:]
    sec := binary.BigEndian.Uint32(ttl)
    if sec > 0 && time.Unix(int64(sec), 0).Before(time.Now()) {
        return false, int(sec), data
    }
    return true, int(sec), data
}
func CombineData(ttl int, val []byte) []byte {
    buf := bytes.Buffer{}
    ttlByte := make([]byte, 4)
    binary.BigEndian.PutUint32(ttlByte, uint32(ttl))
    buf.Write(ttlByte)
    buf.Write(val)
    return buf.Bytes()
}
func CopyBytes(data []byte) []byte {
    result := make([]byte, len(data))
    copy(result, data)
    return result
}
