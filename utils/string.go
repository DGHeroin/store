package utils

import (
    "sort"
    "strings"
)

func CutStringSlice(keys []string, prefix, limit string) []string {
    sort.Strings(keys)
    // seek prefix
    if prefix != "" {
        idx := -1
        for i, k := range keys {
            if !strings.HasPrefix(k, prefix) {
                continue
            }
            idx = i
            break
        }
        if idx != -1 {
            keys = keys[idx:]
        }
    }
    // check tail
    if limit != "" {
        idx := -1
        for i, k := range keys {
            if strings.HasPrefix(k, limit) {
                idx = i
                break
            }
        }
        if idx != -1 {
            keys = keys[:idx]
        }
    }
    return keys
}
