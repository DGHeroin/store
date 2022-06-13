package tests

import (
    "github.com/DGHeroin/store/store/StoreS3"
    "testing"
)

func TestS3(t *testing.T) {
    endpoint := "play.min.io"
    accessKeyID := "Q3AM3UQ867SPQQA43P2F"
    secretAccessKey := "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
    s := StoreS3.New("666", endpoint, accessKeyID, secretAccessKey)
    doTestStore(t, s)
}
