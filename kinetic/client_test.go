package kinetic

import (
	"testing"
)

// cd kinetic/
// go test

func TestPut(t *testing.T) {
	client, _ := Connect("localhost:8123")
	defer client.Close()

	rxs, _ := client.Put([]byte("Key"), []byte("Value"))

	if err := <-rxs; err != nil {
		t.Errorf("got %v\nwant success", err)
	}
}
