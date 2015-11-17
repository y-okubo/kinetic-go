package kinetic

import (
	"testing"
  "math/rand"
  "time"
  "strconv"
)

// go test ./kinetic

func TestPut(t *testing.T) {
  var error <-chan error
  var value <-chan interface{}
  var client Client

	client, _ = Connect("localhost:8123")
	defer client.Close()

	value, error, _ = client.Put([]byte("Key"), []byte("Value"))

  <- value
  err := <- error

	if err != nil {
		t.Errorf("got %v\nwant success", err)
	}
}

func TestDelete(t *testing.T) {
  var error <-chan error
  var value <-chan interface{}
  var client Client

  client, _ = Connect("localhost:8123")
  defer client.Close()

  value, error, _ = client.Delete([]byte("Key"))

  <- value
  err := <- error

  if err != nil {
    t.Errorf("got %v\nwant success", err)
  }
}

func TestGet(t *testing.T) {
  var error <-chan error
  var value <-chan interface{}
  var client Client
  var specKey, specVal string
  var val interface{}
  var err interface{}

  client, _ = Connect("localhost:8123")
  defer client.Close()

  rand.Seed(time.Now().UnixNano())
  specKey = "Key_" + strconv.FormatInt(rand.Int63(), 10)
  specVal = "Val_" + strconv.FormatInt(rand.Int63(), 10)

  value, error, _ = client.Put([]byte(specKey), []byte(specVal))
  
  <- value
  err = <- error

  if err != nil {
    t.Errorf("got %v\nwant success", err)
  }

  value, error, _ = client.Get([]byte(specKey))
  val = <- value
  err = <- error

  if err != nil {
    t.Errorf("got %v\nwant success", err)
  } else {
    if string(val.([]byte)) != specVal {
      t.Errorf("got %v\nwant %v", string(val.([]byte)), string(specVal))
    }
  }
}

func TestGetVersion(t *testing.T) {
  var error <-chan error
  var value <-chan interface{}
  var client Client
  var specKey, specVal string
  var val interface{}
  var err interface{}

  client, _ = Connect("localhost:8123")
  defer client.Close()

  rand.Seed(time.Now().UnixNano())
  specKey = "Key_" + strconv.FormatInt(rand.Int63(), 10)
  specVal = "Val_" + strconv.FormatInt(rand.Int63(), 10)

  value, error, _ = client.Put([]byte(specKey), []byte(specVal))
  <-value
  err = <-error
  
  if err != nil {
    t.Errorf("got %v\nwant success", err)
  }

  value, error, _ = client.GetVersion([]byte(specKey))
  val = <- value
  err = <- error

  if err != nil {
    t.Errorf("got %v\nwant success", err)
  } else {
    if string(val.([]byte)) != "" {
      t.Errorf("got %v\nwant %v", string(val.([]byte)), string(""))
    }
  }
}

func TestGetNextKey(t *testing.T) {
  var error <-chan error
  var value <-chan interface{}
  var client Client
  var specKey, specVal string
  var val interface{}
  var err interface{}

  client, _ = Connect("localhost:8123")
  defer client.Close()

  specKey = "Key_0"
  specVal = "Val_0"

  value, error, _ = client.Put([]byte(specKey), []byte(specVal))
  <-value
  err = <-error
  
  if err != nil {
    t.Errorf("got %v\nwant success", err)
  }

  specKey = "Key_1"
  specVal = "Val_1"

  value, error, _ = client.Put([]byte(specKey), []byte(specVal))
  <-value
  err = <-error
  
  if err != nil {
    t.Errorf("got %v\nwant success", err)
  }

  value, error, _ = client.GetNextKey([]byte(specKey))
  val = <-value
  err = <-error

  if err != nil {
    t.Errorf("got %v\nwant success", err)
  } else {
    if string(val.([]byte)) != specKey {
      t.Errorf("got %v\nwant %v", string(val.([]byte)), string(specKey))
    }
  }
}

func TestGetPreviousKey(t *testing.T) {
  var error <-chan error
  var value <-chan interface{}
  var client Client
  var specKey, specVal string
  var val interface{}
  var err interface{}

  client, _ = Connect("localhost:8123")
  defer client.Close()

  specKey = "Key_0"
  specVal = "Val_0"

  value, error, _ = client.Put([]byte(specKey), []byte(specVal))
  <-value
  err = <-error
  
  if err != nil {
    t.Errorf("got %v\nwant success", err)
  }

  specKey = "Key_1"
  specVal = "Val_1"

  value, error, _ = client.Put([]byte(specKey), []byte(specVal))
  <-value
  err = <-error
  
  if err != nil {
    t.Errorf("got %v\nwant success", err)
  }

  value, error, _ = client.GetPreviousKey([]byte(specKey))
  val = <-value
  err = <-error

  if err != nil {
    t.Errorf("got %v\nwant success", err)
  } else {
    if string(val.([]byte)) != specKey {
      t.Errorf("got %v\nwant %v", string(val.([]byte)), string(specKey))
    }
  }
}

func TestGetKeyRange(t *testing.T) {
  var error <-chan error
  var value <-chan interface{}
  var client Client
  var startKey, startVal string
  var endKey, endVal string
  var val interface{}
  var err interface{}

  client, _ = Connect("localhost:8123")
  defer client.Close()

  startKey = "Key_0"
  startVal = "Val_0"

  value, error, _ = client.Put([]byte(startKey), []byte(startVal))
  <-value
  err = <-error
  
  if err != nil {
    t.Errorf("got %v\nwant success", err)
  }

  endKey = "Key_1"
  endVal = "Val_1"

  value, error, _ = client.Put([]byte(endKey), []byte(endVal))
  <-value
  err = <-error
  
  if err != nil {
    t.Errorf("got %v\nwant success", err)
  }

  value, error, _ = client.GetKeyRange([]byte(startKey), []byte(endKey))
  val = <- value
  err = <- error

  if err != nil {
    t.Errorf("got %v\nwant success", err)
  } else {
    if len(val.([][]byte)) != 2 {
      t.Errorf("got %v\nwant %v", len(val.([][]byte)), 2)
    }
  }
}
