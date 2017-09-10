package segment

import (
    "fmt"
    "os"
    "reflect"
    "strconv"
    "testing"
)

var (
    dir     = "segment_test"
    segSize = 800
)

func TestSegmentCollectionEndToEnd(t *testing.T) {
    os.RemoveAll(dir)
    os.Mkdir(dir, os.ModePerm)
    c := NewCollection(Config{
        SegSize: segSize,
        Dir:     dir,
    })

    var data []*KeyDocument
    for i := 0; i < 10; i++ {
        doc := make(Document)
        name := "person" + strconv.Itoa(i)
        doc["name"] = name
        doc["age"] = i
        data = append(data, &KeyDocument{
            Key: name,
            Doc: doc,
        })
    }
    err := c.Dump(data)
    if err != nil {
        fmt.Sprintf("c.Dump(data) =  %v, want nil", err)
    }

    fmt.Println("Collection", c)
    for i := 0; i < 10; i++ {
        key := "person" + strconv.Itoa(i)
        want := Document{"name": key, "age": strconv.Itoa(i)}
        got, err := c.Lookup(key)
        if err != nil {
            fmt.Sprintf("c.Lookup(%q) = _, %v, want %v, nil", key, err, want)
        }

        if !reflect.DeepEqual(want, got) {
            fmt.Sprintf("c.Lookup(%q) = %v, %v, want %v, nil", key, got, err, want)
        }
    }

    os.RemoveAll(dir)
}
