package segment

import (
    "fmt"
    "os"
    "reflect"
    "strconv"
    "testing"
)

var (
    dir     = "test"
    segSize = 800
)

func TestSegmentCollectionEndToEnd(t *testing.T) {
    os.RemoveAll(dir)
    os.Mkdir(dir, os.ModePerm)
    c := NewCollection(Config{
        SegSize: segSize,
        Dir:     dir,
    })

    var data []*Object
    for i := 0; i < 10; i++ {
        obj := make(map[string]interface{})
        name := "person" + strconv.Itoa(i)
        obj["name"] = name
        obj["age"] = i
        data = append(data, &Object{
            Key: name,
            Obj: obj,
        })
    }
    err := c.Dump(data)
    if err != nil {
        fmt.Sprintf("c.Dump(data) =  %v, want nil", err)
    }

    fmt.Println("Collection", c)
    for i := 0; i < 10; i++ {
        key := "person" + strconv.Itoa(i)
        want := map[string]interface{}{"name": key, "age": strconv.Itoa(i)}
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
