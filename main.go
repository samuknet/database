package main

import (
    "fmt"
    "github.com/samuknet/database/segment"
    "log"
    "os"
    "strconv"
)

const (
    segSize = 800
    dir     = "fs"
)

func main() {
    os.RemoveAll(dir)
    os.Mkdir(dir, os.ModePerm)
    c := segment.NewCollection(segment.Config{
        SegSize: segSize,
        Dir:     dir,
    })

    var data []*segment.Object
    for i := 0; i < 10; i++ {
        obj := make(map[string]interface{})
        name := "person" + strconv.Itoa(i)
        obj["name"] = name
        obj["age"] = 21
        data = append(data, &segment.Object{
            Key: name,
            Obj: obj,
        })
    }
    err := c.Dump(data)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("Collection", c)
    for i := 0; i < 10; i++ {
        person, _ := c.Lookup("person" + strconv.Itoa(i))
        fmt.Println(person)
    }
}
