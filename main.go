package main

import (
    "fmt"
    "github.com/samuknet/database/segment"
    "log"
    "os"
    "strconv"
    "sync"
)

const (
    segSize = 1800
    dir     = "fs"
)

func main() {
    os.RemoveAll(dir)
    os.Mkdir(dir, os.ModePerm)
    c := segment.NewCollection(segment.Config{
        SegSize: segSize,
        Dir:     dir,
    })

    var data []*segment.KeyDocument
    for i := 0; i < 10; i++ {
        doc := make(segment.Document)
        name := "person" + strconv.Itoa(i)
        doc["name"] = name
        doc["age"] = 21
        data = append(data, &segment.KeyDocument{
            Key: name,
            Doc: doc,
        })
    }
    err := c.Dump(data)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("Collection", c)
    var wg sync.WaitGroup
    n := 10
    wg.Add(n)
    for i := 0; i < n; i++ {
        go func(key string) {
            defer wg.Done()
            person, _ := c.Lookup(key)
            fmt.Println(person)
        }("person" + strconv.Itoa(i))
    }
    wg.Wait()
}
