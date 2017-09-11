package main

import (
    "fmt"
    "github.com/samuknet/database/db"
    "github.com/samuknet/database/segment"

    "os"
    "strconv"
    "sync"
)

const (
    segSize = 1000000000
    dir     = "fs"
)

func getName(i int) string {
    return "person" + strconv.Itoa(i)
}

func main() {
    os.RemoveAll(dir)
    os.Mkdir(dir, os.ModePerm)
    dbh := db.NewHandle(segSize, dir)
    dbh.Start()
    n := 500  // number of reader/writer threads
    is := 500 // unique key inserts per thread
    // Total writes = n * is

    var wg sync.WaitGroup
    wg.Add(n * is)
    // chan to communicate when a key has been written
    ch := make(chan int)

    // Spawn writing threads.
    for i := 0; i < n; i++ {
        go func(i int, ch chan int) {
            limit := ((i * is) + is)
            for j := (i * is); j < limit; j++ {
                doc := make(segment.Document)
                name := getName(j)
                doc["name"] = name
                doc["age"] = j
                dbh.Insert(name, doc)
                ch <- j
                // fmt.Println("insert", j)
                // time.Sleep(time.Duration(10) * time.Millisecond)
            }
        }(i, ch)
    }

    // Spawn reading threads.
    for i := 0; i < n; i++ {
        go func(ch chan int) {
            for j := range ch {
                // Receiving int j means that j has been inserted so we test it cna be found.
                // fmt.Println("lookup", j)
                key := getName(j)
                want := segment.Document{"name": key, "age": strconv.Itoa(j)}
                got, err := dbh.Lookup(key)
                if err != nil {
                    fmt.Printf("dbh.Lookup(%q) = _, %v, want %v, <nil>\n", key, err, want)
                }
                if got["name"] == want["name"] && got["age"] == want["age"] {
                    fmt.Printf("dbh.Lookup(%q) = %v, %v, want %v, <nil>\n", key, got, err, want)
                }
                wg.Done()
            }
        }(ch)
    }
    wg.Wait()
    // os.RemoveAll(dir)
}
