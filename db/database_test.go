package db

import (
	"fmt"
	"github.com/samuknet/database/db"
	"github.com/samuknet/database/segment"

	"os"
	"reflect"
	"strconv"
	"testing"
)

var (
	segSize = 800
	dir     = "db_test"
)

func TestDatabaseEndToEnd(t *testing.T) {
	os.RemoveAll(dir)
	os.Mkdir(dir, os.ModePerm)
	dbh := db.NewHandle(segSize, dir)
	n := 10

	var data []segment.Document
	for i := 0; i < n; i++ {
		doc := make(segment.Document)
		name := "person" + strconv.Itoa(i)
		doc["name"] = name
		doc["age"] = i
		data = append(data, doc)
		dbh.Insert(name, doc)
	}
	for i := 0; i < n; i++ {
		key := "person" + strconv.Itoa(i)
		want := segment.Document{"name": key, "age": strconv.Itoa(i)}
		got, err := dbh.Lookup(key)
		if err != nil {
			fmt.Sprintf("dbh.Lookup(%q) = _, %v, want %v, nil", key, err, want)
		}

		if !reflect.DeepEqual(want, got) {
			fmt.Sprintf("dbh.Lookup(%q) = %v, %v, want %v, nil", key, got, err, want)
		}
	}

	os.RemoveAll(dir)
}
