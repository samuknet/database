// package db exposes a Handle representing a database instance.
package db

import (
    "github.com/samuknet/database/segment"

    "math"
    "sort"
    "sync"
    "sync/atomic"
    "time"
)

// byKey implements sort.Interface for []*KeyDocument based on
// the key.
type byKey []*segment.KeyDocument

func (a byKey) Len() int           { return len(a) }
func (a byKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// topLevelIndex is an in memory data structure to buffer writes to.
type topLevelIndex struct {
    sync.RWMutex // the lock on the topLevelIndex locks the pointers to the indexes.
    index        *sync.Map
    bufferIndex  *sync.Map // The index currently being written to disk. Nil if nothing is being written.
    bytes        uint32    // estimated number of bytes currently in the index.
    maxBytes     uint32    // the max number of bytes that should be stored in the index.
}

func newTopLevelIndex(maxBytes uint32) *topLevelIndex {
    return &topLevelIndex{
        index:       &sync.Map{},
        bufferIndex: nil,
        bytes:       0,
        maxBytes:    maxBytes,
    }
}

// lookup the given key string in the index.
// First checks index for the key, if that fails it will check
// the bufferIndex, if any.
func (tli *topLevelIndex) lookup(key string) (*segment.KeyDocument, bool) {
    tli.RLock()
    defer tli.RUnlock()
    kd, ok := tli.index.Load(key)
    if ok {
        return kd.(*segment.KeyDocument), ok
    }
    if tli.bufferIndex != nil {
        kd, ok = tli.bufferIndex.Load(key)
        return kd.(*segment.KeyDocument), ok
    }

    return nil, false
}

// insert the given KeyDocument into the index.
// Will fail with an error if there is not enough space.
// Use topLevelIndex.hasSpace(segment.KeyDocument) before calling.
func (tli *topLevelIndex) insert(kd *segment.KeyDocument) error {
    tli.RLock()
    defer tli.RUnlock()
    tli.index.Store(kd.Key, kd)
    tli.incrementBytes(kd.SizeInBytes())
    return nil
}

func (tli *topLevelIndex) incrementBytes(delta uint32) {
    atomic.AddUint32(&tli.bytes, delta)
}

// atCapacity returns true if the topLevelIndex is full.
// TODO: Check that this won't conflict with the writing.
func (tli *topLevelIndex) atCapacity() bool {
    return tli.bytes >= tli.maxBytes
}

// Handle is the main database struct, containing everything
// needed to make reads and writes.
type Handle struct {
    // TODO: Consider putting index and segments into a list of interfaces.
    // The interface would have functions for Insert and Lookup.
    tli           *topLevelIndex
    segments      *segment.Collection
    maxIndexBytes uint32
    writes        chan (*sync.Map)
}

// NewHandle creates a new handle, with the given segment size, storing files
// in the provided dir.
func NewHandle(segSize int, dir string) *Handle {
    maxIndexBytes := uint32(math.Ceil(0.8 * float64(segSize)))
    return &Handle{
        tli: newTopLevelIndex(maxIndexBytes), // Index max size is 80% of seg size to account for underestimating actual size.
        segments: segment.NewCollection(segment.Config{
            SegSize: segSize,
            Dir:     dir,
        }),
        maxIndexBytes: maxIndexBytes,
        writes:        make(chan *sync.Map),
    }
}

// Lookup first scans through the various data stores, looking for the
// given key, in the following order:
// 1. Top level index.
// 2. Index currently being written to disk, if any.
// 3. Disk.
func (db *Handle) Lookup(key string) (segment.Document, error) {
    // TODO: Make this work concurrently.
    if kd, ok := db.tli.lookup(key); ok {
        return kd.Doc, nil
    }
    kd, err := db.segments.Lookup(key)
    if err != nil {
        return nil, err
    }
    return kd.Doc, nil
}

// Insert takes a string and segment.Document and inserts it into
// the top level index.  If the top level index becomes full after
// of the insert, it will create a new index and then issue a command
// to write the old index to disk.  Will block if the old index
// is already being written to disk.
func (db *Handle) Insert(key string, d segment.Document) error {
    kd := &segment.KeyDocument{
        Key: key,
        Doc: d,
    }

    db.tli.insert(kd)
    return nil
}

func (db *Handle) Start() {
    // Dispatches disk writing tasks when index gets too full.
    go func() {
        db.diskDispatcher()
    }()

    // Accepts disk writing tasks.
    go func() {
        db.diskWorker()
    }()
}

// diskDispatcher monitors the size of the index and if it is at capacity,
// dispatches a task to write it to the disk
func (db *Handle) diskDispatcher() {
    // TODO: Change sleep time based on the current load and the size of the top level index.
    t := 3
    for {
        if db.tli.atCapacity() {
            db.tli.Lock()
            // MID: There is nothing else reading or writing to the top level index.
            // TODO: Allow 'n' old indexes here.
            // Blocks until there is no index being written to disk.
            db.writes <- db.tli.index
            db.tli.bufferIndex = db.tli.index
            db.tli.index = &sync.Map{}
            db.tli.bytes = 0
            db.tli.Unlock()
        }
        time.Sleep(time.Duration(t) * time.Second)
    }
}

// diskWorker accepts disk writing tasks. It reads indexes
// one by one and writes them to disk.  Only one index can be written
// at any one time.
func (db *Handle) diskWorker() {
    for {
        for index := range db.writes {
            // Pre: index is immutable.  It is only being read from.
            var res []*segment.KeyDocument
            index.Range(func(key, kd interface{}) bool {
                res = append(res, kd.(*segment.KeyDocument))
                return true
            })
            // Sort the slice.
            sort.Sort(byKey(res))
            db.segments.Dump(res)
            // TODO: Log error if it failed. Maybe retry.

            // Once the disk write has completed, clear the bufferIndex
            // if it is the same as the work we just did.
            // This avoids potentially redundant disk reads and reduces
            // the time for which there is a bufferIndex which is also on disk.
            db.tli.Lock()
            if db.tli.bufferIndex == index {
                // If this has just been written to disk, remove it from the buffer.
                db.tli.bufferIndex = nil
            }
            db.tli.Unlock()
        }
    }
}
