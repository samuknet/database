// package segment exports SegmentCollection for dumping data to.
// TODO: Add tests
package segment

import (
    "bytes"
    "encoding/gob"
    "fmt"
    "io/ioutil"
    "os"
    "path/filepath"
    "strconv"
    "sync"
)

// TODO: rename this to be lowercase.
type Object struct {
    Key string
    Obj map[string]interface{}
}

type indexPair struct {
    offset int64
    size   int
}

// segment is an SS Table file.
// TODO: Make this use m'mapped files.
// TODO: Store the index at the end of the file.
type segment struct {
    fn      string
    segSize int
    index   map[string]indexPair
}

func newSegment(fn string, segSize int) *segment {
    return &segment{
        fn:      fn,
        segSize: segSize,
        index:   make(map[string]indexPair),
    }
}

// serialize takes an arbitrary map and converts it to a byte buffer.
func serialize(data map[string]interface{}) (*bytes.Buffer, error) {
    buf := new(bytes.Buffer)
    enc := gob.NewEncoder(buf)
    err := enc.Encode(data)
    if err != nil {
        return nil, err
    }
    return buf, nil
}

// deserialize takes a byte buffer and converts it to an arbitrary map.
func deserialize(buf *bytes.Buffer) (map[string]interface{}, error) {
    var data map[string]interface{}
    dec := gob.NewDecoder(buf)
    err := dec.Decode(&data)
    if err != nil {
        return nil, err
    }
    return data, nil
}

// write the provided data to the segment.
func (s *segment) write(objs []*Object) error {
    // Serialize data into byte buffer.
    buf := new(bytes.Buffer)
    for _, obj := range objs {
        part, err := serialize(obj.Obj)
        if err != nil {
            return err
        }
        // Add this key to the index.
        s.index[obj.Key] = indexPair{int64(buf.Len()), len(part.Bytes())}
        // Write the key.
        buf.Write(part.Bytes())
    }

    if buf.Len() > s.segSize {
        // TODO: What to do in this error case?
        return fmt.Errorf("Tried to dump %v bytes. Maximum size is %v.", buf.Len(), s.segSize)
    }
    return ioutil.WriteFile(s.fn, buf.Bytes(), os.ModePerm)
}

func (s *segment) lookup(key string) (map[string]interface{}, error) {
    if _, ok := s.index[key]; !ok {
        return nil, fmt.Errorf("Key %q not found in %v.", key, s.fn)
    }

    indexPair := s.index[key]
    file, err := os.Open(s.fn)
    if err != nil {
        return nil, err
    }
    var bs = make([]byte, indexPair.size)

    _, err = file.ReadAt(bs, indexPair.offset)
    // TODO check the number of bytes read (the first return value of file.ReadAt)
    if err != nil {
        return nil, err
    }
    obj, err := deserialize(bytes.NewBuffer(bs))

    if err != nil {
        return nil, err
    }
    return obj, nil
}

// Collection manages a set of segments that can be dumped to.
type Collection struct {
    // RWMutex guards ss and nextSegId
    sync.RWMutex
    ss []*segment
    // TODO: Consider giving nextSegId its own RWMutex.
    nextSegId int

    segSize int
    dir     string
}

type Config struct {
    SegSize int
    Dir     string
}

func NewCollection(config Config) *Collection {
    return &Collection{
        segSize: config.SegSize,
        dir:     config.Dir,
    }
}

// String gives a human readable representation of the collection.
func (c *Collection) String() string {
    // TODO: Consider locking on nextSegId.
    return fmt.Sprintf("{nextSegId: %v, segSize: %v, dir: %v}", c.nextSegId, c.segSize, c.dir)
}

// nextFn gets the next filename for the segment.
// Will block until c.nextSegId is available.
// TODO: Get unique filenames without relying on an int.
func (c *Collection) nextFn() string {
    c.RLock()
    defer c.RUnlock()
    fn := filepath.Join(c.dir, strconv.Itoa(c.nextSegId))
    c.nextSegId++
    return fn
}

// Dump takes an arbitrary map and records it into a new segment.
func (c *Collection) Dump(objs []*Object) error {
    // TODO: Write to the existing segment if it is not full.
    // Make new segment.
    s := newSegment(c.nextFn(), c.segSize)

    // Fill segment with objects.

    // Write objects to segment.
    err := s.write(objs)
    if err != nil {
        return err
    }

    // Append the segment to the collection.
    c.Lock()
    c.ss = append(c.ss, s)
    c.Unlock()

    return nil
}

func (c *Collection) Lookup(key string) (map[string]interface{}, error) {
    c.RLock()
    segNum := len(c.ss) - 1
    c.RUnlock()

    // Look through seg indexes for the key, from newest to oldest.
    for segNum >= 0 {
        s := c.ss[segNum]
        obj, err := s.lookup(key)
        if err == nil {
            return obj, nil
        }
        segNum--
    }

    return nil, fmt.Errorf("Key %q not found in any segments.", key)
}

// CompactAndMerge compacts each segment file and joins them into one
func (c Collection) CompactAndMerge() error {
    // TODO: Implement this.
    return nil
}
