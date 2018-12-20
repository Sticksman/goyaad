package persistence

// import (
// 	"bytes"
// 	"encoding/binary"
// 	"os"
// 	"path"

// 	"github.com/pkg/errors"
// )

// // DiskPersister saves data to disk
// type DiskPersister struct {
// 	stream         chan *Entry // Internal stream so that all writes are ordered
// 	dataDir        string
// 	namespaceFDMap map[Namespace]*os.File
// 	ErrChan        chan error
// }

// type record struct {
// 	length int
// 	data   *bytes.Buffer
// }

// // NewDiskPersister initializes a disk persister
// func NewDiskPersister(dataDir string) Persister {
// 	dp := &DiskPersister{
// 		stream:  make(chan *Entry, 1000),
// 		dataDir: dataDir,
// 	}
// 	// kick off listener
// 	go dp.writer()

// 	return dp
// }

// // Persist stores an entry to disk
// func (dp *DiskPersister) Persist(e *Entry) error {
// 	return nil
// }

// // PersistStream listens to the input channel and persists entries to disk
// func (dp *DiskPersister) PersistStream(ec chan *Entry) error {
// 	return nil
// }

// // Errors returns a channel that clients of this persister should listen on for errors
// func (dp *DiskPersister) Errors() chan error {
// 	return dp.ErrChan
// }

// // Recover reads back persisted data and emits entries
// func (dp *DiskPersister) Recover(namespace Namespace) (chan *Entry, error) {
// 	return nil, nil
// }

// func (dp *DiskPersister) writer() {
// 	for e := range dp.stream {
// 		file, ok := dp.namespaceFDMap[e.Namespace]
// 		if !ok {
// 			var err error
// 			file, err = os.OpenFile(path.Join(dp.dataDir, e.Namespace),
// 				os.O_RDWR|os.O_APPEND|os.O_CREATE, 0)
// 			if err != nil {
// 				dp.ErrChan <- errors.Wrap(err, "Failed to open peristence file for Namespace: "+e.Namespace)
// 				continue
// 			}
// 			dp.namespaceFDMap[e.Namespace] = file
// 		}

// 		// We have a file for this Namespace
// 		buf := new(bytes.Buffer)
// 		err := binary.Write(buf, binary.BigEndian, e.Data)
// 		if err != nil {
// 			dp.ErrChan <- errors.Wrap(err, "Failed to encode entry")
// 			continue
// 		}
// 		r := record{
// 			length: buf.Len(),
// 			data:   buf,
// 		}
// 		file.Write(r.length)
// 	}
// }
