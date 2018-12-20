package persistence

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	leveldb "github.com/syndtr/goleveldb/leveldb"
)

// LevelDBPersister saves data in an embedded leveldb store
type LevelDBPersister struct {
	stream         chan *Entry // Internal stream so that all writes are ordered
	dataDir        string
	namespaceDBMap map[Namespace]*leveldb.DB
	errChan        chan error

	finalize chan struct{}
}

// NewLevelDBPersister initializes a LevelDB backed persister
func NewLevelDBPersister(dataDir string) Persister {
	lp := &LevelDBPersister{
		stream:         make(chan *Entry, 10),
		dataDir:        dataDir,
		namespaceDBMap: make(map[Namespace]*leveldb.DB),
		errChan:        make(chan error, 10),
		finalize:       make(chan struct{}, 1),
	}
	go lp.writer()

	return lp
}

// Finalize tells persister that it can finalize and close writes
// It is an error to send new items to persist once Finalize has been called
func (lp *LevelDBPersister) Finalize() {
	logrus.Info("LevelDBPersister: finalizing")
	lp.finalize <- struct{}{}
}

// Persist stores an entry to disk
func (lp *LevelDBPersister) Persist(e *Entry) error {
	logrus.Info("LevelDBPersister/Persist: persisting an entry")
	lp.stream <- e
	return nil
}

// PersistStream listens to the input channel and persists entries to disk
func (lp *LevelDBPersister) PersistStream(ec chan *Entry) error {
	for e := range ec {
		logrus.Info("LevelDBPersister/Stream saving entry")
		lp.stream <- e
	}
	return nil
}

// Errors returns a channel that clients of this persister should listen on for errors
func (lp *LevelDBPersister) Errors() chan error {
	return lp.errChan
}

// Recover reads back persisted data and emits entries
func (lp *LevelDBPersister) Recover(namespace Namespace) (chan *Entry, error) {
	db, err := leveldb.OpenFile(path.Join(lp.dataDir, namespace), nil)
	if err != nil {
		err = errors.Wrap(err, "Failed to open peristence file for Namespace: "+namespace)
		logrus.Error("LevelDBPersister: ", err)
		return nil, err
	}
	defer db.Close()

	ec := make(chan *Entry, 100)

	go func() {
		iter := db.NewIterator(nil, nil)
		for iter.Next() {
			value := iter.Value()
			r := bytes.NewReader(value)
			e := &Entry{
				Data:      new(bytes.Buffer),
				Namespace: namespace,
			}
			err = binary.Read(r, binary.BigEndian, e.Data)
			if err != nil {
				lp.errChan <- errors.Wrap(err, "Failed to read entry file for Namespace: "+namespace)
				continue
			}
		}
	}()

	return ec, nil
}

func (lp *LevelDBPersister) writer() {
	var counterKey uint64

	logrus.Info("LevelDBPersister: Starting leveldb persister writer process")

	for {
		select {
		case e, ok := <-lp.stream:
			if !ok {
				break
			}
			logrus.Info("LevelDBPersister/writer: persisting entry", e)

			db, ok := lp.namespaceDBMap[e.Namespace]
			if !ok {
				var err error
				db, err = leveldb.OpenFile(path.Join(lp.dataDir, e.Namespace), nil)
				if err != nil {
					err = errors.Wrap(err, "Failed to open peristence file for Namespace: "+e.Namespace)
					logrus.Error("LevelDBPersister: persisting entry error", err)
					lp.errChan <- err
					continue
				}
				lp.namespaceDBMap[e.Namespace] = db
			}

			err := db.Put([]byte(fmt.Sprintf("%d", counterKey)), e.Data.Bytes(), nil)
			if err != nil {
				lp.errChan <- errors.Wrap(err, "Failed to persist entry")
				continue
			}

			logrus.Info("LevelDBPersister/writer: persisting entry done")
		case <-lp.finalize:

			logrus.Info("LevelDBPersister: finalizing persister")
			for ns, db := range lp.namespaceDBMap {
				logrus.Warn("Closing writer db for ", ns)
				err := db.Close()
				if err != nil {
					logrus.Error("LevelDBPersister: error finalizing persister")
				}
			}
			close(lp.finalize)
			close(lp.stream)

			break
		}
		break
	}
}
