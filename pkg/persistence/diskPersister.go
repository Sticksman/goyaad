package persistence

// DiskPersister saves data to disk
type DiskPersister struct{}

// NewDiskPersister initializes a disk persister
func NewDiskPersister() Persister {
	return &DiskPersister{}
}

// Persist stores an entry to disk
func (dp *DiskPersister) Persist(e *Entry) error {
	return nil
}

// PersistStream listens to the input channel and persists entries to disk
func (dp *DiskPersister) PersistStream(ec chan<- *Entry) error {
	return nil
}
