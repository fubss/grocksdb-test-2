package grocksdbtest2

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	rocksdb "github.com/linxGnu/grocksdb"
	"github.com/pkg/errors"
)

type dbState int32

const (
	closed dbState = iota
	opened
)

// DB - a wrapper on an actual store
type DB struct {
	conf    *Conf
	db      *rocksdb.DB
	dbState dbState
	mutex   sync.RWMutex
}

// CreateDB constructs a `DB`
func CreateDB(conf *Conf) *DB {
	fmt.Printf("\t-------->RocksDB constructing...\n")
	return &DB{
		conf:    conf,
		dbState: closed,
	}
}

// Open opens the underlying db
func (dbInst *DB) Open() {
	fmt.Printf("\t-------->Opening DB...\n")
	dbInst.mutex.Lock()
	defer dbInst.mutex.Unlock()
	if dbInst.dbState == opened {
		return
	}
	//block based table from the example
	//bbto := rocksdb.NewDefaultBlockBasedTableOptions()
	//bbto.SetBlockCache(rocksdb.NewLRUCache(3 << 30)) //3 GB

	dbOpts := rocksdb.NewDefaultOptions()
	//dbOpts.SetBlockBasedTableFactory(bbto)

	dbPath := dbInst.conf.DBPath
	var err error
	fmt.Printf("\t-------->ParanoidChecks is: %t\n", dbOpts.ParanoidChecks())

	isDirEmpty, err := CreateDirIfMissing(dbPath)
	if err != nil {
		panic(fmt.Sprintf("Error creating dir if missing: %s", err))
	}
	dbOpts.SetParanoidChecks(false) //docs says that default value is false
	dbOpts.SetCreateIfMissing(isDirEmpty)
	if dbInst.db, err = rocksdb.OpenDb(dbOpts, dbPath); err != nil {
		panic(fmt.Sprintf("Error opening rocksdb: %s", err))
	}
	fmt.Printf("\t-------->DB was successfully opened in path: [ %s ]\n", dbPath)
	dbInst.dbState = opened
}

// IsEmpty returns whether or not a database is empty
func (dbInst *DB) IsEmpty() (bool, error) {
	fmt.Printf("\t-------->Checkin if DB is empty...\n")
	if dbInst.dbState == closed {
		return false, errors.New("RocksDB is closed.")
	}
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()
	itr := dbInst.db.NewIterator(rocksdb.NewDefaultReadOptions())
	defer itr.Close()
	itr.SeekToFirst()
	hasItems := itr.Valid()
	fmt.Printf("\t-------->Checking for emptiness has finished\n")
	return !hasItems,
		errors.Wrapf(itr.Err(), "error while trying to see if the rocksdb at path [%s] is empty", dbInst.conf.DBPath)
}

// Close closes the underlying db
func (dbInst *DB) Close() {
	dbInst.mutex.Lock()
	defer dbInst.mutex.Unlock()
	if dbInst.dbState == closed {
		return
	}
	fmt.Printf("\t-------->Closing db...\n")
	dbInst.db.Close() //TODO: should we check if db closed here?
	dbInst.dbState = closed
}

// Get returns the value for the given key
func (dbInst *DB) Get(key []byte) ([]byte, error) {
	fmt.Printf("\t-------->Getting key [%s] from RocksDB...\n", key)
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()
	value, err := dbInst.db.Get(rocksdb.NewDefaultReadOptions(), key)
	if err != nil {
		fmt.Printf("\t-------->Error retrieving rocksdb key [%#v]: %s\n", key, err)
		return nil, errors.Wrapf(err, "error retrieving rocksdb key [%#v]", key)
	}
	fmt.Printf("\t-------->got data [%s]\n", value.Data())
	return value.Data(), nil
}

// Put saves the key/value
func (dbInst *DB) Put(key []byte, value []byte, sync bool) error {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()
	wo := rocksdb.NewDefaultWriteOptions()
	if sync {
		wo.SetSync(true)
	}
	err := dbInst.db.Put(wo, key, value)
	if err != nil {
		fmt.Printf("\t-------->Error writing rocksdb key [%#v]\n", key)
		return errors.Wrapf(err, "error writing rocksdb key [%#v]", key)
	}
	return nil
}

// Delete deletes the given key
func (dbInst *DB) Delete(key []byte, sync bool) error {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()
	wo := rocksdb.NewDefaultWriteOptions()
	if sync {
		wo.SetSync(true)
	}
	err := dbInst.db.Delete(wo, key)
	if err != nil {
		fmt.Printf("\t-------->Error deleting rocksdb key [%#v]\n", key)
		return errors.Wrapf(err, "error deleting rocksdb key [%#v]", key)
	}
	return nil
}

// GetIterator returns an iterator over key-value store. The iterator should be closed after the use.
// The resultset contains all the keys that are present in the db between the startKey (inclusive) and the endKey (exclusive).
// A nil startKey represents the first available key and a nil endKey represent a logical key after the last available key
func (dbInst *DB) GetIterator(startKey []byte, endKey []byte) (*rocksdb.Iterator, error) {
	fmt.Printf("\t-------->Getting new RocksDB Iterator... for start key: [%s (%+v: [%s (%+v)]\n", startKey, startKey, endKey, endKey) //TODO: delete this
	if dbInst.dbState == closed {
		err := errors.New("error while obtaining db iterator: rocksdb: closed")
		fmt.Printf("\t-------->itr.Err()=[%+v]. Impossible to create an iterator\n", err)
		return nil, err
	}
	//ro := dbInst.readOpts
	ro := rocksdb.NewDefaultReadOptions()
	// Docs says that If you want to avoid disturbing your live traffic
	// while doing the bulk read, be sure to call SetFillCache(false)
	// on the ReadOptions you use when creating the Iterator.
	ro.SetFillCache(false)
	ro.SetBackgroundPurgeOnIteratorCleanup(true)
	///	dbInst.mutex.RUnlock()
	if endKey != nil {
		fmt.Printf("\t-------->if-case: endKey!=nil, UpperBound set\n")
		ro.SetIterateUpperBound(endKey)
	} else {
		fmt.Println("endKey is nil, no UpperBound would be set in the previous variant")
		ro.SetIterateUpperBound(endKey)

	}
	fmt.Printf("\t-------->PurgeOnIterCleanup = %t\n", ro.GetBackgroundPurgeOnIteratorCleanup())
	ni := dbInst.db.NewIterator(ro)
	if ni.Valid() {
		fmt.Printf("\t-------->ni is Valid, err=[%+v]\n", ni.Err())
	} else {
		fmt.Printf("\t-------->ni is not Valid, err=[%+v]\n", ni.Err())
	}
	ni.Seek(startKey)

	return ni, nil
}

// WriteBatch writes a batch
func (dbInst *DB) WriteBatch(batch *rocksdb.WriteBatch, sync bool) error {
	fmt.Printf("\t-------->WritingBatch.Count()=[%d]\n", batch.Count()) //TODO: delete this
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()
	wo := rocksdb.NewDefaultWriteOptions()
	if sync {
		wo.SetSync(true)
	}
	if err := dbInst.db.Write(wo, batch); err != nil {
		return errors.Wrap(err, "error writing batch to rocksdb")
	}
	return nil
}

// CreateDirIfMissing makes sure that the dir exists and returns whether the dir is empty
func CreateDirIfMissing(dirPath string) (bool, error) {
	if err := os.MkdirAll(dirPath, 0o755); err != nil {
		return false, errors.Wrapf(err, "error while creating dir: %s", dirPath)
	}
	if err := SyncParentDir(dirPath); err != nil {
		return false, err
	}
	return DirEmpty(dirPath)
}

// DirEmpty returns true if the dir at dirPath is empty
func DirEmpty(dirPath string) (bool, error) {
	f, err := os.Open(dirPath)
	if err != nil {
		return false, errors.Wrapf(err, "error opening dir [%s]", dirPath)
	}
	defer f.Close()

	_, err = f.Readdir(1)
	if err == io.EOF {
		return true, nil
	}
	err = errors.Wrapf(err, "error checking if dir [%s] is empty", dirPath)
	return false, err
}

// SyncParentDir fsyncs the parent dir of the given path
func SyncParentDir(path string) error {
	return SyncDir(filepath.Dir(path))
}

// SyncDir fsyncs the given dir
func SyncDir(dirPath string) error {
	dir, err := os.Open(dirPath)
	if err != nil {
		return errors.Wrapf(err, "error while opening dir:%s", dirPath)
	}
	if err := dir.Sync(); err != nil {
		dir.Close()
		return errors.Wrapf(err, "error while synching dir:%s", dirPath)
	}
	if err := dir.Close(); err != nil {
		return errors.Wrapf(err, "error while closing dir:%s", dirPath)
	}
	return err
}
