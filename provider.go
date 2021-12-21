package grocksdbtest2

import (
	"bytes"
	"fmt"
	"sync"

	rocksdb "github.com/linxGnu/grocksdb"
	"github.com/pkg/errors"
)

const (
	// internalDBName is used to keep track of data related to internals such as data format
	// _ is used as name because this is not allowed as a channelname
	internalDBName = "_"
	// maxBatchSize limits the memory usage (1MB) for a batch. It is measured by the total number of bytes
	// of all the keys in a batch.
	maxBatchSize = 1000000
)

var (
	dbNameKeySep = []byte{0x00} //TODO: should we change this to different from leveldb?
	//lastKeyIndicator = []byte{'~'}
	lastKeyIndicator = byte(0x01)
	formatVersionKey = []byte{'f'} // a single key in db whose value indicates the version of the data format
)

// DBHandle is an handle to a named db
type DBHandle struct {
	dbName    string
	db        *DB
	closeFunc closeFunc
}

// closeFunc closes the db handle
type closeFunc func()

// Conf configuration for `Provider`
//
// `ExpectedFormat` is the expected value of the format key in the internal database.
// At the time of opening the db, A check is performed that
// either the db is empty (i.e., opening for the first time) or the value
// of the formatVersionKey is equal to `ExpectedFormat`. Otherwise, an error is returned.
// A nil value for ExpectedFormat indicates that the format is never set and hence there is no such record.
type Conf struct {
	DBPath         string
	ExpectedFormat string
}

// Provider enables to use a single rocksdb as multiple logical leveldbs
type Provider struct {
	db *DB

	mux       sync.Mutex
	dbHandles map[string]*DBHandle
}

// NewProvider constructs a Provider
func NewProvider(conf *Conf) (*Provider, error) {
	fmt.Printf("\t-------->NewProvider intialization...\n")
	db, err := openDBAndCheckFormat(conf)
	if err != nil {
		return nil, err
	}
	return &Provider{
		db:        db,
		dbHandles: make(map[string]*DBHandle),
	}, nil
}

func openDBAndCheckFormat(conf *Conf) (d *DB, e error) {
	fmt.Printf("\t-------->Opening DB and checking format...\n")
	db := CreateDB(conf)
	db.Open()

	defer func() {
		if e != nil {
			fmt.Printf("\t-------->Closing RocksDB...\n")
			db.Close()
		}
	}()

	internalDB := &DBHandle{
		db:     db,
		dbName: internalDBName,
	}

	dbEmpty, err := db.IsEmpty()
	if err != nil {
		return nil, err
	}
	fmt.Printf("\t-------->rocks db IsEmpty()=%t\n", dbEmpty) //TODO: delete this

	if dbEmpty && conf.ExpectedFormat != "" {
		fmt.Printf("\t-------->DB is empty Setting db format as %s\n", conf.ExpectedFormat)
		if err := internalDB.Put(formatVersionKey, []byte(conf.ExpectedFormat), true); err != nil {
			return nil, err
		}
		fmt.Printf("\t-------->formatVersionKey succesfully put into a rocksdb\n") //TODO: delete this
		return db, nil
	}

	formatVersion, err := internalDB.Get(formatVersionKey)
	if err != nil {
		return nil, err
	}
	fmt.Printf("\t-------->Checking for db format at path [%s]\n", conf.DBPath)

	if !bytes.Equal(formatVersion, []byte(conf.ExpectedFormat)) {
		fmt.Printf("\t-------->The db at path [%s] contains data in unexpected format. expected data format = [%s] (%#v), data format = [%s] (%#v).",
			conf.DBPath, conf.ExpectedFormat, []byte(conf.ExpectedFormat), formatVersion, formatVersion)
		return nil, &ErrFormatMismatch{
			ExpectedFormat: conf.ExpectedFormat,
			Format:         string(formatVersion),
			DBInfo:         fmt.Sprintf("rocksdb at [%s]", conf.DBPath),
		}
	}
	fmt.Print("format is latest, nothing to do")
	return db, nil
}

// GetDBHandle returns a handle to a named db
func (p *Provider) GetDBHandle(dbName string) *DBHandle {
	p.mux.Lock()
	defer p.mux.Unlock()
	dbHandle := p.dbHandles[dbName]
	if dbHandle == nil {
		closeFunc := func() {
			p.mux.Lock()
			defer p.mux.Unlock()
			delete(p.dbHandles, dbName)
		}
		dbHandle = &DBHandle{dbName, p.db, closeFunc}
		p.dbHandles[dbName] = dbHandle
	}
	return dbHandle
}

// Close closes the underlying leveldb
func (p *Provider) Close() {
	p.db.Close()
}

// Drop drops all the data for the given dbName
func (p *Provider) Drop(dbName string) error {
	dbHandle := p.GetDBHandle(dbName)
	defer dbHandle.Close()
	return dbHandle.deleteAll()
}

// Get returns the value for the given key
func (h *DBHandle) Get(key []byte) ([]byte, error) {
	return h.db.Get(constructRocksKey(h.dbName, key))
}

// Put saves the key/value
func (h *DBHandle) Put(key []byte, value []byte, sync bool) error {
	return h.db.Put(constructRocksKey(h.dbName, key), value, sync)
}

// Delete deletes the given key
func (h *DBHandle) Delete(key []byte, sync bool) error {
	return h.db.Delete(constructRocksKey(h.dbName, key), sync)
}

// DeleteAll deletes all the keys that belong to the channel (dbName).
func (h *DBHandle) deleteAll() error {
	iter, err := h.GetIterator(nil, nil)
	if err != nil {
		return err
	}
	defer iter.Close()

	// use leveldb iterator directly to be more efficient

	// This is common code shared by all the leveldb instances. Because each leveldb has its own key size pattern,
	// each batch is limited by memory usage instead of number of keys. Once the batch memory usage reaches maxBatchSize,
	// the batch will be committed.
	numKeys := 0
	batchSize := 0
	batch := rocksdb.NewWriteBatch()
	for dbIter := iter.Iterator; dbIter.Valid(); dbIter.Next() {
		if err := dbIter.Err(); err != nil {
			return errors.Wrap(err, "internal rocksdb error while retrieving data from db iterator")
		}
		rocksdbKey := dbIter.Key()
		keyData := rocksdbKey.Data()
		key := make([]byte, len(keyData))
		copy(key, keyData)
		rocksdbKey.Free()
		numKeys++
		batchSize = batchSize + len(key)
		batch.Delete(key)
		if batchSize >= maxBatchSize {
			if err := h.db.WriteBatch(batch, true); err != nil {
				return err
			}
			fmt.Printf("\t-------->Have removed %d entries for channel %s in rocksdb %s\n", numKeys, h.dbName, h.db.conf.DBPath)
			batchSize = 0
			batch.Clear()
		}
	}
	if batch.Count() > 0 {
		return h.db.WriteBatch(batch, true)
	}
	return nil
}

// IsEmpty returns true if no data exists for the DBHandle
func (h *DBHandle) IsEmpty() (bool, error) {
	fmt.Print("IsEmpty(), getting Iterator with nil start&end keys...\n")
	itr, err := h.GetIterator(nil, nil)
	if err != nil {
		return false, err
	}
	defer itr.Close()

	if err := itr.Err(); err != nil {
		return false, errors.WithMessagef(itr.Err(), "internal rocksdb error while obtaining next entry from iterator")
	}

	return !itr.Valid(), nil
}

// NewUpdateBatch returns a new UpdateBatch that can be used to update the db
func (h *DBHandle) NewUpdateBatch() *UpdateBatch {
	wb := rocksdb.NewWriteBatch()
	return &UpdateBatch{
		dbName:     h.dbName,
		WriteBatch: wb,
	}
}

// WriteBatch writes a batch in an atomic way
func (h *DBHandle) WriteBatch(batch *UpdateBatch, sync bool) error {
	if batch == nil || batch.Count() == 0 {
		return nil
	}
	if h.db.dbState == closed {
		return fmt.Errorf("error writing batch to rocksdb")
	}
	fmt.Printf("\t-------->WriteBatch()..., sync=[%+v]\n", sync)
	if err := h.db.WriteBatch(batch.WriteBatch, sync); err != nil {
		return err
	}
	return nil
}

// GetIterator gets an handle to iterator. The iterator should be released after the use.
// The resultset contains all the keys that are present in the db between the startKey (inclusive) and the endKey (exclusive).
// A nil startKey represents the first available key and a nil endKey represent a logical key after the last available key
func (h *DBHandle) GetIterator(startKey []byte, endKey []byte) (*Iterator, error) {
	eKey := constructRocksKey(h.dbName, endKey)
	sKey := constructRocksKey(h.dbName, startKey)
	if endKey == nil {
		fmt.Print("endKey is nil\n")
		// replace the last byte 'dbNameKeySep' by 'lastKeyIndicator'
		eKey[len(eKey)-1] = lastKeyIndicator
		fmt.Printf("\t-------->endKey is nil: eKey=[%s(%#v)]\n", eKey, eKey)
	} else {
		fmt.Printf("\t-------->endKey is not nil: (%#v)\n", endKey)

	}
	fmt.Printf("\t-------->Constructing iterator with sKey=[%s(%#v)] and eKey=[%s(%#v)]\n", sKey, sKey, eKey, eKey)
	itr, err := h.db.GetIterator(sKey, eKey)
	if err != nil {
		fmt.Printf("\t-------->Error! Closing iterator...\n")
		return nil, err
	}
	if itr.Valid() {
		fmt.Printf("\t-------->itr is Valid\n")
	} else {
		fmt.Printf("\t-------->itr is not Valid\n")
	}
	return &Iterator{h.dbName, itr}, nil
}

// Close closes the DBHandle after its db data have been deleted
func (h *DBHandle) Close() {
	if h.closeFunc != nil {
		h.closeFunc()
	}
}

// UpdateBatch encloses the details of multiple `updates`
type UpdateBatch struct {
	*rocksdb.WriteBatch
	dbName string
}

// Put adds a KV
func (b *UpdateBatch) Put(key []byte, value []byte) {
	if value == nil {
		panic("Nil value not allowed")
	}
	b.WriteBatch.Put(constructRocksKey(b.dbName, key), value)
}

// Delete deletes a Key and associated value
func (b *UpdateBatch) Delete(key []byte) {
	b.WriteBatch.Delete(constructRocksKey(b.dbName, key))
}

// Iterator extends actual rocksdb iterator
type Iterator struct {
	dbName string
	*rocksdb.Iterator
}

//Next wraps actual rocksdb iterator method.
//It prevents a fatal error when Next() called after the last db key
//if iterator.Valid() == false
func (itr *Iterator) Next() {
	if itr.Iterator.Valid() {
		//itr.FreeKey()
		//itr.FreeValue()
		itr.Iterator.Next()
	} else {
		fmt.Printf("\t-------->iterator is not valid anymore\n")
	}
	if err := itr.Iterator.Err(); err != nil {
		fmt.Printf("\t-------->Error during iteration: %s\n", err)
	}
}

// Key wraps actual rocksdb iterator method
func (itr *Iterator) Key() []byte {
	rocksdbKey := itr.Iterator.Key()
	keyData := rocksdbKey.Data()
	key := make([]byte, len(keyData))
	copy(key, keyData)
	rocksdbKey.Free()
	return retrieveAppKey(key)
}

// Key wraps actual rocksdb iterator method
func (itr *Iterator) Value() []byte {
	rocksdbValue := itr.Iterator.Value()
	valueData := rocksdbValue.Data()
	value := make([]byte, len(valueData))
	copy(value, valueData)
	rocksdbValue.Free()
	return value
}

// Seek moves the iterator to the first key/value pair
// whose key is greater than or equal to the given key.
// It returns whether such pair exist.
func (itr *Iterator) Seek(key []byte) {
	rocksKey := constructRocksKey(itr.dbName, key)
	itr.Iterator.Seek(rocksKey)
}

//TODO: should we make the mechanism differ from level db one?
func constructRocksKey(dbName string, key []byte) []byte {
	return append(append([]byte(dbName), dbNameKeySep...), key...)
}

func retrieveAppKey(levelKey []byte) []byte {
	return bytes.SplitN(levelKey, dbNameKeySep, 2)[1]
}

// ErrFormatMismatch is returned if it is detected that the version of the format recorded in
// the internal database is different from what is specified in the `Conf` that is used for opening the db
type ErrFormatMismatch struct {
	DBInfo         string
	ExpectedFormat string
	Format         string
}

func (e *ErrFormatMismatch) Error() string {
	return fmt.Sprintf("unexpected format. db info = [%s], data format = [%s], expected format = [%s]",
		e.DBInfo, e.Format, e.ExpectedFormat,
	)
}

// IsVersionMismatch returns true if err is an ErrFormatMismatch
func IsVersionMismatch(err error) bool {
	_, ok := err.(*ErrFormatMismatch)
	return ok
}
