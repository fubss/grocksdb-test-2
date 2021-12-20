package grocksdbtest2

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	rocksdb "github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/require"
)

const testDBPath = "/tmp/tests/grocksdbtest2"

func TestIteratorUpperBoundWithDirectAPICall(t *testing.T) {
	//=================================================================================================================================
	//input data from google sheet https://docs.google.com/spreadsheets/d/1NOK5iuf4z1_SFngkWYHOHB478g1bp-TjaZ8fC7e81gs/edit?usp=sharing
	IterationsNumber := 200
	KeysNumber := 200
	UseSubtestPerIteration := true
	//=================================================================================================================================
	loopId := 0

	//settle db with data
	db := newTestDB(t, "TestIterator", nil)
	wo := rocksdb.NewDefaultWriteOptions()
	givenKeys := createTestKeys(0, KeysNumber-1)
	for i := 0; i < KeysNumber; i++ {
		db.Put(wo, []byte(givenKeys[i]), []byte(createTestValue("db", i)))
	}
	//prepare ReadOptions
	ro := rocksdb.NewDefaultReadOptions()
	ro.SetIterateUpperBound([]byte("keya"))

	testFunc := func(t *testing.T) {
		var actualKeys []string
		iter := db.NewIterator(ro)

		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			key := make([]byte, 10)
			copy(key, iter.Key().Data())
			actualKeys = append(actualKeys, string(key))

		}
		require.Nil(t, iter.Err())
		iter.Close()
		//require.EqualValues(t, givenKeys, actualKeys)
		require.EqualValues(t, len(givenKeys), len(actualKeys))
		if !UseSubtestPerIteration {
			t.Logf("Loop_%d, givenKeysLen=%d, actualKeysLen=%d", loopId, len(givenKeys), len(actualKeys))
		}
	}
	for loopId := 0; loopId < IterationsNumber; loopId++ {
		if UseSubtestPerIteration {
			t.Run(fmt.Sprintf("Loop_%06d", loopId), testFunc)
		} else {
			testFunc(t)
		}
	}
	db.Close()

}

func TestDrop(t *testing.T) {
	env := newTestProviderEnv(t, testDBPath)
	//cleanup was commented because rocksdb panics if it closes second time
	//defer env.cleanup()
	p := env.provider

	db1 := p.GetDBHandle("db1")
	db2 := p.GetDBHandle("db2")
	db3 := p.GetDBHandle("db3")

	require.Contains(t, p.dbHandles, "db1")
	require.Contains(t, p.dbHandles, "db2")
	require.Contains(t, p.dbHandles, "db3")

	for i := 0; i < 20; i++ {
		db1.Put([]byte(createTestKey(i)), []byte(createTestValue("db1", i)), false)
		db2.Put([]byte(createTestKey(i)), []byte(createTestValue("db2", i)), false)
	}
	// db3 is used to test remove when multiple batches are needed (each long key has 125 bytes)
	for i := 0; i < 10000; i++ {
		db3.Put([]byte(createTestLongKey(i)), []byte(createTestValue("db3", i)), false)
	}

	expectedSetup := []struct {
		db             *DBHandle
		expectedKeys   []string
		expectedValues []string
	}{
		{
			db:             db1,
			expectedKeys:   createTestKeys(1, 19),
			expectedValues: createTestValues("db1", 1, 19),
		},
		{
			db:             db2,
			expectedKeys:   createTestKeys(1, 19),
			expectedValues: createTestValues("db2", 1, 19),
		},
		{
			db:             db3,
			expectedKeys:   createTestLongKeys(1, 9999),
			expectedValues: createTestValues("db3", 1, 9999),
		},
		{
			db:             db3,
			expectedKeys:   createTestLongKeys(1, 9999),
			expectedValues: createTestValues("db3", 1, 9999),
		},
		{
			db:             db3,
			expectedKeys:   createTestLongKeys(1, 9999),
			expectedValues: createTestValues("db3", 1, 9999),
		},
	}

	for i, dbSetup := range expectedSetup {
		t.Logf("expextedSetup_%d", i)
		itr, err := dbSetup.db.GetIterator(nil, nil)
		require.NoError(t, err)
		checkItrResults(t, itr, dbSetup.expectedKeys, dbSetup.expectedValues)
		itr.Close()
	}

	require.NoError(t, p.Drop("db1"))
	require.NoError(t, p.Drop("db3"))

	require.NotContains(t, p.dbHandles, "db1")
	require.NotContains(t, p.dbHandles, "db3")
	require.Contains(t, p.dbHandles, "db2")

	expectedResults := []struct {
		db             *DBHandle
		expectedKeys   []string
		expectedValues []string
	}{
		{
			db:             db1,
			expectedKeys:   nil,
			expectedValues: nil,
		},
		{
			db:             db2,
			expectedKeys:   createTestKeys(1, 19),
			expectedValues: createTestValues("db2", 1, 19),
		},
		{
			db:             db3,
			expectedKeys:   nil,
			expectedValues: nil,
		},
	}

	for i, result := range expectedResults {
		t.Logf("expextedResults_%d", i)
		itr, err := result.db.GetIterator(nil, nil)
		require.NoError(t, err)
		checkItrResults(t, itr, result.expectedKeys, result.expectedValues)
		itr.Close()
	}

	// negative test
	p.Close()
	require.EqualError(t, db2.deleteAll(), "error while obtaining db iterator: rocksdb: closed")
}

func checkItrResults(t *testing.T, itr *Iterator, expectedKeys []string, expectedValues []string) {
	var actualKeys []string
	var actualValues []string
	for itr.Next(); itr.Valid(); itr.Next() {
		actualKeys = append(actualKeys, string(itr.Key()))
		actualValues = append(actualValues, string(itr.Value()))
	}
	t.Logf("Iterator error is: [%s]", itr.Iterator.Err())
	if err := itr.Iterator.Err(); err != nil {
		t.Logf("Error-catch-2 during iteration: %s", err)
	}
	require.Equal(t, len(expectedKeys), len(actualKeys))
	//require.Equal(t, expectedKeys, actualKeys)
	//require.Equal(t, expectedValues, actualValues)
	itr.Next()
	require.Equal(t, false, itr.Valid())
}

func newTestDB(t *testing.T, name string, applyOpts func(opts *rocksdb.Options)) *rocksdb.DB {
	dir, err := ioutil.TempDir("", "gorocksdb-"+name)
	require.Nil(t, err)
	t.Logf("DB will be opened in: %s", dir)
	//dir := testDBPath

	opts := rocksdb.NewDefaultOptions()
	// test the ratelimiter
	rateLimiter := rocksdb.NewRateLimiter(1024, 100*1000, 10)
	opts.SetRateLimiter(rateLimiter)
	opts.SetCreateIfMissing(true)
	opts.SetCompression(rocksdb.ZSTDCompression)
	if applyOpts != nil {
		applyOpts(opts)
	}
	db, err := rocksdb.OpenDb(opts, dir)
	require.Nil(t, err)

	return db
}

type testDBProviderEnv struct {
	t        *testing.T
	path     string
	provider *Provider
}

func newTestProviderEnv(t *testing.T, path string) *testDBProviderEnv {
	testProviderEnv := &testDBProviderEnv{t: t, path: path}
	testProviderEnv.cleanup()
	var err error
	testProviderEnv.provider, err = NewProvider(&Conf{DBPath: path})
	if err != nil {
		panic(err)
	}
	return testProviderEnv
}

func (providerEnv *testDBProviderEnv) cleanup() {
	if providerEnv.provider != nil {
		providerEnv.provider.Close()
	}
	require.NoError(providerEnv.t, os.RemoveAll(providerEnv.path))
}

func createTestKey(i int) string {
	return fmt.Sprintf("key_%06d", i)
}

const padding100 = "_0123456789_0123456789_0123456789_0123456789_0123456789_0123456789_0123456789_0123456789_0123456789_"

func createTestLongKey(i int) string {
	return fmt.Sprintf("key_%s_%10d", padding100, i)
}

func createTestValue(dbname string, i int) string {
	return fmt.Sprintf("value_%s_%06d", dbname, i)
}

func createTestKeys(start int, end int) []string {
	var keys []string
	for i := start; i <= end; i++ {
		keys = append(keys, createTestKey(i))
	}
	return keys
}

func createTestLongKeys(start int, end int) []string {
	var keys []string
	for i := start; i <= end; i++ {
		keys = append(keys, createTestLongKey(i))
	}
	return keys
}

func createTestValues(dbname string, start int, end int) []string {
	var values []string
	for i := start; i <= end; i++ {
		values = append(values, createTestValue(dbname, i))
	}
	return values
}
