# grocksdb-test-2
1. `export GO111MODULE=off`
2. `go get -d  github.com/fubss/grocksdb-test-2`
3. run test in new terminal window without saving results to cache
 - call my iterator test:
```
GOFLAGS="-count=1" go test -timeout 30s -run ^TestDrop$ github.com/fubss/grocksdb-test-2 -v
```
- call direct API test:
```
GOFLAGS="-count=1" go test -timeout 30s -run ^TestIteratorUpperBoundWithDirectAPICall$ github.com/fubss/grocksdb-test-2 -v
```
