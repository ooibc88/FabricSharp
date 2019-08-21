
src/status.o: src/status.cc 
	g++-4.8 -std=c++11 -fPIC -o $@ -c $^ 

src/db.o: src/db.cc 
	g++-4.8 -std=c++11 -fPIC -o $@ -c $^ 

src/db_dasl.o: src/db.cc
	g++-4.8 -std=c++11 -fPIC -D GO_USE_DASL -o $@ -c $^

lib: src/status.o src/db.o
	g++-4.8 -std=c++11 -fPIC -shared -o ${USTORE_HOME}/lib/libprovdb.so $^

lib_dasl: src/status.o src/db_dasl.o
	g++-4.8 -std=c++11 -fPIC -shared -o ${USTORE_HOME}/lib/libprovdb_dasl.so $^

.PHONY: lib lib_dasl

test: lib_dasl
	g++-4.8 -std=c++11 -o test.bin -I/provdb/src -I/provdb/test test/gtest/*.cc test/*.cc -lpthread -lustore -lgflags -lprovdb_dasl

.PHONY: test

go: 
	cd ${USTORE_GOPATH} && swig -go -cgo -c++ -intgosize 64 -outdir ${USTORE_GOPATH} ustore.i

.PHONY: go

${GOPATH}/bin/hyperledgerPlus: go lib
	cd ${GOPATH}/src/microexp/hyperledgerPlus/main && CGO_LDFLAGS="-lprovdb -lustore" CGO_CXXFLAGS="-std=c++11 -I/provdb/src" go build -o $@

${GOPATH}/bin/lineagechainMinus: go lib
	cd ${GOPATH}/src/microexp/lineagechain/main && CGO_LDFLAGS="-lprovdb -lustore" CGO_CXXFLAGS="-std=c++11 -I/provdb/src" go build -o $@

${GOPATH}/bin/lineagechain: go lib_dasl
	cd ${GOPATH}/src/microexp/lineagechain/main && CGO_LDFLAGS="-lprovdb_dasl -lustore" CGO_CXXFLAGS="-std=c++11 -I/provdb/src" go build -o $@

all: ${GOPATH}/bin/hyperledgerPlus ${GOPATH}/bin/lineagechainMinus ${GOPATH}/bin/lineagechain test


clean:
	rm -f test.bin src/status.o src/db.o src/db_dasl.o
	rm -f ${GOPATH}/bin/hyperledgerPlus ${GOPATH}/bin/lineagechainMinus ${GOPATH}/bin/lineagechain
	rm -rf ${USTORE_GOPATH}

.PHONY: clean
