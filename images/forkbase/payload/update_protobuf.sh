PROTOBUF_VER=3.6.1
PROTOBUF_PKG=v$PROTOBUF_VER.tar.gz

cd /tmp
wget --quiet https://github.com/google/protobuf/archive/$PROTOBUF_PKG
tar xpzf $PROTOBUF_PKG
cd protobuf-$PROTOBUF_VER
./autogen.sh
./configure --prefix=/usr

make -j8
make install
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
