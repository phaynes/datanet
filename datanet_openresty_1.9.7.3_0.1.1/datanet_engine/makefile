
CPP = g++ -std=c++11 -Wall -g
CC = gcc -Wall

OPENRESTY_INSTALL_DIR = /usr/local/openresty/lualib/

COMMONLIB = -lpthread -ldl

JSONDIR = ./jsoncpp/build/debug/src/lib_json/
JSONLIB = -ljsoncpp

LUADIR = ./lua/src/
LUALIB = -llua

LMDBDIR = ./lmdb/libraries/liblmdb/
LMDBLIB = liblmdb.a

MSGPACKINCDIR = ./msgpack-c/include/
MSGPACKLIBDIR = ./msgpack-c/src/.libs/
MSGPACKLIB    = libmsgpackc.a

LZ4LIBDIR  = ./lua-lz4
LZ4LIBNAME = -llz4

INCLUDES = -I./ -I$(LUADIR) -I$(LMDBDIR) -I$(MSGPACKINCDIR)

SHAREDARGS = -shared -fPIC

OBJECTS = Crc32.o sha256.o sqlite3.o engine_network.o helper.o external_hooks.o shared.o dcompress.o mp.o lz4_api.o xact.o convert.o merge.o deltas.o cache.o doc.o oplog.o subscriber_delta.o subscriber_merge.o ooo_replay.o apply_delta.o dack.o dt.o handler.o internal_auth.o isolated.o agent_daemon.o activesync.o gc.o creap.o gc_reaper.o latency.o heartbeat.o fixlog.o aio.o auth.o datastore.o notify.o trace.o signals.o storage.o engine.o deprecated_engine.o lua_engine_hooks.o

#BIN = sqlite datanet_sqlite_engine datanet_ngx_shared_engine datanet_lmdb_engine datanet_memory_lmdb_engine
#BIN = sqlite datanet_ngx_shared_engine datanet_lmdb_engine
#BIN = sqlite datanet_lmdb_engine
#BIN = sqlite datanet_lmdb_engine storage_ngx_shared.o storage_sql.o
#FIXME building all STORAGE_* to insure code is up-to-date across all
BIN = sqlite datanet_memory_lmdb_engine storage_lmdb.o storage_ngx_shared.o storage_sql.o

all : $(BIN)

deps :
	(cd jsoncpp/build/debug; make)
	(cd lua; make linux)
	(cd lmdb/libraries/liblmdb/; make)
	(cd msgpack-c; make)
	(cd lua-lz4; make)

initial: deps $(BIN)

install: all
	cp datanet_memory_lmdb_engine.so $(OPENRESTY_INSTALL_DIR)

sha256.o : sha256.cpp
	$(CC) $(SHAREDARGS) -o $@ -c $^

sqlite3.o : sqlite3.c
	$(CC) $(SHAREDARGS) -o $@ -c $^

sqlite : sqlite3.c shell.c
	$(CC) $(SHAREDARGS) -o $@ $^ $(COMMONLIB) 

%.o: %.cpp
	$(CPP) $(SHAREDARGS) $(INCLUDES) -c $<


datanet_sqlite_engine : $(OBJECTS) storage_sql.o
	$(CPP) -o datanet_sqlite_engine.so $^ $(COMMONLIB) $(SHAREDARGS) -L$(JSONDIR) $(JSONLIB) -L$(LUADIR) $(LUALIB)

datanet_ngx_shared_engine : $(OBJECTS) storage_common.o storage_ngx_shared.o
	$(CPP) -o datanet_ngx_shared_engine.so $^ $(COMMONLIB) $(SHAREDARGS) -L$(JSONDIR) $(JSONLIB) -L$(LUADIR) $(LUALIB)

datanet_lmdb_engine : $(OBJECTS) logic_storage_lmdb.o storage_common.o storage_lmdb.o
	$(CPP) -o datanet_lmdb_engine.so $^ $(COMMONLIB) $(SHAREDARGS) -L$(JSONDIR) $(JSONLIB) -L$(LUADIR) $(LUALIB) $(LMDBDIR)/$(LMDBLIB) $(MSGPACKLIBDIR)/$(MSGPACKLIB) -L$(LZ4LIBDIR) $(LZ4LIBNAME)

datanet_memory_lmdb_engine : $(OBJECTS) logic_storage_lmdb.o storage_common.o storage_memory_lmdb.o
	$(CPP) -o datanet_memory_lmdb_engine.so $^ $(COMMONLIB) $(SHAREDARGS) -L$(JSONDIR) $(JSONLIB) -L$(LUADIR) $(LUALIB) $(LMDBDIR)/$(LMDBLIB) $(MSGPACKLIBDIR)/$(MSGPACKLIB) -L$(LZ4LIBDIR) $(LZ4LIBNAME)

clean :
	rm -f $(BIN) *.o *.so


.PHONY: all, clean

