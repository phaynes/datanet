#include <cstdio>
#include <cstring>
#include <string>
#include <sstream>  
#include <iostream>
#include <vector>

extern "C" {
  #include <stdio.h>
  #include <unistd.h> 
  #include <stdlib.h> 
}

extern "C" {
  #include "lmdb.h"
}

#include "json/json.h"
#include "easylogging++.h"
#include <msgpack.h>

#include "logic_storage_lmdb.h"
#include "lz4_api.h"
#include "mp.h"
#include "dcompress.h"
#include "helper.h"
#include "storage.h"

using namespace std;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EXTERNS -------------------------------------------------------------------

extern bool StorageInitialized;

extern map<string, string> TablePrimaryKeyMap;
extern map<string, string> TableSimpleValueMap;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GLOBALS -------------------------------------------------------------------

lua_State *L = NULL;

bool OverrideStoreAsJson = true;

#define NUM_TABLES 100
#define DB_SIZE    134217728 /* 128MB */

map<string, uint> TableMap;
vector<string>    TableNames;

MDB_env *Lenv; 
MDB_dbi  Ldbi[NUM_TABLES]; 
MDB_txn *Ltxn[NUM_TABLES];


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEBUG SETTINGS ------------------------------------------------------------

#define DISABLE_LT
#ifdef DISABLE_LT
  #undef LT
  #define LT(text) /* text */
#endif

#define DISABLE_LDATA
#ifdef DISABLE_LDATA
  #undef LDATA
  #define LDATA(text) /* text */
#else
  #define LDATA(text) LD(text)
#endif


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// LMDB DEFINE HELPERS -------------------------------------------------------

#define LMDB_ERROR_CHECK(expr, eret)                    \
  if ((rc = (expr)) != MDB_SUCCESS) {                   \
    LE(__FILE__ << ":" << __LINE__ << ": " << #expr <<  \
       ": " << mdb_strerror(rc));                       \
    return eret;                                        \
  }


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STARTUP HELPERS -----------------------------------------------------------

static void populate_table_names() {
  LT("populate_table_names");
  TableNames.clear();
  for (map<string,string>::iterator it  = TablePrimaryKeyMap.begin();
                                    it != TablePrimaryKeyMap.end(); it++) {
    string tname = it->first;
    TableNames.push_back(tname);
  }
}

static bool init_lmdb_tables() {
  LT("init_lmdb_tables");
  int rc;
  populate_table_names();
  TableMap.clear();
  for (uint i = 0; i < TableNames.size(); i++) {
    string tname = TableNames[i];
    LMDB_ERROR_CHECK(mdb_txn_begin(Lenv, NULL, 0, &(Ltxn[i])), false);
    LMDB_ERROR_CHECK(mdb_dbi_open(Ltxn[i], tname.c_str(), MDB_CREATE,
                     &(Ldbi[i])), false);
    LMDB_ERROR_CHECK(mdb_txn_commit(Ltxn[i]), false);
    TableMap.insert(make_pair(tname, i));
  }
  return true;
}

static bool init_lmdb_env(const char *ldbd) {
  LD("init_lmdb_env: LDBD: " << ldbd);
  int rc;
  LMDB_ERROR_CHECK(mdb_env_create(&Lenv), false);
  LMDB_ERROR_CHECK(mdb_env_set_mapsize(Lenv, DB_SIZE), false);
  LMDB_ERROR_CHECK(mdb_env_set_maxdbs (Lenv, NUM_TABLES), false);
  LMDB_ERROR_CHECK(mdb_env_open(Lenv, ldbd, MDB_WRITEMAP|MDB_NOSYNC, 0664),
                   false);
  if (!init_lmdb_tables()) return false;
  return true;
}

bool lmdb_storage_cleanup() { return true; /* NO-OP */ }

bool lmdb_storage_init_nonprimary(void *p, const char *dfile,
                                  const char *ldbd) {
  LT("storage_init_nonprimary");
  L = (lua_State *)p;
  storage_populate_table_primary_key_map();
  if (!init_lmdb_env(ldbd)) return false;
  StorageInitialized = true;
  return true;
}

bool lmdb_storage_init_primary(void *p, const char *dfile, const char *ldbd) {
  LT("storage_init_primary");
  L = (lua_State *)p;
  storage_populate_table_primary_key_map();
  if (!init_lmdb_env(ldbd)) return false;
  string tname = "WORKERS";
  plugin_delete_table(tname);
  tname        = "LOCKS";
  plugin_delete_table(tname);
  StorageInitialized = true;
  return true;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// LMDB HELPERS --------------------------------------------------------------

#define INIT_PLUGIN_IO(tname)            \
  int      rc;                           \
  uint     slot = get_table_slot(tname); \
  MDB_dbi  dbi  = Ldbi[slot];            \
  MDB_txn *txn  = Ltxn[slot];

static uint get_table_slot(string &tname) {
  LT("get_table_slot");
  map<string, uint>::iterator it = TableMap.find(tname);
  if (it == TableMap.end()) zh_fatal_error("LOGIC(get_table_slot)");
  uint slot = it->second;
  return slot;
}

static string get_pk_name(string &tname) {
  LT("get_pk_name");
  map<string, string>::iterator it = TablePrimaryKeyMap.find(tname);
  if (it == TablePrimaryKeyMap.end()) zh_fatal_error("LOGIC(get_pk_name)");
  string pkn = it->second;
  return pkn;
}

static string get_table_simple_field_name(string &tname) {
  LT("get_table_simple_field_name");
  map<string, string>::iterator it = TableSimpleValueMap.find(tname);
  string fname;
  if (it != TableSimpleValueMap.end()) fname = it->second;
  return fname;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// COMPRESS CRDT/META --------------------------------------------------------

static void compress_meta(string desc, jv *jmeta) {
  if (zjn(jmeta)) return;
  else            zcmp_compress_meta(jmeta);
}

static void compress_crdt(string desc, jv *jcrdt) {
  if (zjn(jcrdt)) return;
  else            zcmp_compress_crdt(jcrdt);
}

static void compress_jval_crdt(jv &jval) {
  if (zh_jv_is_member(&jval, "CRDT")) compress_crdt("CRDT", &(jval["CRDT"]));
  if (zh_jv_is_member(&jval, "DENTRY")) {
    compress_meta("DENTRY.delta._meta", &(jval["DENTRY"]["delta"]["_meta"]));
  }
  if (zh_jv_is_member(&jval, "PC")) {
    jv *pc = &(jval["PC"]);
    if (zh_jv_is_member(pc, "xcrdt")) {
      compress_crdt("PC.xcrdt", &((*pc)["xcrdt"]));
    }
    if (zh_jv_is_member(pc, "dentry")) {
      jv *jdentry = &((*pc)["dentry"]);
      compress_meta("PC.dentry.delta._meta", &((*jdentry)["delta"]["_meta"]));
    }
    if (zh_jv_is_member(pc, "extra_data")) {
      jv *ed = &((*pc)["extra_data"]);
      if (zh_jv_is_member(ed, "md")) {
        jv *md = &((*ed)["md"]);
        compress_crdt("PC.extra_data.md.ocrdt", &((*md)["ocrdt"]));
      }
    }
  }
}

static void decompress_meta(string desc, jv *jmeta) {
  if (zjn(jmeta)) return;
  else            zcmp_decompress_meta(jmeta);
}

static void decompress_crdt(string desc, jv *jcrdt) {
  if (zjn(jcrdt)) return;
  else            zcmp_decompress_crdt(jcrdt);
}

static void decompress_jval_crdt(jv &jval) {
  if (zh_jv_is_member(&jval, "CRDT")) decompress_crdt("CRDT", &(jval["CRDT"]));
  if (zh_jv_is_member(&jval, "DENTRY")) {
    decompress_crdt("DENTRY.crdt",        &(jval["DENTRY"]["crdt"]));
    decompress_meta("DENTRY.delta._meta", &(jval["DENTRY"]["delta"]["_meta"]));
  }
  if (zh_jv_is_member(&jval, "PC")) {
    jv *pc = &(jval["PC"]);
    if (zh_jv_is_member(pc, "xcrdt")) {
      decompress_crdt("PC.xcrdt", &((*pc)["xcrdt"]));
    }
    if (zh_jv_is_member(pc, "dentry")) {
      jv *jdentry = &((*pc)["dentry"]);
      decompress_crdt("PC.dentry.crdt",        &((*jdentry)["crdt"]));
      decompress_meta("PC.dentry.delta._meta", &((*jdentry)["delta"]["_meta"]));
    }
    if (zh_jv_is_member(pc, "extra_data")) {
      jv *ed = &((*pc)["extra_data"]);
      if (zh_jv_is_member(ed, "md")) {
        jv *md = &((*ed)["md"]);
        decompress_crdt("PC.extra_data.md.ocrdt", &((*md)["ocrdt"]));
      }
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SERIALIALIZATION ----------------------------------------------------------

#define STORAGE_FLAG_NONE 0x0
#define STORAGE_FLAG_LZ4  0x1

static void copy_buffer(byte_buffer &v, char *d, size_t len) {
  for (uint i = 0; i < len; i++) {
    char *b = (d + i);
    v.push_back(*b);
  }
}

static jv do_lmdb_deserialize_value(string &tname, string &pkn, MDB_val *data) {
  LT("do_lmdb_deserialize_value");
  string sfname = get_table_simple_field_name(tname);
  if (sfname.size()) {
    jv jval;
    UInt64 *u    = (UInt64 *)data->mv_data;
    jval[sfname] = *u;
    LDATA("DESERIALIZE: SIMPLE: JVAL: " << jval);
    return jval;
  } else {
    char   *d      = (char *)data->mv_data;
    size_t  dsize  = data->mv_size;
    LDATA("DESERIALIZE: STORED DATA SIZE: " << dsize);
    bool    do_lz4 = (*d & STORAGE_FLAG_LZ4);
    d++; dsize--; // NEXT BYE AFTER FLAG
    jv jval;
    if (do_lz4) {
      byte_buffer u = lz4_decompress(d, dsize);
      jval          = zmp_unpack(&u[0], u.size());
    } else {
      jval          = zmp_unpack(d, dsize);
    }
    decompress_jval_crdt(jval);
    LDATA("DESERIALIZE: JVAL: " << jval);
    return jval;
  }
}

static byte_buffer do_lmdb_serialize_non_simple_value(jv &jval) {
  compress_jval_crdt(jval);
  msgpack_sbuffer sbuf = zmp_pack(&jval);
  byte_buffer     c;
  c.push_back(STORAGE_FLAG_LZ4);
  lz4_compress_append(c, sbuf.data, sbuf.size); // APPEND -> SINGLE COPY
  LDATA("SERIALIZE: MSGPACK_PACK(): SIZE: " << sbuf.size <<
        " LZ4_COMPRESS(): SIZE: " << c.size());
  if (c.size() < sbuf.size) return c;
  else {
    byte_buffer v;
    v.push_back(STORAGE_FLAG_NONE);
    copy_buffer(v, (char *)sbuf.data, sbuf.size);
    return v;
  }
}

static byte_buffer do_lmdb_serialize_value(string &tname, string &pkn,
                                           jv &jval) {
  LT("do_lmdb_serialize_value");
  string sfname = get_table_simple_field_name(tname);
  jv     jcval  = jval;    // NOTE: gets modified
  jcval.removeMember(pkn); // PK-VALUE stored in KEY
  if (sfname.size()) {
    LDATA("do_lmdb_serialize_value: SIMPLE: JVAL: " << jcval);
    byte_buffer v;
    UInt64      u = jcval[sfname].asUInt64();
    copy_buffer(v, (char *)(&u), sizeof(UInt64)); // COPY 8 Bytes
    return v;
  } else {
    LDATA("do_lmdb_serialize_value: NORMAL: JVAL: " << jcval);
    byte_buffer b = do_lmdb_serialize_non_simple_value(jcval);
    LDATA("SERIALIZE: STORED DATA SIZE: " << b.size());
    return b;
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// LMDB I/O ------------------------------------------------------------------

bool lmdb_plugin_set_key(string &tname, string &pks, jv &jval) {
  if (!StorageInitialized) return false;
  LDATA("plugin_set_key");
  INIT_PLUGIN_IO(tname)
  MDB_val     key, data;
  key.mv_data       = (void *)pks.c_str();
  key.mv_size       = pks.size() + 1;
  string      pkn   = get_pk_name(tname);
  byte_buffer v;    // NORMAL SERIALIZATION
  string      sval; // OverrideStoreAsJson
  if (OverrideStoreAsJson) {
    sval         = zh_convert_jv_to_string(&jval);
    data.mv_data = (void *)sval.c_str();
    data.mv_size = sval.size() + 1;
  } else {
    v            = do_lmdb_serialize_value(tname, pkn, jval);
    data.mv_data = (void *)&v[0];
    data.mv_size = v.size();
  }
  LMDB_ERROR_CHECK(mdb_txn_begin(Lenv, NULL, 0, &txn), false);
  rc                = mdb_put(txn, dbi, &key, &data, 0);
  LMDB_ERROR_CHECK(mdb_txn_commit(txn), false);
  return rc ? false : true;
}

jv lmdb_plugin_get_key_field(string &tname, string &pks, string &fname) {
  if (!StorageInitialized) return JNULL;
  LDATA("plugin_get_key_field");
  jv jempty; // NOTE EMPTY
  sqlres sr;
  jv jval = lmdb_plugin_get_key(tname, pks);
  if (zjn(&jval)) return jempty;
  else {
    if (!zh_jv_is_member(&jval, fname)) return jempty;
    else                                return jval[fname]; // COPY
  }
}

jv lmdb_plugin_get_key(string &tname, string &pks) {
  if (!StorageInitialized) return JNULL;
  LDATA("plugin_get_key");
  INIT_PLUGIN_IO(tname)
  MDB_val key, data;
  key.mv_data = (void *)pks.c_str();
  key.mv_size = pks.size() + 1;
  LMDB_ERROR_CHECK(mdb_txn_begin(Lenv, NULL, MDB_RDONLY, &txn), JNULL);
  rc          = mdb_get(txn, dbi, &key, &data);
  mdb_txn_abort(txn); // READ-ONLY END-OF-TRANSACTIO
  if (rc) return JNULL;
  else {
    if (OverrideStoreAsJson) {
      char *d = (char *)data.mv_data;
      return zh_parse_json_text(d, false);
    } else {
      string pkn  = get_pk_name(tname);
      jv     jval = do_lmdb_deserialize_value(tname, pkn, &data);
      jval[pkn]   = pks; // POPULATE PK-VALUE
      return jval;
    }
  }
}

bool lmdb_plugin_add_key(string &tname, string &pks) {
  if (!StorageInitialized) return false;
  LDATA("plugin_add_key");
  INIT_PLUGIN_IO(tname)
  MDB_val key, data;
  key.mv_data  = (void *)pks.c_str();
  key.mv_size  = pks.size() + 1;
  jv jval;
  jval["exists"] = true; // NOTE: stores BOOL: EXISTS
  string sval  = zh_convert_jv_to_string(&jval);
  data.mv_data = (void *)sval.c_str();
  data.mv_size = sval.size() + 1;
  LMDB_ERROR_CHECK(mdb_txn_begin(Lenv, NULL, 0, &txn), false);
  rc           = mdb_put(txn, dbi, &key, &data, MDB_NOOVERWRITE);
  LMDB_ERROR_CHECK(mdb_txn_commit(txn), false);
  return (rc != MDB_KEYEXIST);
}

bool lmdb_plugin_delete_key(string &tname, string &pks) {
  if (!StorageInitialized) return false;
  LDATA("plugin_delete_key");
  INIT_PLUGIN_IO(tname)
  MDB_val key;
  key.mv_data = (void *)pks.c_str();
  key.mv_size = pks.size() + 1;
  LMDB_ERROR_CHECK(mdb_txn_begin(Lenv, NULL, 0, &txn), false);
  rc          = mdb_del(txn, dbi, &key,NULL);
  LMDB_ERROR_CHECK(mdb_txn_commit(txn), false);
  return rc ? false : true;
}

bool lmdb_plugin_delete_table(string &tname) {
  if (!StorageInitialized) return false;
  LDATA("plugin_delete_table");
  INIT_PLUGIN_IO(tname)
  LMDB_ERROR_CHECK(mdb_txn_begin(Lenv, NULL, 0, &txn), false);
  rc = mdb_drop(txn, dbi, 0);
  LMDB_ERROR_CHECK(mdb_txn_commit(txn), false);
  return rc ? false : true;
}

jv lmdb_plugin_get_table(string &tname) {
  if (!StorageInitialized) return JNULL;
  LDATA("plugin_get_table");
  INIT_PLUGIN_IO(tname)
  MDB_val     key, data;
  jv          jval;
  MDB_cursor *cursor;
  string      pkn  = get_pk_name(tname);
  LMDB_ERROR_CHECK(mdb_txn_begin(Lenv, NULL, MDB_RDONLY, &txn), JNULL);
  LMDB_ERROR_CHECK(mdb_cursor_open(txn, dbi, &cursor), JNULL);
  while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
    char *k = (char *)key.mv_data;
    if (OverrideStoreAsJson) {
      char *d      = (char *)data.mv_data;
      jval[k]      = zh_parse_json_text(d, false);
    } else {
      jval[k]      = do_lmdb_deserialize_value(tname, pkn, &data);
      jval[k][pkn] = k; // POPULATE PK-VALUE
    }
  }
  mdb_cursor_close(cursor);
  mdb_txn_abort(txn);
  return jval;
}

