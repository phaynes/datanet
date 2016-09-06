#include <cstdio>
#include <cstring>
#include <string>
#include <map>

extern "C" {
  #include <stdio.h>
  #include <unistd.h> 
  #include <stdlib.h> 
}

#include "json/json.h"
#include "easylogging++.h"

#include "logic_storage_lmdb.h"
#include "helper.h"
#include "storage.h"

using namespace std;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GLOBALS -------------------------------------------------------------------

extern bool   StorageInitialized;

extern UInt64 DeltasMaxBytes;

extern map<string, bool> TableSharedMap;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GLOBALS -------------------------------------------------------------------

//typedef map<string, jv> MemoryTable;
map<string, MemoryTable> MemoryStorage;

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
// STARTUP -------------------------------------------------------------------

bool storage_cleanup() {
  return lmdb_storage_cleanup();
}

bool storage_init_nonprimary(void *p, const char *dfile, const char *ldbd) {
  return lmdb_storage_init_nonprimary(p, dfile, ldbd);
}

bool storage_init_primary(void *p, const char *dfile, const char *ldbd) {
  return lmdb_storage_init_primary(p, dfile, ldbd);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// MEMORY-LMDB HELPERS -------------------------------------------------------

bool is_shared_table(string &tname) {
  map<string, bool>::iterator it = TableSharedMap.find(tname);
  return (it != TableSharedMap.end());
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DELTA STORAGE BOOKKEEPING -------------------------------------------------

UInt64 DeltaStorageSize = 0;

bool increment_delta_storage_size(jv *jval) {
  if (!DeltasMaxBytes) return true;
  UInt64 dbytes    = (*jval)["delta"]["_meta"]["delta_bytes"].asUInt64();
  UInt64 ntot      = DeltaStorageSize + dbytes;
  if (ntot >= DeltasMaxBytes) {
    LE("NEW: DeltaStorageSize: " << ntot << " EXCEEDS: DeltasMaxBytes: " <<
       DeltasMaxBytes << " -> FAIL OPERATION");
    return false;
  } else {
    LD("increment_delta_storage_size: dbytes: " << dbytes << " TOT: " << ntot);
    DeltaStorageSize = ntot;
    return true;
  }
}

bool conditional_increment_delta_storage_size(string &pks, jv *jval) {
  if (!DeltasMaxBytes) return true;
  string tname = "DELTAS";
  uint   cnt   = MemoryStorage[tname].count(pks);
  if (cnt != 0) return true;                               // OVERWRITE
  else          return increment_delta_storage_size(jval); // INITIALIZE
}

static void decrement_delta_storage_size(string &tname, string &pks) {
  if (!DeltasMaxBytes) return;
  if (!tname.compare("DELTAS")) {
    uint    cnt       = MemoryStorage[tname].count(pks);
    if (cnt == 0) return;
    jv     *jval      = &(MemoryStorage[tname][pks]);
    jv     *jdmeta    = &((*jval)["DENTRY"]["delta"]["_meta"]);
    UInt64  dbytes    = (*jdmeta)["delta_bytes"].asUInt64();
    DeltaStorageSize -= dbytes;
    LD("decrement_delta_storage_size: dbytes: " << dbytes <<
       " TOT: " << DeltaStorageSize);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// MEMORY I/O ----------------------------------------------------------------

bool plugin_set_key(string &tname, string &pks, jv &jval) {
  if (!StorageInitialized) return false;
  LDATA("plugin_set_key");
  bool isst = is_shared_table(tname);
  if (isst) {
    return lmdb_plugin_set_key(tname, pks, jval);
  } else {
    MemoryStorage[tname][pks] = jval; // COPY
    return true;
  }
}

jv plugin_get_key_field(string &tname, string &pks, string &fname) {
  if (!StorageInitialized) return JNULL;
  LDATA("plugin_get_key_field");
  bool isst = is_shared_table(tname);
  if (isst) {
    return lmdb_plugin_get_key_field(tname, pks, fname);
  } else {
    jv jempty; // NOTE EMPTY
    uint cnt = MemoryStorage.count(tname);
    if (cnt == 0) return jempty;
    cnt = MemoryStorage[tname].count(pks);
    if (cnt == 0) return jempty;
    if (!zh_jv_is_member(&(MemoryStorage[tname][pks]), fname)) return jempty;
    return MemoryStorage[tname][pks][fname]; // COPY
  }
}

jv plugin_get_key(string &tname, string &pks) {
  if (!StorageInitialized) return JNULL;
  LDATA("plugin_get_key");
  bool isst = is_shared_table(tname);
  if (isst) {
    return lmdb_plugin_get_key(tname, pks);
  } else {
    uint cnt = MemoryStorage.count(tname);
    if (cnt == 0) return JNULL;
    cnt = MemoryStorage[tname].count(pks);
    if (cnt == 0) return JNULL;
    return MemoryStorage[tname][pks]; // COPY
  }
}

bool plugin_add_key(string &tname, string &pks) {
  if (!StorageInitialized) return false;
  LDATA("plugin_add_key");
  // NOTE: ONLY CALLED FROM lock_field -> ALWAYS SHARED
  return lmdb_plugin_add_key(tname, pks);
}

bool plugin_delete_key(string &tname, string &pks) {
  if (!StorageInitialized) return false;
  LDATA("plugin_delete_key");
  bool isst = is_shared_table(tname);
  if (isst) {
    return lmdb_plugin_delete_key(tname, pks);
  } else {
    uint cnt = MemoryStorage.count(tname);
    if (cnt == 0) return true;
    cnt = MemoryStorage[tname].count(pks);
    if (cnt == 0) return true;
    decrement_delta_storage_size(tname, pks);
    (void)MemoryStorage[tname].erase(pks);
    return true;
  }
}

bool plugin_delete_table(string &tname) {
  if (!StorageInitialized) return false;
  LDATA("plugin_delete_table");
  bool isst = is_shared_table(tname);
  if (isst) {
    return lmdb_plugin_delete_table(tname);
  } else {
    (void)MemoryStorage.erase(tname);
    return true;
  }
}

jv plugin_get_table(string &tname) {
  if (!StorageInitialized) return JNULL;
  LDATA("plugin_get_table");
  bool isst = is_shared_table(tname);
  if (isst) {
    return lmdb_plugin_get_table(tname);
  } else {
    uint cnt = MemoryStorage.count(tname);
    if (cnt == 0) return JNULL;
    jv jtbl = JOBJECT;
    for (map<string, jv>::iterator it = MemoryStorage[tname].begin();
                                   it != MemoryStorage[tname].end(); it++) {
      string pks  = it->first;
      jv     jval = it->second;
      jtbl[pks]   = jval;
    }
    return jtbl;
  }
}

