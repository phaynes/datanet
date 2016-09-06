
#ifndef __DATANET_STORAGE_MEMORY_LMDB__H
#define __DATANET_STORAGE_MEMORY_LMDB__H

#include <string>

using namespace std;

#define USING_MEMORY_LMDB_PLUGIN

extern map<string, MemoryTable> MemoryStorage;

bool increment_delta_storage_size(jv *jval);
bool conditional_increment_delta_storage_size(string &pks, jv *jval);

#define FETCH_CRDT(kqk)                                           \
{                                                                 \
  string tname = "DOCUMENTS";                                     \
  string fname = "CRDT";                                          \
  uint   cnt   = MemoryStorage.count(tname);                      \
  if (cnt != 0) {                                                 \
    cnt = MemoryStorage[tname].count(kqk);                        \
    if (cnt != 0) {                                               \
      if (zh_jv_is_member(&(MemoryStorage[tname][kqk]), fname)) { \
        sc.jres = MemoryStorage[tname][kqk][fname]; /* COPY */    \
      }                                                           \
    }                                                             \
  }                                                               \
}

#define PERSIST_CRDT(kqk, jcrdt)                         \
{                                                        \
  string tname                      = "DOCUMENTS";       \
  string pkname                     = "KQK";             \
  string fname                      = "CRDT";            \
  MemoryStorage[tname][kqk][pkname] = kqk;    /* COPY */ \
  MemoryStorage[tname][kqk][fname]  = *jcrdt; /* COPY */ \
  jv *pcrdt = &(MemoryStorage[tname][kqk][fname]);       \
  zstorage_cleanup_crdt_before_persist(pcrdt);           \
}

#define PERSIST_DELTA(kqk, avrsn, jdentry, jauth)           \
{                                                           \
  string tname  = "DELTAS";                                 \
  string dkey   = zs_get_persist_delta_key(kqk, avrsn);     \
  string pkname = "DELTA_KEY";                              \
  string fname  = "DENTRY";                                 \
  string aname  = "AUTH";                                   \
  bool ok = increment_delta_storage_size(jdentry);          \
  if (!ok) {                                                \
    sd.err = ZS_Errors["DeltaStorageMaxHit"];               \
    return sd;                                              \
  }                                                         \
  MemoryStorage[tname][dkey][pkname] = dkey;     /* COPY */ \
  MemoryStorage[tname][dkey][fname]  = *jdentry; /* COPY */ \
  MemoryStorage[tname][dkey][aname]  = *jauth;   /* COPY */ \
}

#define UPDATE_DELTA(kqk, avrsn, jdentry)                            \
{                                                                    \
  string tname  = "DELTAS";                                          \
  string dkey   = zs_get_persist_delta_key(kqk, avrsn);              \
  string pkname = "DELTA_KEY";                                       \
  string fname  = "DENTRY";                                          \
  bool ok = conditional_increment_delta_storage_size(dkey, jdentry); \
  if (!ok) {                                                         \
    sd.err = ZS_Errors["DeltaStorageMaxHit"];                        \
    return sd;                                                       \
  }                                                                  \
  MemoryStorage[tname][dkey][pkname] = dkey;     /* COPY */          \
  MemoryStorage[tname][dkey][fname]  = *jdentry; /* COPY */          \
}

#define FETCH_SUBSCRIBER_DELTA(kqk, avrsn)                         \
{                                                                  \
  string tname  = "DELTAS";                                        \
  string dkey   = zs_get_persist_delta_key(kqk, avrsn);            \
  string pkname = "DELTA_KEY";                                     \
  string fname  = "DENTRY";                                        \
  uint   cnt    = MemoryStorage.count(tname);                      \
  if (cnt != 0) {                                                  \
    cnt = MemoryStorage[tname].count(dkey);                        \
    if (cnt != 0) {                                                \
      if (zh_jv_is_member(&(MemoryStorage[tname][dkey]), fname)) { \
        sd.jres = MemoryStorage[tname][dkey][fname]; /* COPY */    \
      }                                                            \
    }                                                              \
  }                                                                \
}

#endif
