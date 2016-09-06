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
  #include "lua.h"
  #include "lualib.h"
  #include "lauxlib.h"
}

extern "C" {
  #include "lua.h"
  #include "lualib.h"
  #include "lauxlib.h"
}

#include "json/json.h"
#include "easylogging++.h"

#include "helper.h"
#include "external_hooks.h"
#include "cache.h"
#include "creap.h"

using namespace std;

extern lua_State *GL;

extern string MyDataCenterUUID;

extern jv     zh_nobody_auth;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SHARED GLOBALS ------------------------------------------------------------

UInt64 CacheMaxBytes   = 0;;

bool   CacheLocalEvict = false;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CACHE REAPER --------------------------------------------------------------

static bool cmp_when_desc(jv *jk1, jv *jk2) {
  UInt64 ts1 = (*jk1)["WHEN"].asUInt64();
  UInt64 ts2 = (*jk2)["WHEN"].asUInt64();
  return (ts1 > ts2); // NOTE DESC
}

static sqlres do_evict_key(string &kqk) {
  LD("do_evict_key: K: " << kqk);
  sqlres sr = remove_to_evict_key(kqk);
  RETURN_SQL_ERROR(sr)
  jv jks = zh_create_ks(kqk, NULL);
  bool send_central = true;
  bool need_merge   = false;
  //TODO CacheLocalEvict option
  return zcache_internal_evict(&jks, send_central, need_merge);
}

jv process_internal_run_cache_reaper(lua_State *L, string id, jv *params) {
  sqlres  sr  = fetch_all_to_evict_keys();
  PROCESS_GENERIC_ERROR(id, sr)
  jv     *kqks = &(sr.jres);
  for (uint i = 0; i < kqks->size(); i++) {
    string kqk = (*kqks)[i].asString();
    if (zh_is_key_local_to_worker(kqk)) {
      sqlres sr = do_evict_key(kqk);
      PROCESS_GENERIC_ERROR(id, sr)
    }
  }
  return zh_create_jv_response_ok(id);
}

static bool do_reap_evict(vector<string> *toe) { LT("do_reap_evict");
  for (vector<string>::iterator it = toe->begin(); it != toe->end(); it++) {
    string kqk = (*it);
    sqlres sr  = persist_to_evict_key(kqk);
    if (sr.err.size()) return false;
  }
  jv prequest = zh_create_json_rpc_body("InternalRunCacheReaper",
                                        &zh_nobody_auth);
  return zexh_broadcast_internal_request(prequest, 0);
}

// ALGORITHM: array divided in half (OLD & NEW)
//            in NEW part, 20% is reserved for local_modification KEYS
//            in NEW part, 20% is reserved for local_read         KEYS
//            All other keys are kept following LRU
static UInt64 analyze_cache_for_reap(UInt64 cnbytes, UInt64 want_bytes,
                                     jv *klrus, vector<string> *toe) {
  LT("analyze_cache_for_reap");
  vector<jv *>   clrus;
  vector<string> mbrs = zh_jv_get_member_names(klrus);
  for (uint i = 0; i < mbrs.size(); i++) {
    string  kqk    = mbrs[i];
    jv     *klru   = &((*klrus)[mbrs[i]]);
    bool    cached = zh_get_bool_member(klru, "CACHED");
    bool    pin    = zh_get_bool_member(klru, "PIN");
    bool    watch  = zh_get_bool_member(klru, "WATCH");
    LD("K: " << kqk << " C: " << cached << " P: " << pin << " W: " << watch);
    if (cached && !pin && !watch) {
      (*klru)["KQK"] = kqk;
      clrus.push_back(klru);
    }
  }

  if (clrus.size() == 0) return 0;

  // DESC (i.e. START is youngest/biggest)
  std::sort(clrus.begin(), clrus.end(), cmp_when_desc);
  UInt64 mid   = (UInt64)(clrus.size() / 2);
  UInt64 ot    = (*(clrus[mid]))["WHEN"].asUInt64();
  UInt64 rsize = (UInt64)(want_bytes * 0.2); // RESERVED SIZE for:
  UInt64 msize = rsize;                      //   1.) Local Modifications
  UInt64 asize = rsize;                      //   2.) Local Reads
  vector<jv *>   olds;
  for (uint i = 0; i < mid; i++) {
    jv     *clru   = clrus[i];
    string  kqk    = (*clru)["KQK"].asString();
    UInt64  locr   = (*clru)["LOCAL_READ"].asUInt64();
    UInt64  lmod   = (*clru)["LOCAL_MODIFICATION"].asUInt64();
    UInt64  nbytes = (*clru)["NUM_BYTES"].asUInt64();
    bool    is_lm  = lmod && (lmod > ot);
    bool    is_lr  = locr && (locr > ot);

    LD("CLRU: K: " << kqk << " is_lm: " << is_lm << " is_lr: " << is_lr);
    if (is_lm) {
      msize -= nbytes;
      if (msize <= 0) olds.push_back(clru);
      else            LD("RESERVE: LMOD: K: " << kqk);
    } else if (is_lr) {
      asize -= nbytes;
      if (asize <= 0) olds.push_back(clru);
      else            LD("RESERVE: LOCR: K: " << kqk);
    } else {
      olds.push_back(clru);
    }
  }
  for (uint i = mid; i < clrus.size(); i++) {
    jv *clru = clrus[i];
    olds.push_back(clru);
  }

  for (int i = (int)(olds.size() - 1); i >= 0; i--) { // START with OLDEST
    jv     *old     = olds[i];
    string  kqk     = (*old)["KQK"].asString();
    UInt64  nbytes  = (*old)["NUM_BYTES"].asUInt64(); // NEW
    cnbytes        -= nbytes;
    toe->push_back(kqk);
    LE("EVICT: K: " << kqk << " #B: " << nbytes << " C(#B): " << cnbytes);
    if (cnbytes <= want_bytes) break;
  }
  return cnbytes;
}

static sqlres do_agent_cache_reap(UInt64 cnbytes, UInt64 want_bytes,
                                  jv *klrus) {
  LT("do_agent_cache_reap");
  vector<string> toe;
  UInt64 ocnbytes = cnbytes;
  cnbytes         = analyze_cache_for_reap(cnbytes, want_bytes, klrus, &toe);
  if (!do_reap_evict(&toe)) RETURN_SQL_RESULT_ERROR("do_reap_evict");
  Int64 diff      = (cnbytes - ocnbytes);
  LD("POST EVICTION: C(#B): " << cnbytes << " D: " << diff);
  return increment_agent_num_cache_bytes(diff);
}

static sqlres start_agent_cache_reaper(UInt64 cnbytes) {
  LT("start_agent_cache_reaper");
  UInt64  max_bytes  = CacheMaxBytes;
  UInt64  want_bytes = (UInt64)(max_bytes * 0.8);
  sqlres  sr         = fetch_all_lru_keys();
  RETURN_SQL_ERROR(sr)
  jv     *klrus      = &(sr.jres);
  return do_agent_cache_reap(cnbytes, want_bytes, klrus);
}

static void signal_agent_cache_reaper(UInt64 cnbytes) {
  LT("signal_agent_cache_reaper");
  sqlres sr = lock_cache_reaper();
  if (sr.err.size()) return;
  bool   ok = sr.ures;
  if (!ok) return;
  sr        = start_agent_cache_reaper(cnbytes);
  if (sr.err.size()) LE("ERROR: run_agent_cache_reaper_thread: " << sr.err);
  unlock_cache_reaper();
}

static void check_agent_cache_limits(UInt64 cnbytes) {
  if (!CacheMaxBytes) return;
  UInt64 max_bytes = CacheMaxBytes;
  if (cnbytes > max_bytes) {
    LE("THRESHOLD: C(#B): " << cnbytes << " MAX(#B): " << max_bytes);
    signal_agent_cache_reaper(cnbytes);
  } else {
    LD("OK: cache(#b): " << cnbytes << " MAX(#B): " << max_bytes);
  }
}

// NOTE: LRU FIELD "WHEN" IS REQUIRED -> used in ZAS.do_sync_missing_key
sqlres zcreap_set_lru_local_read(string &kqk) {
  UInt64 ts = zh_get_ms_time();
  return persist_lru_key_fields(kqk, {"WHEN", "LOCAL_READ"}, {ts, ts});
}

// NOTE: LRU FIELD "WHEN" IS REQUIRED -> used in ZAS.do_sync_missing_key
static sqlres store_modification(string &kqk,   bool selfie, bool cached,
                                 UInt64 nbytes, bool mod) {
  UInt64 ts = zh_get_ms_time();
  return persist_lru_key_fields(kqk, {"WHEN", "CACHED", "NUM_BYTES",
                                      "LOCAL_MODIFICATION",
                                      "EXTERNAL_MODIFICATION"},
                                     {ts, (UInt64)cached, nbytes,
                                      (UInt64)mod, (UInt64)mod});
}

UInt64 zcreap_get_ocrdt_num_bytes(jv *md) {
  bool    oexists  = zh_jv_is_non_null_member(md, "ocrdt");
  UInt64  o_nbytes = 0; // NO OLD-CRDT BY DEFAULT
  if (oexists) {
    if (zh_jv_is_member(&((*md)["ocrdt"]["_meta"]), "num_bytes")) {
      o_nbytes = (*md)["ocrdt"]["_meta"]["num_bytes"].asUInt64();
    }
  }
  return o_nbytes;
}

sqlres zcreap_store_num_bytes(jv *pc, UInt64 o_nbytes, bool selfie) {
  LT("zcreap_store_num_bytes");
  jv     *jks      = &((*pc)["ks"]);
  string  kqk      = (*jks)["kqk"].asString();
  sqlres  sr       = fetch_cached_keys(kqk);
  RETURN_SQL_ERROR(sr)
  bool    cached   = sr.ures ? true : false;
  bool    nexists  = zh_jv_is_non_null_member(pc, "ncrdt");
  if (!nexists) {
    if (!zh_jv_is_non_null_member(pc, "remove")) {
      LD("ZCR.StoreNumBytes: NOT REMOVE & NO NCRDT -> SKIP");
      RETURN_EMPTY_SQLRES
    }
  }
  UInt64  n_nbytes = 0; // REMOVE by default
  UInt64  mod      = 0; // REMOVE by default
  if (nexists) {
    if (zh_jv_is_member(&((*pc)["ncrdt"]["_meta"]), "num_bytes")) {
      n_nbytes = (*pc)["ncrdt"]["_meta"]["num_bytes"].asUInt64();
    }
    if (selfie) {
      mod = (*pc)["ncrdt"]["_meta"]["@"].asUInt64();
    } else {
      mod = (*pc)["ncrdt"]["_meta"]["created"][MyDataCenterUUID].asUInt64();
    }
  }
  LD("ZCR.StoreNumBytes: cached : " << cached << " mod: " << mod <<
     " o_nbytes: " << o_nbytes << " n_nbytes: " << n_nbytes);
  sr = store_modification(kqk, selfie, cached, n_nbytes, mod);
  RETURN_SQL_ERROR(sr)
  if (!cached)                   RETURN_EMPTY_SQLRES
  else if (o_nbytes == n_nbytes) RETURN_EMPTY_SQLRES
  else {
    Int64  diff    = (n_nbytes - o_nbytes);
    sqlres sr      = increment_agent_num_cache_bytes(diff);
    RETURN_SQL_ERROR(sr)
    UInt64 cnbytes = sr.ures;
    LD("DIFF: " << diff << " C(#B): " << cnbytes);
    check_agent_cache_limits(cnbytes);
    RETURN_EMPTY_SQLRES
  }
}

