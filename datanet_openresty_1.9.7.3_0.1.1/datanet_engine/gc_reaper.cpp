#include <cstdio>
#include <cstring>
#include <string>
#include <sstream>  
#include <iostream>
#include <vector>

extern "C" {
  #include <time.h>
  #include <sys/time.h>
  #include <sys/types.h>
  #include <sys/un.h>
  #include <stdio.h>
  #include <unistd.h> 
  #include <stdlib.h> 
}

#include "json/json.h"
#include "easylogging++.h"

#include "helper.h"
#include "subscriber_delta.h"
#include "gc.h"
#include "gc_reaper.h"
#include "storage.h"

using namespace std;

extern jv zh_nobody_auth;
extern bool DebugDaemons;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT SETTINGS ------------------------------------------------------------

UInt64 AgentMaxNumberGCVPerKey       = 2;
UInt64 AgentMaxNumberTombstonePerKey = 0;
UInt64 AgentMaxGCVStaleness          = 0;
// Agents must [FETCH->MODIFY->COMMIT] within (AgentMinGCVStaleness) 5 SECS
UInt64 AgentMinGCVStaleness          = 5000;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

// NOTE: Sort to kqk, then gcn
static bool cmp_kqk_gcv(jv *a_gcv, jv *b_gcv) {
  string a_kqk = (*a_gcv)["kqk"].asString();
  UInt64 a_gcn = (*a_gcv)["gcv"].asUInt64();
  string b_kqk = (*b_gcv)["kqk"].asString();
  UInt64 b_gcn = (*b_gcv)["gcv"].asUInt64();
  if (a_kqk.compare(b_kqk)) {
    return (a_kqk.compare(b_kqk) > 1) ? false : true;
  } else {
    if (a_gcn == b_gcn) zh_fatal_error("LOGIC(cmp_kqk_gcv)");
    return (a_gcn < b_gcn);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PURGE ENGINE --------------------------------------------------------------

static sqlres do_agent_gcv_reap(string kqk, UInt64 gcv) {
  sqlres sr   = fetch_gc_version(kqk);
  RETURN_SQL_ERROR(sr)
  UInt64 cgcv = sr.ures;
  if (gcv >= cgcv) {
    LE("FAIL: REAP: PURGE-GCV: " << gcv << " >= (C)GCV: " << cgcv);
    RETURN_EMPTY_SQLRES
  } else {
    jv     md;
    sqlres sr     = zsd_get_agent_sync_status(kqk, &md);
    RETURN_SQL_ERROR(sr)
    bool   tosync = zh_get_bool_member(&md, "to_sync_key");
    bool   oosync = zh_get_bool_member(&md, "out_of_sync_key");
    bool   ok     = (!tosync && !oosync);
    if (!ok) {
      LE("FAIL: REAP ON NOT IN-SYNC-KEY: K: " << kqk);
      RETURN_EMPTY_SQLRES
    } else {
      LD("do_agent_gcv_reap: K: " << kqk << " GCV: " << gcv);
      return zgc_remove_gcv_summary(kqk, gcv);
    }
  }
}

static sqlres purge_single_gcv_summ(string kqk, UInt64 gcv) {
  return do_agent_gcv_reap(kqk, gcv);
}

static sqlres purge_gcv_summaries(string kqk, jv *jgcvs) {
  for (uint i = 0; i < jgcvs->size(); i++) {
    UInt64  gcv = (*jgcvs)[i]["gcv"].asUInt64();
    LD("purge_gcv: REMOVE: K: " << kqk << " GCV: " << gcv);
    sqlres  sr  = purge_single_gcv_summ(kqk, gcv);
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}

static sqlres purge_gcv_keys(map <string, vector<jv>> *purge_kqk) {
  for (map<string, vector<jv>>::iterator it =  purge_kqk->begin();
                                         it != purge_kqk->end(); it++) {
    string     kqk   = it->first;
    vector<jv> gcvs  = it->second;
    jv         jgcvs = JARRAY;
    for (uint i = 0; i < gcvs.size(); i++) {
      jv *gcv = &(gcvs[i]);
      zh_jv_append(&jgcvs, gcv);
    }
    sqlres sr = purge_gcv_summaries(kqk, &jgcvs);
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PURGE SELECTION ALGORITHM -------------------------------------------------

static UInt64 add_purge_num_tombstones(map <string, vector<jv>> *purge_kqk,
                                       map <string, UInt64>     *ntmbs_kqk,
                                       map <string, vector<jv>> *gcv_kqk,
                                       UInt64                    max_num_tmbs) {
  UInt64 ndlts = 0;
  if (!max_num_tmbs) return ndlts;
  for (map<string, UInt64>::iterator it =  ntmbs_kqk->begin();
                                     it != ntmbs_kqk->end(); it++) {
    string kqk   = it->first;
    UInt64 ntmbs = it->second;
    if (ntmbs > max_num_tmbs) {
      UInt64     diff = ntmbs - max_num_tmbs;
      vector<jv> gcvs = (*gcv_kqk)[kqk];
      UInt64     cnt  = 0;
      for (uint i = 0; i < gcvs.size(); i++) {
        jv     *gcv   = &(gcvs[i]);
        UInt64  ntmb  = (*gcv)["num_tombstones"].asUInt64();
        cnt          += ntmb;
        if (cnt > diff) break;
        zh_push_gcv_on_vector_jv_map(purge_kqk, kqk, gcv);
        ndlts += 1;
      }
    }
  }
  return ndlts;

}

static UInt64 add_purge_num_gcv(map <string, vector<jv>> *purge_kqk,
                                map <string, vector<jv>> *gcv_kqk,
                                UInt64 max_num_gcv) {
  UInt64 ndlts = 0;
  for (map<string, vector<jv>>::iterator it =  gcv_kqk->begin();
                                         it != gcv_kqk->end(); it++) {
    string kqk = it->first;
    map<string, vector<jv>>::iterator fit = purge_kqk->find(kqk);
    if (fit != purge_kqk->end()) continue; // ntmbs_kqk has precedence
    vector<jv> gcvs = it->second;
    if (gcvs.size() > max_num_gcv) {
      vector<jv>  kpurge;
      Int64       dlen = gcvs.size() - max_num_gcv;
      for (uint i = 0; i < dlen; i++) {
        kpurge.push_back(gcvs[i]);
      }
      purge_kqk->insert(make_pair(kqk, kpurge));
      ndlts += kpurge.size();
    }
  }
  return ndlts;
}

static UInt64 add_purge_stale(map <string, vector<jv>> *purge_kqk,
                              map <string, vector<jv>> *stale_kqk) {
  UInt64 ndlts = 0;
  for (map<string, vector<jv>>::iterator it =  stale_kqk->begin();
                                         it != stale_kqk->end(); it++) {
    string kqk = it->first;
    map<string, vector<jv>>::iterator fit = purge_kqk->find(kqk);
    if (fit != purge_kqk->end()) continue;// ntmbs_kqk & gcv_kqk have precedence
    vector<jv> gcvs = it->second;
    purge_kqk->insert(make_pair(kqk, gcvs));
    ndlts += gcvs.size();
  }
  return ndlts;
}

static UInt64 remove_purge_min_stale(map <string, vector<jv>> *purge_kqk,
                                     UInt64 now, UInt64 min_stale) {
  UInt64 ndlts = 0;
  for (map<string, vector<jv>>::iterator it =  purge_kqk->begin();
                                         it != purge_kqk->end(); it++) {
    string      kqk  = it->first;
    vector<jv>  gcvs = it->second;
    vector<int> tor;
    for (uint i = 0; i < gcvs.size(); i++) {
      jv     jgcv  = gcvs[i];
      UInt64 gwhen = jgcv["when"].asUInt64();
      UInt64 tdiff = now - gwhen;
      if (tdiff < min_stale) {
        LD("remove_purge_min_stale: GID: " << jgcv["_id"] <<
           " tdiff: " << tdiff);
        tor.push_back(i);
      }
    }
    for (int i = (int)(tor.size() - 1); i >= 0; i--) {
      gcvs.erase(gcvs.begin() + tor[i]);
      ndlts += 1;
    }
    if (gcvs.size() == 0) purge_kqk->erase(kqk);
  }
  return ndlts;
}

static sqlres do_gc_purge_reaper(UInt64 max_num_gcv, UInt64 max_num_tmbs,
                                 UInt64 max_stale,   UInt64 min_stale) {
  if (DebugDaemons) LD("do_gc_purge_reaper");
  sqlres  sr    = fetch_all_gcv_summaries();
  RETURN_SQL_ERROR(sr)
  jv     *jgcvs = &(sr.jres);
  if (!jgcvs->size()) {
    if (DebugDaemons) LD("GC_PURGE_REAPER: ZERO GCV(KEYS)");
    RETURN_EMPTY_SQLRES
  }
  zh_jv_sort(jgcvs, cmp_kqk_gcv);
  map <string, vector<jv>> gcv_kqk;
  map <string, UInt64>     ntmbs_kqk;
  map <string, vector<jv>> stale_kqk;
  UInt64 now = zh_get_ms_time();
  for (uint i = 0; i < jgcvs->size(); i++) {
    jv     *jgcv = &((*jgcvs)[i]);
    string  kqk  = (*jgcv)["kqk"].asString();
    if (zh_is_key_local_to_worker(kqk)) {
      zh_push_gcv_on_vector_jv_map(&gcv_kqk, kqk, jgcv);
      {
        UInt64 ntmbs = (*jgcv)["num_tombstones"].asUInt64();
        map <string, UInt64>::iterator it = ntmbs_kqk.find(kqk);
        if (it == ntmbs_kqk.end()) {
          ntmbs_kqk.insert(make_pair(kqk, ntmbs));
        } else {
          UInt64 o_ntmbs = it->second;
          ntmbs_kqk.insert(make_pair(kqk, (o_ntmbs + ntmbs)));
        }
      }
      if (max_stale) {
        UInt64 gwhen = (*jgcv)["when"].asUInt64();
        UInt64 tdiff = now - gwhen;
        if (tdiff > max_stale) {
          zh_push_gcv_on_vector_jv_map(&stale_kqk, kqk, jgcv);
        }
      }
    }
  }
  map <string, vector<jv>> purge_kqk;
  UInt64 ndlts = 0;
  ndlts += add_purge_num_tombstones(&purge_kqk, &ntmbs_kqk, &gcv_kqk,
                                    max_num_tmbs);
  ndlts += add_purge_num_gcv(&purge_kqk, &gcv_kqk, max_num_gcv);
  ndlts += add_purge_stale(&purge_kqk, &stale_kqk);
  ndlts -= remove_purge_min_stale(&purge_kqk, now, min_stale);
  LD("GC_PURGE_REAPER: #K: " << purge_kqk.size() << " #Ds: " << ndlts);
  return purge_gcv_keys(&purge_kqk);
}

void do_gcv_summary_reaper() {
  if (DebugDaemons) LD("START: do_gcv_summary_reaper");
  sqlres sr      = zh_get_agent_offline();
  if (sr.err.size()) return;
  bool   offline = sr.ures ? true : false;
  if (offline) return;
  UInt64 max_num_gcv  = AgentMaxNumberGCVPerKey;
  UInt64 max_num_tmbs = AgentMaxNumberTombstonePerKey;
  UInt64 max_stale    = AgentMaxGCVStaleness;
  UInt64 min_stale    = AgentMinGCVStaleness;
  do_gc_purge_reaper(max_num_gcv, max_num_tmbs, max_stale, min_stale);
  if (DebugDaemons) LD("FINISH: do_gcv_summary_reaper");
}


