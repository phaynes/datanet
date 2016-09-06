#include <cstdio>
#include <cstring>
#include <string>
#include <sstream>  
#include <iostream>

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
#include "shared.h"
#include "merge.h"
#include "gc.h"
#include "datastore.h"
#include "trace.h"
#include "storage.h"

using namespace std;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STORE DELTA HELPERS -------------------------------------------------------

static sqlres store_last_remove_delta(string kqk, jv *jdentry) {
  jv   *jmeta  = &((*jdentry)["delta"]["_meta"]);
  bool  remove = zh_get_bool_member(jmeta, "remove");
  if (!remove) RETURN_EMPTY_SQLRES
  else {
    LD("persist_last_remove_delta: K: " << kqk);
    return persist_last_remove_delta(kqk, jmeta);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT-SIDE AGENT DELTA ----------------------------------------------------

sqlres zds_remove_agent_delta(string kqk, string avrsn) {
  return remove_delta(kqk, avrsn);
}

sqlres zds_store_agent_delta(string kqk, jv *jdentry, jv *jauth) {
  jv     *jmeta = &((*jdentry)["delta"]["_meta"]);
  string  avrsn = (*jmeta)["author"]["agent_version"].asString();
  LD("zds_store_agent_delta: K: " << kqk << " AV: " << avrsn);
  sqlres sd;
  PERSIST_DELTA(kqk, avrsn, jdentry, jauth)
  RETURN_SQL_ERROR(sd)
  return store_last_remove_delta(kqk, jdentry);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT-SIDE SUBSCRIBER DELTA -----------------------------------------------

sqlres zds_remove_subscriber_delta(string kqk, jv *jauthor) {
  string avrsn = (*jauthor)["agent_version"].asString();
  LD("zds_remove_subscriber_delta: K: " << kqk << " AV: " << avrsn);
  return remove_delta(kqk, avrsn);
}

sqlres zds_store_subscriber_delta(string kqk, jv *jdentry) {
  jv     *jauthor = &((*jdentry)["delta"]["_meta"]["author"]);
  string  avrsn   = (*jauthor)["agent_version"].asString();
  LD("zds_store_subscriber_delta: K: " << kqk << " AV: " << avrsn);
  // NOTE: update() -> AUTH not overwritten
  sqlres sd;
  UPDATE_DELTA(kqk, avrsn, jdentry);
  RETURN_SQL_ERROR(sd)
  return store_last_remove_delta(kqk, jdentry);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CRDTS ---------------------------------------------------------------------

static jv remove_rchans(jv *dest, jv *src) {
  map<string, bool> dexists;
  for (uint i = 0; i < dest->size(); i++) {
    string k   = (*dest)[i].asString();
    bool   isi = (*dest)[i].isInt();
    dexists.insert(make_pair(k, isi));
  }
  for (uint i = 0; i < src->size(); i++) {
    string k = (*src)[i].asString();
    map<string, bool>::iterator it = dexists.find(k);
    if (it != dexists.end()) dexists.erase(it);
  }
  jv res = JARRAY;
  for (map<string, bool>::iterator it  = dexists.begin();
                                   it != dexists.end(); it++) {
    string k   = it->first;
    bool   isi = it->second;
    if (!isi) {
      jv jk = k;
      zh_jv_append(&res, &jk);
    } else {
      Int64 i  = strtol(k.c_str(), NULL, 10);
      jv    ji = i;
      zh_jv_append(&res, &ji);
    }
  }
  return res;
}

void zds_remove_channels(jv *jmeta) {
  jv res = remove_rchans(&((*jmeta)["replication_channels"]),
                         &((*jmeta)["removed_channels"]));
  (*jmeta)["replication_channels"] = res;
  jmeta->removeMember("removed_channels");
}

static void cleanup_crdt_before_store(jv *jcrdt, string &kqk) {
  jv *jmeta = &((*jcrdt)["_meta"]);
  jmeta->removeMember("last_member_count");
  jmeta->removeMember("remove");
  jmeta->removeMember("auto_cache");
  jmeta->removeMember("author");
  jmeta->removeMember("dependencies");
  jmeta->removeMember("delta_bytes");
  jmeta->removeMember("xaction");
  jmeta->removeMember("OOO_GCV");
  jmeta->removeMember("from_central");
  jmeta->removeMember("dirty_central");
  jmeta->removeMember("DO_GC");
  jmeta->removeMember("DO_DS");
  jmeta->removeMember("DO_REORDER");
  jmeta->removeMember("DO_REAP");
  jmeta->removeMember("reap_gc_version");
  jmeta->removeMember("DO_IGNORE");
  jmeta->removeMember("AUTO");
  jmeta->removeMember("reference_uuid");
  jmeta->removeMember("reference_version");
  jmeta->removeMember("reference_ignore");
  if (zh_jv_is_member(jmeta, "removed_channels")) zds_remove_channels(jmeta);
  (*jcrdt)["_id"] = kqk; // NOTE json._id !== crdt._id
}

sqlres zds_store_crdt(string kqk, jv *jcrdt) {
  LT("zds_store_crdt: K: " << kqk);
  zmerge_debug_crdt(jcrdt);
  cleanup_crdt_before_store(jcrdt, kqk);
  jv     *jrchans = &((*jcrdt)["_meta"]["replication_channels"]);
  sqlres  sr      = persist_key_repchans(kqk, jrchans);
  RETURN_SQL_ERROR(sr)
  sqlres sc;
  PERSIST_CRDT(kqk, jcrdt);
  return sc;
}

sqlres zds_retrieve_crdt(string kqk) {
  sqlres  sc;
  FETCH_CRDT(kqk);
  RETURN_SQL_ERROR(sc)
  jv     *jcrdt = &(sc.jres);
  if (zjn(jcrdt)) RETURN_EMPTY_SQLRES
  else {
    jv     *jcmeta     = &((*jcrdt)["_meta"]);
    UInt64  expiration = zh_get_uint64_member(jcmeta, "expiration");
    if (!expiration) return sc;
    else {
      UInt64 nows = (zh_get_ms_time() / 1000);
      if (expiration > nows) return sc; // NOT YET EXPIRED -> OK
      else {
        LE("LOCAL-RETRIEVE-EXPIRATION: K: " << kqk << " E: " << expiration <<
             " NOW: " << nows);
        RETURN_EMPTY_SQLRES
      }
    }
  }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REMOVE KEY ----------------------------------------------------------------

sqlres zds_remove_key(string kqk) { LT("zds_remove_key");
  sqlres sr = remove_key_info(kqk);
  RETURN_SQL_ERROR(sr)
  sr        =  remove_crdt(kqk);
  RETURN_SQL_ERROR(sr)
  return zgc_remove_gcv_summaries(kqk);
}

