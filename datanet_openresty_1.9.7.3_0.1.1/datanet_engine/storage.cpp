#include <cstdio>
#include <cstring>
#include <string>
#include <sstream>  
#include <iostream>
#include <vector>
#include <queue>

extern "C" {
  #include <stdio.h>
  #include <unistd.h> 
  #include <stdlib.h> 
}

#include <sqlite3.h>

#include "json/json.h"

#include "easylogging++.h"

#include "shared.h"
#include "storage.h"

using namespace std;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EXTERNS -------------------------------------------------------------------

extern Int64  MyUUID;
extern string MyDataCenterUUID;
extern bool   Isolation;

extern bool   DisableFixlog;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GLOBALS -------------------------------------------------------------------

bool   StorageInitialized = false;

UInt64 DeltasMaxBytes     = 0;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEBUG OVERRIDES -----------------------------------------------------------

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
// HELPERS -------------------------------------------------------------------

void zstorage_cleanup_crdt_before_persist(jv *jcrdt) {
  LT("zstorage_cleanup_crdt_before_persist");
  if (zjd(jcrdt)) {
    jv *jmeta = &((*jcrdt)["_meta"]);
    jmeta->removeMember("last_member_count");
    jmeta->removeMember("remove");
    jmeta->removeMember("expire");
    jmeta->removeMember("auto_cache");
    jmeta->removeMember("author");
    jmeta->removeMember("dependencies");
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
  }
}

static string get_author(jv *jauthor) {
  string auuid = (*jauthor)["agent_uuid"].asString();
  string avrsn = (*jauthor)["agent_version"].asString();
  return auuid + "-" + avrsn;
}

static string get_ks_author(string &kqk, jv *jauthor) {
  return kqk + "-" + get_author(jauthor);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PRIMARY KEY CLASS ---------------------------------------------------------

PrimaryKey::PrimaryKey() {
}

PrimaryKey::PrimaryKey(Int64 i) {
  _pk = std::to_string(i);
}

PrimaryKey::PrimaryKey(UInt64 u) {
  _pk = std::to_string(u);
}

PrimaryKey::PrimaryKey(string &s) {
  _pk = s;
}

string& PrimaryKey::to_string() {
  return _pk;
}



// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STORAGE VALUE CLASS -------------------------------------------------------

StorageValue::StorageValue() {
  _val = "NULL";
}

StorageValue::StorageValue(Int64 i) {
  _raw = i;
  _val = std::to_string(i);
}

StorageValue::StorageValue(UInt64 u) {
  _raw = u;
  _val = std::to_string(u);
}

StorageValue::StorageValue(string &s) {
  _raw = s;
  _val = "'" + s + "'";
}

StorageValue::StorageValue(jv *jval) {
  _raw = *jval;
  _val = "'" + zh_convert_jv_to_string(jval) + "'";
}

jv StorageValue::raw() {
  return _raw;
};

string StorageValue::to_string() {
  return _val;
};


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STORAGE CONDITION CLASS ---------------------------------------------------

vector<string> StorageCompOperators = {"=", "<=", ",="};

StorageCondition::StorageCondition(string key_name, string &key_value) {
  _knames = {key_name};
  _kvals  = {"'" + key_value + "'"};
  _kivals = {0};
  _kscts  = {STORAGE_COND_EQ};
}

StorageCondition::StorageCondition(string key_name, Int64 key_value) {
  _knames = {key_name};
  _kvals  = {std::to_string(key_value)};
  _kivals = {key_value};
  _kscts  = {STORAGE_COND_EQ};
}

StorageCondition::StorageCondition(string key_name, Int64 key_value,
                                   storage_cmp_type sct) {
  _knames = {key_name};
  _kvals  = {std::to_string(key_value)};
  _kivals = {key_value};
  _kscts  = {sct};
}

void StorageCondition::add(string key_name, string &key_value) {
  _knames.push_back(key_name);
  _kvals.push_back ("'" + key_value + "'");
  _kivals.push_back(0);
  _kscts.push_back (STORAGE_COND_EQ);
}

void StorageCondition::add(string key_name, Int64 key_value) {
  _knames.push_back(key_name);
  _kvals.push_back (std::to_string(key_value));
  _kivals.push_back(key_value);
  _kscts.push_back (STORAGE_COND_EQ);
}

void StorageCondition::add(string key_name, Int64 key_value,
                           storage_cmp_type sct) {
  _knames.push_back(key_name);
  _kvals.push_back (std::to_string(key_value));
  _kivals.push_back(key_value);
  _kscts.push_back (sct);
}

string StorageCondition::to_string() {
  string                             ret;
  bool                               first = true;
  vector<string>::iterator           itn   = _knames.begin();
  vector<storage_cmp_type>::iterator itc   = _kscts.begin();
  vector<string>::iterator           itv   = _kvals.begin();
  for (; itn != _knames.end() && itv != _kvals.end() && itc != _kscts.end();
       itn++, itv++, itc++) {
    if (!first) ret += " AND ";
    ret   += *itn + " " + StorageCompOperators[*itc] + " " + *itv;
    first  = false;
  }
  return ret;
}

SingleStorageCondition StorageCondition::get(unsigned int i) {
  SingleStorageCondition sc;
  if (i < _knames.size()) {
    sc.key       = _knames[i];
    sc.value     = _kvals[i];
    sc.ivalue    = _kivals[i];
    sc.condition = _kscts[i];
  }
  return sc;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PRIMARY-KEY-TABLE ---------------------------------------------------------

map<string, string> TablePrimaryKeyMap;
map<string, string> TableSimpleValueMap;
map<string, bool>   TableSharedMap;

void storage_populate_table_primary_key_map() {
  TablePrimaryKeyMap["AGENT_PROCESSED"]                 = "REFERENCE";
  TablePrimaryKeyMap["NUM_WORKERS"]                     = "REFERENCE";
  TablePrimaryKeyMap["AGENT_HEARTBEAT"]                 = "REFERENCE";
  TablePrimaryKeyMap["AGENT_INFO"]                      = "REFERENCE";
  TablePrimaryKeyMap["CONNECTION_STATUS"]               = "AGENT_UUID";
  TablePrimaryKeyMap["AGENT_CREATEDS"]                  = "DATACENTER_UUID";
  TablePrimaryKeyMap["DEVICE_KEY"]                      = "REFERENCE";
  TablePrimaryKeyMap["NOTIFY"]                          = "REFERENCE";
  TablePrimaryKeyMap["NOTIFY_URL"]                      = "URL";

  TablePrimaryKeyMap["WORKER_PARTITIONS"]               = "PID";

  TablePrimaryKeyMap["REPLICATION_CHANNELS"]            = "CHANNEL";
  TablePrimaryKeyMap["DEVICE_CHANNELS"]                 = "CHANNEL";

  TablePrimaryKeyMap["STATIONED_USERS"]                 = "USERNAME";

  TablePrimaryKeyMap["CACHED_KEYS"]                     = "KQK";
  TablePrimaryKeyMap["TO_EVICT_KEYS"]                   = "KQK";
  TablePrimaryKeyMap["EVICTED_KEYS"]                    = "KQK";
  TablePrimaryKeyMap["OUT_OF_SYNC_KEYS"]                = "KQK";
  TablePrimaryKeyMap["AGENT_WATCH_KEYS"]                = "KQK";
  TablePrimaryKeyMap["AGENT_KEY_GC_WAIT_INCOMPLETE"]    = "KQK";

  TablePrimaryKeyMap["OOO_DELTA"]                       = "DELTA_KEY";
  TablePrimaryKeyMap["DELTA_COMMITTED"]                 = "DELTA_KEY";
  TablePrimaryKeyMap["REMOVE_REFERENCE_DELTA"]          = "DELTA_KEY";

  TablePrimaryKeyMap["KEY_GC_VERSION"]                  = "KQK";
  TablePrimaryKeyMap["AGENT_KEY_MAX_GC_VERSION"]        = "KQK";
  TablePrimaryKeyMap["DIRTY_KEYS"]                      = "KQK";
  TablePrimaryKeyMap["OOO_KEY"]                         = "KQK";

  TablePrimaryKeyMap["AGENT_SENT_DELTAS"]               = "DELTA_KEY";
  TablePrimaryKeyMap["DIRTY_DELTAS"]                    = "DELTA_KEY";

  TablePrimaryKeyMap["AGENT_KEY_OOO_GCV"]               = "KA_KEY";
  TablePrimaryKeyMap["AGENT_DELTA_OOO_GCV"]             = "KAV_KEY";
  TablePrimaryKeyMap["PERSISTED_SUBSCRIBER_DELTA"]      = "KAV_KEY";

  TablePrimaryKeyMap["KEY_AGENT_VERSION"]               = "KQK";
  TablePrimaryKeyMap["TO_SYNC_KEYS"]                    = "KQK";
  TablePrimaryKeyMap["NEED_MERGE_REQUEST_ID"]           = "KQK";
  TablePrimaryKeyMap["KEY_INFO"]                        = "KQK";
  TablePrimaryKeyMap["LRU_KEYS"]                        = "KQK";

  TablePrimaryKeyMap["USER_AUTHENTICATION"]             = "USERNAME";

  TablePrimaryKeyMap["DEVICE_SUBSCRIBER_VERSION"]       = "KA_KEY";

  TablePrimaryKeyMap["DOCUMENTS"]                       = "KQK";
  TablePrimaryKeyMap["LAST_REMOVE_DENTRY"]              = "KQK";
  TablePrimaryKeyMap["USER_CACHED_KEYS"]                = "KQK";
  TablePrimaryKeyMap["PER_KEY_SUBSCRIBER_VERSION"]      = "KQK";
  TablePrimaryKeyMap["OOO_DELTA_VERSIONS"]              = "KQK";
  TablePrimaryKeyMap["DELTA_VERSIONS"]                  = "KQK";
  TablePrimaryKeyMap["KEY_GC_SUMMARY_VERSIONS"]         = "KQK";
  TablePrimaryKeyMap["AGENT_KEY_GCV_NEEDS_REORDER"]     = "KQK";

  TablePrimaryKeyMap["USER_CHANNEL_PERMISSIONS"]        = "USERNAME";
  TablePrimaryKeyMap["USER_CHANNEL_SUBSCRIPTIONS"]      = "USERNAME";

  TablePrimaryKeyMap["DELTAS"]                          = "DELTA_KEY";
  TablePrimaryKeyMap["DELTA_NEED_REFERENCE"]            = "DELTA_KEY";
  TablePrimaryKeyMap["DELTA_NEED_REORDER"]              = "DELTA_KEY";

  TablePrimaryKeyMap["GCV_SUMMARIES"]                   = "G_KEY";

  TablePrimaryKeyMap["FIXLOG"]                          = "FID";
  TablePrimaryKeyMap["SUBSCRIBER_LATENCY_HISTOGRAM"]    = "DATACENTER_NAME";

  TablePrimaryKeyMap["WORKERS"]                         = "UUID";
  TablePrimaryKeyMap["LOCKS"]                           = "NAME";


  TableSimpleValueMap["AGENT_PROCESSED"]              = "COUNT";
  TableSimpleValueMap["NUM_WORKERS"]                  = "COUNT";
  TableSimpleValueMap["AGENT_CREATEDS"]               = "CREATED";
  TableSimpleValueMap["NOTIFY"]                       = "VALUE";
  TableSimpleValueMap["NOTIFY_URL"]                   = "VALUE";
  TableSimpleValueMap["WORKER_PARTITIONS"]            = "PARTITION";
  TableSimpleValueMap["REPLICATION_CHANNELS"]         = "NUM_SUBSCRIPTIONS";
  TableSimpleValueMap["DEVICE_CHANNELS"]              = "NUM_SUBSCRIPTIONS";
  TableSimpleValueMap["STATIONED_USERS"]              = "VALUE";
  TableSimpleValueMap["CACHED_KEYS"]                  = "VALUE";
  TableSimpleValueMap["TO_EVICT_KEYS"]                = "VALUE";
  TableSimpleValueMap["EVICTED_KEYS"]                 = "VALUE";
  TableSimpleValueMap["OUT_OF_SYNC_KEYS"]             = "VALUE";
  TableSimpleValueMap["AGENT_WATCH_KEYS"]             = "VALUE";
  TableSimpleValueMap["AGENT_KEY_GC_WAIT_INCOMPLETE"] = "VALUE";
  TableSimpleValueMap["OOO_DELTA"]                    = "VALUE";
  TableSimpleValueMap["DELTA_COMMITTED"]              = "VALUE";
  TableSimpleValueMap["REMOVE_REFERENCE_DELTA"]       = "VALUE";
  TableSimpleValueMap["KEY_GC_VERSION"]               = "GC_VERSION";
  TableSimpleValueMap["AGENT_KEY_MAX_GC_VERSION"]     = "GC_VERSION";
  TableSimpleValueMap["DIRTY_KEYS"]                   = "NUM_DIRTY";
  TableSimpleValueMap["OOO_KEY"]                      = "COUNT";
  TableSimpleValueMap["AGENT_SENT_DELTAS"]            = "TS";
  TableSimpleValueMap["DIRTY_DELTAS"]                 = "TS";
  TableSimpleValueMap["AGENT_KEY_OOO_GCV"]            = "VALUE";
  TableSimpleValueMap["AGENT_DELTA_OOO_GCV"]          = "VALUE";
  TableSimpleValueMap["PERSISTED_SUBSCRIBER_DELTA"]   = "VALUE";

  TableSharedMap["AGENT_PROCESSED"]              = true;
  TableSharedMap["NUM_WORKERS"]                  = true;
  TableSharedMap["AGENT_HEARTBEAT"]              = true;
  TableSharedMap["AGENT_INFO"]                   = true;
  TableSharedMap["CONNECTION_STATUS"]            = true;
  TableSharedMap["AGENT_CREATEDS"]               = true;
  TableSharedMap["DEVICE_KEY"]                   = true;
  TableSharedMap["NOTIFY"]                       = true;
  TableSharedMap["NOTIFY_URL"]                   = true;
  TableSharedMap["WORKER_PARTITIONS"]            = true;
  TableSharedMap["REPLICATION_CHANNELS"]         = true;
  TableSharedMap["DEVICE_CHANNELS"]              = true;
  TableSharedMap["STATIONED_USERS"]              = true;
  TableSharedMap["NEED_MERGE_REQUEST_ID"]        = true;
  TableSharedMap["USER_AUTHENTICATION"]          = true;
  TableSharedMap["USER_CHANNEL_PERMISSIONS"]     = true;
  TableSharedMap["USER_CHANNEL_SUBSCRIPTIONS"]   = true;
  // NOTE: KEY_AGENT_VERSION: SURVIVE PROCESS DEATH
  TableSharedMap["KEY_AGENT_VERSION"]            = true;
  TableSharedMap["SUBSCRIBER_LATENCY_HISTOGRAM"] = true;
  TableSharedMap["WORKERS"]                      = true;
  TableSharedMap["LOCKS"]                        = true;

  LE("POPULATED TablePrimaryKeyMap");
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DAEMON LOCKS --------------------------------------------------------------

sqlres lock_cache_reaper() {
  string           tname = "LOCKS";
  string           name  = "CACHE_REAPER";
  PrimaryKey       pk(name);
  StorageCondition cond("NAME", name);
  return lock_field(tname, pk, cond);
}

sqlres unlock_cache_reaper() {
  string           tname = "LOCKS";
  string           name  = "CACHE_REAPER";
  PrimaryKey       pk(name);
  StorageCondition cond("NAME", name);
  return unlock_field(tname, pk, cond);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PERSIST/FETCH DATA --------------------------------------------------------

sqlres fetch_all_fixlog() { LT("fetch_all_fixlog");
  if (DisableFixlog) RETURN_EMPTY_SQLRES
  string         tname  = "FIXLOG";
  vector<string> fields = {"FID", "PC"};
  return scan_fetch_fields_json_map(tname, fields);
}

sqlres fetch_all_worker_uuid() { LT("fetch_all_worker_uuid");
  string         tname  = "WORKERS";
  vector<string> fields = {"UUID", "PORT"};
  return scan_fetch_fields_json_map(tname, fields);
}

sqlres fetch_all_agent_createds() { LT("fetch_all_agent_createds");
  string         tname  = "AGENT_CREATEDS";
  vector<string> fields = {"DATACENTER_UUID", "CREATED"};
  return scan_fetch_fields_json_map(tname, fields);
}

sqlres fetch_all_user_permissions() { LT("fetch_all_user_permissions");
  string         tname  = "USER_CHANNEL_PERMISSIONS";
  vector<string> fields = {"USERNAME", "CHANNELS"};
  return scan_fetch_fields_json_map(tname, fields);
}

sqlres fetch_all_user_subscriptions() { LT("fetch_all_user_subscriptions");
  string         tname  = "USER_CHANNEL_SUBSCRIPTIONS";
  vector<string> fields = {"USERNAME", "CHANNELS"};
  return scan_fetch_fields_json_map(tname, fields);
}

sqlres fetch_all_subscriber_latency_histogram() {
  LT("fetch_all_subscriber_latency_histogram");
  string         tname  = "SUBSCRIBER_LATENCY_HISTOGRAM";
  vector<string> fields = {"DATACENTER_NAME", "HISTOGRAM"};
  return scan_fetch_fields_json_map(tname, fields);
}

sqlres fetch_all_dirty_deltas() { LT("fetch_all_dirty_deltas");
  string         tname  = "DIRTY_DELTAS";
  vector<string> fields = {"DELTA_KEY", "TS"};
  return scan_fetch_fields_json_map(tname, fields);
}

sqlres fetch_all_to_sync_keys() { LT("fetch_all_to_sync_keys");
  string         tname  = "TO_SYNC_KEYS";
  vector<string> fields = {"KQK", "SECURITY_TOKEN"};
  return scan_fetch_fields_json_map(tname, fields);
}

sqlres fetch_all_stationed_users() { LT("fetch_all_stationed_users");
  string         tname  = "STATIONED_USERS";
  vector<string> fields = { "USERNAME"};
  return scan_fetch_fields_array(tname, fields);
}

sqlres fetch_all_to_evict_keys() { LT("fetch_all_to_evict_keys");
  string         tname  = "TO_EVICT_KEYS";
  vector<string> fields = {"KQK"};
  return scan_fetch_fields_array(tname, fields);
}

sqlres fetch_all_gcv_summaries() { LT("fetch_all_gcv_summaries");
  string         tname  = "GCV_SUMMARIES";
  vector<string> fields = {"SUMMARY"};
  return scan_fetch_fields_array(tname, fields);
}

sqlres fetch_all_lru_keys() { LT("fetch_all_lru_keys");
  string tname   = "LRU_KEYS";
  string pkfield = "KQK";
  return scan_fetch_all_fields(tname, pkfield);
}

sqlres fetch_all_agent_dirty_keys() { LT("fetch_all_agent_dirty_keys");
  string tname   = "DIRTY_KEYS";
  string pkfield = "KQK";
  return scan_fetch_all_fields(tname, pkfield);
}


sqlres persist_agent_uuid() { LT("persist_agent_uuid");
  string               ref("ME");
  LE("PERIST AGENT-UUID: " << MyUUID);
  string               tname  = "AGENT_INFO";
  PrimaryKey           pk(ref);
  vector<string>       fields = {"AGENT_UUID"};
  vector<StorageValue> vals   = {StorageValue(MyUUID)};
  return set_fields(tname, pk, fields, vals, true);
}

sqlres startup_fetch_agent_uuid() { LT("startup_fetch_agent_uuid");
  string ref("ME");
  if (MyUUID == -1) { //SCAN -> return FIRST (CASE: NewPrimaryElected)
    string         tname  = "AGENT_INFO";
    vector<string> fields = {"AGENT_UUID", "NEXT_OP_ID"};
    sqlres         sr     = scan_fetch_fields_json_map(tname, fields);
    RETURN_SQL_ERROR(sr)
    jv     juuids = sr.jres;
    if (!juuids.size()) {
      LE("CHERRY: startup_fetch_agent_uuid: MyUUID NOT PRESENT");
      RETURN_EMPTY_SQLRES
    } else {
      zh_type_object_assert(&juuids, "LOGIC(startup_fetch_agent_uuid)");
      for (jv::iterator it = juuids.begin(); it != juuids.end(); it++) {
        jv     juuid = it.key();
        string uuid  = juuid.asString();
        MyUUID       = atol(uuid.c_str());
        sr.ures      = MyUUID;
        break;
      }
      LE("CHERRY: startup_fetch_agent_uuid: MyUUID: " << MyUUID);
      return sr;
    }
  } else {
    string           tname  = "AGENT_INFO";
    PrimaryKey       pk(ref);
    string           fname  = "AGENT_UUID";
    StorageCondition cond("REFERENCE", ref);
    sqlres           sr     = fetch_field(tname, pk, fname, cond, VT_UINT);
    RETURN_SQL_ERROR(sr)
    UInt64           iduuid = sr.ures;
    MyUUID                  = (iduuid == 0) ? -1 : iduuid;
    LE("startup_fetch_agent_uuid: MyUUID: " << MyUUID);
    return sr;
  }
}

sqlres persist_datacenter_uuid(string &dcuuid) { LT("persist_datacenter_uuid");
  string               ref("ME");
  string               tname  = "AGENT_INFO";
  PrimaryKey           pk(ref);
  vector<string>       fields = {"AGENT_UUID", "DC_UUID"};
  vector<StorageValue> vals   = {StorageValue(MyUUID), StorageValue(dcuuid)};
  return set_fields(tname, pk, fields, vals, true);
}

sqlres startup_fetch_datacenter_uuid() { LT("startup_fetch_datacenter_uuid");
  string           ref("ME");
  string           tname     = "AGENT_INFO";
  PrimaryKey       pk(ref);
  string           fname     = "DC_UUID";
  StorageCondition cond("REFERENCE", ref);
  sqlres           sr        = fetch_field(tname, pk, fname, cond, VT_STRING);
  MyDataCenterUUID           = sr.sres;
  LOG(INFO) << "startup_fetch_datacenter_uuid: " << MyDataCenterUUID << endl;
  return sr;
}

sqlres persist_next_op_id(UInt64 cnt) { LT("persist_next_op_id");
  string               ref("ME");
  string               tname  = "AGENT_INFO";
  PrimaryKey           pk(ref);
  vector<string>       fields = {"AGENT_UUID", "NEXT_OP_ID"};
  vector<StorageValue> vals   = {StorageValue(MyUUID), StorageValue(cnt)};
  return set_fields(tname, pk, fields, vals, true);
}

sqlres persist_agent_num_cache_bytes(UInt64 val) {
  LT("persist_agent_num_cache_bytes");
  string               ref("ME");
  string               tname  = "AGENT_INFO";
  PrimaryKey           pk(ref);
  vector<string>       fields = {"AGENT_UUID", "CACHE_NUM_BYTES"};
  vector<StorageValue> vals   = {StorageValue(MyUUID), StorageValue(val)};
  return set_fields(tname, pk, fields, vals, true);
}

sqlres persist_agent_backoff_end_time(UInt64 endt) {
  LT("persist_agent_backoff_end_time");
  string               ref("ME");
  string               tname  = "AGENT_INFO";
  PrimaryKey           pk(ref);
  vector<string>       fields = {"BACKOFF_END_TIME"};
  vector<StorageValue> vals   = {StorageValue(endt)};
  return set_fields(tname, pk, fields, vals, true);
}

sqlres fetch_agent_backoff_end_time() { LT("fetch_agent_backoff_end_time");
  string           ref("ME");
  string           tname     = "AGENT_INFO";
  PrimaryKey       pk(ref);
  string           fname     = "BACKOFF_END_TIME";
  StorageCondition cond("REFERENCE", ref);
  return fetch_field(tname, pk, fname, cond, VT_STRING);
}


sqlres fetch_last_remove_delta(string &kqk) {
  LT("fetch_last_remove_delta");
  string           tname = "LAST_REMOVE_DENTRY";
  PrimaryKey       pk(kqk);
  string           fname = "META";
  StorageCondition cond("KQK", kqk);
  return fetch_field(tname, pk, fname, cond, VT_JMAP);
}

sqlres persist_last_remove_delta(string &kqk, jv *jmeta) {
  LT("persist_last_remove_delta");
  string               tname  = "LAST_REMOVE_DENTRY";
  PrimaryKey           pk(kqk);
  vector<string>       fields = {"KQK", "META"};
  vector<StorageValue> vals   = {StorageValue(kqk), StorageValue(jmeta)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres remove_last_remove_delta(string &kqk) {
  LT("remove_last_remove_delta");
  string           tname = "LAST_REMOVE_DENTRY";
  PrimaryKey       pk(kqk);
  StorageCondition cond("KQK", kqk);
  return delete_key(tname, pk, cond);
}


sqlres fetch_gc_version(string &kqk) {
  LT("fetch_gc_version");
  string           tname = "KEY_GC_VERSION";
  PrimaryKey       pk(kqk);
  string           fname = "GC_VERSION";
  StorageCondition cond("KQK", kqk);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}

sqlres persist_gc_version(string &kqk, UInt64 gcv) {
  LT("persist_gc_version");
  string               tname  = "KEY_GC_VERSION";
  PrimaryKey           pk(kqk);
  vector<string>       fields = {"KQK", "GC_VERSION"};
  vector<StorageValue> vals   = {StorageValue(kqk), StorageValue(gcv)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres remove_gc_version(string &kqk) {
  LT("remove_gc_version");
  string           tname = "KEY_GC_VERSION";
  PrimaryKey       pk(kqk);
  StorageCondition cond("KQK", kqk);
  return delete_key(tname, pk, cond);
}

sqlres fetch_agent_key_max_gc_version(string &kqk) {
  LT("fetch_agent_key_max_gc_version");
  string           tname = "AGENT_KEY_MAX_GC_VERSION";
  PrimaryKey       pk(kqk);
  string           fname = "GC_VERSION";
  StorageCondition cond("KQK", kqk);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}

sqlres persist_agent_key_max_gc_version(string &kqk, UInt64 gcv) {
  LT("persist_agent_key_max_gc_version");
  string               tname  = "AGENT_KEY_MAX_GC_VERSION";
  PrimaryKey           pk(kqk);
  vector<string>       fields = {"KQK", "GC_VERSION"};
  vector<StorageValue> vals   = {StorageValue(kqk), StorageValue(gcv)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres remove_agent_key_max_gc_version(string &kqk) {
  LT("remove_agent_key_max_gc_version");
  string           tname = "AGENT_KEY_MAX_GC_VERSION";
  PrimaryKey       pk(kqk);
  StorageCondition cond("KQK", kqk);
  return delete_key(tname, pk, cond);
}

sqlres persist_crdt(string &kqk, jv *jcrdt) { LT("persist_crdt");
  jv                    pcrdt = *jcrdt; // NOTE: gets modified
  zstorage_cleanup_crdt_before_persist(&pcrdt);
  string                tname  = "DOCUMENTS";
  PrimaryKey            pk(kqk);
  vector<string>        fields = {"KQK", "CRDT"};
  vector<StorageValue>  vals   = {StorageValue(kqk), StorageValue(&pcrdt)};
  return set_fields(tname, pk, fields, vals, false);
}

#ifndef USING_MEMORY_LMDB_PLUGIN
  #define PERSIST_CRDT(kqk, jcrdt) {sc = persist_crdt(kqk, jcrdt);}
#endif

sqlres fetch_crdt(string &kqk) { LT("fetch_crdt");
  string           tname = "DOCUMENTS";
  PrimaryKey       pk(kqk);
  string           fname = "CRDT";
  StorageCondition cond("KQK", kqk);
  return fetch_field(tname, pk, fname, cond, VT_JMAP);
}

#ifndef USING_MEMORY_LMDB_PLUGIN
  #define FETCH_CRDT(kqk) {sc = fetch_crdt(kqk);}
#endif

sqlres remove_crdt(string &kqk) { LT("remove_crdt");
  string           tname = "DOCUMENTS";
  PrimaryKey       pk(kqk);
  StorageCondition cond("KQK", kqk);
  return delete_key(tname, pk, cond);
}


sqlres persist_agent_createds(jv *jcreateds) { LT("persist_agent_createds");
  vector<string> mbrs = zh_jv_get_member_names(jcreateds);
  for (uint i = 0; i < mbrs.size(); i++) {
    string                guuid    = mbrs[i];
    jv                   *jcreated = &((*jcreateds)[guuid]);
    UInt64                created  = jcreated->asUInt64();
    string                tname    = "AGENT_CREATEDS";
    PrimaryKey            pk(guuid);
    vector<string>        fields   = {"DATACENTER_UUID", "CREATED"};
    vector<StorageValue>  vals     = {StorageValue(guuid),
                                      StorageValue(created)};
    sqlres                sr       = set_fields(tname, pk, fields, vals, false);
    RETURN_SQL_ERROR(sr);
  }
  RETURN_EMPTY_SQLRES
}

sqlres fetch_agent_created(string &guuid) { LT("fetch_agent_created");
  string           tname = "AGENT_CREATEDS";
  PrimaryKey       pk(guuid);
  string           fname = "CREATED";
  StorageCondition cond("DATACENTER_UUID", guuid);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}

sqlres remove_all_agent_created() { LT("remove_all_agent_created");
  string          tname  = "AGENT_CREATEDS";
  vector<string>  fields = { "DATACENTER_UUID"};
  sqlres          sr     =  scan_fetch_fields_array(tname, fields);
  RETURN_SQL_ERROR(sr);
  jv             *guuids = &(sr.jres);
  for (uint i = 0; i < guuids->size(); i++) {
    string           guuid = (*guuids)[i].asString();
    PrimaryKey       pk(guuid);
    StorageCondition cond("DATACENTER_UUID", guuid);
    sqlres           sr    = delete_key(tname, pk, cond);
    RETURN_SQL_ERROR(sr);
  }
  RETURN_EMPTY_SQLRES
}

sqlres persist_to_sync_key(string &kqk, string &sectok) {
  string               tname  = "TO_SYNC_KEYS";
  PrimaryKey           pk(kqk);
  vector<string>       fields = {"KQK", "SECURITY_TOKEN"};
  vector<StorageValue> vals   = {StorageValue(kqk), StorageValue(sectok)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres fetch_to_sync_key(string &kqk) {
  string           tname = "TO_SYNC_KEYS";
  PrimaryKey       pk(kqk);
  string           fname = "KQK";
  StorageCondition cond("KQK", kqk);
  return fetch_field(tname, pk, fname, cond, VT_STRING);
}

sqlres remove_to_sync_key(string &kqk) {
  string           tname = "TO_SYNC_KEYS";
  PrimaryKey       pk(kqk);
  StorageCondition cond("KQK", kqk);
  return delete_key(tname, pk, cond);
}

sqlres persist_to_evict_key(string &kqk) {
  UInt64               one    = 1;
  string               tname  = "TO_EVICT_KEYS";
  PrimaryKey           pk(kqk);
  vector<string>       fields = {"KQK", "VALUE"};
  vector<StorageValue> vals   = {StorageValue(kqk), StorageValue(one)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres remove_to_evict_key(string &kqk) {
  string           tname = "TO_EVICT_KEYS";
  PrimaryKey       pk(kqk);
  StorageCondition cond("KQK", kqk);
  return delete_key(tname, pk, cond);
}



sqlres persist_out_of_sync_key(string &kqk) {
  UInt64               one    = 1;
  string               tname  = "OUT_OF_SYNC_KEYS";
  PrimaryKey           pk(kqk);
  vector<string>       fields = {"KQK", "VALUE"};
  vector<StorageValue> vals   = {StorageValue(kqk), StorageValue(one)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres fetch_out_of_sync_key(string &kqk) {
  string           tname = "OUT_OF_SYNC_KEYS";
  PrimaryKey       pk(kqk);
  string           fname = "VALUE";
  StorageCondition cond("KQK", kqk);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}

sqlres remove_out_of_sync_key(string &kqk) {
  string           tname = "OUT_OF_SYNC_KEYS";
  PrimaryKey       pk(kqk);
  StorageCondition cond("KQK", kqk);
  return delete_key(tname, pk, cond);
}


sqlres persist_delta(string &kqk, string &avrsn, jv *jdentry, jv *jauth) {
  LT("persist_delta");
  string               dkey    = zs_get_persist_delta_key(kqk, avrsn);
  string               tname   = "DELTAS";
  PrimaryKey           pk(dkey);
  vector<string>       fields  = {"DELTA_KEY", "DENTRY", "AUTH"};
  vector<StorageValue> vals    = {StorageValue(dkey), StorageValue(jdentry),
                                  StorageValue(jauth)};
  return set_fields(tname, pk, fields, vals, false);
}

#ifndef USING_MEMORY_LMDB_PLUGIN
  #define PERSIST_DELTA(kqk, avrsn, jdentry, jauth) { \
    sd = persist_delta(kqk, avrsn, jdentry, jauth);   \
  }
#endif

sqlres update_delta(string &kqk, string &avrsn, jv *jdentry) {
  LT("update_delta");
  string               dkey    = zs_get_persist_delta_key(kqk, avrsn);
  string               tname   = "DELTAS";
  PrimaryKey           pk(dkey);
  vector<string>       fields  = {"DELTA_KEY", "DENTRY"};
  vector<StorageValue> vals    = {StorageValue(dkey), StorageValue(jdentry)};
  return set_fields(tname, pk, fields, vals, true);
}

#ifndef USING_MEMORY_LMDB_PLUGIN
  #define UPDATE_DELTA(kqk, avrsn, jdentry, jauth) { \
    sd = update_delta(kqk, avrsn, jdentry, jauth);    \
  }
#endif

sqlres remove_delta(string &kqk, string &avrsn) {
  LT("remove_delta");
  string           dkey  = zs_get_persist_delta_key(kqk, avrsn);
  string           tname = "DELTAS";
  PrimaryKey       pk(dkey);
  StorageCondition cond("DELTA_KEY", dkey);
  return delete_key(tname, pk, cond);
}

sqlres fetch_agent_delta(string &kqk, string &avrsn) {
  LT("fetch_agent_delta");
  string           dkey  = zs_get_persist_delta_key(kqk, avrsn);
  string           tname = "DELTAS";
  PrimaryKey       pk(dkey);
  string           fname = "DENTRY";
  StorageCondition cond("DELTA_KEY", dkey);
  return fetch_field(tname, pk, fname, cond, VT_JMAP);
}

sqlres fetch_agent_delta_auth(string &kqk, string &avrsn) {
  LT("fetch_agent_delta_auth");
  string           dkey  = zs_get_persist_delta_key(kqk, avrsn);
  string           tname = "DELTAS";
  PrimaryKey       pk(dkey);
  string           fname = "AUTH";
  StorageCondition cond("DELTA_KEY", dkey);
  return fetch_field(tname, pk, fname, cond, VT_JMAP);
}

sqlres fetch_subscriber_delta(string &kqk, string &avrsn) {
  LT("fetch_subscriber_delta");
  return fetch_agent_delta(kqk, avrsn);
}

#ifndef USING_MEMORY_LMDB_PLUGIN
  #define FETCH_SUBSCRIBER_DELTA(kqk, avrsn) { \
    sd = fetch_subscriber_delta(kqk, avrsn);   \
  }
#endif

sqlres fetch_subscriber_delta_aversions(string &kqk) {
  LT("fetch_subscriber_delta_aversions");
  string           tname  = "DELTA_VERSIONS";
  PrimaryKey       pk(kqk);
  string           fname = "AGENT_VERSIONS";
  StorageCondition cond("KQK", kqk);
  return fetch_field(tname, pk, fname, cond, VT_ARRAY);
}

// DELTA_VERSIONS.AGENT_VERSIONS populated @PERSIST/REMOVE
static sqlres modify_subscriber_delta_version(string &kqk, string &avrsn,
                                              bool remove) {
  LT("modify_subscriber_delta_version");
  sqlres            sr      = fetch_subscriber_delta_aversions(kqk);
  RETURN_SQL_ERROR(sr)
  jv               *javrsns = &(sr.jres);
  jv                javrsn  = avrsn;
  if (!remove) zh_jv_append(javrsns, &javrsn);      // PERSIST
  else         zh_jv_string_splice(javrsns, avrsn); // REMOVE
  string            tname   = "DELTA_VERSIONS";
  PrimaryKey        pk(kqk);
  StorageCondition  cond("KQK", kqk);
  if (javrsns->size() == 0) {
    return delete_key(tname, pk, cond);
  } else {
    vector<string>        fields   = {"KQK", "AGENT_VERSIONS"};
    vector<StorageValue>  vals     = {StorageValue(kqk), StorageValue(javrsns)};
    return set_fields(tname, pk, fields, vals, false);
  }
}

sqlres persist_subscriber_delta_version(string &kqk, string &avrsn) {
  LT("persist_subscriber_delta_version");
  return modify_subscriber_delta_version(kqk, avrsn, false);
}

sqlres remove_subscriber_delta_version(string &kqk, string &avrsn) {
  LT("remove_subscriber_delta_version");
  return modify_subscriber_delta_version(kqk, avrsn, true);
}

sqlres persist_persisted_subscriber_delta(string &kqk, string &avrsn) {
  LT("persist_persisted_subscriber_delta");
  Int64                 one    = 1;
  string                kavkey = zs_get_kav_key(kqk, avrsn);
  string                tname  = "PERSISTED_SUBSCRIBER_DELTA";
  PrimaryKey            pk(kavkey);
  vector<string>        fields = {"KAV_KEY", "VALUE"};
  vector<StorageValue>  vals   = {StorageValue(kavkey), StorageValue(one)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres fetch_persisted_subscriber_delta(string &kqk, string &avrsn) {
  LT("fetch_persisted_subscriber_delta");
  string           kavkey = zs_get_kav_key(kqk, avrsn);
  string           tname  = "PERSISTED_SUBSCRIBER_DELTA";
  PrimaryKey       pk(kavkey);
  string           fname = "VALUE";
  StorageCondition cond("KAV_KEY", kavkey);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}

sqlres remove_persisted_subscriber_delta(string &kqk, string &avrsn) {
  LT("remove_persisted_subscriber_delta");
  string           kavkey = zs_get_kav_key(kqk, avrsn);
  string           tname  = "PERSISTED_SUBSCRIBER_DELTA";
  PrimaryKey       pk(kavkey);
  StorageCondition cond("KAV_KEY", kavkey);
  return delete_key(tname, pk, cond);
}


sqlres persist_agent_sent_delta(string &kqk, string &avrsn, UInt64 now) {
  LT("persist_agent_sent_delta");
  string               dkey    = zs_get_persist_delta_key(kqk, avrsn);
  string               tname   = "AGENT_SENT_DELTAS";
  PrimaryKey           pk(dkey);
  vector<string>       fields  = {"DELTA_KEY", "TS"};
  vector<StorageValue> vals    = {StorageValue(dkey), StorageValue(now)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres remove_agent_sent_delta(string &kqk, string &avrsn) {
  LT("remove_agent_sent_delta");
  string           dkey  = zs_get_persist_delta_key(kqk, avrsn);
  string           tname = "AGENT_SENT_DELTAS";
  PrimaryKey       pk(dkey);
  StorageCondition cond("DELTA_KEY", dkey);
  return delete_key(tname, pk, cond);
}


sqlres persist_agent_permission(string &channel, string &perm) {
  LT("persist_agent_permission");
  RETURN_EMPTY_SQLRES // AGENT_PERMISSONS NOT USED ON AGENT
}

sqlres persist_agent_subscription(string &channel, string &perm) {
  LT("persist_agent_subscription");
  RETURN_EMPTY_SQLRES // AGENT_SUBSCRIPTIONS NOT USED ON AGENT
}

sqlres persist_permissions(jv *perms, string *username) {
  LT("persist_permissions");
  sqlres sr;
  vector<string> mbrs = zh_jv_get_member_names(perms);
  for (uint i = 0; i < mbrs.size(); i++) {
    string channel  = mbrs[i];
    string perm     = (*perms)[channel].asString();
    if (username) sr = persist_user_permission(*username, channel, perm);
    else          sr = persist_agent_permission(channel, perm);
    RETURN_SQL_ERROR(sr)
   }
   RETURN_EMPTY_SQLRES
}

sqlres persist_agent_permissions(jv *perms) { LT("persist_agent_permissions");
  return persist_permissions(perms, NULL);
}

sqlres persist_user_permissions(string *username, jv *perms) {
  LT("persist_user_permissions");
  return persist_permissions(perms, username);
}

sqlres persist_subcriptions(jv *subs, string *username) {
  LT("persist_subcriptions");
  vector<string> mbrs = zh_jv_get_member_names(subs);
  for (uint i = 0; i < mbrs.size(); i++) {
    sqlres sr;
    string channel  = mbrs[i];
    string perm     = (*subs)[channel].asString();
    if (username) sr = persist_user_subscription(*username, channel, perm);
    else          sr = persist_agent_subscription(channel, perm);
    RETURN_SQL_ERROR(sr)
  }
  RETURN_EMPTY_SQLRES
}

sqlres persist_agent_subscriptions(jv *subs) {
  LT("persist_agent_subscriptions");
  return persist_subcriptions(subs, NULL);
}

sqlres persist_user_subscriptions(string *username, jv *subs) {
  LT("persist_user_subscriptions");
  return persist_subcriptions(subs, username);
}

sqlres persist_fixlog(string &fid, jv *pc) { LT("persist_fixlog");
  if (DisableFixlog) RETURN_EMPTY_SQLRES
  string               tname  = "FIXLOG";
  PrimaryKey           pk(fid);
  vector<string>       fields = {"FID", "PC"};
  vector<StorageValue> vals   = {StorageValue(fid), StorageValue(pc)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres remove_fixlog(string &fid) { LT("remove_fixlog");
  if (DisableFixlog) RETURN_EMPTY_SQLRES
  string           tname = "FIXLOG";
  PrimaryKey       pk(fid);
  StorageCondition cond("FID", fid);
  return delete_key(tname, pk, cond);
}

sqlres persist_dirty_delta(string &dkey) { LT("persist_dirty_delta");
  UInt64               now     = zh_get_ms_time();
  string               tname   = "DIRTY_DELTAS";
  PrimaryKey           pk(dkey);
  vector<string>       fields  = {"DELTA_KEY", "TS"};
  vector<StorageValue> vals    = {StorageValue(dkey), StorageValue(now)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres remove_dirty_subscriber_delta(string &dkey) {
  LT("remove_dirty_subscriber_delta");
  string           tname = "DIRTY_DELTAS";
  PrimaryKey       pk(dkey);
  StorageCondition cond("DELTA_KEY", dkey);
  return delete_key(tname, pk, cond);
}

sqlres persist_agent_dirty_keys(string &kqk, UInt64 ndk) {
  LT("persist_agent_dirty_keys");
  string     tname = "DIRTY_KEYS";
  PrimaryKey pk(kqk);
  if (ndk == 0) {
    StorageCondition cond("KQK", kqk);
    return delete_key(tname, pk, cond);
  } else {
    vector<string>       fields  = {"KQK", "NUM_DIRTY"};
    vector<StorageValue> vals    = {StorageValue(kqk), StorageValue(ndk)};
    return set_fields(tname, pk, fields, vals, false);
  }
}

sqlres fetch_agent_dirty_keys(string &kqk) { LT("fetch_agent_dirty_keys");
  string           tname = "DIRTY_KEYS";
  PrimaryKey       pk(kqk);
  string           fname = "NUM_DIRTY";
  StorageCondition cond("KQK", kqk);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}


sqlres increment_agent_num_workers(Int64 cnt) {
  LT("increment_agent_num_workers");
  string           ref("ME");
  string           tname = "NUM_WORKERS";
  PrimaryKey       pk(ref);
  string           fname = "COUNT";
  StorageCondition cond("REFERENCE", ref);
  Int64            byval = cnt;
  return update_increment_field(tname, pk, fname, byval, cond);
}

sqlres fetch_agent_num_workers() {
  LT("fetch_agent_num_workers");
  string           ref("ME");
  string           tname = "NUM_WORKERS";
  PrimaryKey       pk(ref);
  string           fname = "COUNT";
  StorageCondition cond("REFERENCE", ref);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}


sqlres persist_worker_partition(UInt64 pid, UInt64 partition) {
  LT("persist_worker_partition");
  string               tname  = "WORKER_PARTITIONS";
  PrimaryKey           pk(pid);
  vector<string>       fields = {"PID", "PARTITION"};
  vector<StorageValue> vals   = {StorageValue(pid), StorageValue(partition)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres fetch_worker_partition(UInt64 pid) {
  LT("fetch_worker_partition");
  string           tname = "WORKER_PARTITIONS";
  PrimaryKey       pk(pid);
  string           fname = "PARTITION";
  StorageCondition cond("PID", pid);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}


sqlres persist_callback_server(const char *cbhost, unsigned int cbport) {
  LT("persist_callback_server");
  string               scbhost(cbhost);
  UInt64               ucbport = cbport;
  string               ref("ME");
  string               tname  = "AGENT_INFO";
  PrimaryKey           pk(ref);
  vector<string>       fields = {"REFERENCE", "CALLBACK_SERVER",
                                 "CALLBACK_PORT"};
  vector<StorageValue> vals   = {StorageValue(ref), StorageValue(scbhost),
                                 StorageValue(ucbport)};
  return set_fields(tname, pk, fields, vals, true);
}

sqlres fetch_callback_server_hostname() {
  LT("fetch_callback_server_hostname");
  string           ref("ME");
  string           tname  = "AGENT_INFO";
  PrimaryKey       pk(ref);
  string           fname = "CALLBACK_SERVER";
  StorageCondition cond("REFERENCE", ref);
  return fetch_field(tname, pk, fname, cond, VT_STRING);
}

sqlres fetch_callback_server_port() {
  LT("fetch_callback_server_port");
  string           ref("ME");
  string           tname  = "AGENT_INFO";
  PrimaryKey       pk(ref);
  string           fname = "CALLBACK_PORT";
  StorageCondition cond("REFERENCE", ref);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}


sqlres persist_agent_heartbeat_info(jv *ahb) {
  LT("persist_agent_heartbeat_info");
  string               ref("ME");
  string               tname  = "AGENT_HEARTBEAT";
  PrimaryKey           pk(ref);
  vector<string>       fields = {"REFERENCE", "INFO"};
  vector<StorageValue> vals   = {StorageValue(ref), StorageValue(ahb)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres fetch_agent_heartbeat_info() {
  LT("fetch_agent_heartbeat_info");
  string           ref("ME");
  string           tname = "AGENT_HEARTBEAT";
  PrimaryKey       pk(ref);
  string           fname = "INFO";
  StorageCondition cond("REFERENCE", ref);
  return fetch_field(tname, pk, fname, cond, VT_JMAP);
}

sqlres increment_next_op_id(UInt64 cnt) { LT("increment_next_op_id");
  string           ref("ME");
  string           tname = "AGENT_INFO";
  PrimaryKey       pk(ref);
  string           fname = "NEXT_OP_ID";
  StorageCondition cond("REFERENCE", ref);
  Int64            byval = cnt;
  return update_increment_field(tname, pk, fname, byval, cond);
}

sqlres increment_agent_num_cache_bytes(Int64 cnt) {
  LT("increment_agent_num_cache_bytes");
  string           ref("ME");
  string           tname = "AGENT_INFO";
  PrimaryKey       pk(ref);
  string           fname = "CACHE_NUM_BYTES";
  StorageCondition cond("REFERENCE", ref);
  Int64            byval = cnt;
  return update_increment_field(tname, pk, fname, byval, cond);
}


sqlres fetch_gc_summary_versions(string &kqk) { LT("fetch_gc_summary_versions");
  string           tname = "KEY_GC_SUMMARY_VERSIONS";
  PrimaryKey       pk(kqk);
  string           fname = "GC_VERSION";
  StorageCondition cond("KQK", kqk);
  return fetch_field(tname, pk, fname, cond, VT_ARRAY);
}

static sqlres modify_gc_summary_version(string &kqk, UInt64 gcv, bool remove) {
  LT("modify_gc_summary_version");
  sqlres                sr     = fetch_gc_summary_versions(kqk);
  RETURN_SQL_ERROR(sr)
  jv                   *jgcvs  = &(sr.jres);
  jv                    jgcv   = gcv;
  if (remove) zh_jv_value_splice(jgcvs, gcv); // REMOVE
  else { // NOTE: REPEATS POSSIBLE W/ zgc_add_reorder_to_gc_summary()
    for (int i = 0; i < (int)jgcvs->size(); i++) {
      UInt64 gcsumm_gcv = (*jgcvs)[i].asUInt64();
      if (gcsumm_gcv == gcv) RETURN_EMPTY_SQLRES
    }
    zh_jv_append(jgcvs, &jgcv);     // PERSIST
  }
  string                tname  = "KEY_GC_SUMMARY_VERSIONS";
  PrimaryKey            pk(kqk);
  vector<string>        fields = {"KQK", "GC_VERSIONS"};
  vector<StorageValue>  vals   = {StorageValue(kqk), StorageValue(jgcvs)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres persist_gc_summary_version(string &kqk, UInt64 gcv) {
  LT("persist_gc_summary_version");
  return modify_gc_summary_version(kqk, gcv, false);
}

sqlres remove_gc_summary_version(string &kqk, UInt64 gcv) {
  LT("remove_gc_summary_version");
  return modify_gc_summary_version(kqk, gcv, true);
}


sqlres persist_gcv_summary(string &kqk, UInt64 gcv, jv *jgcsumm) {
  LT("persist_gcv_summary: K: " << kqk << " GCV: " << gcv);
  (*jgcsumm)["kqk"] = kqk;   // NOTE: stash KQK in SUMMARY
  (*jgcsumm)["gcv"] = gcv;   //       Used in fetch_all_gcv_summaries()
  string               g_key   = zs_get_g_key(kqk, gcv);
  (*jgcsumm)["_id"] = g_key; //      Used in zgc_reaper.cpp
  string               tname   = "GCV_SUMMARIES";
  PrimaryKey           pk(g_key);
  vector<string>       fields  = {"G_KEY", "SUMMARY"};
  vector<StorageValue> vals    = {StorageValue(g_key), StorageValue(jgcsumm)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres fetch_gcv_summary(string &kqk, UInt64 gcv) { LT("fetch_gcv_summary");
  string           g_key = zs_get_g_key(kqk, gcv);
  string           tname = "GCV_SUMMARIES";
  PrimaryKey       pk(g_key);
  string           fname = "SUMMARY";
  StorageCondition cond("G_KEY", g_key);
  return fetch_field(tname, pk, fname, cond, VT_JMAP);
}

sqlres remove_gcv_summary(string &kqk, UInt64 gcv) { LT("remove_gcv_summary");
  string           g_key = zs_get_g_key(kqk, gcv);
  string           tname = "GCV_SUMMARIES";
  PrimaryKey       pk(g_key);
  StorageCondition cond("G_KEY", g_key);
  return delete_key(tname, pk, cond);
}



sqlres persist_stationed_user(string &username) {
  UInt64               one    = 1;
  string               tname  = "STATIONED_USERS";
  PrimaryKey           pk(username);
  vector<string>       fields = {"USERNAME", "VALUE"};
  vector<StorageValue> vals   = {StorageValue(username), StorageValue(one)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres fetch_is_user_stationed(string &username) {
  string           tname = "STATIONED_USERS";
  PrimaryKey       pk(username);
  string           fname = "VALUE";
  StorageCondition cond("USERNAME", username);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}

sqlres remove_stationed_user(string &username) {
  string           tname  = "STATIONED_USERS";
  PrimaryKey       pk(username);
  StorageCondition cond("USERNAME", username);
  return delete_key(tname, pk, cond);
}

static sqlres increment_number_subscriptions(string &tblname, string &schanid,
                                             bool incr) {
  string           tname  = tblname;
  PrimaryKey       pk(schanid);
  string           fname  = "NUM_SUBSCRIPTIONS";
  StorageCondition cond("CHANNEL", schanid);
  sqlres           sr     =  fetch_field(tname, pk, fname, cond, VT_UINT);
  RETURN_SQL_ERROR(sr)
  UInt64           numsub = sr.ures;
  if (incr) numsub++;
  else      numsub--;
  vector<string>       fields = {"CHANNEL", "NUM_SUBSCRIPTIONS"};
  vector<StorageValue> vals   = {StorageValue(schanid), StorageValue(numsub)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres persist_channel_device(string &schanid) {
  string tblname = "REPLICATION_CHANNELS";
  return increment_number_subscriptions(tblname, schanid, true);
}

sqlres remove_channel_device(string &schanid) {
  string tblname = "REPLICATION_CHANNELS";
  return increment_number_subscriptions(tblname, schanid, false);
}

sqlres persist_device_subscription(string &schanid) {
  string tblname = "DEVICE_CHANNELS";
  return increment_number_subscriptions(tblname, schanid, true);
}

sqlres remove_device_subscriptions(string &schanid) {
  string tblname = "DEVICE_CHANNELS";
  return increment_number_subscriptions(tblname, schanid, false);
}


sqlres fetch_user_subscriptions(string &username) {
  LT("fetch_user_subscriptions");
  string           tname = "USER_CHANNEL_SUBSCRIPTIONS";
  PrimaryKey       pk(username);
  string           fname = "CHANNELS";
  StorageCondition cond("USERNAME", username);
  return fetch_field(tname, pk, fname, cond, VT_JMAP);
}

// NOTE: USER_CHANNEL_SUBSCRIPTIONS.CHANNELS populated @PERSIST/REMOVE
static sqlres modify_user_subscription(string &username, string &schanid,
                                       string &perm) {
  LT("modify_user_subscription");
  sqlres                sr     = fetch_user_subscriptions(username);
  RETURN_SQL_ERROR(sr)
  jv                   *jchans = &(sr.jres);
  if (perm.size()) (*jchans)[schanid] = perm;     // PERSIST
  else             jchans->removeMember(schanid); // REMOVE
  string                tname  = "USER_CHANNEL_SUBSCRIPTIONS";
  PrimaryKey            pk(username);
  vector<string>        fields = {"USERNAME", "CHANNELS"};
  vector<StorageValue>  vals   = {StorageValue(username), StorageValue(jchans)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres persist_user_subscription(string &username, string &schanid,
                                 string &perm) {
  LT("persist_user_subscription");
  return modify_user_subscription(username, schanid, perm);
}

sqlres remove_user_subscription(string &username, string &schanid) {
  LT("remove_user_subscription");
  string perm; // EMPTY PERM -> REMOVE
  return modify_user_subscription(username, schanid, perm);
}

sqlres fetch_user_subscription(string &username, string &schanid) {
  LT("fetch_user_subscription");
  sqlres  sr     = fetch_user_subscriptions(username);
  RETURN_SQL_ERROR(sr)
  jv     *jchans = &(sr.jres);
  if (zjn(jchans) || !zh_jv_is_member(jchans, schanid)) {
    RETURN_EMPTY_SQLRES
  } else {
    string val = (*jchans)[schanid].asString();
    RETURN_SQL_RESULT_STRING(val);
  }
}


sqlres fetch_user_permissions(string &username) {
  LT("fetch_user_permissions");
  string           tname = "USER_CHANNEL_PERMISSIONS";
  PrimaryKey       pk(username);
  string           fname = "CHANNELS";
  StorageCondition cond("USERNAME", username);
  return fetch_field(tname, pk, fname, cond, VT_JMAP);
}

// NOTE: USER_CHANNEL_PERMISSIONS.CHANNELS populated @PERSIST/REMOVE
static sqlres modify_user_permission(string &username, string &schanid,
                                     string &pabbr) {
  LT("modify_user_permission");
  sqlres                sr     = fetch_user_permissions(username);
  RETURN_SQL_ERROR(sr)
  jv                   *jchans = &(sr.jres);
  if (pabbr.size()) (*jchans)[schanid] = pabbr;    // PERSIST
  else              jchans->removeMember(schanid); // REMOVE
  string                tname  = "USER_CHANNEL_PERMISSIONS";
  PrimaryKey            pk(username);
  vector<string>        fields = {"USERNAME", "CHANNELS"};
  vector<StorageValue>  vals   = {StorageValue(username), StorageValue(jchans)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres persist_user_permission(string &username, string &schanid,
                               string &pabbr) {
  LT("persist_user_permission");
  return modify_user_permission(username, schanid, pabbr);
}

sqlres remove_user_permission(string &username, string &schanid) {
  LT("remove_user_permission");
  string pabbr; // EMPTY PABBR -> REMOVE
  return modify_user_permission(username, schanid, pabbr);
}

sqlres fetch_user_permission(string &username, string &schanid) {
  LT("fetch_user_permission");
  sqlres  sr     = fetch_user_permissions(username);
  RETURN_SQL_ERROR(sr)
  jv     *jchans = &(sr.jres);
  if (zjn(jchans) || !zh_jv_is_member(jchans, schanid)) {
    RETURN_EMPTY_SQLRES
  } else {
    string val = (*jchans)[schanid].asString();
    RETURN_SQL_RESULT_STRING(val);
  }
}

sqlres remove_user_permissions(string &username) {
  LT("remove_user_permissions");
  string           tname = "USER_CHANNEL_PERMISSIONS";
  PrimaryKey       pk(username);
  StorageCondition cond("USERNAME", username);
  return delete_key(tname, pk, cond);
}

sqlres fetch_users_caching_key(string &kqk) {
  string           tname = "USER_CACHED_KEYS";
  PrimaryKey       pk(kqk);
  string           fname = "USERS";
  StorageCondition cond("KQK", kqk);
  return fetch_field(tname, pk, fname, cond, VT_JMAP);
}

// NOTE: USER_CACHED_KEYS.USERS populated @PERSIST/REMOVE
static sqlres modify_key_cached_by_user(string &kqk, string &username,
                                        bool remove) {
  LT("modify_key_cached_by_user");
  sqlres                sr     = fetch_users_caching_key(kqk);
  RETURN_SQL_ERROR(sr)
  jv                   *jusers = &(sr.jres);
  if (!remove) (*jusers)[username] = 1;        // PERSIST
  else         jusers->removeMember(username); // REMOVE
  string                tname  = "USER_CACHED_KEYS";
  PrimaryKey            pk(kqk);
  vector<string>        fields = {"KQK", "USERS"};
  vector<StorageValue>  vals   = {StorageValue(kqk), StorageValue(jusers)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres persist_key_cached_by_user(string &kqk, string &username) {
  return modify_key_cached_by_user(kqk, username, false);
}

sqlres remove_key_cached_by_user(string &kqk, string &username) {
  return modify_key_cached_by_user(kqk, username, true);
}

sqlres fetch_key_cached_by_user(string &kqk, string &username) {
  sqlres sr     = fetch_users_caching_key(kqk);
  RETURN_SQL_ERROR(sr)
  jv    *jusers = &(sr.jres);
  if (zjn(jusers) || !zh_jv_is_member(jusers, username)) {
    RETURN_EMPTY_SQLRES
  } else {
    string val = (*jusers)[username].asString();
    RETURN_SQL_RESULT_STRING(val);
  }
}


sqlres persist_user_authentication(string &username, string &hash,
                                   string &role) {
  LT("persist_user_authentication");
  string               tname  = "USER_AUTHENTICATION";
  PrimaryKey           pk(username);
  vector<string>       fields = {"USERNAME", "HASH", "ROLE"};
  vector<StorageValue> vals   = {StorageValue(username), StorageValue(hash),
                                 StorageValue(role)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres fetch_user_password_hash(string &username) {
  LT("fetch_user_password_hash");
  string           tname = "USER_AUTHENTICATION";
  PrimaryKey       pk(username);
  string           fname = "HASH";
  StorageCondition cond("USERNAME", username);
  return fetch_field(tname, pk, fname, cond, VT_STRING);
}



sqlres remove_worker(UInt64 wid) { LT("remove_worker");
  string           tname = "WORKERS";
  PrimaryKey       pk(wid);
  StorageCondition cond("UUID", wid);
  return delete_key(tname, pk, cond);
}

sqlres persist_worker(UInt64 wid, UInt64 wport, bool primary) {
  LT("persist_worker");
  UInt64               p      = primary ? 1 : 0;
  string               tname  = "WORKERS";
  PrimaryKey           pk(wid);
  vector<string>       fields = {"UUID", "PORT", "MASTER"};
  vector<StorageValue> vals   = {StorageValue(wid), StorageValue(wport),
                                 StorageValue(p)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres fetch_worker_port(UInt64 wid) { LT("fetch_worker_port");
  string           tname = "WORKERS";
  PrimaryKey       pk(wid);
  string           fname = "PORT";
  StorageCondition cond("UUID", wid);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}

sqlres fetch_primary_worker_uuid() { LT("fetch_primary_worker_uuid");
  Int64            one   = 1;
  string           tname = "WORKERS";
  string           fname = "UUID";
  StorageCondition cond("MASTER", one);
  return scan_fetch_single_field(tname, fname, cond, VT_UINT);
}

sqlres fetch_primary_worker_port() { LT("fetch_primary_worker_port");
  Int64            one   = 1;
  string           tname = "WORKERS";
  string           fname = "PORT";
  StorageCondition cond("MASTER", one);
  return scan_fetch_single_field(tname, fname, cond, VT_UINT);
}

sqlres persist_agent_connected(bool b) { LT("persist_agent_connected");
  if (MyUUID == -1) RETURN_EMPTY_SQLRES
  UInt64               val    = b ? 1 : 0;
  string               tname  = "CONNECTION_STATUS";
  PrimaryKey           pk(MyUUID);
  vector<string>       fields = {"AGENT_UUID", "CONNECTED"};
  vector<StorageValue> vals   = {StorageValue(MyUUID), StorageValue(val)};
  return set_fields(tname, pk, fields, vals, true);
}

sqlres fetch_agent_connected() { LT("fetch_agent_connected");
  if (MyUUID == -1) RETURN_EMPTY_SQLRES
  string           tname = "CONNECTION_STATUS";
  PrimaryKey       pk(MyUUID);
  string           fname = "CONNECTED";
  StorageCondition cond("AGENT_UUID", MyUUID);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}

sqlres persist_isolation(bool b) { LT("persist_isolation");
  if (MyUUID == -1) RETURN_EMPTY_SQLRES
  Isolation                   = b;
  UInt64               val    = b ? 1 : 0;
  string               tname  = "CONNECTION_STATUS";
  PrimaryKey           pk(MyUUID);
  vector<string>       fields = {"AGENT_UUID", "ISOLATION"};
  vector<StorageValue> vals   = {StorageValue(MyUUID), StorageValue(val)};
  return set_fields(tname, pk, fields, vals, true);
}

sqlres fetch_isolation() { LT("fetch_isolation");
  if (MyUUID == -1) RETURN_EMPTY_SQLRES
  string           tname = "CONNECTION_STATUS";
  PrimaryKey       pk(MyUUID);
  string           fname = "ISOLATION";
  StorageCondition cond("AGENT_UUID", MyUUID);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}

sqlres persist_subscriber_latency_histogram(string &rguuid, jv *jhist) {
  string               tname  = "SUBSCRIBER_LATENCY_HISTOGRAM";
  PrimaryKey           pk(rguuid);
  vector<string>       fields = {"DATACENTER_NAME", "HISTOGRAM"};
  vector<StorageValue> vals   = {StorageValue(rguuid), StorageValue(jhist)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres fetch_subscriber_latency_histogram(string &rguuid) {
  string           tname = "SUBSCRIBER_LATENCY_HISTOGRAM";
  PrimaryKey       pk(rguuid);
  string           fname = "HISTOGRAM";
  StorageCondition cond("DATACENTER_NAME", rguuid);
  return fetch_field(tname, pk, fname, cond, VT_JMAP);
}


sqlres persist_agent_watch_keys(string &kqk) { LT("persist_agent_watch_keys");
  UInt64                one    = 1;
  string                tname  = "AGENT_WATCH_KEYS";
  PrimaryKey            pk(kqk);
  vector<string>        fields = {"KQK", "VALUE"};
  vector<StorageValue>  vals   = {StorageValue(kqk), StorageValue(one)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres fetch_agent_watch_keys(string &kqk) { LT("fetch_agent_watch_keys");
  string           tname = "AGENT_WATCH_KEYS";
  PrimaryKey       pk(kqk);
  string           fname = "VALUE";
  StorageCondition cond("KQK", kqk);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}

sqlres remove_agent_watch_keys(string &kqk) { LT("remove_agent_watch_keys");
  string           tname = "AGENT_WATCH_KEYS";
  PrimaryKey       pk(kqk);
  StorageCondition cond("KQK", kqk);
  return delete_key(tname, pk, cond);
}


sqlres fetch_agent_version(string &kqk) { LT("fetch_agent_version");
  string           tname = "KEY_AGENT_VERSION";
  PrimaryKey       pk(kqk);
  string           fname = "AGENT_VERSION";
  StorageCondition cond("KQK", kqk);
  return fetch_field(tname, pk, fname, cond, VT_STRING);
}

sqlres persist_agent_version(string &kqk, string &avrsn) {
  LT("persist_agent_version");
  string                tname  = "KEY_AGENT_VERSION";
  PrimaryKey            pk(kqk);
  vector<string>        fields = {"KQK", "AGENT_VERSION"};
  vector<StorageValue>  vals   = {StorageValue(kqk), StorageValue(avrsn)};
  return set_fields(tname, pk, fields, vals, false);
}


sqlres fetch_device_subscriber_version(string &kqk, UInt64 auuid) {
  LT("fetch_device_subscriber_version");
  string           kakey  = zs_get_ka_key(kqk, auuid);
  string           tname = "DEVICE_SUBSCRIBER_VERSION";
  PrimaryKey       pk(kakey);
  string           fname = "AGENT_VERSION";
  StorageCondition cond("KA_KEY", kakey);
  return fetch_field(tname, pk, fname, cond, VT_STRING);
}

sqlres fetch_device_subscriber_version_timestamp(string &kqk, UInt64 auuid) {
  LT("fetch_device_subscriber_version");
  string           kakey = zs_get_ka_key(kqk, auuid);
  string           tname = "DEVICE_SUBSCRIBER_VERSION";
  PrimaryKey       pk(kakey);
  string           fname = "TS";
  StorageCondition cond("KA_KEY", kakey);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}

sqlres persist_device_subscriber_version(string &kqk, UInt64 auuid,
                                         string &avrsn) {
  LT("persist_device_subscriber_version");
  UInt64                ts     = zh_get_ms_time();
  string                kakey  = zs_get_ka_key(kqk, auuid);
  string                tname  = "DEVICE_SUBSCRIBER_VERSION";
  PrimaryKey            pk(kakey);
  vector<string>        fields = {"KA_KEY", "AGENT_VERSION", "TS",};
  vector<StorageValue>  vals   = {StorageValue(kakey),
                                  StorageValue(avrsn), StorageValue(ts)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres remove_device_subscriber_version(string &kqk, UInt64 auuid) {
  LT("remove_device_subscriber_version");
  string           kakey = zs_get_ka_key(kqk, auuid);
  string           tname = "DEVICE_SUBSCRIBER_VERSION";
  PrimaryKey       pk(kakey);
  StorageCondition cond("KA_KEY", kakey);
  return delete_key(tname, pk, cond);
}


sqlres fetch_per_key_subscriber_versions(string &kqk) {
  LT("fetch_per_key_subscriber_versions");
  string           tname = "PER_KEY_SUBSCRIBER_VERSION";
  PrimaryKey       pk(kqk);
  string           fname = "AGENT_VERSION_MAP";
  StorageCondition cond("KQK", kqk);
  return fetch_field(tname, pk, fname, cond, VT_JMAP);
}

// NOTE: PER_KEY_SUBSCRIBER_VERSION.AGENT_VERSIONS populated @PERSIST/REMOVE
static sqlres modify_per_key_subscriber_version(string &kqk, UInt64 auuid,
                                                string &avrsn) {
  LT("modify_per_key_subscriber_version");
  sqlres                sr     = fetch_per_key_subscriber_versions(kqk);
  RETURN_SQL_ERROR(sr)
  jv                   *jdeps  = &(sr.jres);
  string                sauuid = to_string(auuid);
  if (avrsn.size()) (*jdeps)[sauuid] = avrsn;    // PERSIST
  else              jdeps->removeMember(sauuid); // REMOVE
  string                tname  = "PER_KEY_SUBSCRIBER_VERSION";
  PrimaryKey            pk(kqk);
  vector<string>        fields = {"KQK", "AGENT_VERSION_MAP"};
  vector<StorageValue>  vals   = {StorageValue(kqk), StorageValue(jdeps)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres persist_per_key_subscriber_version(string &kqk, UInt64 auuid,
                                          string &avrsn) {
  LT("persist_per_key_subscriber_version");
  return modify_per_key_subscriber_version(kqk, auuid, avrsn);
}

sqlres remove_per_key_subscriber_version(string &kqk, UInt64 auuid) {
  LT("remove_per_key_subscriber_version");
  string avrsn; // EMPTY AVRSN -> REMOVE
  return modify_per_key_subscriber_version(kqk, auuid, avrsn);
}


sqlres persist_key_repchans(string &kqk, jv *jrchans) {
  LT("persist_key_repchans");
  RETURN_EMPTY_SQLRES // KEY_REPLICATION_CHANNELS NOT USED ON AGENT
}

sqlres remove_key_repchans(string &kqk) { LT("remove_key_repchans");
  RETURN_EMPTY_SQLRES // KEY_REPLICATION_CHANNELS NOT USED ON AGENT
}


sqlres persist_delta_need_reference(string &kqk, jv *reo_author,
                                    jv *ref_author) {
  LT("persist_delta_need_reference");
  string                dkey    = get_ks_author(kqk, reo_author);
  string                tname   = "DELTA_NEED_REFERENCE";
  PrimaryKey            pk(dkey);
  vector<string>        fields   = {"DELTA_KEY", "REFERENCE_AUTHOR"};
  vector<StorageValue>  vals     = {StorageValue(kqk),
                                    StorageValue(ref_author)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres fetch_delta_need_reference(string &kqk, jv *reo_author) {
  LT("fetch_delta_need_reference");
  string           dkey  = get_ks_author(kqk, reo_author);
  string           tname = "DELTA_NEED_REFERENCE";
  PrimaryKey       pk(dkey);
  string           fname = "REFERENCE_AUTHOR";
  StorageCondition cond("DELTA_KEY", dkey);
  return fetch_field(tname, pk, fname, cond, VT_JMAP);
}

sqlres remove_delta_need_reference(string &kqk, jv *reo_author) {
  LT("remove_delta_need_reference");
  string           dkey  = get_ks_author(kqk, reo_author);
  string           tname = "DELTA_NEED_REFERENCE";
  PrimaryKey       pk(dkey);
  StorageCondition cond("DELTA_KEY", dkey);
  return delete_key(tname, pk, cond);
}


sqlres persist_delta_need_reorder(string &kqk, jv *ref_author,
                                  jv *reo_author) {
  LT("persist_delta_need_reorder");
  string                dkey    = get_ks_author(kqk, ref_author);
  string                tname   = "DELTA_NEED_REORDER";
  PrimaryKey            pk(dkey);
  vector<string>        fields   = {"DELTA_KEY", "REORDER_AUTHOR"};
  vector<StorageValue>  vals     = {StorageValue(kqk),
                                    StorageValue(reo_author)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres fetch_delta_need_reorder(string &kqk, jv *ref_author) {
  LT("fetch_delta_need_reorder");
  string           dkey  = get_ks_author(kqk, ref_author);
  string           tname = "DELTA_NEED_REORDER";
  PrimaryKey       pk(dkey);
  string           fname = "REORDER_AUTHOR";
  StorageCondition cond("DELTA_KEY", dkey);
  return fetch_field(tname, pk, fname, cond, VT_JMAP);
}

sqlres remove_delta_need_reorder(string &kqk, jv *ref_author) {
  LT("remove_delta_need_reorder");
  string           dkey  = get_ks_author(kqk, ref_author);
  string           tname = "DELTA_NEED_REORDER";
  PrimaryKey       pk(dkey);
  StorageCondition cond("DELTA_KEY", dkey);
  return delete_key(tname, pk, cond);
}


sqlres fetch_all_agent_key_gcv_needs_reorder(string &kqk) {
  LT("fetch_all_agent_key_gcv_needs_reorder");
  string           tname = "AGENT_KEY_GCV_NEEDS_REORDER";
  PrimaryKey       pk(kqk);
  string           fname = "GCV_MAP";
  StorageCondition cond("KQK", kqk);
  return fetch_field(tname, pk, fname, cond, VT_JMAP);
}

sqlres fetch_agent_key_gcv_needs_reorder(string &kqk, UInt64 gcv) {
  LT("fetch_agent_key_gcv_needs_reorder");
  string  sgcv     = to_string(gcv);
  sqlres  sr       = fetch_all_agent_key_gcv_needs_reorder(kqk);
  RETURN_SQL_ERROR(sr)
  jv     *jgavrsns = &(sr.jres);
  if (zjn(jgavrsns) || !zh_jv_is_member(jgavrsns, sgcv)) RETURN_EMPTY_SQLRES
  else {
    jv *javrsns = &((*jgavrsns)[sgcv]);
    RETURN_SQL_RESULT_JSON(*javrsns)
  }
}

static sqlres save_agent_key_gcv_needs_reorder(string &kqk, jv *jgavrsns) {
  string                tname   = "AGENT_KEY_GCV_NEEDS_REORDER";
  PrimaryKey            pk(kqk);
  vector<string>        fields  = {"KQK", "GCV_MAP"};
  vector<StorageValue>  vals    = {StorageValue(kqk), StorageValue(jgavrsns)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres persist_agent_key_gcv_needs_reorder(string &kqk, UInt64 gcv,
                                           jv *javrsns) {
  LT("persist_agent_key_gcv_needs_reorder");
  string  sgcv      = to_string(gcv);
  sqlres  sr        = fetch_all_agent_key_gcv_needs_reorder(kqk);
  RETURN_SQL_ERROR(sr)
  jv     *jgavrsns  = &(sr.jres);
  (*jgavrsns)[sgcv] = *javrsns;
  return save_agent_key_gcv_needs_reorder(kqk, jgavrsns);
}

sqlres remove_agent_key_gcv_needs_reorder(string &kqk, UInt64 gcv) {
  LT("remove_agent_key_gcv_needs_reorder");
  string  sgcv     = to_string(gcv);
  sqlres  sr       = fetch_all_agent_key_gcv_needs_reorder(kqk);
  RETURN_SQL_ERROR(sr)
  jv     *jgavrsns  = &(sr.jres);
  jgavrsns->removeMember(sgcv);
  return save_agent_key_gcv_needs_reorder(kqk, jgavrsns);
}

sqlres remove_all_agent_key_gcv_needs_reorder(string &kqk) {
  LT("remove_all_agent_key_gcv_needs_reorder");
  string           tname = "AGENT_KEY_GCV_NEEDS_REORDER";
  PrimaryKey       pk(kqk);
  StorageCondition cond("KQK", kqk);
  return delete_key(tname, pk, cond);
}


sqlres persist_agent_delta_gcv_needs_reorder(string &kqk, string &avrsn) {
  LT("persist_agent_delta_gcv_needs_reorder");
  Int64                 one    = 1;
  string                kavkey = zs_get_kav_key(kqk, avrsn);
  string                tname  = "AGENT_DELTA_OOO_GCV";
  PrimaryKey            pk(kavkey);
  vector<string>        fields = {"KAV_KEY", "VALUE"};
  vector<StorageValue>  vals   = {StorageValue(kavkey), StorageValue(one)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres fetch_agent_delta_gcv_needs_reorder(string &kqk, string &avrsn) {
  LT("fetch_agent_delta_gcv_needs_reorder");
  string           kavkey = zs_get_kav_key(kqk, avrsn);
  string           tname  = "AGENT_DELTA_OOO_GCV";
  PrimaryKey       pk(kavkey);
  string           fname = "VALUE";
  StorageCondition cond("KAV_KEY", kavkey);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}

sqlres remove_agent_delta_gcv_needs_reorder(string &kqk, string &avrsn) {
  LT("remove_agent_delta_gcv_needs_reorder");
  string           kavkey = zs_get_kav_key(kqk, avrsn);
  string           tname  = "AGENT_DELTA_OOO_GCV";
  PrimaryKey       pk(kavkey);
  StorageCondition cond("KAV_KEY", kavkey);
  return delete_key(tname, pk, cond);
}


sqlres persist_agent_key_ooo_gcv(string &kqk, UInt64 auuid) {
  LT("persist_agent_key_ooo_gcv");
  Int64                 one    = 1;
  string                kakey  = zs_get_ka_key(kqk, auuid);
  string                tname  = "AGENT_KEY_OOO_GCV";
  PrimaryKey            pk(kakey);
  vector<string>        fields = {"KA_KEY", "VALUE"};
  vector<StorageValue>  vals   = {StorageValue(kakey), StorageValue(one)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres fetch_agent_key_ooo_gcv(string &kqk, UInt64 auuid) {
  LT("fetch_agent_key_ooo_gcv");
  string           kakey = zs_get_ka_key(kqk, auuid);
  string           tname = "AGENT_KEY_OOO_GCV";
  PrimaryKey       pk(kakey);
  string           fname = "VALUE";
  StorageCondition cond("KA_KEY", kakey);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}

sqlres remove_agent_key_ooo_gcv(string &kqk, UInt64 auuid) {
  LT("remove_agent_key_ooo_gcv");
  string           kakey = zs_get_ka_key(kqk, auuid);
  string           tname = "AGENT_KEY_OOO_GCV";
  PrimaryKey       pk(kakey);
  StorageCondition cond("KA_KEY", kakey);
  return delete_key(tname, pk, cond);
}


sqlres increment_ooo_key(string &kqk, Int64 byval) { LT("increment_ooo_key");
  string           tname = "OOO_KEY";
  PrimaryKey       pk(kqk);
  string           fname = "COUNT";
  StorageCondition cond("KQK", kqk);
  return update_increment_field(tname, pk, fname, byval, cond);
}

sqlres fetch_ooo_key(string &kqk) { LT("fetch_ooo_key");
  string           tname = "OOO_KEY";
  PrimaryKey       pk(kqk);
  string           fname = "COUNT";
  StorageCondition cond("KQK", kqk);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}

sqlres remove_ooo_key(string &kqk) { LT("remove_ooo_key");
  string           tname = "OOO_KEY";
  PrimaryKey       pk(kqk);
  StorageCondition cond("KQK", kqk);
  return delete_key(tname, pk, cond);
}


sqlres persist_ooo_delta(string &kqk, jv *jauthor) { LT("persist_ooo_delta");
  UInt64                one   = 1;
  string                dkey  = get_ks_author(kqk, jauthor);
  string                tname  = "OOO_DELTA";
  PrimaryKey            pk(dkey);
  vector<string>        fields = {"DELTA_KEY", "VALUE"};
  vector<StorageValue>  vals   = {StorageValue(dkey), StorageValue(one)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres fetch_ooo_delta(string &kqk, jv *jauthor) { LT("fetch_ooo_delta");
  string           dkey  = get_ks_author(kqk, jauthor);
  string           tname = "OOO_DELTA";
  PrimaryKey       pk(dkey);
  string           fname = "VALUE";
  StorageCondition cond("DELTA_KEY", dkey);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}

sqlres remove_ooo_delta(string &kqk, jv *jauthor) { LT("remove_ooo_delta");
  string           dkey  = get_ks_author(kqk, jauthor);
  string           tname = "OOO_DELTA";
  PrimaryKey       pk(dkey);
  StorageCondition cond("DELTA_KEY", dkey);
  return delete_key(tname, pk, cond);
}


// OOODELTA_VERSIONS.AGENT_VERSIONS populated @PERSIST/REMOVE
sqlres fetch_ooo_key_delta_versions(string &kqk) {
  LT("fetch_ooo_key_delta_versions");
  string           tname  = "OOO_DELTA_VERSIONS";
  PrimaryKey       pk(kqk);
  string           fname = "AGENT_VERSIONS";
  StorageCondition cond("KQK", kqk);
  return fetch_field(tname, pk, fname, cond, VT_ARRAY);
}

static sqlres modify_ooo_key_delta_version(string &kqk, string &avrsn,
                                           bool remove) {
  LT("modify_ooo_key_delta_version");
  sqlres            sr      = fetch_ooo_key_delta_versions(kqk);
  RETURN_SQL_ERROR(sr)
  jv               *javrsns = &(sr.jres);
  jv                javrsn  = avrsn;
  if (!remove) zh_jv_append(javrsns, &javrsn);      // PERSIST
  else         zh_jv_string_splice(javrsns, avrsn); // REMOVE
  string            tname   = "OOO_DELTA_VERSIONS";
  PrimaryKey        pk(kqk);
  StorageCondition  cond("KQK", kqk);
  if (javrsns->size() == 0) {
    return delete_key(tname, pk, cond);
  } else {
    vector<string>        fields  = {"KQK", "AGENT_VERSIONS"};
    vector<StorageValue>  vals    = {StorageValue(kqk), StorageValue(javrsns)};
    return set_fields(tname, pk, fields, vals, false);
  }
}

sqlres persist_ooo_key_delta_version(string &kqk, string &avrsn) {
  LT("persist_ooo_key_delta_version");
  return modify_ooo_key_delta_version(kqk, avrsn, false);
}

sqlres remove_ooo_key_delta_version(string &kqk, string &avrsn) {
  LT("remove_ooo_key_delta_version");
  return modify_ooo_key_delta_version(kqk, avrsn, true);
}


sqlres persist_delta_committed(string &kqk, jv *jauthor) {
  LT("persist_delta_committed");
  Int64                 one    = 1;
  string                dkey   = get_ks_author(kqk, jauthor);
  string                tname  = "DELTA_COMMITTED";
  PrimaryKey            pk(dkey);
  vector<string>        fields = {"DELTA_KEY", "VALUE"};
  vector<StorageValue>  vals   = {StorageValue(kqk), StorageValue(one)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres fetch_delta_committed(string &kqk, jv *jauthor) {
  LT("fetch_delta_committed");
  string           dkey  = get_ks_author(kqk, jauthor);
  string           tname = "DELTA_COMMITTED";
  PrimaryKey       pk(dkey);
  string           fname = "VALUE";
  StorageCondition cond("DELTA_KEY", dkey);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}


sqlres persist_remove_reference_delta(string &kqk, jv *jauthor) {
  LT("persist_remove_reference_delta");
  Int64                 one    = 1;
  string                dkey   = get_ks_author(kqk, jauthor);
  string                tname  = "REMOVE_REFERENCE_DELTA";
  PrimaryKey            pk(dkey);
  vector<string>        fields = {"DELTA_KEY", "VALUE"};
  vector<StorageValue>  vals   = {StorageValue(kqk), StorageValue(one)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres fetch_remove_reference_delta(string &kqk, jv *jauthor) {
  LT("fetch_remove_reference_delta");
  string           dkey  = get_ks_author(kqk, jauthor);
  string           tname = "REMOVE_REFERENCE_DELTA";
  PrimaryKey       pk(dkey);
  string           fname = "VALUE";
  StorageCondition cond("DELTA_KEY", dkey);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}

sqlres remove_remove_reference_delta(string &kqk, jv *jauthor) {
  LT("remove_remove_reference_delta");
  string           dkey  = get_ks_author(kqk, jauthor);
  string           tname = "REMOVE_REFERENCE_DELTA";
  PrimaryKey       pk(dkey);
  StorageCondition cond("DELTA_KEY", dkey);
  return delete_key(tname, pk, cond);
}


sqlres persist_key_info_field(string &kqk, string fname, UInt64 val) {
  LT("persist_key_info_field");
  string                tname   = "KEY_INFO";
  PrimaryKey            pk(kqk);
  vector<string>        fields   = {"KQK", fname};
  vector<StorageValue>  vals     = {StorageValue(kqk), StorageValue(val)};
  return set_fields(tname, pk, fields, vals, true);
}

sqlres remove_key_info_field(string &kqk, string fname) {
  LT("remove_key_info_field");
  UInt64 val = 0;
  return persist_key_info_field(kqk, fname, val);
}

sqlres fetch_key_info(string &kqk, string fname) {
  LT("fetch_key_info");
  string           tname = "KEY_INFO";
  PrimaryKey       pk(kqk);
  StorageCondition cond("KQK", kqk);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}

sqlres remove_key_info(string &kqk) {
  LT("remove_key_info");
  string           tname = "KEY_INFO";
  PrimaryKey       pk(kqk);
  StorageCondition cond("KQK", kqk);
  return delete_key(tname, pk, cond);
}


sqlres persist_cached_keys(string &kqk) {
  LT("persist_cached_keys");
  UInt64               one    = 1;
  string               tname  = "CACHED_KEYS";
  PrimaryKey           pk(kqk);
  vector<string>       fields = {"KQK", "VALUE"};
  vector<StorageValue> vals   = {StorageValue(kqk), StorageValue(one)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres fetch_cached_keys(string &kqk) {
  LT("fetch_cached_keys");
  string           tname  = "CACHED_KEYS";
  PrimaryKey       pk(kqk);
  string           fname = "VALUE";
  StorageCondition cond("KQK", kqk);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}

sqlres remove_cached_keys(string &kqk) {
  LT("remove_cached_keys");
  string           tname  = "CACHED_KEYS";
  PrimaryKey       pk(kqk);
  StorageCondition cond("KQK", kqk);
  return delete_key(tname, pk, cond);
}


sqlres persist_evicted(string &kqk){
  LT("persist_evicted");
  UInt64               one    = 1;
  string               tname  = "EVICTED_KEYS";
  PrimaryKey           pk(kqk);
  vector<string>       fields = {"KQK", "VALUE"};
  vector<StorageValue> vals   = {StorageValue(kqk), StorageValue(one)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres fetch_evicted(string &kqk){
  LT("fetch_evicted");
  string           tname  = "EVICTED_KEYS";
  PrimaryKey       pk(kqk);
  string           fname = "VALUE";
  StorageCondition cond("KQK", kqk);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}

sqlres remove_evicted(string &kqk){
  LT("remove_evicted");
  string           tname  = "EVICTED_KEYS";
  PrimaryKey       pk(kqk);
  StorageCondition cond("KQK", kqk);
  return delete_key(tname, pk, cond);
}


sqlres persist_lru_key_fields(string &kqk, vector<string> fields,
                              vector<StorageValue> vals) {
  LT("persist_lru_key_fields");
  string     tname  = "LRU_KEYS";
  PrimaryKey pk(kqk);
  return set_fields(tname, pk, fields, vals, true);
}

sqlres persist_lru_key_field(string &kqk, string fname, UInt64 val) {
  LT("persist_lru_key_field");
  string               tname  = "LRU_KEYS";
  PrimaryKey           pk(kqk);
  vector<string>       fields = {"KQK", fname};
  vector<StorageValue> vals   = {StorageValue(kqk), StorageValue(val)};
  return set_fields(tname, pk, fields, vals, true);
}

sqlres fetch_lru_key_field(string &kqk, string fname) {
  LT("fetch_lru_key_field");
  string           tname = "LRU_KEYS";
  PrimaryKey       pk(kqk);
  StorageCondition cond("KQK", kqk);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}

sqlres remove_lru_key_field(string &kqk, string fname) {
  LT("remove_lru_key_field");
  UInt64 val = 0;
  return persist_lru_key_field(kqk, fname, val);
}

sqlres remove_lru_key(string &kqk) {
  LT("remove_lru_key");
  string           tname  = "LRU_KEYS";
  PrimaryKey       pk(kqk);
  StorageCondition cond("KQK", kqk);
  return delete_key(tname, pk, cond);
}

sqlres persist_device_key(string &dkey) {
  LT("persist_device_key");
  string               ref("ME");
  string               tname  = "DEVICE_KEY";
  PrimaryKey           pk(ref);
  vector<string>       fields = {"REFERENCE", "KEY"};
  vector<StorageValue> vals   = {StorageValue(ref), StorageValue(dkey)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres fetch_device_key() {
  LT("fetch_device_key");
  string           ref("ME");
  string           tname = "DEVICE_KEY";
  PrimaryKey       pk(ref);
  string           fname = "KEY";
  StorageCondition cond("REFERENCE", ref);
  return fetch_field(tname, pk, fname, cond, VT_STRING);
}

sqlres persist_notify_set() {
  LT("persist_notify_set");
  string               ref("ME");
  UInt64               one    = 1;
  string               tname  = "NOTIFY";
  PrimaryKey           pk(ref);
  vector<string>       fields = {"REFERENCE", "VALUE"};
  vector<StorageValue> vals   = {StorageValue(ref), StorageValue(one)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres fetch_notify_set() {
  LT("fetch_notify_set");
  string           ref("ME");
  string           tname = "NOTIFY";
  PrimaryKey       pk(ref);
  string           fname = "VALUE";
  StorageCondition cond("REFERENCE", ref);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}

sqlres remove_notify_set() {
  LT("remove_notify_set");
  string           ref("ME");
  string           tname = "NOTIFY";
  PrimaryKey       pk(ref);
  StorageCondition cond("REFERENCE", ref);
  return delete_key(tname, pk, cond);
}


sqlres persist_notify_url(string &url) {
  LT("persist_notify_url");
  UInt64               one    = 1;
  string               tname  = "NOTIFY_URL";
  PrimaryKey           pk(url);
  vector<string>       fields = {"URL", "VALUE"};
  vector<StorageValue> vals   = {StorageValue(url), StorageValue(one)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres remove_notify_url(string &url) {
  LT("remove_notify_url");
  string           tname = "NOTIFY_URL";
  PrimaryKey       pk(url);
  StorageCondition cond("URL", url);
  return delete_key(tname, pk, cond);
}

sqlres fetch_all_notify_urls() {
  LT("fetch_all_notify_urls");
  string         tname  = "NOTIFY_URL";
  vector<string> fields = {"URL"};
  return scan_fetch_fields_array(tname, fields);
}

sqlres persist_need_merge_request_id(string &kqk, string &rid) {
  string               tname  = "NEED_MERGE_REQUEST_ID";
  PrimaryKey           pk(kqk);
  vector<string>       fields = {"KQK", "ID"};
  vector<StorageValue> vals   = {StorageValue(kqk), StorageValue(rid)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres fetch_need_merge_request_id(string &kqk) {
  string           tname = "NEED_MERGE_REQUEST_ID";
  PrimaryKey       pk(kqk);
  string           fname = "ID";
  StorageCondition cond("KQK", kqk);
  return fetch_field(tname, pk, fname, cond, VT_STRING);
}


sqlres persist_agent_key_gc_wait_incomplete(string &kqk) {
  LT("persist_agent_key_gc_wait_incomplete");
  UInt64                one    = 1;
  string                tname  = "AGENT_KEY_GC_WAIT_INCOMPLETE";
  PrimaryKey            pk(kqk);
  vector<string>        fields = {"KQK", "VALUE"};
  vector<StorageValue>  vals   = {StorageValue(kqk), StorageValue(one)};
  return set_fields(tname, pk, fields, vals, false);
}

sqlres fetch_agent_key_gc_wait_incomplete(string &kqk) {
  LT("fetch_agent_key_gc_wait_incomplete");
  string           tname = "AGENT_KEY_GC_WAIT_INCOMPLETE";
  PrimaryKey       pk(kqk);
  string           fname = "VALUE";
  StorageCondition cond("KQK", kqk);
  return fetch_field(tname, pk, fname, cond, VT_UINT);
}

sqlres remove_agent_key_gc_wait_incomplete(string &kqk) {
  LT("remove_agent_key_gc_wait_incomplete");
  string           tname = "AGENT_KEY_GC_WAIT_INCOMPLETE";
  PrimaryKey       pk(kqk);
  StorageCondition cond("KQK", kqk);
  return delete_key(tname, pk, cond);
}

