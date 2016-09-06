
#ifndef __DATANET_STORAGE__H
#define __DATANET_STORAGE__H

#include <string>

#include "fixlog.h"
#include "helper.h"

using namespace std;

#define pair_UInt64 pair<UInt64, UInt64>
typedef map<string, jv> MemoryTable;

#include "storage_memory_lmdb.h"

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INITIALIZATION ------------------------------------------------------------

bool storage_init_primary   (void *p, const char *dfile, const char *ldbd);
bool storage_init_nonprimary(void *p, const char *dfile, const char *ldbd);
bool storage_cleanup();

void storage_populate_table_primary_key_map();


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// BASE DATA CLASS -----------------------------------------------------------

class sqlres {
  public :
    sqlres() {
      ures = 0;
    }
    sqlres(string s) {
      sqlres();
      sres = s;
    }
    sqlres(UInt64 u) {
      sqlres();
      ures = u;
    }
    string         err;
    UInt64         ures;
    string         sres;
    jv             jres;
};

#define PROCESS_GENERIC_ERROR(id, sr)                          \
  if (sr.err.size()) {                                         \
    LOG(ERROR) << "PROCESS_GENERIC_ERROR: " << sr.err << endl; \
    return zh_process_error(id, -32007, sr.err);                  \
  }

#define RETURN_SQL_ERROR(sr) \
  if (sr.err.size()) { return sr; }

#define RETURN_SQL_ERROR_AS_JNULL(sr) \
  if (sr.err.size()) { return JNULL; }

#define RETURN_SQL_RESULT_UINT(u) \
  { sqlres srret;                 \
    srret.ures = u;               \
    return srret;  }

#define RETURN_SQL_RESULT_STRING(s) \
  { sqlres srret;                   \
    srret.sres = s;                 \
    return srret;  }

#define RETURN_SQL_RESULT_JSON(j) \
  { sqlres srret;                 \
    srret.jres = j;               \
    return srret;  }

#define RETURN_SQL_RESULT_ERROR(e) \
  { sqlres srret;                  \
    srret.err = e;                 \
    return srret;  }

#define ACTIVATE_FIXLOG_RETURN_SQL_ERROR(sr) \
  if (sr.err.size()) {                       \
    fixlog_activate();                       \
   return sr;                                \
  }

#define RETURN_EMPTY_SQLRES {sqlres sr; return sr;}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PLUGIN ABSTRACTION --------------------------------------------------------

void zstorage_cleanup_crdt_before_persist(jv *jcrdt);

bool is_shared_table(string &tname);

bool   plugin_set_key(string &tname, string &pks, jv &jval);
jv     plugin_get_key(string &tname, string &pks);
jv     plugin_get_key_field(string &tname, string &pks, string &fname);
bool   plugin_add_key(string &tname, string &pks);
bool   plugin_delete_key(string &tname, string &pks);
bool   plugin_delete_table(string &tname);
jv     plugin_get_table(string &tname);


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// QUERY ABSTRACTION ---------------------------------------------------------

class PrimaryKey {
  public :
    PrimaryKey();
    PrimaryKey(Int64 i);
    PrimaryKey(UInt64 u);
    PrimaryKey(string &s);
    string& to_string();
  private :
    string _pk;
};

typedef enum {VT_UINT, VT_STRING, VT_ARRAY, VT_JMAP} value_type;

class StorageValue {
  public :
    StorageValue();
    StorageValue(Int64 i);
    StorageValue(UInt64 u);
    StorageValue(string &s);
    StorageValue(jv     *jval);
    jv     raw();
    string to_string();

  private :
    jv     _raw;
    string _val;
};

typedef enum {STORAGE_COND_EQ,
              STORAGE_COND_LTE,
              STORAGE_COND_GTE} storage_cmp_type;

class SingleStorageCondition {
  public :
    SingleStorageCondition() {}
    string           key;
    string           value;
    Int64            ivalue;
    storage_cmp_type condition;
};

class StorageCondition {
  public :
    StorageCondition(string key_name, string &key_value);
    StorageCondition(string key_name, Int64 key_value);
    StorageCondition(string key_name, Int64 key_value, storage_cmp_type sct);
    void add(string key_name, string &key_value);
    void add(string key_name, Int64 key_value);
    void add(string key_name, Int64 key_value, storage_cmp_type sct);
    string to_string();

    unsigned int size() { return _knames.size(); }
    SingleStorageCondition get(unsigned int i);

  private :
    vector<string>           _knames;
    vector<string>           _kvals;
    vector<Int64>            _kivals;
    vector<storage_cmp_type> _kscts;
};


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ABSTRACT DATA CALLS -------------------------------------------------------

sqlres lock_field  (string &tname, PrimaryKey &pk, StorageCondition &cond);
sqlres unlock_field(string &tname, PrimaryKey &pk, StorageCondition &cond);

sqlres set_fields(string &tname, PrimaryKey &pk, vector<string> &fields,
                  vector<StorageValue> &vals, bool update);

sqlres fetch_field(string &tname, PrimaryKey &pk, string fname,
                   StorageCondition &cond, value_type vt);

sqlres scan_fetch_single_field(string &tname, string fname, 
                               StorageCondition &cond, value_type vt);


sqlres scan_fetch_fields_json_map(string &tname, vector<string> &fields);
sqlres scan_fetch_fields_array   (string &tname, vector<string> &fields);
sqlres scan_fetch_all_fields     (string &tname, string &pkfield);

sqlres update_increment_field(string &tname, PrimaryKey &pk, string &fname,
                              Int64 byval, StorageCondition &cond);

sqlres delete_key(string &tname, PrimaryKey &pk, StorageCondition &cond);


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STORAGE I/O ---------------------------------------------------------------

// LOCKS
sqlres lock_cache_reaper();
sqlres unlock_cache_reaper();

// SCANNED TABLES
sqlres fetch_all_fixlog();
sqlres fetch_all_worker_uuid();
sqlres fetch_all_agent_createds();
sqlres fetch_all_user_permissions();
sqlres fetch_all_user_subscriptions();
sqlres fetch_all_stationed_users();
sqlres fetch_all_subscriber_latency_histogram();
sqlres fetch_all_dirty_deltas();
sqlres fetch_all_to_sync_keys();
sqlres fetch_all_to_evict_keys();
sqlres fetch_all_gcv_summaries();
sqlres fetch_all_lru_keys();
sqlres fetch_all_agent_dirty_keys();


sqlres persist_agent_uuid();
sqlres persist_datacenter_uuid(string &dcuuid);
sqlres startup_fetch_agent_uuid();
sqlres startup_fetch_datacenter_uuid();
sqlres persist_next_op_id(UInt64 cnt);
sqlres persist_agent_num_cache_bytes(UInt64 val);
sqlres persist_agent_backoff_end_time(UInt64 endt);
sqlres fetch_agent_backoff_end_time();


sqlres fetch_last_remove_delta(string &kqk);
sqlres persist_last_remove_delta(string &kqk, jv *jmeta);
sqlres remove_last_remove_delta(string &kqk);

sqlres fetch_gc_version(string &kqk);
sqlres persist_gc_version(string &kqk, UInt64 gcv);
sqlres remove_gc_version(string &kqk);

sqlres fetch_agent_key_max_gc_version(string &kqk);
sqlres persist_agent_key_max_gc_version(string &kqk, UInt64 gcv);
sqlres remove_agent_key_max_gc_version(string &kqk);

sqlres persist_crdt(string &kqk, jv *jcrdt);
sqlres fetch_crdt(string &kqk);
sqlres remove_crdt(string &kqk);

sqlres persist_agent_createds(jv *jcreateds);
sqlres fetch_agent_created(string &guuid);
sqlres remove_all_agent_created();

sqlres persist_to_sync_key(string &kqk, string &sectok);
sqlres fetch_to_sync_key(string &kqk);
sqlres remove_to_sync_key(string &kqk);

sqlres persist_to_evict_key(string &kqk);
sqlres remove_to_evict_key(string &kqk);

sqlres persist_out_of_sync_key(string &kqk);
sqlres fetch_out_of_sync_key(string &kqk);
sqlres remove_out_of_sync_key(string &kqk);

sqlres persist_delta(string &kqk, string &avrsn, jv *jdentry, jv *jauth);
sqlres update_delta(string &kqk, string &avrsn, jv *jdentry);
sqlres remove_delta(string &kqk, string &avrsn);

sqlres fetch_agent_delta(string &kqk, string &avrsn);
sqlres fetch_agent_delta_auth(string &kqk, string &avrsn);
sqlres fetch_subscriber_delta(string &kqk, string &avrsn);


sqlres fetch_subscriber_version(string &kqk, string &avrsn);
sqlres persist_subscriber_version(string &kqk, UInt64 auuid, string &avrsn);
sqlres remove_subscriber_version(string &kqk, string &avrsn);
sqlres fetch_subscriber_versions(string &kqk);
sqlres fetch_subscriber_versions_map(string &kqk);

sqlres persist_subscriber_delta_version(string &kqk, string &avrsn);
sqlres fetch_subscriber_delta_aversions(string &kqk);
sqlres remove_subscriber_delta_version (string &kqk, string &avrsn);

sqlres persist_persisted_subscriber_delta(string &kqk, string &avrsn);
sqlres fetch_persisted_subscriber_delta(string &kqk, string &avrsn);
sqlres remove_persisted_subscriber_delta(string &kqk, string &avrsn);

sqlres persist_agent_sent_delta(string &kqk, string &avrsn, UInt64 now);
sqlres remove_agent_sent_delta(string &kqk, string &avrsn);

sqlres persist_agent_permission(string &channel, string &perm);
sqlres persist_agent_subscription(string &channel, string &perm);
sqlres persist_permissions(jv *perms, string *username);
sqlres persist_agent_permissions(jv *perms);
sqlres persist_user_permissions(string *username, jv *perms);
sqlres persist_subcriptions(jv *subs, string *username);
sqlres persist_agent_subscriptions(jv *subs);
sqlres persist_user_subscriptions(string *username, jv *subs);
sqlres persist_fixlog(string &fid, jv *pc);
sqlres remove_fixlog(string &fid);
sqlres persist_dirty_delta(string &ddkey);
sqlres remove_dirty_subscriber_delta(string &ddkey);
sqlres persist_agent_dirty_keys(string &kqk, UInt64 ndk);
sqlres fetch_agent_dirty_keys(string &kqk);

sqlres increment_agent_num_workers(Int64 cnt);
sqlres fetch_agent_num_workers();

sqlres increment_next_op_id(UInt64 cnt);
sqlres increment_agent_num_cache_bytes(Int64 cnt);

sqlres persist_gc_summary_version(string &kqk, UInt64 gcv);
sqlres fetch_gc_summary_versions(string &kqk);
sqlres remove_gc_summary_version(string &kqk, UInt64 gcv);

sqlres persist_gcv_summary(string &kqk, UInt64 gcv, jv *jgcsumm);
sqlres fetch_gcv_summary(string &kqk, UInt64 gcv);
sqlres remove_gcv_summary(string &kqk, UInt64 gcv);

sqlres persist_stationed_user(string &username);
sqlres fetch_is_user_stationed(string &username);
sqlres remove_stationed_user(string &username);

sqlres persist_channel_device(string &schanid);
sqlres remove_channel_device(string &schanid);
sqlres persist_device_subscription(string &schanid);
sqlres remove_device_subscriptions(string &schanid);

sqlres fetch_user_subscriptions(string &username);
sqlres persist_user_subscription(string &username, string &schanid,
                                 string &perm);
sqlres fetch_user_subscription(string &username, string &schanid);
sqlres remove_user_subscription(string &username, string &schanid);
sqlres fetch_user_subscriptions(string &username);

sqlres fetch_user_permissions(string &username);
sqlres persist_user_permission(string &username, string &schanid,
                               string &pabbr);
sqlres fetch_user_permission(string &username, string &rchan);
sqlres remove_user_permission(string &username, string &schanid);
sqlres remove_user_permissions(string &username);

sqlres persist_key_cached_by_user(string &kqk, string &username);
sqlres fetch_key_cached_by_user  (string &kqk, string &username);
sqlres fetch_users_caching_key   (string &kqk);
sqlres remove_key_cached_by_user (string &kqk, string &username);

sqlres persist_user_authentication(string &username, string &hash,
                                   string &role);

sqlres fetch_user_password_hash(string &username);

sqlres persist_worker(UInt64 wid, UInt64 wport, bool primary);
sqlres fetch_worker_port(UInt64 wid);
sqlres fetch_primary_worker_uuid();
sqlres fetch_primary_worker_port();
sqlres remove_worker(UInt64 wid);

sqlres persist_worker_partition(UInt64 pid, UInt64 partition);
sqlres fetch_worker_partition(UInt64 pid);

sqlres persist_callback_server(const char *cbhost, unsigned int cbport);
sqlres fetch_callback_server_hostname();
sqlres fetch_callback_server_port();

sqlres persist_agent_heartbeat_info(jv *ahb);
sqlres fetch_agent_heartbeat_info();

sqlres persist_agent_connected(bool b);
sqlres fetch_agent_connected();
sqlres persist_isolation(bool b);
sqlres fetch_isolation();

sqlres persist_subscriber_latency_histogram(string &rguuid, jv *jhist);
sqlres fetch_subscriber_latency_histogram(string &rguuid);

sqlres persist_agent_watch_keys(string &kqk);
sqlres fetch_agent_watch_keys(string &kqk);
sqlres remove_agent_watch_keys(string &kqk);

sqlres fetch_agent_version(string &kqk);
sqlres persist_agent_version(string &kqk, string &avrsn);

sqlres fetch_device_subscriber_version(string &kqk, UInt64 auuid);
sqlres fetch_device_subscriber_version_timestamp(string &kqk, UInt64 auuid);
sqlres persist_device_subscriber_version(string &kqk, UInt64 auuid,
                                         string &avrsn);
sqlres remove_device_subscriber_version(string &kqk, UInt64 auuid);

sqlres persist_per_key_subscriber_version(string &kqk, UInt64 auuid,
                                          string &avrsn);
sqlres remove_per_key_subscriber_version(string &kqk, UInt64 auuid);
sqlres fetch_per_key_subscriber_versions(string &kqk);


sqlres persist_key_repchans(string &kqk, jv *jrchans);
sqlres remove_key_repchans(string &kqk);

sqlres persist_need_reorder(string &kqk, jv *jauthor);
sqlres fetch_need_reorder(string &kqk, jv *jauthor);
sqlres remove_need_reorder(string &kqk, jv *jauthor);


sqlres persist_delta_need_reference(string &kqk, jv *reo_author,
                                    jv *ref_author);
sqlres fetch_delta_need_reference(string &kqk, jv *reo_author);
sqlres remove_delta_need_reference(string &kqk, jv *reo_author);


sqlres persist_delta_need_reorder(string &kqk, jv *ref_author,
                                  jv *reo_author);
sqlres fetch_delta_need_reorder(string &kqk, jv *ref_author);
sqlres remove_delta_need_reorder(string &kqk, jv *ref_author); 


sqlres fetch_all_agent_key_gcv_needs_reorder(string &kqk);
sqlres fetch_agent_key_gcv_needs_reorder(string &kqk, UInt64 gcv);
sqlres persist_agent_key_gcv_needs_reorder(string &kqk, UInt64 gcv,
                                           jv *javrsns);
sqlres remove_agent_key_gcv_needs_reorder(string &kqk, UInt64 gcv);
sqlres remove_all_agent_key_gcv_needs_reorder(string &kqk);

sqlres persist_agent_delta_gcv_needs_reorder(string &kqk, string &avrsn);
sqlres fetch_agent_delta_gcv_needs_reorder(string &kqk, string &avrsn);
sqlres remove_agent_delta_gcv_needs_reorder(string &kqk, string &avrsn);


sqlres persist_agent_key_ooo_gcv(string &kqk, UInt64 auuid);
sqlres fetch_agent_key_ooo_gcv(string &kqk, UInt64 auuid);
sqlres remove_agent_key_ooo_gcv(string &kqk, UInt64 auuid);

sqlres increment_ooo_key(string &kqk, Int64 byval);
sqlres fetch_ooo_key(string &kqk);
sqlres remove_ooo_key(string &kqk);

sqlres persist_ooo_delta(string &kqk, jv *jauthor);
sqlres fetch_ooo_delta(string &kqk, jv *jauthor);
sqlres remove_ooo_delta(string &kqk, jv *jauthor);

sqlres fetch_ooo_key_delta_version(string &kqk, string &avrsn);
sqlres persist_ooo_key_delta_version(string &kqk, string &avrsn);
sqlres remove_ooo_key_delta_version(string &kqk, string &avrsn);
sqlres fetch_ooo_key_delta_versions(string &kqk);

sqlres persist_delta_committed(string &kqk, jv *jauthor);
sqlres fetch_delta_committed(string &kqk, jv *jauthor);

sqlres persist_remove_reference_delta(string &kqk, jv *jauthor);
sqlres fetch_remove_reference_delta(string &kqk, jv *jauthor);
sqlres remove_remove_reference_delta(string &kqk, jv *jauthor) ;

sqlres persist_key_info_field(string &kqk, string fname, UInt64 val);
sqlres remove_key_info_field(string &kqk, string fname);
sqlres fetch_key_info(string &kqk, string fname);
sqlres remove_key_info(string &kqk);

sqlres persist_cached_keys(string &kqk);
sqlres fetch_cached_keys(string &kqk);
sqlres remove_cached_keys(string &kqk);

sqlres persist_evicted(string &kqk);
sqlres fetch_evicted(string &kqk);
sqlres remove_evicted(string &kqk);

sqlres persist_lru_key_fields(string &kqk, vector<string> fields, 
                              vector<StorageValue> vals);
sqlres persist_lru_key_field(string &kqk, string fname, UInt64 val);
sqlres fetch_lru_key_field(string &kqk, string fname);
sqlres remove_lru_key_field(string &kqk, string fname);
sqlres remove_lru_key(string &kqk);

sqlres persist_device_key(string &dkey);
sqlres fetch_device_key();

sqlres persist_notify_set();
sqlres fetch_notify_set();
sqlres remove_notify_set();

sqlres persist_notify_url(string &url);
sqlres remove_notify_url(string &url);
sqlres fetch_all_notify_urls();

sqlres persist_need_merge_request_id(string &kqk, string &rid);
sqlres fetch_need_merge_request_id(string &kqk);

sqlres persist_agent_key_gc_wait_incomplete(string &kqk);
sqlres fetch_agent_key_gc_wait_incomplete(string &kqk);
sqlres remove_agent_key_gc_wait_incomplete(string &kqk);

#endif
