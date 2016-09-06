
#ifndef __DATANET_HELPER__H
#define __DATANET_HELPER__H

#include <string>
#include <vector>

extern "C" {
  #include "lua.h"
  #include "lualib.h"
  #include "lauxlib.h"
}

using namespace std;

class sqlres; // FORWARD DECLARATION

typedef unsigned int uint;
typedef vector<char> byte_buffer;

#define BUFFER_SIZE   16384
#define BUFFER_LENGTH (16384 - 1)

typedef Json::Value jv;
typedef jv::UInt64  UInt64;
typedef jv::Int64   Int64;

#define JARRAY      Json::arrayValue
#define JOBJECT     Json::objectValue
#define JNULL       Json::nullValue
#define JNULLSTRING Json::Value("null")

typedef bool (*compare_jv)(jv *a, jv *b);

typedef struct get_jv {
  jv      op_path;
  jv     *got;
  string  gkey;
  jv     *jdelta;
  jv     *jcrdt;
} gv;

#define CHAOS_MODE_ENTRY_POINT(num)                                          \
  if (ChaosMode == num) {                                                    \
    sr.err = ChaosDescription[ChaosMode];                                    \
    LOG(ERROR) << "CHAOS-MODE: " << ChaosMode << " ERR: " << sr.err << endl; \
    return sr;                                                               \
  }

string zh_get_pid_tid();

enum log_level_t {TRACE = 0, NOTICE = 1, ERROR = 2};
extern log_level_t LogLevel;

#define LT(text)                                                   \
  if (LogLevel == TRACE) {                                         \
    LOG(TRACE) << zh_get_pid_tid() << " (TRACE) " << text << endl; \
  } else /* NO-OP */  /* NOTE: NO SEMI-COLON AT END */

#define LD(text)                                                    \
  if (LogLevel != ERROR) {                                          \
    LOG(DEBUG) << zh_get_pid_tid() << " (NOTICE) " << text << endl; \
  } else /* NO-OP */  /* NOTE: NO SEMI-COLON AT END */

#define LE(text)                                                    \
  LOG(ERROR) << zh_get_pid_tid() << " (ERROR) " << text << endl

string zh_create_internal_rpc_id();
jv     zh_create_json_rpc_body(string method, jv *jauth);
sqlres zh_get_agent_offline();

vector<Int64> zh_get_worker_uuids();

void zh_init();
string zh_get_debug_json_value(jv *jval);
void zh_debug_json_value(string name, jv *jval);
void zh_debug_vector_json_value(string &name, vector<jv> *vjv);

bool zjn(jv *x);
bool zjd(jv *x);

jv     zh_get_existing_member(jv *jdmeta, string mname);
bool   zh_get_bool_member    (jv *jdmeta, string mname);
UInt64 zh_get_uint64_member  (jv *jdmeta, string mname);
string zh_get_string_member  (jv *jdmeta, string mname);

vector<string> &split(const string &s, char delim, vector<string> &elems);
vector<string> split(const string &s, char delim);
string get_key(string kqk);

string zh_join(jv *jarr, string delim);

string zh_create_avrsn(UInt64 duuid, UInt64 avnum);
UInt64 zh_get_avnum(string &avrsn);
UInt64 zh_get_avuuid(string &avrsn);

void   zh_remove_central_dependencies(jv *jdeps);
bool   zh_check_ooo_dependencies(string &kqk, string &avrsn, 
                                 jv *jdeps, jv *mavrsns);
string zh_create_kqk(string ns, string cn, string key);
string zh_create_kqk_from_meta(jv *meta);
jv zh_create_ks(string kqk, const char *sectok);
jv zh_get_my_device_agent_info();
jv zh_init_pre_commit_info(string op, jv *jks, jv *dentry,
                           jv *xcrdt, jv *edata);
UInt64 zh_get_gcv_version(jv *jmeta);
UInt64 zh_get_dentry_gcversion(jv *jdentry);
UInt64 zh_get_ms_time();

jv zh_format_crdt_for_storage(string kqk, jv *jmeta, jv *jmember);
void zh_set_new_crdt(jv *pc, jv *md, jv *jncrdt, bool do_gcv);
void zh_override_pc_ncrdt_with_md_gcv(jv *pc);

string zh_to_upper(string &s);
bool   zh_validate_datatype(string &dtype);

string zh_summarize_delta(string kqk, jv *jdentry);
jv zh_create_pretty_json(jv *jcrdt);

jv zh_create_dentry(jv *jdelta, string kqk);
#define ZH_CREATE_DENTRY(srret, jdelta, kqk)                        \
{                                                                   \
  jv jks              = zh_create_ks(kqk, NULL);                    \
  srret.jres          = JOBJECT;                                    \
  jv *jdentry         = &(srret.jres);                              \
  (*jdentry)["_"]     = (*jdelta)["_meta"]["author"]["agent_uuid"]; \
  (*jdentry)["#"]     = (*jdelta)["_meta"]["delta_version"];        \
  (*jdentry)["ns"]    = jks["ns"];                                  \
  (*jdentry)["cn"]    = jks["cn"];                                  \
  (*jdentry)["delta"] = *jdelta;                                    \
}


bool zh_jv_append(jv *aval, jv *jval);
string zh_path_get_dot_notation(jv *op_path);

void zh_type_object_assert(jv *jval, const char *desc);
bool zh_jv_is_member(jv *jval, const char *name);
vector<string> zh_jv_get_member_names(jv *jval);
bool zh_jv_is_member(jv *jval, string &sname);
bool zh_jv_is_non_null_member(jv *jval, const char *name);
bool zh_calculate_rchan_match(jv *pc, jv *jocrdt, jv *jdmeta);
jv   zh_jv_shift(jv *jval);
void zh_jv_pop(jv *jval);
void zh_jv_splice(jv *carr, int x);
void zh_jv_value_splice (jv *carr, UInt64  v);
void zh_jv_string_splice(jv *carr, string &s);
void zh_jv_insert(jv *carr, Int64 leaf, jv *aval);
void zh_jv_sort(jv *carr, compare_jv cfunc);
jv zh_convert_numbered_object_to_sorted_array(jv *jgcsumms);

jv zh_parse_json_text(string &sjson, bool debug);
jv zh_parse_json_text(const char *cjson,  bool debug);

bool zh_is_json_element_int(string &key);
jv zh_parse_json_input_text(string &sjson);
jv zh_create_head_lhn();

jv zh_init_delta(jv *cmeta);
void zh_init_author(jv *jmeta, jv *jdeps);
jv zh_init_meta(string &kqk, jv *rchans, UInt64 expiration, string &sfname);

jv *zh_get_replication_channels(jv *jmeta);

jv *zh_lookup_by_dot_notation(jv *jjson, string &dots);
vector<string> zh_parse_op_path(string &path);

jv zh_create_jv_response_ok(string &id);
void zh_push_gcv_on_vector_jv_map(map <string, vector<jv>> *vjmap,
                                  string &kqk, jv *gcv);

string zh_convert_jv_to_string(jv *prequest);

void zh_fatal_error(const char *msg);

string zh_local_hash(string &password);

bool zh_is_ordered_array(jv *jcdata);
bool zh_is_large_list(jv *jcdata);
UInt64 zh_get_ordered_array_min(jv *pclm);
bool zh_is_late_comer(jv *jcdata);

bool zh_isNaN(string &s);

bool zh_same_master(jv *cn1, jv *cn2);

bool zh_is_primary_thread();
bool zh_is_reconnect_thread();
bool zh_is_worker_thread();

unsigned int zh_crc32(string &s);
bool zh_is_key_local_to_worker(string &kqk);

void zh_remove_repeat_author(jv *jauthors);

string zh_get_connection_name();
bool zh_handle_lua_error(lua_State *L);
jv zh_process_error(string &id, int code, string &message);

#endif
