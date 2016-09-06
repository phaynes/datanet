#include <cstdio>
#include <cstring>
#include <string>
#include <sstream>  
#include <iostream>
#include <vector>
#include <algorithm>

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

#include "Crc32.h"
#include "json/json.h"
#include "easylogging++.h"

#include "sha256.h"
SHA256 sha256;

#include "helper.h"
#include "convert.h"

using namespace std;

extern lua_State *GL;

extern Int64  MyUUID;
extern string MyDataCenterUUID;
extern jv     CentralMaster;

extern Int64  MyWorkerUUID;

extern bool   AmPrimaryProcess;
extern bool   EnginedInited;

extern pthread_t PrimaryTid;
extern pthread_t ReconnectTid;
extern pthread_t WorkerTid;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GLOBALS -------------------------------------------------------------------

UInt64 MaxDataCenterUUID = 10000;

string DeviceKey;

bool Debug            = false;
bool DebugIOBuffers   = false;
bool NetworkDebug     = false;
bool DatabaseDisabled = false;

int ChaosMode = 0;
vector<string> ChaosDescription{"NONE",
                                "AGENT-DELTA COMMIT FAILURE",
                                "AGENT-DELTA ROLLBACK FAILURE",
                                "SUBSCRIBER-DELTA COMMIT FAILURE",
                                "SUBSCRIBER-MERGE COMMIT FAILURE",
                                "ACK-AGENT-DELTA COMMIT FAILURE",
                                "SUBSCRIBER-COMMIT-DELTA COMMIT FAILURE"};
int ChaosMax  = ChaosDescription.size();

string zs_create_kqk(jv &jns, jv &jcn, jv &jid);

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

string zh_get_pid_tid() {
  pid_t     pid   = getpid();
  pthread_t tid   = pthread_self();
  bool      isp   = (tid == PrimaryTid);
  string    trole = isp ? "P" : "W";
  string    prole = EnginedInited ? (AmPrimaryProcess ? "M" : "S") : "X";
  return "(P:" + to_string(pid) + "-T:" + to_string(tid) + ")(" +
          prole + "-" + trole + ")";
}

void zh_fatal_error(const char *msg) {
  LOG(FATAL) << "FATAL-ERROR: " << msg << endl;
  perror(msg);
  exit(-2);
}

unsigned long NextRpcId = 0;

static string GetNextRpcId() {
  NextRpcId += 1;
  char d[21];
  sprintf(d, "%lld", MyUUID);
  char w[21];
  sprintf(w, "%lld", MyWorkerUUID);
  char r[21];
  sprintf(r, "C%lu", NextRpcId);
  string m     = "-";
  string u     = "_";
  string rpcid = d + u + w + m + MyDataCenterUUID + m + + r;
  return rpcid;
}

string zh_create_internal_rpc_id() {
  return GetNextRpcId();
}

jv zh_get_my_device_agent_info() {
  jv dinfo;
  dinfo["device"]["uuid"] = MyUUID;
  if (DeviceKey.size()) dinfo["device"]["key"] = DeviceKey;
  dinfo["agent"]["uuid"]  = MyUUID;
  return dinfo;
}

jv zh_create_json_rpc_body(string method, jv *jauth) {
  jv prequest;
  prequest["jsonrpc"]        = "2.0";
  prequest["id"]             = GetNextRpcId();
  prequest["method"]         = method;
  prequest["worker_id"]      = MyWorkerUUID; // TODO: DEPRECATE WORKER_UUID
  if (jauth) prequest["params"]["authentication"] = *jauth;
  prequest["params"]["data"] = zh_get_my_device_agent_info();
  return prequest;
}

sqlres zh_get_agent_offline() {
  if (MyUUID == -1) RETURN_SQL_RESULT_UINT(1) // OFFLINE
  sqlres sr        = fetch_isolation();
  RETURN_SQL_ERROR(sr)
  bool   isolation = sr.ures ? true : false;
  if (isolation) RETURN_SQL_RESULT_UINT(1)    // OFFLINE
  sr               = fetch_agent_connected();
  bool connected   = sr.ures ? true : false;
  if (!connected) RETURN_SQL_RESULT_UINT(1)   // OFFLINE
  else            RETURN_EMPTY_SQLRES         // ONLINE
}

vector<Int64> zh_get_worker_uuids() {
  vector<Int64> wids;
  sqlres sr    = fetch_all_worker_uuid();
  if (sr.err.size()) return wids;
  if (zjn(&sr.jres)) return wids;
  jv     jwids = sr.jres;
  zh_type_object_assert(&jwids, "LOGIC(zh_get_worker_uuids)");
  for (jv::iterator it = jwids.begin(); it != jwids.end(); it++) {
    jv     jkey = it.key();
    string swid = jkey.asString();
    Int64  wid  = atol(swid.c_str());
    wids.push_back(wid);
  }
  return wids;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// JSON:VALUE HELPERS --------------------------------------------------------

void zh_type_object_assert(jv *jval, const char *desc) {
  if (!jval->isObject()) {
    LE("FAIL: zh_type_object_assert: " << *jval);
    zh_fatal_error(desc);
  }
}

bool zh_jv_is_member(jv *jval, const char *name) {
  if (!jval->isObject()) zh_fatal_error("LOGIC(zh_jv_is_member)");
  if (zjn(jval)) return false;
  return jval->isMember(name);
}

vector<string> zh_jv_get_member_names(jv *jval) {
  if (zjn(jval)) { vector<string> empty; return empty; }
  if (!jval->isObject()) zh_fatal_error("LOGIC(zh_jv_get_member_names)");
  return jval->getMemberNames();
}

bool zh_jv_is_member(jv *jval, string &sname) {
  return zh_jv_is_member(jval, sname.c_str());
}

bool zh_jv_append(jv *aval, jv *jval) {
  if (!aval->isArray()) zh_fatal_error("LOGIC(zh_jv_append)");
  aval->append(*jval);
  return true;
}

bool zh_jv_is_non_null_member(jv *jval, const char *name) {
  if (!zh_jv_is_member(jval, name)) return false;
  else {
    jv *el = &((*jval)[name]);
    return zjd(el);
  }
}

jv zh_jv_shift(jv *jval) {
  if (!jval->isArray()) zh_fatal_error("LOGIC(zh_jv_shift)");
  if (!jval->size()) return JNULL;
  else {
    jv     jcp   = (*jval);    // COPY ARRAY
    jv     jres  = (*jval)[0]; // FIRST ELEMENT
    *jval        = JARRAY;     // EMPTY ARRAY
    for (uint i = 0; i < jcp.size(); i++) {
      if (i) { // COPY ALL BUT FIRST
        jv *jitem = &(jcp[i]);
        zh_jv_append(jval, jitem);
      }
    }
    return jres; // RETURN FIRST
  }
}

void zh_jv_pop(jv *jval) {
  if (!jval->isArray()) zh_fatal_error("LOGIC(zh_jv_pop)");
  if (!jval->size()) return;
  else {
    uint nsize = jval->size() - 1;
    jval->resize(nsize);
  }
}

void zh_jv_splice(jv *carr, int x) {
  if (!carr->isArray()) zh_fatal_error("LOGIC(zh_jv_splice)");
  jv jval;
  carr->removeIndex(x, &jval);
}

void zh_jv_string_splice(jv *carr, string &s) {
  if (!carr->isArray()) zh_fatal_error("LOGIC(zh_jv_string_splice)");
  int hit = -1;
  for (int i = 0; i < (int)carr->size(); i++) {
    string as = (*carr)[i].asString();
    if (!s.compare(as)) {
      hit = i;
      break;
    }
  }
  if (hit == -1) return;
  zh_jv_splice(carr, hit);
}

// NOTE: removes ALL 'v' instances
void zh_jv_value_splice(jv *carr, UInt64 v) {
  if (!carr->isArray()) zh_fatal_error("LOGIC(zh_jv_value_splice)");
  jv jval = JARRAY;
  for (int i = 0; i < (int)carr->size(); i++) {
    UInt64 av  = (*carr)[i].asUInt64();
    if (av == v) continue;
    jv     jav = av;
    zh_jv_append(&jval, &jav);
  }
  carr->clear();
  *carr = jval;
}

void zh_jv_insert(jv *carr, Int64 leaf, jv *aval) {
  if (!carr->isArray()) zh_fatal_error("LOGIC(zh_jv_insert)");
  jv ncarr = JARRAY;
  UInt64 csize = carr->size();
  if (leaf < 0)            leaf = 0;
  if (leaf > (Int64)csize) leaf = csize;
  for (int i = 0; i < leaf; i++) {
    zh_jv_append(&ncarr, &((*carr)[i]));
  }
  zh_jv_append(&ncarr, aval);
  if (leaf != (Int64)csize) {
    for (uint i = leaf; i < csize; i++) {
      zh_jv_append(&ncarr, &((*carr)[i]));
    }
  }
  *carr = ncarr;
}

void zh_jv_sort(jv *carr, compare_jv cfunc) {
  if (!carr->isArray()) zh_fatal_error("LOGIC(zh_jv_sort)");
  if (!carr->size()) return;
  vector<jv *> jarr;
  for (uint i = 0; i < carr->size(); i++) {
    jv *cval = &((*carr)[i]);
    jarr.push_back(cval);
  }
  sort(jarr.begin(), jarr.end(), cfunc);
  jv ncarr = JARRAY;;
  for (uint i = 0; i < jarr.size(); i++) {
    zh_jv_append(&ncarr, jarr[i]);
  }
  *carr = ncarr;
}

static bool cmp_jagcsumm_gcv(jv *a_jgcsumm, jv *b_jgcsumm) {
  UInt64 a_gcv = (*a_jgcsumm)["gcv"].asUInt64();
  UInt64 b_gcv = (*b_jgcsumm)["gcv"].asUInt64();
  return (a_gcv < b_gcv);
}

jv zh_convert_numbered_object_to_sorted_array(jv *jgcsumms) {
  if (!jgcsumms->isObject()) {
    zh_fatal_error("LOGIC(zh_convert_numbered_object_to_sorted_array)");
  }
  jv jagcsumms = JARRAY;
  vector<string> mbrs = zh_jv_get_member_names(jgcsumms);
  for (uint i = 0; i < mbrs.size(); i++) {
    string  snum      = mbrs[i];
    jv     *jgcsumm   = &((*jgcsumms)[snum]);
    UInt64  num       = strtoul(snum.c_str(), NULL, 10);
    (*jgcsumm)["gcv"] = num; // NOTE: Used in cmp_jagcsumm_gcv()
    zh_jv_append(&jagcsumms, jgcsumm);
  }
  zh_jv_sort(&jagcsumms, cmp_jagcsumm_gcv);
  return jagcsumms;
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ZHELPERS ------------------------------------------------------------------

jv zh_nobody_auth;

string zh_wildcard_user = "*";

void zh_init() {
  zh_nobody_auth["username"] = "nobody";
  zh_nobody_auth["password"] = "nobody";
}

vector<string> ZH_DeltaFields {"modified",
                               "tombstoned",
                               "added",
                               "deleted",
                               "datatyped"};

string zh_get_debug_json_value(jv *jval) {
  Json::FastWriter writer;
  return writer.write(*jval);
}

void zh_debug_json_value(string name, jv *jval) {
  string sdbg = zh_get_debug_json_value(jval);
  LOG(DEBUG) << name << ": " << sdbg << endl;
}

void zh_debug_vector_json_value(string &name, vector<jv> *vjv) {
  LOG(DEBUG) << "START: VECTOR_DEBUG: " << name << endl;
  for (uint i = 0; i < vjv->size(); i++) {
    jv     *jval = &((*vjv)[i]);
    string  desc = name + "[" + to_string(i) + "]";
    zh_debug_json_value(desc, jval);
  }
  LOG(DEBUG) << "END: VECTOR_DEBUG: " << name << endl;
}

bool zjn(jv *x) {
  if (!x) return true;
  return x->isNull();
}

bool zjd(jv *x) {
  if (!x) return false;
  return !(x->isNull());
}

jv zh_get_existing_member(jv *jdmeta, string mname) {
  if (!zh_jv_is_member(jdmeta, mname)) return JNULL;
  else                                 return (*jdmeta)[mname];
}

bool zh_get_bool_member(jv *jdmeta, string mname) {
  if (!zh_jv_is_member(jdmeta, mname)) return false;
  else                                 return (*jdmeta)[mname].asBool();
}

UInt64 zh_get_uint64_member(jv *jdmeta, string mname) {
  if (!zh_jv_is_member(jdmeta, mname)) return 0;
  else                                 return (*jdmeta)[mname].asUInt64();
}

string zh_get_string_member(jv *jdmeta, string mname) {
  if (!zh_jv_is_member(jdmeta, mname)) { string empty; return empty; }
  else                                 return (*jdmeta)[mname].asString();
}

vector<string> &split(const string &s, char delim, vector<string> &elems) {
  stringstream ss(s);
  string item;
  while (getline(ss, item, delim)) {
      elems.push_back(item);
  }
  return elems;
}

vector<string> split(const string &s, char delim) {
  vector<string> elems;
  split(s, delim, elems);
  return elems;
}

string zh_join(jv *jarr, string delim) {
  string ret = "";
  for (uint i = 0; i < jarr->size(); i++) {
    ret += (*jarr)[i].asString();
    ret += delim;
  }
  return ret;
}

string zh_create_avrsn(UInt64 duuid, UInt64 avnum) {
  return to_string(duuid) + "|" + to_string(avnum);
}

UInt64 zh_get_avnum(string &avrsn) {
  if (!avrsn.size()) return 0;
  vector<string> res = split(avrsn, '|');
  return atol(res[1].c_str());
}

UInt64 zh_get_avuuid(string &avrsn) {
  if (!avrsn.size()) return 0;
  vector<string> res = split(avrsn, '|');
  return atol(res[0].c_str());
}

bool zh_check_ooo_dependencies(string &kqk, string &avrsn,
                               jv *jdeps, jv *mdeps) {

  if (zjn(jdeps)) return false; // NO DEPENDENCIES -> ALWAYS NOT OOO
  UInt64         auuid = zh_get_avuuid(avrsn);
  vector<string> mbrs  = zh_jv_get_member_names(mdeps);
  for (uint i = 0; i < mbrs.size(); i++) {
    string suuid  = mbrs[i];
    UInt64 isuuid = strtoul(suuid.c_str(), NULL, 10);
    if (isuuid != auuid) {
      string mavrsn = (*mdeps)[suuid].asString();
      UInt64 mavnum = zh_get_avnum(mavrsn);
      string davrsn;
      UInt64 davnum = 0;
      if (zh_jv_is_member(jdeps, suuid)) {
        davrsn = (*jdeps)[suuid].asString();
        davnum = zh_get_avnum(davrsn);
      }
      if (davnum > mavnum) {
        LE("OOODEP: check_ooo_storage_deps: K: " << kqk <<
                  " MAV: " << mavrsn << " DAV: " << davrsn);
        return true; // OOO
      }
    }
  }
  return false; // NOT OOO
}

string zh_create_kqk(string ns, string cn, string key) {
  return ns + "|" + cn + "|" + key;
}

string zh_create_kqk_from_meta(jv *meta) {
  return zh_create_kqk((*meta)["ns"].asString(), (*meta)["cn"].asString(),
                       (*meta)["_id"].asString());
}

jv zh_create_ks(string kqk, const char *sectok) {
  jv ks;
  ks["kqk"]          = kqk;
  if (sectok) {
    ks["security_token"] = sectok;
  }
  vector<string> res = split(kqk, '|');
  ks["ns"]           = res[0];
  ks["cn"]           = res[1];
  ks["key"]          = res[2];
  return ks;
}

string kqk_get_key(string kqk) {
  vector<string> res = split(kqk, '|');
  return res[2];
}

jv zh_init_pre_commit_info(string op, jv *jks, jv *dentry,
                           jv *xcrdt, jv *edata) {
  jv pc = JOBJECT;
  pc["op"] = op;
  pc["ks"] = *jks;
  if (dentry) pc["dentry"]     = *dentry;
  if (xcrdt)  pc["xcrdt"]      = *xcrdt;
  if (edata)  pc["extra_data"] = *edata;
  return pc;
}

UInt64 zh_get_gcv_version(jv *jmeta) {
  if (!jmeta) return 0;
  jv jgcv = zh_jv_is_member(jmeta, "GC_version") ? (*jmeta)["GC_version"] :
                                                   JNULL;
  if (zjn(&jgcv)) return 0;
  else            return jgcv.asUInt64();
}

UInt64 zh_get_dentry_gcversion(jv *jdentry) {
  jv *jmeta = &((*jdentry)["delta"]["_meta"]);
  return zh_get_gcv_version(jmeta);
}

UInt64 zh_get_ms_time() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return ((tv.tv_sec * 1000000 + tv.tv_usec) / 1000);
}

jv zh_format_crdt_for_storage(string kqk, jv *jmeta, jv *jmember) {
  jv jcrdt;
  jcrdt["_id"]   = kqk_get_key(kqk);
  jcrdt["_meta"] = *jmeta;
  if (zjd(jmember)) {
    jcrdt["_data"] = zconv_add_OR_set_member(jmember, "O", jmeta, false);
  }
  return jcrdt;
}

void zh_set_new_crdt(jv *pc, jv *md, jv *jncrdt, bool do_gcv) {
  jv     *jks = &((*pc)["ks"]);
  string  kqk = (*jks)["kqk"].asString();
  UInt64  gcv = zjd(jncrdt) ? (*jncrdt)["_meta"]["GC_version"].asUInt64() : 0;
  if (do_gcv) LD("ZH.SetNewCrdt: K: " << kqk << " MD.GCV: " << gcv);
  else        LD("ZH.SetNewCrdt: K: " << kqk);
  (*pc)["ncrdt"] = *jncrdt; // NEW CRDT RESULT
  (*md)["ocrdt"] = *jncrdt; // OLD CRDT -> NEXT APPLY-DELTA
  if (do_gcv) (*md)["gcv"] = gcv;
}

void zh_override_pc_ncrdt_with_md_gcv(jv *pc) {
  if (zh_jv_is_member(pc, "ncrdt") && zjd(&((*pc)["ncrdt"]))) {
    (*pc)["ncrdt"]["_meta"]["GC_version"] = (*pc)["extra_data"]["md"]["gcv"];
  }
}

string zh_to_upper(string &s) {
  transform(s.begin(), s.end(), s.begin(), ::toupper);
  return s;
}

bool zh_validate_datatype(string &dtype) {
  string s = zh_to_upper(dtype);
  if (!s.compare("LIST")) return true;
  else                    return false;
}

string zh_summarize_delta(string kqk, jv *jdentry) {
  jv     *jmeta   = &((*jdentry)["delta"]["_meta"]);
  jv     *jauthor = &((*jmeta)["author"]);
  string  guuid   = (*jauthor)["datacenter"].asString();
  string  avrsn   = (*jauthor)["agent_version"].asString();
  UInt64  gcv     = zh_get_gcv_version(jmeta);
  return " K: " + kqk + " AV: " + avrsn +
          " GCV: " + to_string(gcv) + " GU: " + guuid;
}

jv zh_create_pretty_json(jv *jcrdt) {
  if (zjn(jcrdt)) return JNULL;
  jv *jcrdtd         = &((*jcrdt)["_data"]);
  jv *jcmeta         = &((*jcrdt)["_meta"]);
  jv  jjson          = zconv_crdt_element_to_json(jcrdtd, false);
  jjson["_id"]       = (*jcmeta)["_id"];
  jjson["_channels"] = (*jcmeta)["replication_channels"];
  if (zh_jv_is_member(jcmeta, "server_filter")) {
    jjson["_server_filter"] = (*jcmeta)["server_filter"];
  }
  return jjson;
}

jv zh_create_dentry(jv *jdelta, string kqk) {
  jv jks           = zh_create_ks(kqk, NULL);
  jv jdentry;
  jdentry["_"]     = (*jdelta)["_meta"]["author"]["agent_uuid"];
  jdentry["#"]     = (*jdelta)["_meta"]["delta_version"];
  jdentry["ns"]    = jks["ns"];
  jdentry["cn"]    = jks["cn"];
  jdentry["delta"] = *jdelta;
  return jdentry;
}

bool zh_calculate_rchan_match(jv *pc, jv *jocrdt, jv *jdmeta) {
  if (zjn(jocrdt)) {
    (*pc)["rchan_match"] = false;
    return true;
  } else {
    jv     *orchans  = &((*jocrdt)["_meta"]["replication_channels"]);
    string  oschanid = (*orchans)[0].asString();
    jv     *drchans  = &((*jdmeta)["replication_channels"]);
    string  dschanid = (*drchans)[0].asString();
    if (oschanid.compare(dschanid)) return false;
    else {
      (*pc)["rchan_match"] = true;
      return true;
    }
  }
}

string zh_path_get_dot_notation(jv *op_path) {
  string path = "";
  for (uint i = 0; i < op_path->size(); i++) {
    if (i > 0) path += ".";
    path += (*op_path)[i]["K"].asString();
  }
  return path;
}

static bool failsafe_json_parse(string &sjson) {
  if (sjson.size() < 2) return false;
  const char firstc = sjson[0];
  const char penulc = sjson[(sjson.size() - 2)];
  const char lastc  = sjson[(sjson.size() - 1)];
  if (lastc == '\n') { // FAIL-SAFE, string must be "{....}[\n]" EXACTLY
    if ((firstc != '{') || (penulc != '}')) return false;
  } else {
    if ((firstc != '{') || (lastc != '}')) return false;
  }
  return true;
}

jv zh_parse_json_text(string &sjson, bool debug) {
  jv empty;
  bool ok = failsafe_json_parse(sjson);
  if (!ok) return empty;
  jv           json;
  Json::Reader rdr;
  bool parsedSuccess = rdr.parse(sjson, json, false);
  if (parsedSuccess) return json;
  else {
    if (debug) {
      string perr = rdr.getFormattedErrorMessages();
      LOG(DEBUG) << "JSON:TEXT: PARSE ERROR: " << perr << endl;
    }
    return empty;
  }
}

jv zh_parse_json_text(const char *cjson, bool debug) {
  string sjson(cjson);
  return zh_parse_json_text(sjson, debug);
}

bool zh_is_json_element_int(string &key) {
  char *endptr = NULL;
  strtol(key.c_str(), &endptr, 10);
  return (endptr && *endptr) ? false : true;
}

jv zh_parse_json_input_text(string &sjson) {
  jv json = zh_parse_json_text(sjson, true);
  if (json == JNULL) { // STRING or NUMBER
    if (zh_is_json_element_int(sjson)) { // NUMBER
      Int64 ival = strtol(sjson.c_str(), NULL, 10);
      json       = ival;
    } else {
      json       = sjson;                // STRING
    }
  }
  return json;
}

jv zh_create_head_lhn() {
  jv jlhn;
  jlhn["_"] = 0;
  jlhn["#"] = 0;
  return jlhn;
}

jv zh_init_delta(jv *jmeta) {
  jv jdelta;
  jdelta["_meta"]      = *jmeta;
  jdelta["modified"]   = JARRAY;
  jdelta["added"]      = JARRAY;
  jdelta["deleted"]    = JARRAY;
  jdelta["tombstoned"] = JARRAY;
  jdelta["datatyped"]  = JARRAY;
  return jdelta;
}

void zh_init_author(jv *jmeta, jv *jdeps) {
  (*jmeta)["author"]["agent_uuid"] = MyUUID; // NO AGENT_VERSION YET
  (*jmeta)["dependencies"]         = *jdeps;
  (*jmeta)["_"]                    = MyUUID; // New Delta AuthorUUID
}

jv zh_init_meta(string &kqk, jv *rchans, UInt64 expiration, string &sfname) {
  jv jmeta;
  jv ks                         = zh_create_ks(kqk, NULL);
  jmeta["ns"]                   = ks["ns"];
  jmeta["cn"]                   = ks["cn"];
  jmeta["_id"]                  = ks["key"];
  jmeta["author"]["agent_uuid"] = MyUUID;
  jmeta["member_count"]         = 1;
  jmeta["last_member_count"]    = 1;
  jmeta["replication_channels"] = *rchans;
  if (expiration)    jmeta["expiration"]    = expiration;
  if (sfname.size()) jmeta["server_filter"] = sfname;
  return jmeta;
}

jv *zh_get_replication_channels(jv *jmeta) {
  if (!jmeta) return NULL;
  if (!zh_jv_is_member(jmeta, "replication_channels")) return NULL;
  return &((*jmeta)["replication_channels"]);
}

jv *zh_lookup_by_dot_notation(jv *jjson, string &dots) {
  if (!jjson) return NULL;
  vector<string> steps = split(dots, '.');
  for (uint i = 0; i < steps.size(); i++) {
    string f = steps[i];
    if (!zh_jv_is_member(jjson, f)) return NULL;
    jjson = &((*jjson)[f]);
  }
  return jjson;
}

vector<string> zh_parse_op_path(string &path) {
  vector<string> keys = split(path, '.');
  return keys;
}

jv zh_create_jv_response_ok(string &id) {
  jv response;
  response["jsonrpc"]          = "2.0";
  response["id"]               = id;
  response["result"]["status"] = "OK";
  return response;
}

void zh_push_gcv_on_vector_jv_map(map <string, vector<jv>> *vjmap,
                                  string &kqk, jv *gcv) {
  map <string, vector<jv>>::iterator it = vjmap->find(kqk);
  if (it == vjmap->end()) {
    vector<jv> v_gcv;
    vjmap->insert(make_pair(kqk, v_gcv));
    it = vjmap->find(kqk);
  }
  vector<jv> *pv_gcv = &(it->second);
  pv_gcv->push_back(*gcv);
}

string zh_convert_jv_to_string(jv *prequest) {
  if (zjn(prequest)) {
    string empty;
    return empty;
  } else {
    Json::FastWriter writer;
    return writer.write(*prequest);
  }
}

//TODO use bcrypt, its slower but UserPasswordMap means its only slow ONCE
string zh_local_hash(string &password) {
  return sha256(password.c_str());
}

static bool fetch_e_field_as_bool(jv *jcdata, string fname) {
  if (!zh_jv_is_member(jcdata, "E")) return false;
  else {
    if (!zh_jv_is_member(&((*jcdata)["E"]), fname)) return false;
    else {
      return (*jcdata)["E"][fname].asBool();
    }
  }
}

bool zh_is_ordered_array(jv *jcdata) {
  return fetch_e_field_as_bool(jcdata, "ORDERED");
}

bool zh_is_large_list(jv *jcdata) {
  return fetch_e_field_as_bool(jcdata, "LARGE");
}

UInt64 zh_get_ordered_array_min(jv *pclm) {
  if (!zh_jv_is_member(pclm, "E")) {
    zh_fatal_error("LOGIC(zh_get_ordered_array_min)");
  }
  if (zh_jv_is_member(&((*pclm)["E"]), "min") &&
      (*pclm)["E"]["min"].isUInt64()) {
    return (*pclm)["E"]["min"].asUInt64();
  } else {
    return 0;
  }
}

bool zh_is_late_comer(jv *jcdata) {
  if (!zh_jv_is_member(jcdata, "E")) return false;
  else {
    return zh_jv_is_member(&((*jcdata)["E"]), "LATE-COMER");
  }
}

bool zh_isNaN(string &s) {
  char *endptr = NULL;
  strtol(s.c_str(), &endptr, 10);
  return (endptr && *endptr) ? false : true;
}

bool zh_same_master(jv *cn1, jv *cn2) {
  string conname = zh_get_connection_name();
  string host1   = (*cn1)[conname]["server"]["hostname"].asString();
  UInt64 port1   = (*cn1)[conname]["server"]["port"].asUInt64();
  string host2   = (*cn2)[conname]["server"]["hostname"].asString();
  UInt64 port2   = (*cn2)[conname]["server"]["port"].asUInt64();
  if (!host1.compare(host2) && port1 == port2) return true;
  else                                         return false;
}

bool zh_is_primary_thread() {
  pthread_t tid = pthread_self();
  return (tid == PrimaryTid);
}

bool zh_is_reconnect_thread() {
  pthread_t tid = pthread_self();
  return (tid == ReconnectTid);
}

bool zh_is_worker_thread() {
  pthread_t tid = pthread_self();
  return (tid == WorkerTid);
}

unsigned int zh_crc32(string &s) {
  return crc32_16bytes(s.c_str(), s.size());
}

#define DEBUG_ZH_IS_KEY_LOCAL                                                 \
  LD("zh_is_key_local_to_worker: nwrkrs: " << nwrkrs << " hval: " << hval <<  \
     " kslot: " << kslot << " pid: " << pid << " partition: " << partition << \
     " local: " << (kslot == partition));

bool zh_is_key_local_to_worker(string &kqk) {
  sqlres       sr        = fetch_agent_num_workers();
  if (sr.err.size()) return false;
  UInt64       nwrkrs    = sr.ures;
  unsigned int hval      = zh_crc32(kqk);
  unsigned int kslot     = hval % nwrkrs;
  pid_t        pid       = getpid();
  sr                     = fetch_worker_partition(pid);
  if (sr.err.size()) return false;
  UInt64       partition = sr.ures;
  //DEBUG_ZH_IS_KEY_LOCAL
  return (kslot == partition);
}

void zh_remove_repeat_author(jv *jauthors) {
  if (!jauthors->isArray()) zh_fatal_error("LOGIC(jauthors)");
  jv           javrsns = JOBJECT;
  vector<uint> tor;
  for (uint i = 0; i < jauthors->size(); i++) {
    jv     *jauthor = &((*jauthors)[i]);
    string  avrsn   = (*jauthor)["agent_version"].asString();
    if (!zh_jv_is_member(&javrsns, avrsn)) javrsns[avrsn] = true;
    else                                   tor.push_back(i);
  }
  for (int i = (int)(tor.size() - 1); i >= 0; i--) {
    zh_jv_splice(jauthors, tor[i]);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// LUA HELPERS ---------------------------------------------------------------

bool ConnectViaWss = true;

string zh_get_connection_name() {
  if (ConnectViaWss) return "wss";
  else               return "socket";
}

bool zh_handle_lua_error(lua_State *L) {
  LD("zh_handle_lua_error");
  string lerr = lua_tostring(L, -1);
  LE("LUA:ERROR: lua_pcall: " << lerr);
  lua_close(L);
  return false;
}

jv zh_process_error(string &id, int code, string &message) {
  jv response;
  response["jsonrpc"]          = "2.0";
  response["id"]               = id;
  response["error"]["code"]    = code;
  response["error"]["message"] = message;
  return response;
}

