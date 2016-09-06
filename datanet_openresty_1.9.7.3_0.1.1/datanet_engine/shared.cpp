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

#include "shared.h"

using namespace std;

extern Int64 MyUUID;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ZSHARED -------------------------------------------------------------------

map<string, string> ZS_Errors;

string zs_get_dirty_subscriber_delta_key(string &kqk, UInt64 auuid,
                                         string &avrsn) {
  return kqk + "-" + to_string(auuid) + "-" + avrsn;
}

string zs_get_dirty_subscriber_delta_key(string &kqk, jv *jauthor) {
  UInt64 auuid = (*jauthor)["agent_uuid"].asUInt64();
  string avrsn = (*jauthor)["agent_version"].asString();
  return zs_get_dirty_subscriber_delta_key(kqk, auuid, avrsn);
}

string zs_get_dirty_agent_delta_key(string &kqk, string &avrsn) {
  return kqk + "-" + to_string(MyUUID) + "-" + avrsn;
}

string zs_create_kqk_from_string(string &ns, string &cn, string &id) {
  return ns + "|" + cn + "|" + id;
}

string zs_create_kqk(jv &jns, jv &jcn, jv &jid) {
  string ns = jns.asString();
  string cn = jcn.asString();
  string id = jid.asString();
  return zs_create_kqk_from_string(ns, cn, id);
}

jv zs_create_jks(string &kqk) {
  jv jks;
  jks["kqk"]         = kqk;
  vector<string> res = split(kqk, '|');
  jks["ns"]          = res[0];
  jks["cn"]          = res[1];
  jks["key"]         = res[2];
  return jks;

}

string zs_get_persist_delta_key(string &kqk, string &avrsn) {
  return kqk + "-" + avrsn;
}

string zs_get_ka_key(string &kqk, UInt64 auuid) {
  return kqk + "-" + to_string(auuid);
}

string zs_get_kav_key(string &kqk, string &avrsn) {
  return kqk + "-" + avrsn;
}

string zs_get_g_key(string &kqk, UInt64 gcv) {
  return kqk + "-" + to_string(gcv);
}

void zs_init_ZS_Errors() {
  ZS_Errors["None"]            = "";
  ZS_Errors["NoDataFound"]     = "-ERROR: No Data Found";
  ZS_Errors["DeltaNotSync"]    = "-ERROR: Delta not SYNCED";
  ZS_Errors["FieldNotFound"]   = "-ERROR: Field not Found: NAME: ";
  ZS_Errors["FetchCachedKey"]  =
                        "-ERROR: called FETCH on CACHED-KEY -> must use CACHE";
  ZS_Errors["ModifyWatchKey"]  =
     "-ERROR: COMMIT/REMOVE on WATCHED KEY -> must CACHE KEY to COMMIT/REMOVE";

  ZS_Errors["StaleAgentDelta"] =
               "-ERROR: AgentDelta is STALE -> RETRY before 60 second timeout";

  ZS_Errors["AuthenticationMiss"] = "-ERROR: Authentication MISSING on AGENT";
  ZS_Errors["BasicAuthMissing"]   = "-ERROR: Basic Authentication MISSING";
  ZS_Errors["BasicAuthFail"]      = "-ERROR: Basic Authentication FAIL";
  ZS_Errors["WritePermsFail"]     = "-ERROR: Write permissions FAIL";
  ZS_Errors["ReadPermsFail"]      = "-ERROR: Read permissions FAIL";
  ZS_Errors["SubscribePermsFail"] = "-ERROR: Subscribe permissions FAIL";

  ZS_Errors["NotStationed"]       = "-ERROR: User NOT Stationed";
  ZS_Errors["EvictUncached"]      =
                      "-ERROR: Evicting KEY that has NOT been CACHED";
  ZS_Errors["CacheOnSubscribe"]   =
                      "-ERROR: CACHE Key -> ALREADY SUBSCRIBED to KEYs CHANNEL";

  ZS_Errors["RepeatDelta"]     = "-ERROR: REPEAT DELTA -> IGNORED";
  ZS_Errors["OutOfOrderDelta"] = "-ERROR: OUT-OF-ORDER Delta";
  ZS_Errors["CommitOnOutOfSyncKey"] =
               "-ERROR: COMMIT on KEY currently in NeedsSync state -> FAIL";
  ZS_Errors["AgentHeartbeatUsage"] = 
               "-ERROR: USAGE: HEARTBEAT START|STOP|QUERY [fieldname]";
  ZS_Errors["AgentHeartbeatBadStart"] = 
               "-ERROR: USAGE: HEARTBEAT START fieldname";
  ZS_Errors["NoKeyToRemove"]     = "-ERROR: REMOVE on NON-existent key";
  ZS_Errors["NoKeyToExpire"]     = "-ERROR: EXPIRE on NON-existent key";
  ZS_Errors["AlreadyStationed"] = "-ERROR: User ALREADY Stationed";
  ZS_Errors["FixLogRunning"]    = "-ERROR: FixLog Replay Running";
  ZS_Errors["RequestNotLocal"]  = "-ERROR: Key range not local to this process";
  ZS_Errors["MethodNotDefined"] = "-ERROR: Method not defined (library broken)";

  ZS_Errors["ServerFilterFailed"] = "-ERROR: SERVER-FILTER FAILED";

  ZS_Errors["IncrOnNaN"]           = "-ERROR: INCR only supported on Numbers";
  ZS_Errors["ExpireNaN"]           = "-ERROR: '_expire' field must be a NUMBER";
  ZS_Errors["ReservedFieldMod"]    =
          "-ERROR: Changing reserved fields (_id, _meta,_channels) prohibited";
  ZS_Errors["SetArrayInvalid"]     =
                 "-ERROR: SET() on an array invalid format, use SET(num, val)";
  ZS_Errors["SetValueInvalid"]     = "SET(key, val) invalid typeof(val)";
  ZS_Errors["SetPrimitiveInvalid"] =
        "-ERROR: Treating primitive (number,string) as nestable(object,array)";
  ZS_Errors["InsertArgsInvalid"]   =
    "-ERROR: USAGE: insert(key, index, value) - \"index\" must be a positive integer";
  ZS_Errors["InsertType"]          =
                 "-ERROR: USAGE: TYPE-ERROR: insert() only possible on ARRAYs";
  ZS_Errors["NestedFieldMissing"]  =
                           "-ERROR: Nested field does not exist, field_name: ";
  ZS_Errors["LargeListOnlyRPUSH"]  = "-ERROR: LARGE_LIST supports ONLY RPUSH";
  ZS_Errors["RChansNotArray"]      =
                           "-ERROR: FIELD: '_channels' must be an ARRAY";
  ZS_Errors["RChanChange"]         =
                           "-ERROR: \"_channels\" can NOT be changed once set";

  ZS_Errors["OverlappingSubscriberMergeRequests"] =
            "-ERROR: more recent SubscriberMerge on its way -> IGNORE this one";

  ZS_Errors["NotifyFormat"]        = "-ERROR: NOTIFY [ADD|REMOVE] URL";
  ZS_Errors["NotifyURLNotHttps"]   =
                                  "-ERROR: NOTIFY URL MUST BEGIN w/ 'https://'";

  // CENGINE specific
  ZS_Errors["DeltaStorageMaxHit"] = "-ERROR: DELTA STORAGE MAX-SIZE EXCEEEDED";
  ZS_Errors["QueryParseError"]    = "-ERROR: Query (JSON) Parse Error";
  ZS_Errors["QueryEngineMissing"]   =
           "-ERROR: Query ENGINE currenty only supports '{\"_id\" : \"XYZ\"}'";
  ZS_Errors["StorageNotInitialized"] = 
           "-ERROR: STORAGE NOT INITIALIZED'";
}

