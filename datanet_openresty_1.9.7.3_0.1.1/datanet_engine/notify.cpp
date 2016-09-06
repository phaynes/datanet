#include <cstdio>
#include <cstring>
#include <string>

extern "C" {
  #include <time.h>
  #include <sys/time.h>
  #include <sys/types.h>
  #include <sys/un.h>
  #include <stdio.h>
  #include <unistd.h> 
  #include <stdlib.h> 
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
#include "shared.h"
#include "auth.h"
#include "storage.h"

using namespace std;

extern Int64  MyUUID;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DATA-CHANGE EVENT ---------------------------------------------------------

static void send_notify(string &body, bool debug) {
  LD("send_notify: BODY: " << body);
  sqlres  sr   = fetch_all_notify_urls();
  if (sr.err.size()) return;
  jv     *urls = &(sr.jres);
  for (uint i = 0; i < urls->size(); i++) {
    string url = (*urls)[i].asString();
    LD("send_notify: URL: " << url);
    (void)zexh_send_url_https_request(url, body);
  }
}

void znotify_notify_on_data_change(jv *pc, bool full_doc, bool selfie) {
  LD("znotify_notify_on_data_change");
  jv     *jks      = &((*pc)["ks"]);
  jv     *jncrdt   = &((*pc)["ncrdt"]);
  bool    remove   = zh_get_bool_member(pc, "remove");
  bool    store    = zh_get_bool_member(pc, "store");
  jv      jpmd     = zh_get_existing_member(pc, "post_merge_deltas");
  // Nothing happened -> NO-OP
  if (zjn(jncrdt) && !remove && (zjn(&jpmd) || !jpmd.size())) return;
  string  id       = zh_create_internal_rpc_id();
  string  kqk      = (*jks)["kqk"].asString();
  jv      json     = zjd(jncrdt) ? zh_create_pretty_json(jncrdt) : JNULL;
  jv      flags               = JOBJECT;
  flags["selfie"]             = selfie;
  flags["full_document_sync"] = full_doc;
  flags["remove"]             = remove;
  flags["initialize"]         = store;
  jv      data                = JOBJECT;
  data["device"]["uuid"]      = MyUUID;
  data["id"]                  = id;
  data["kqk"]                 = kqk;
  data["json"]                = json;
  if (zjd(&jpmd)) data["post_merge_deltas"] = jpmd;
  data["flags"]               = flags;
  string  body                = zh_convert_jv_to_string(&data);
  send_notify(body, false);
}

void znotify_debug_notify(string &kqk, jv *debug) {
  LD("znotify_debug_notify");
  jv     data   = JOBJECT;
  data["kqk"]   = kqk;
  data["debug"] = *debug;
  string body   = zh_convert_jv_to_string(&data);
  send_notify(body, true);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CLIENT COMMAND HANDLERS ---------------------------------------------------

static jv respond_client_notify(string &id) {
  jv response = zh_create_jv_response_ok(id);
  return response;
}

static sqlres set_notify(string &cmd, string &url) {
  LT("set_notify");
  if (cmd.compare("ADD") && cmd.compare("REMOVE")) {
    RETURN_SQL_RESULT_ERROR(ZS_Errors["NotifyFormat"])
  } else {
    string prot = url.substr(0, 8);
    transform(prot.begin(), prot.end(), prot.begin(), ::toupper);
    if (prot.compare("HTTPS://")) {
      RETURN_SQL_RESULT_ERROR(ZS_Errors["NotifyURLNotHttps"])
    } else {
      if (!cmd.compare("ADD")) { // ADD
        LD("ZNotify.SetNotify: cmd: " << cmd << " url: " << url);
        sqlres sr = persist_notify_set();
        LD("NotifySet -> ON");
        RETURN_SQL_ERROR(sr)
        sr        = persist_notify_url(url);
        RETURN_SQL_ERROR(sr)
      } else {                   // REMOVE
        sqlres  sr   = remove_notify_url(url);
        RETURN_SQL_ERROR(sr)
        sr           = fetch_all_notify_urls();
        RETURN_SQL_ERROR(sr)
        jv     *urls = &(sr.jres);
        if (urls->size() == 0) {
          LD("NotifySet -> OFF");
          sqlres sr = remove_notify_set();
        }
      }
      RETURN_EMPTY_SQLRES
    }
  }
}

jv process_client_notify(lua_State *L, string id, jv *params) {
  string cmd = (*params)["data"]["command"].asString();
  string url = (*params)["data"]["url"].asString();
  LT("process_client_notify: CMD: " << cmd << " URL: " << url);
  sqlres sr  = set_notify(cmd, url);
  PROCESS_GENERIC_ERROR(id, sr)
  return respond_client_notify(id);
}

