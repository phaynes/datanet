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
#include "deltas.h"
#include "apply_delta.h"
#include "subscriber_delta.h"
#include "subscriber_merge.h"
#include "dack.h"
#include "activesync.h"
#include "storage.h"

using namespace std;

extern bool   DisableFixlog;

extern UInt64 FixLog_Sleep_seconds;
extern UInt64 FixLog_Sleep_nanoseconds;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EXTERNS -------------------------------------------------------------------

bool FixLogActive = false;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// TYPEDEF FUNCTIONS ---------------------------------------------------------

typedef sqlres (*replay_func)(jv *pc);


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FIXLOG --------------------------------------------------------------------

static sqlres replay_data(string fid, jv *ulog) {
  string op = (*ulog)["op"].asString();
  LD("fixlog.replay_data: OP: " << op);
  replay_func rfunc;
  if        (!op.compare("AgentDelta")) {
    rfunc = rollback_client_commit;
  } else if (!op.compare("RemoveAgentDelta")) {
    rfunc = retry_remove_agent_delta;
  } else if (!op.compare("SubscriberDelta")) {
    rfunc = retry_subscriber_delta;
  } else if (!op.compare("SubscriberCommitDelta")) {
    rfunc = retry_subscriber_commit_delta;
  } else if (!op.compare("SubscriberMerge")) {
    rfunc = retry_subscriber_merge;
  }
  sqlres sr = rfunc(ulog);
  RETURN_SQL_ERROR(sr)
  sr        = remove_fixlog(fid);
  return sr;
}

static sqlres replay_fixlog() {
  if (DisableFixlog) {
    LE("FIXLOG DISABLED");
    RETURN_EMPTY_SQLRES
  }

  sqlres  sr    = fetch_all_fixlog();
  RETURN_SQL_ERROR(sr)
  jv     *ulogs = &(sr.jres);
  zh_debug_json_value("ulogs", ulogs);
  //NOTE sort(cmp_agent_fixlog) is NOT NEEDED
  vector<string> mbrs = zh_jv_get_member_names(ulogs);
  for (uint i = 0; i < mbrs.size(); i++) {
    string  fid  = mbrs[i];
    jv     *ulog = &((*ulogs)[fid]);
    sqlres  srf  = replay_data(fid, ulog);
    RETURN_SQL_ERROR(srf)
  }
  RETURN_EMPTY_SQLRES
}

static void run_fixlog() {
  FixLogActive = true;
  while (1) {
    LOG(INFO) << "run_fixlog" << endl;
    sqlres sr = startup_fetch_agent_uuid();
    if (sr.err.size()) {
      LOG(ERROR) << "-ERROR: FIXLOG: " << sr.err << endl;
    } else {
      sqlres sr = replay_fixlog();
      if (sr.err.size()) {
        LOG(ERROR) << "-ERROR: FIXLOG: " << sr.err << endl;
      } else {
        return;
      }
    }
  }
}

static void handle_fixlog_complete() {
  LOG(INFO) << "handle_fixlog_complete" << endl;
  FixLogActive = false;
}

void fixlog_activate() {
  if (FixLogActive) return;
  LOG(INFO) << "ACTIVATING FIX LOG" << endl;
  run_fixlog();
  handle_fixlog_complete();
}

