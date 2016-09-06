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
#include "storage.h"

using namespace std;

extern bool zh_AgentDisableSubscriberLatencies;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

static uint ComputeLatencyThreshold = 10;
static uint NumHistBuckets          = 10;

static jv flatten_histogram(jv *ohist) {
  jv    hist   = *ohist;
  int   mindex = -1;
  Int64 max    = -1;
  for (uint i = 0; i < hist.size(); i++) {
    jv    *lat = &(hist[i]);
    Int64  cnt = (*lat)["count"].asInt64();
    if (cnt > max) {
      max    = cnt;
      mindex = i;
    }
  }

  if      (mindex == 0)                      mindex += 1;
  else if (mindex == (int)(hist.size() - 1)) mindex -= 1;

  UInt64 mcnt     = hist[mindex]["count"].asUInt64();
  double halfc    = (mcnt / 2);
  UInt64 lhalfc   = (UInt64)halfc;
  UInt64 rhalfc   = ((double)lhalfc == halfc) ? lhalfc : (lhalfc + 1);

  UInt64 prema    = hist[(mindex - 1)]["average"].asUInt64();
  UInt64 ma       = hist[ mindex     ]["average"].asUInt64();
  UInt64 postma   = hist[(mindex + 1)]["average"].asUInt64();

  UInt64 lhalfa   = (UInt64)(prema + ((ma     - prema) / 2));
  UInt64 rhalfa   = (UInt64)(ma    + ((postma - ma)    / 2));

  jv llat;
  llat["average"] = lhalfa;
  llat["count"]   = lhalfc;
  jv rlat;
  rlat["average"] = rhalfa;
  rlat["count"]   = rhalfc;

  hist[mindex] = rlat;
  zh_jv_insert(&hist, mindex, &llat);
  if (hist.size() == NumHistBuckets) return hist;
  else                               return flatten_histogram(&hist);// RECURSE
}

static jv make_histogram(jv *nlats) {
  jv     hist = JARRAY;
  UInt64 tot  = 0;
  for (uint i = 0; i < nlats->size(); i++) {
    tot += (*nlats)[i]["count"].asUInt64();
  }
  UInt64 step = (UInt64)(tot / NumHistBuckets);
  UInt64 high = (*nlats)[0]["value"].asUInt64();
  UInt64 low  = high;
  UInt64 cnt  = 0;
  for (uint i = 0; i < nlats->size(); i++) {
    cnt += (*nlats)[i]["count"].asUInt64();
    if (cnt >= step) {
      high              = (*nlats)[i]["value"].asUInt64();
      UInt64 avg        = low + (UInt64)((high - low) / 2);
      jv hentry;
      hentry["average"] = avg;
      hentry["count"]   = cnt;
      zh_jv_append(&hist, &hentry);
      cnt               = 0;
      low               = high;
    }
  }
  if (cnt != 0) {
    high        = (*nlats)[(nlats->size() - 1)]["value"].asUInt64();
    UInt64 avg  = low + (UInt64)((high - low) / 2);
    UInt64 lavg = hist[(hist.size() - 1)]["average"].asUInt64();
    if (avg == lavg) {
      UInt64 lcnt = hist[(hist.size() - 1)]["count"].asUInt64();
      hist[(hist.size() - 1)]["count"] = lcnt + cnt;
    } else {
      jv hentry;
      hentry["average"] = avg;
      hentry["count"]   = cnt;
      zh_jv_append(&hist, &hentry);
    }
  }
  if (hist.size() == NumHistBuckets) return hist;
  else                               return flatten_histogram(&hist);
}

static bool cmp_lat(jv *a_lat, jv *b_lat) {
  UInt64 a_val = (*a_lat)["value"].asUInt64();
  UInt64 b_val = (*b_lat)["value"].asUInt64();
  return (a_val < b_val);
}

static jv zh_create_histogram(vector<UInt64> *lats) {
  jv nlats = JARRAY;
  for (uint i = 0; i < lats->size(); i++) {
    jv lentry;
    lentry["count"] = 1;
    lentry["value"] = (*lats)[i];
    zh_jv_append(&nlats, &lentry);
  }
  zh_jv_sort(&nlats, cmp_lat);
  return make_histogram(&nlats);
}

static jv zh_merge_histograms(jv *ohist, vector<UInt64> *lats) {
  jv nlats = JARRAY;
  for (uint i = 0; i < ohist->size(); i++) {
    jv olat = (*ohist)[i];
    jv lentry;
    lentry["count"] = olat["count"];
    lentry["value"] = olat["average"];
    zh_jv_append(&nlats, &lentry);
  }
  for (uint i = 0; i < lats->size(); i++) {
    jv lentry;
    lentry["count"] = 1;
    lentry["value"] = (*lats)[i];
    zh_jv_append(&nlats, &lentry);
  }
  zh_jv_sort(&nlats, cmp_lat);
  return make_histogram(&nlats);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ADD TO SUBSCRIBER LATENCIES -----------------------------------------------

static map<string, vector<UInt64>> SubscriberLatencies;

static UInt64 EndToEndSuspectTraceLatency     = 1000;
static UInt64 MaxIntraGeoClusterPercent       = 90;
/* TODO(V2)
static UInt64 IntraGeoClusterMaxTraceLatency  = 1500;
static UInt64 AgentToDCWarnTraceLatency       = 1000;
static UInt64 DCToDCWarnTraceLatency          = 1000;
*/

static bool analyze_end_to_end_latencies(string kqk, jv *jmeta) {
  string  lt;
  bool    skip = false;
  UInt64  lat  = (*jmeta)["subscriber_received"].asUInt64() -
                 (*jmeta)["agent_sent"].asUInt64();
  if (!zh_jv_is_member(jmeta, "agent_received")) {
    lt   += " AGENT_RECEIVED NOT DEFINED";
    skip  = true;
  }
  if (lat >= EndToEndSuspectTraceLatency) {
    Int64 ingeo  = (*jmeta)["subscriber_sent"].asUInt64() -
                   (*jmeta)["agent_received"].asUInt64();
    UInt64 prct  = (UInt64)((ingeo * 100) / lat);
    if (prct >= MaxIntraGeoClusterPercent) {
      lt   += " [END_TO_END: " + to_string(lat) + " INTRA_GEO_CLUSTER: " +
              to_string(ingeo) + " %: " + to_string(prct) + "]";
      skip  = true;
    }
  }
  if (lt.size()) {
    string avrsn = (*jmeta)["author"]["agent_version"].asString();
    LOG(DEBUG) << "LATENCY_TRACE: K: " << kqk << " AV: " << avrsn << lt << endl;
  }
  return skip;
}

sqlres zlat_add_to_subscriber_latencies(string kqk, jv *jdentry) {
  if (zh_AgentDisableSubscriberLatencies) RETURN_EMPTY_SQLRES
  jv   *jmeta = &((*jdentry)["delta"]["_meta"]);
  bool  skip  = zh_jv_is_member(jmeta, "dirty_centra") ||
                !zh_jv_is_member(jmeta, "agent_sent")  ||
                !zh_jv_is_member(jmeta, "subscriber_received");
  if (skip) RETURN_EMPTY_SQLRES
  bool  lskip = analyze_end_to_end_latencies(kqk, jmeta);
  if (lskip) RETURN_EMPTY_SQLRES
  string rguuid = (*jmeta)["author"]["datacenter"].asString();
  Int64  lat    = (*jmeta)["subscriber_received"].asUInt64() -
                  (*jmeta)["agent_sent"].asUInt64();
  LOG(DEBUG) << "DC: " << rguuid << " LATENCY: " << lat << endl;
  map<string, vector<UInt64>>::iterator it = SubscriberLatencies.find(rguuid);
  if (it == SubscriberLatencies.end()) {
    vector<UInt64> lats;
    lats.push_back(lat);
    SubscriberLatencies.insert(make_pair(rguuid, lats));
    it = SubscriberLatencies.find(rguuid);
  } else {
    vector<UInt64> *plats = &(it->second);
    plats->push_back(lat);
  }
  vector<UInt64> *plats = &(it->second);
  if (plats->size() < ComputeLatencyThreshold) RETURN_EMPTY_SQLRES
  sqlres sr = fetch_subscriber_latency_histogram(rguuid);
  RETURN_SQL_ERROR(sr)
  jv nhist = sr.jres;
  if (zjn(&nhist)) nhist = zh_create_histogram(plats);
  else             nhist = zh_merge_histograms(&nhist, plats);
  sr = persist_subscriber_latency_histogram(rguuid, &nhist);
  RETURN_SQL_ERROR(sr)
  SubscriberLatencies.erase(it);
  RETURN_EMPTY_SQLRES
}



// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CLIENT GET LATENCIES -------------------------------------------------------

static jv respond_client_get_latencies(string id, jv *jlats) {
  jv response = zh_create_jv_response_ok(id);
  response["result"]["information"]["latencies"] = *jlats;
  return response;
}

jv process_client_get_latencies(lua_State *L, string id, jv *params) {
  LOG(DEBUG) << "process_client_get_latencies" << endl;
  sqlres sr    = fetch_all_subscriber_latency_histogram();
  PROCESS_GENERIC_ERROR(id, sr)
  jv     jlats = sr.jres;
  return respond_client_get_latencies(id, &jlats);
}

