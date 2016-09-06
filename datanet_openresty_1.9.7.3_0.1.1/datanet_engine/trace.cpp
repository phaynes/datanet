#include <cstdio>
#include <cstring>
#include <string>
#include <sstream>
#include <iostream>
#include <map>
#include <vector>

extern "C" {
  #include <time.h>
  #include <stdio.h>
  #include <unistd.h> 
  #include <stdlib.h> 
}

#include "json/json.h"
#include "easylogging++.h"

#include "helper.h"
#include "trace.h"
#include "storage.h"

using namespace std;

typedef ostringstream oss;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GLOBALS -------------------------------------------------------------------

extern bool DisableTrace;

#define DUMP_EVERY_X_CALLS 1000
uint NumCalls = 0;

typedef struct trace {
  double total_time;
  uint   num_calls;
} trace_t;

typedef struct ftrace {
  trace_t tr;
  string  tname;
} ftrace_t;

map<string, trace_t> TraceTotals;

map<string, clock_t> CurrentTrace;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PRIVATE FUNCTIONS ---------------------------------------------------------

static bool cmp_full_trace_total_time_desc(ftrace_t &aftr, ftrace_t &bftr) {
  return (aftr.tr.total_time > bftr.tr.total_time); // NOTE: DESC
}

static bool cmp_full_trace_avg_desc(ftrace_t &aftr, ftrace_t &bftr) {
  double  a_avgt = (aftr.tr.total_time / (double)aftr.tr.num_calls);
  double  b_avgt = (bftr.tr.total_time / (double)bftr.tr.num_calls);
  return (a_avgt > b_avgt); // NOTE: DESC
}

static void dump_full_traces(string prfx, vector<ftrace_t> ftarr) {
  oss res;
  for (uint i = 0; i < ftarr.size(); i++) {
    ftrace_t ftr  = ftarr[i];
    double   avgt = (ftr.tr.total_time / (double)ftr.tr.num_calls);
    res << "\n\t" << prfx << "-TRACE: (" << ftr.tname << ")" <<
           " NUM: " << ftr.tr.num_calls << " TIME: " << ftr.tr.total_time <<
           " AVG: " << avgt;
  }
  LE(res.str());
}

static void dump_trace_to_log_file() {
  vector<ftrace_t> ftarr;
  for (map<string, trace_t>::iterator it = TraceTotals.begin();
                                      it != TraceTotals.end(); it++) {
    string   stname = it->first;
    trace_t  tr     = it->second;
    ftrace_t ftr;
    ftr.tr          = tr;
    ftr.tname       = stname;
    ftarr.push_back(ftr);
  }

  std::sort(ftarr.begin(), ftarr.end(), cmp_full_trace_total_time_desc);
  dump_full_traces("TOTAL", ftarr);

  std::sort(ftarr.begin(), ftarr.end(), cmp_full_trace_avg_desc);
  dump_full_traces("AVERAGE", ftarr);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PUBLIC API ----------------------------------------------------------------

void ztrace_start(const char *tname) {
  if (DisableTrace) return;
  string  stname(tname);
  clock_t c = clock();
  CurrentTrace[stname] = c;
}

void ztrace_finish(const char *tname) {
  if (DisableTrace) return;
  string stname(tname);
  map<string, clock_t>::iterator cit = CurrentTrace.find(stname);
  if (cit == CurrentTrace.end()) {
    LE("ERROR: ztrace_finish: (" << tname << ") NO ztrace_start");
    zh_fatal_error("LOGIC(ztrace_finish)");
  }

  clock_t c    = cit->second;
  double  diff = clock() - c;

  trace_t tr;
  map<string, trace_t>::iterator tit = TraceTotals.find(stname);
  if (tit == TraceTotals.end()) {
    bzero(&tr, sizeof(trace_t));
  } else {
    tr = tit->second;
  }

  tr.num_calls  += 1;
  tr.total_time += diff;
  
  TraceTotals[stname] = tr;

  if ((++NumCalls % DUMP_EVERY_X_CALLS) == 0) {
    dump_trace_to_log_file();
  }
}

