#include <cstdio>
#include <cstring>
#include <string>
#include <sstream>  
#include <iostream>
#include <csignal>

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

using namespace std;

extern bool Debug;
extern bool NetworkDebug;

extern bool DatabaseDisabled;

extern int ChaosMode;
extern vector<string> ChaosDescription;
extern int ChaosMax;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SIGNAL HANDLERS -----------------------------------------------------------

static void sighup_handler(int signal) {
  LOG(INFO) << "sighup_handler " << endl;
  ChaosMode += 1;
  if (ChaosMode > ChaosMax) ChaosMode = 0;
  LOG(INFO) << "ChaosMode: " << ChaosMode << endl;
}

static void sigusr2_handler(int signal) {
  LOG(INFO) << "sigusr2_handler " << endl;
  if (DatabaseDisabled) DatabaseDisabled = false;
  else                  DatabaseDisabled = true;
  LOG(INFO) << "DatabaseDisabled: " << DatabaseDisabled << endl;
}

static void sigquit_handler(int signal) {
  LOG(INFO) << "sigquit_handler " << endl;
  ChaosMode        = 0;
  DatabaseDisabled = false;
  LOG(INFO) << "ChaosMode: " << ChaosMode << endl;
  LOG(INFO) << "DatabaseDisabled: " << DatabaseDisabled << endl;
}

static void sigprof_handler(int signal) {
  LOG(INFO) << "sigprof_handler " << endl;
  if (NetworkDebug) NetworkDebug = false;
  else              NetworkDebug = true;
  LOG(INFO) << "NetworkDebug: " << NetworkDebug << endl;
}

static void sigalrm_handler(int signal) {
  LOG(INFO) << "sigalrm_handler " << endl;
  if (Debug) Debug = false;
  else       Debug = true;
  LOG(INFO) << "Debug: " << Debug << endl;
}

static void sigint_handler(int signal) {
  LOG(INFO) << "sigint_handler " << endl;
  exit(1);
}

void add_signal_handlers() {
  std::signal(SIGHUP,  sighup_handler);
  std::signal(SIGUSR2, sigusr2_handler);
  std::signal(SIGQUIT, sigquit_handler);
  std::signal(SIGPROF, sigprof_handler);
  std::signal(SIGALRM, sigalrm_handler);
  std::signal(SIGINT,  sigint_handler);
}
 
