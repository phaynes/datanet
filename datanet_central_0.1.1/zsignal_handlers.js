"use strict"

var fs    = require('fs');

var ZDConn = require('./zdconn');
var ZMDC   = require('./zmemory_data_cache');
var ZADaem = require('./zagent_daemon');
var ZQ     = require('./zqueue')
var ZH     = require('./zhelper')

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL SIGNAL HANDLERS ---------------------------------------------------

function debug_signal(sig) {
  var myname = ZH.AmCentral ? "CENTRAL" : "AGENT";
  ZH.e(myname + ': Caught Signal: ' + sig);
}

if (ZH.AmCentral) {

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL TESTING SIGNAL HANDLERS -------------------------------------------

  process.on('SIGXFSZ', function() {
    debug_signal('SIGXFSZ');
    ZH.CentralDisableCentralToSyncKeysDaemon = true;
    ZH.e('ZH.CentralDisableCentralToSyncKeysDaemon: true');
  });

  process.on('SIGXCPU', function() {
    debug_signal('SIGXCPU');
    ZH.CentralDisableCentralToSyncKeysDaemon = false;
    ZH.e('ZH.CentralDisableCentralToSyncKeysDaemon: false');
    ZDConn.SignalCentralToSyncKeysDaemon();
  });

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL SIGNAL HANDLERS ---------------------------------------------------

  process.on('SIGWINCH', function() {
    debug_signal('SIGWINCH');
    ZH.CentralSynced = true;
    ZH.e('SIGWINCH SIGNAL HANDLER: CentralSynced: ' + ZH.CentralSynced);
    ZDConn.SignalCentralSynced();
  });

  process.on('SIGIO', function() {
    debug_signal('SIGIO');
    ZMDC.DebugDumpCache();
  });

  process.on('SIGINT', function() {
    debug_signal('SIGINT');
    ZH.Central.DoShutdown();
  });

} else {

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT TESTING SIGNAL HANDLERS ---------------------------------------------

  process.on('SIGXFSZ', function() {
    debug_signal('SIGXFSZ');
    ZH.Agent.DisableAgentToSync = true;
    ZH.e('ZH.Agent.DisableAgentToSync: true');
  });

  process.on('SIGXCPU', function() {
    debug_signal('SIGXCPU');
    ZH.Agent.DisableAgentToSync = false;
    ZH.e('ZH.Agent.DisableAgentToSync: false');
    ZADaem.SignalAgentToSyncKeys();
  });

  process.on('SIGTRAP', function() {
    debug_signal('SIGTRAP');
    ZH.Agent.DisableAgentDirtyDeltasDaemon = true;
    ZH.e('ZH.Agent.DisableAgentDirtyDeltasDaemon: true');
  });

  process.on('SIGVTALRM', function() {
    debug_signal('SIGVTALRM');
    ZH.Agent.DisableAgentDirtyDeltasDaemon = false;
    ZH.e('ZH.Agent.DisableAgentDirtyDeltasDaemon: false');
    ZADaem.SignalAgentDirtyDeltasReaper();
  });

  process.on('SIGSYS', function() {
    debug_signal('SIGSYS');
    ZH.MsTimeInPerSecond = true;
    ZH.e('ZH.MsTimeInPerSecond: true');
  });

  process.on('SIGPWR', function() {
    debug_signal('SIGPWR');
    ZH.MsTimeInPerSecond = false;
    ZH.e('ZH.MsTimeInPerSecond: false');
  });


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT SIGNAL HANDLERS -----------------------------------------------------

  process.on('SIGINT', function() {
    debug_signal('SIGINT');
    ZH.Agent.DoShutdown();
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SHARED SIGNAL HANDLERS ----------------------------------------------------

process.on('SIGHUP', function() {
  debug_signal('SIGHUP');
  ZH.ChaosMode += 1;
  if (ZH.ChaosMode > ZH.ChaosMax) ZH.ChaosMode = 0;
  ZH.e('ChaosMode: ' + ZH.ChaosMode);
});

process.on('SIGPIPE', function() {
  debug_signal('SIGPIPE');
  ZH.PathologicalMode += 1;
  if (ZH.PathologicalMode > ZH.PathologicalMax) ZH.PathologicalMode = 0;
  ZH.e('PathologicalMode: ' + ZH.PathologicalMode);
});

process.on('SIGUSR2', function() {
  debug_signal('SIGUSR2');
  if (ZH.DatabaseDisabled) ZH.DatabaseDisabled = false;
  else                     ZH.DatabaseDisabled = true;
  ZH.e('DatabaseDisabled: ' + ZH.DatabaseDisabled);
});

process.on('SIGQUIT', function() {
  debug_signal('SIGQUIT');
  ZH.ChaosMode        = 0;
  ZH.PathologicalMode = 0;
  ZH.DatabaseDisabled = false;
  ZH.e('ChaosMode: ' + ZH.ChaosMode);
  ZH.e('PathologicalMode: ' + ZH.PathologicalMode);
  ZH.e('DatabaseDisabled: ' + ZH.DatabaseDisabled);
});

process.on('SIGPROF', function() {
  debug_signal('SIGPROF');
  if (ZH.NetworkDebug) ZH.NetworkDebug = false;
  else                 ZH.NetworkDebug = true;
  ZH.e('NetworkDebug: ' + ZH.NetworkDebug);
});

process.on('SIGALRM', function() {
  debug_signal('SIGALRM');
  if (ZH.Debug) ZH.Debug = false;
  else          ZH.Debug = true;
  ZH.e('Debug: ' + ZH.Debug);
});


