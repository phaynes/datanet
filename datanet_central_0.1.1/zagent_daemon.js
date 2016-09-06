"use strict";

var ZSD, ZOOR, ZAio, ZDS, ZMDC, ZAF, ZQ, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  require('./setImmediate');
  ZSD    = require('./zsubscriber_delta');
  ZOOR   = require('./zooo_replay');
  ZAio   = require('./zaio');
  ZDS    = require('./zdatastore');
  ZMDC   = require('./zmemory_data_cache');
  ZAF    = require('./zaflow');
  ZQ     = require('./zqueue');
  ZS     = require('./zshared');
  ZH     = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SETTINGS ------------------------------------------------------------------

var KeyDrainTimeout   = 1000;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GLOBALS -------------------------------------------------------------------

var KeyDrainMap = {};

exports.ResetKeyDrainMap = function() {
  ZH.l('ZADaem.ResetKeyDrainMap()');
  KeyDrainMap = {};
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT TO-SYNC KEYS --------------------------------------------------------

var MinNextToSyncInterval     = 15000;
var MaxNextToSyncInterval     = 25000;
var AgentToSyncKeysSleep      =   100;

var AgentToSyncKeysTimer      = null;
var AgentToSyncKeysInProgress = false;

function fetch_dentries(net, ks, authors, dentries, next) {
  if (authors.length === 0) next(null, null);
  else {
    var author = authors.shift();
    ZSD.FetchSubscriberDelta(net.plugin, net.collections, ks, author,
    function(gerr, dentry) {
      if (gerr) next(gerr, null);
      else {
        if (dentry) dentries.push(dentry);
        setImmediate(fetch_dentries, net, ks, authors, dentries, next);
      }
    });
  }
}

function fetch_simple_agent_need_merge_metadata(net, ks, next) {
  ZMDC.GetGCVersion(net.plugin, net.collections, ks, function(gerr, gcv) {
    if (gerr) next(gerr, null);
    else {
      var md = {gcv : gcv,
                nd  : 0};
      next(null, md);
    }
  });
}

function fetch_agent_need_merge_metadata(net, ks, next) {
  fetch_simple_agent_need_merge_metadata(net, ks, function(gerr, md) {
    if (gerr) next(gerr, null);
    else {
      var dkey = ZS.GetAgentKeyDeltaVersions(ks.kqk);
      net.plugin.do_get_array_values(net.collections.delta_coll,
                                     dkey, "aversions",
      function(verr, avrsns) {
        if (verr) next(verr, null);
        else {
          if (!avrsns || (avrsns.length === 0)) next(null, md);
          else {
            var authors  = create_authors_from_avrsns(avrsns);
            var dentries = [];
            fetch_dentries(net, ks, authors, dentries, function(ferr, fres) {
              if (ferr) next(ferr, null);
              else {
                var min_gcv = md.gcv;
                for (var i = 0; i < dentries.length; i++) {
                  var dentry = dentries[i];
                  var dgcv   = ZH.GetDentryGCVersion(dentry);
                  if (dgcv < min_gcv) min_gcv = dgcv;
                }
                ZH.l('fetch_agent_need_merge_metadata: MIN-GCV: ' + min_gcv);
                md.gcv = min_gcv;
                md.nd  = dentries.length;
                next(null, md);
              }
            });
          }
        }
      });
    }
  });
}

exports.NeedMerge = function(net, ks, next) {
  fetch_agent_need_merge_metadata(net, ks, function(ferr, md) {
    if (ferr) next(ferr, null);
    else {
      var sm_gcv = (md.gcv + 1); // NEED NEXT GCV
      ZAio.SendCentralNeedMerge(net.plugin, net.collections,
                                ks, sm_gcv, md.nd, next);
    }
  });
}

function do_agent_tosync_keys(net, kss, next) {
  if (kss.length === 0) next(null, null);
  else {
    var ks = kss.shift();
    exports.NeedMerge(net, ks, function(serr, sres) {
      if (serr) next(serr, null);
      else {
        if (kss.length === 0) next(null, null); // SKIP SLEEP
        else {
          ZH.l('do_agent_tosync_keys: SLEEP: ' + AgentToSyncKeysSleep);
          setTimeout(function() {
            do_agent_tosync_keys(net, kss, next);
          }, AgentToSyncKeysSleep);
        }
      }
    });
  }
}

function agent_tosync_keys(net, next) {
  //ZH.l('RUN: agent_tosync_keys');
  var skey = ZS.AgentKeysToSync;
  net.plugin.do_get(net.collections.global_coll, skey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      if (gres.length === 0) {
        ZH.e('AGENT_TOSYNC_KEYS: ZERO KEYS');
        next(null, null);
      } else {
        var kmap = gres[0];
        delete(kmap._id);
        var kss  = [];
        for (var kqk in kmap) {
          var ks            = ZH.ParseKQK(kqk);
          ks.security_token = kmap[kqk];
          kss.push(ks);
        }
        ZH.e('AGENT_TOSYNC_KEYS: #Ks: ' + kss.length);
        do_agent_tosync_keys(net, kss, next);
      }
    }
  });
}

function run_agent_tosync_keys() {
  if (ZH.Agent.DisableAgentToSync || AgentToSyncKeysInProgress) return;
  AgentToSyncKeysInProgress = true;
  if (AgentToSyncKeysTimer) { // DONT WAIT -> RUN NOW
    clearTimeout(AgentToSyncKeysTimer);
    AgentToSyncKeysTimer = null;
  }
  var offline = ZH.GetAgentOffline();
  if (offline) {
    ZH.e('OFFLINE: SKIP: agent_tosync_keys');
    AgentToSyncKeysInProgress = false;
    next_run_agent_tosync_keys();
  } else {
    var net = ZH.CreateNetPerRequest(ZH.Agent);
    agent_tosync_keys(net, function(aerr, ares) {
      AgentToSyncKeysInProgress = false;
      if (aerr) ZH.e(aerr);
      next_run_agent_tosync_keys();
    });
  }
}

function signal_agent_tosync_keys() {
  run_agent_tosync_keys();
}

function next_run_agent_tosync_keys() {
  var min = MinNextToSyncInterval;
  var max = MaxNextToSyncInterval;
  var to  = min + ((max - min) * Math.random());
  AgentToSyncKeysTimer = setTimeout(run_agent_tosync_keys, to);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SETTINGS AGENT DIRTY DELTA REAPER -----------------------------------------

var AgentDirtyDeltaMaximumAllowedStaleness = 80000;
var MinNextAgentDirtyDeltasInterval        = 45000;
var MaxNextAgentDirtyDeltasInterval        = 50000;

var AgentDirtyDeltaInProgress              = false;
var AgentDirtyDeltasTimer                  = null;

var DebugAgentDirtyDeltaDaemon           = false;
if (DebugAgentDirtyDeltaDaemon) {
  AgentDirtyDeltaMaximumAllowedStaleness = 30000;
  MinNextAgentDirtyDeltasInterval        = 20000;
  MaxNextAgentDirtyDeltasInterval        = 21000;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT DIRTY DELTA REAPER DRAIN ------------------------------------------

// NOTE: no SET_TIMER() on TOO SOON failure
//       | -> next run AgentDirtyDeltaReaper is enough
function send_agent_dentries(net, ks, authdentries, next) {
  ZH.l('send_agent_dentries: K: ' + ks.kqk + ' #D: ' + authdentries.length)
  var moo_drain = KeyDrainMap[ks.kqk];
  var now       = ZH.GetMsTime();
  if (moo_drain) { // RATE-LIMITING
    var diff = now - moo_drain;
    if (diff < KeyDrainTimeout) {
      ZH.e('DRAIN-KEY-ON-OOO-DELTA: TOO SOON: diff: ' + diff);
      return next(null, false);
    }
  }
  KeyDrainMap[ks.kqk] = now;
  // NOTE: ZAio.SendCentralAgentDentries() is ASYNC -> (HAS TIMEOUTS)
  ZAio.SendCentralAgentDentries(net, ks, authdentries, ZH.OnErrLog);
  next(null, null);
}

function add_auth_to_dentries(net, ks, dentries, authdentries, next) {
  if (dentries.length === 0) next(null, null);
  else {
    var dentry = dentries.shift();
    var author = dentry.delta._meta.author;
    var auuid  = author.agent_uuid;
    var avrsn  = author.agent_version;
    var pkey   = ZS.GetAgentPersistDelta(ks.kqk, auuid, avrsn);
    net.plugin.do_get_field(net.collections.delta_coll, pkey, "auth",
    function (gerr, auth) {
      if (gerr) next(gerr, null);
      else {
        var authdentry = {dentry : dentry, auth : auth};
        authdentries.push(authdentry);
        setImmediate(add_auth_to_dentries, net,
                     ks, dentries, authdentries, next);
      }
    });
  }
}

function flow_drain_key_versions(net, ks, authors, next) {
  if (authors.length === 0) next(null, null);
  else {
    ZH.RemoveRepeatAuthor(authors);
    ZOOR.DependencySortAuthors(net, ks, true, authors,
    function(serr, dentries) {
      if (serr) next(serr, null);
      else {
        var authdentries = [];
        add_auth_to_dentries(net, ks, dentries, authdentries,
        function(aerr, ares) {
          if (aerr) next(aerr, null);
          else      send_agent_dentries(net, ks, authdentries, next);
        });
      }
    });
  }
}

exports.FlowDrainKeyVersions = function(plugin, collections, qe, next) {
  var net     = qe.net;
  var qnext   = qe.next;
  var ks      = qe.ks;
  var data    = qe.data;
  var authors = data.authors;
  ZH.l('FlowDrainKeyVersions: K: ' + ks.kqk + ' #Ds: ' + authors.length);
  flow_drain_key_versions(net, ks, authors, function(kerr, kres) {
    if (kerr) ZH.e(kerr);
    qnext(null, null);
    next(null, null);
  });
}

function drain_key_versions(net, ks, authors, internal, next) {
  ZH.l('drain_key_versions: K: ' + ks.kqk + ' I: ' + internal);
  if (internal) flow_drain_key_versions(net, ks, authors, next);
  else {
    var data = {authors : authors};
    ZQ.AddToAgentKeyFlow(ZQ.AgentKeySerializationQueue, ks,
                         'DRAIN_KEY_VERSIONS',
                         net, data, null, null, next);
    var flow = {k : ks.kqk,                   q : ZQ.AgentKeySerializationQueue,
                m : ZQ.AgentKeySerialization, f : ZAF.FlowAgentKey };
    ZQ.StartFlow(net.plugin, net.collections, flow);
  }
}

function create_authors_from_avrsns(avrsns) {
  var authors = [];
  for (var i = 0; i < avrsns.length; i++) {
    var avrsn  = avrsns[i];
    var auuid  = ZH.GetAvUUID(avrsn);
    var author = {agent_uuid : auuid, agent_version : avrsn};
    authors.push(author);
  }
  return authors;
}

function fetch_agent_key_versions(net, ks, next) {
  var dkey = ZS.GetAgentKeyDeltaVersions(ks.kqk);
  net.plugin.do_get_array_values(net.collections.delta_coll, dkey, "aversions",
  function(verr, avrsns) {
    if (verr) next(verr, null);
    else {
      var akey = ZS.GetOOOKeyDeltaVersions(ks.kqk);
      net.plugin.do_get_array_values(net.collections.delta_coll,
                                     akey, "aversions",
      function(ferr, oooavrsns) {
        if (ferr) next(ferr, null);
        else {
          if (!avrsns) avrsns = [];
          if (oooavrsns) {
            for (var i = 0; i < oooavrsns.length; i++) {
              avrsns.push(oooavrsns[i]); // APPEND OOO-AVRSNS[] TO AVRNS[]
            }
          }
          var authors = create_authors_from_avrsns(avrsns);
          next(null, authors);
        }
      });
    }
  });
}

// NOTE: DO NOT DRAIN KEYS OUT-OF-SYNC (TO-SYNC is OK)
function check_key_sync(net, ks, next) {
  var md = {};
  ZSD.GetAgentSyncStatus(net, ks, md, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      var ok = (!md.out_of_sync_key);
      next(null, ok);
    }
  });
}

exports.GetAgentKeyDrain = function(net, ks, internal, next) {
  ZH.l('GetAgentKeyDrain: K: ' + ks.kqk);
  check_key_sync(net, ks, function(cerr, ok) {
    if (cerr) next(cerr, null);
    else {
      if (!ok) { // TO-SYNC or OUT-OF-SYNC
        ZH.l('GetAgentKeyDrain: K: ' + ks.kqk + ' KEY NOT-IN-SYNC -> NO-OP');
        next(null, null);
      } else {
        fetch_agent_key_versions(net, ks, function(ferr, authors) {
          if (ferr) next(ferr, null);
          else {
            if (authors.length) {
              ZH.e('GetAgentKeyDrain: K: ' + ks.kqk + ' DELTAS'); ZH.e(authors);
            }
            drain_key_versions(net, ks, authors, internal, next);
          }
        });
      }
    }
  });
}

function get_agent_keys_drain(net, kss, next) {
  if (kss.length === 0) next(null, null);
  else {
    var ks = kss.shift();
    exports.GetAgentKeyDrain(net, ks, false, function(serr, sres) {
      if (serr) next(serr, null);
      else      setImmediate(get_agent_keys_drain, net, kss, next);
    });
  }
}

function run_agent_dirty_deltas_reaper(next) {
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  var dkey = ZS.AgentDirtyDeltas;
  net.plugin.do_get(net.collections.global_coll, dkey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      if (gres.length === 0) {
        ZH.e('AGENT_DIRTY_DELTAS: ZERO DELTAS');
        next(null, null);
      } else {
        var dmap  = gres[0];
        delete(dmap._id);
        var old   = ZH.GetMsTime() - AgentDirtyDeltaMaximumAllowedStaleness;
        var dvals = [];
        for (var dval in dmap) {
          var ts = dmap[dval];
          var ok = (ts < old);
          if (ok) dvals.push(dval);
        }
        if (dvals.length === 0) {
          ZH.e('AGENT_DIRTY_DELTAS: ZERO DIRTY_DELTAS');
          next(null, null);
        } else {
          var ukss = {};
          for (var i = 0; i < dvals.length; i++) {
            var dval  = dvals[i];
            var res   = dval.split('-');
            var kqk   = res[0];
            ukss[kqk] = true;
          }
          var kss = []
          for (var kqk in ukss) {
            var ks    = ZH.ParseKQK(kqk);
            kss.push(ks);
          }
          get_agent_keys_drain(net, kss, next);
        }
      }
    }
  });
}

function do_agent_dirty_deltas_reaper(next) {
  if (ZH.Agent.DisableAgentDirtyDeltasDaemon === true) {
    ZH.e('DISABLED: SKIP: run_agent_dirty_deltas_reaper');
    next(null, null);
  } else {
    ZH.l('do_agent_dirty_deltas_reaper');
    if (AgentDirtyDeltaInProgress) next(null, null);
    else {
      AgentDirtyDeltaInProgress = true;
      run_agent_dirty_deltas_reaper(function(aerr, ares) {
        AgentDirtyDeltaInProgress = false;
        next(aerr, null);
      });
    }
  }
}

function agent_dirty_deltas_reaper() {
  AgentDirtyDeltasTimer = null;
  var offline = ZH.GetAgentOffline();
  if (offline) {
    ZH.e('OFFLINE: SKIP: agent_dirty_deltas_reaper');
    next_run_agent_dirty_deltas();
  } else {
    do_agent_dirty_deltas_reaper(function(nerr, nres) {
      if (nerr) ZH.e(nerr.message);
      next_run_agent_dirty_deltas();
    });
  }
}

function signal_agent_dirty_deltas_reaper() {
  if (AgentDirtyDeltasTimer) {
    clearTimeout(AgentDirtyDeltasTimer);
    AgentDirtyDeltasTimer = null;
  }
  agent_dirty_deltas_reaper();
}

function next_run_agent_dirty_deltas() {
  if (AgentDirtyDeltasTimer) return;
  var min = MinNextAgentDirtyDeltasInterval;
  var max = MaxNextAgentDirtyDeltasInterval;
  var to  = min + ((max - min) * Math.random());
  //ZH.l('next_run_agent_dirty_deltas: to: ' + to);
  AgentDirtyDeltasTimer = setTimeout(agent_dirty_deltas_reaper, to);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRAPPERS ------------------------------------------------------------------

// NOTE: Used by zagent:init_agent_daemons() & zbrowser:start_reapers()
exports.StartupAgentToSyncKeys = function() {
  next_run_agent_tosync_keys();
}

// NOTE: Used by ZISL.on_connect_sync_ack_agent_online(),
//               ZSU.agent_sync_keys(), & ZAS.sync_missing_keys()
exports.SignalAgentToSyncKeys = function() {
  ZH.l('ZADaem.SignalAgentToSyncKeys');
  signal_agent_tosync_keys();
}

// NOTE: Used in zagent.js & zbrowser.js initialization
exports.StartAgentDirtyDeltasDaemon = function() {
  next_run_agent_dirty_deltas();
}

// NOTE: Used by zcentral.js SIGVTALRM handler
exports.SignalAgentDirtyDeltasReaper = function() {
  signal_agent_dirty_deltas_reaper();
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZADaem']={} : exports);

