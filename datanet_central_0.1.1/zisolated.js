"use strict";

var ZDelt, ZSU, ZCache, ZAS, ZADaem, ZDack, ZAio, ZWss;
var ZCLS, ZMDC, ZDS, ZS, ZH, ZDBP;
if (typeof(exports) !== 'undefined') {
  require('./setImmediate');
  ZDelt  = require('./zdeltas');
  ZSU    = require('./zstationuser');
  ZCache = require('./zcache');
  ZAS    = require('./zactivesync');
  ZADaem = require('./zagent_daemon');
  ZDack  = require('./zdack');
  ZAio   = require('./zaio');
  ZWss   = require('./zwss');
  ZCLS   = require('./zcluster');
  ZMDC   = require('./zmemory_data_cache');
  ZDS    = require('./zdatastore');
  ZS     = require('./zshared');
  ZH     = require('./zhelper');
  ZDBP   = require('./zdb_plugin');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEBUG ---------------------------------------------------------------------

var DebugIntentionalDeviceKeyError = false;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// RECHECK AGENT TO CENTRAL --------------------------------------------------

function get_new_kss_elements(a, b) {
  var dkss  = [];
  var adict = {};
  var bdict = {};
  for (var i = 0; i < a.length; i++) adict[a[i].kqk] = a[i];
  for (var i = 0; i < b.length; i++) bdict[b[i].kqk] = b[i];
  for (var k in bdict) {
    if (!adict[k]) dkss.push(bdict[k]);
  }
  return dkss;
}

function handle_agent_check_ack(net, odres, ndres, next) {
  var recheck = ndres.recheck;
  if (recheck) {
    ZH.l('RECHECK: ' + recheck);
    odres.recheck = recheck;
    recheck_central(net, odres);
  } else {
    var pkss = get_new_kss_elements(odres.pkss, ndres.pkss);
    // TODO check if pkss have been sync'ed lately (e.g. via SM)
    var rkss = get_new_kss_elements(odres.rkss, ndres.rkss);
    var dres = {geo_nodes : ndres.geo_nodes,
                pkss      : pkss, // NOTE: ckss not needed on RECHECK
                rkss      : rkss};
    on_connect_sync_ack_agent_online(net, dres, next);
  }
}

function do_recheck_central(net, odres, next) {
  var ocreated = odres.original_created;
  ZAio.SendCentralAgentRecheck(net.plugin, net.collections, ocreated,
  function(derr, ndres) {
    if (derr) next(derr, null);
    else      handle_agent_check_ack(net, odres, ndres, next);
  });
}

var RecheckCentralTimer = null;

function recheck_central(net, dres) {
  var recheck = dres.recheck;
  if (!recheck) return;
  ZH.l('recheck_central: ' + recheck);
  if (RecheckCentralTimer) {
    clearTimeout(RecheckCentralTimer);
    RecheckCentralTimer = null;
  }
  RecheckCentralTimer = setTimeout(function() {
    RecheckCentralTimer = null;
    do_recheck_central(net, dres, ZH.OnErrLog)
  }, recheck);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT HANDLE AGENT-ONLINE RESPONSE FROM CENTRAL ---------------------------

function set_agent_synced(next) {
  ZH.Agent.synced = true;
  ZH.l('ZH.Agent.synced');
  next(null, null);
  // NOTE: ZH.FireOnAgentSynced() is ASYNC
  if (ZH.FireOnAgentSynced) ZH.FireOnAgentSynced();
}

// NOTE: DIFFERS FROM ZAS.AgentSyncChannelKss() -> NO LRU CHECK
function sync_ks(net, skss, next) {
  if (skss.length === 0) next(null, null);
  else {
    var ks = skss.shift();
    // NOTE: Just a SET, not a DO, the DO happens elsewhere/later
    ZAS.SetAgentSyncKey(net.plugin, net.collections, ks, function(serr, sres) {
      if (serr) next(serr, null);
      else      setImmediate(sync_ks, net, skss, next);
    });
  }
}

function do_remove_sync_ack_agent_online(net, rkss, next) {
  if (!rkss || rkss.length === 0) next(null, null);
  else {
    var ks   = rkss.shift();
    var kkey = ZS.GetKeyInfo(ks);
    net.plugin.do_get_field(net.collections.kinfo_coll, kkey, "separate",
    function(gerr, sep) {
      if (gerr) next(gerr, null);
      else {
        ZDS.RemoveKey(net, ks, sep, function(serr, sres) {
          if (serr) next(serr, null);
          else {
            setImmediate(do_remove_sync_ack_agent_online, net, rkss, next);
          }
        });
      }
    });
  }
}

function sync_ack_agent_online(net, skss, rkss, next) {
  do_remove_sync_ack_agent_online(net, rkss, function(rerr, rres) {
    if (rerr) next(rerr, null);
    else {
      sync_ks(net, skss, function(serr, sres) {
        if (serr) next(serr, null);
        else      set_agent_synced(next);
      });
    }
  });
}

function build_sync_arrays(dres, skss) {
  if (dres.pkss) {
    var pkss = dres.pkss;
    for (var i = 0; i < pkss.length; i++) {
      skss.push(pkss[i]);
    }
  }
  if (dres.ckss) {
    var ckss = dres.ckss;
    for (var i = 0; i < ckss.length; i++) {
      skss.push(ckss[i]);
    }
  }
}

// NOTE: LOCAL_ONLY: Local AgentDeltas but NOT in AgentOnline.pkss/ckss[]
function get_local_only_change(net, skss, ks_drain, next) {
  var adkey = ZS.AgentDirtyKeys;
  net.plugin.do_get(net.collections.global_coll, adkey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      var dk;
      if (gres.length !== 0) {
        dk = gres[0];
        delete(dk._id);
      }
      var ksa = {};
      for (var i = 0; i < skss.length; i++) {
        var ks      = skss[i];
        ksa[ks.kqk] = true;
      }
      for (var kqk in dk) {
        if (!ksa[kqk]) { // MISS
          var ks = ZH.ParseKQK(kqk);
          ks_drain.push(ks);
        }
      }
      next(null, null);
    }
  });
}

function drain_local_only_change(net, ks_drain, next) {
  if (ks_drain.length === 0) next(null, null);
  else {
    var ks = ks_drain.shift();
    ZH.l('drain_local_only_change: K: ' + ks.kqk);
    ZADaem.GetAgentKeyDrain(net, ks, false, function(serr, sres) {
      if (serr) next(serr, null);
      else      setImmediate(drain_local_only_change, net, ks_drain, next);
    });
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// RECONNECT AGENT TO CENTRAL-------------------------------------------------

function on_connect_sync_ack_agent_online(net, dres, next) {
  ZH.l('on_connect_sync_ack_agent_online: U: ' + ZH.MyUUID);
  var skss     = [];
  build_sync_arrays(dres, skss);
  var ks_drain = [];
  get_local_only_change(net, skss, ks_drain, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      sync_ack_agent_online(net, skss, dres.rkss, function(serr, sres) {
        if (serr) next(serr, null);
        else {
          exports.SetGeoNodes(net, dres.geo_nodes, function(aerr, ares) {
            if (aerr) next(aerr, null);
            else {
              ZADaem.SignalAgentToSyncKeys();
              ZADaem.ResetKeyDrainMap();
              drain_local_only_change(net, ks_drain, next);
            }
          });
        }
      });
    }
  });
}

function ack_agent_online_engine_callbacks(next) {
  ZH.l('ack_agent_online_engine_callbacks');
  ZH.IfDefinedTryBlock(ZS.EngineCallback.DisplayConnectionStatus);
  ZH.IfDefinedTryBlock(ZS.EngineCallback.AgentReconnectedFunc);
  next(null, null);
}

function check_redirect(plugin, collections, dres, next) {
  var cnode = dres.device.cluster_node;
  if (cnode && !ZH.SameMaster(ZH.CentralMaster, cnode)) {
    ZWss.RedirectWss(cnode, false);
    next(new Error(ZS.Errors.WssReconnecting), null);
  } else {
    ack_agent_online_engine_callbacks(next);
  }
}

function handle_redirect(plugin, collections, dres, next) {
  ZH.MyDataCenter = dres.datacenter;
  var duuid       = dres.device.uuid;
  ZWss.CurrentNumberReconnectFails = 0;
  if (dres.offline) { // WENT OFFLINE
    ZH.l('handle_redirect: OFFLINE');
    next(null, null);
  } else {            // CAME ONLINE
    ZH.l('handle_redirect: -> check_redirect');
    check_redirect(plugin, collections, dres, next)
  }
}

function initialize_agent_device_uuid(net, dres, next) {
  if (dres.device.initialize_device !== true) next(null, null);
  else {
    var duuid = dres.device.uuid;
    ZH.l('ZDBP.SetDeviceUUID: U: ' + duuid);
    ZDBP.SetDeviceUUID(duuid);
    ZH.InitNewDevice(duuid, next);
  }
}

function initialize_agent_device_key(net, dres, next) {
  if (!dres.device.key) next(null, null);
  else {
    ZH.DeviceKey = dres.device.key;
    if (DebugIntentionalDeviceKeyError) {
      ZH.DeviceKey = "INTENTIONAL DEVICE-KEY ERROR"; // USED FOR TESTING
    }
    ZH.e('initialize_agent_device_key: DEVICE-KEY: ' + ZH.DeviceKey);
    var kkey = ZS.GetAgentDeviceKey();
    net.plugin.do_set_field(net.collections.global_coll, kkey,
                            "value", ZH.DeviceKey, next);
  }
}

function handle_ack_agent_online(net, dres, next) {
  ZH.l('<<<<(C): handle_ack_agent_online');
  initialize_agent_device_uuid(net, dres, function(aerr, ares) {
    if (aerr) next(aerr, null);
    else {
      initialize_agent_device_key(net, dres, function(serr, sres) {
        if (serr) next(serr, null);
        else {
          handle_redirect(net.plugin, net.collections, dres,
          function(aerr, ares) {
            if (aerr) {
              if (aerr.message !== ZS.Errors.WssReconnecting) next(aerr, null);
              // else -> NO-OP (RECONNECTING)
            } else {
              if (dres.offline) next(null, null);
              else {
                ZH.AgentLastReconnect = ZH.GetMsTime();
                on_connect_sync_ack_agent_online(net, dres, next);
                // NOTE: recheck_central() is ASYNC
                recheck_central(net, dres);
              }
            }
          });
        }
      });
    }
  });
}

function do_reconnect_to_central(net, next) {
  ZAio.SendCentralAgentOnline(net.plugin, net.collections, true,
  function(derr, dres) {
    if (derr) next(derr, null);
    else      handle_ack_agent_online(net, dres, next);
  });
}

exports.ReconnectToCentral = function(net) {
  ZH.l('ZISL.ReconnectToCentral MGU: ' + ZH.MyDataCenter);
  do_reconnect_to_central(net, function(serr, sres) {
    if (serr) {
      ZH.e('ERROR: ZISL.ReconnectToCentral: ' + serr.message);
      retry_reconnect_to_central(net, serr);
    }
  });
}

var RetryReconnectToCentralTimer = null;
var RetryReconnectToCentralSleep = 5000;

function retry_reconnect_to_central(net, cerr) {
  ZH.e('retry_reconnect_to_central: ERROR: ' + cerr.message);
  if (RetryReconnectToCentralTimer) return;
  ZH.l('RetryReconnectToCentralSleep: ' + RetryReconnectToCentralSleep);
  RetryReconnectToCentralTimer = setTimeout(function() {
    RetryReconnectToCentralTimer = null;
    ZH.l('retry_reconnect_to_central: TIMEOUT -> RETRY');
    exports.ReconnectToCentral(net);
  }, RetryReconnectToCentralSleep);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT SET-GEO-NODES -------------------------------------------------------

exports.AgentGeoNodes = [];
 
// NOTE: (AgentGeoFailoverSleep > ZGDD.CentralDirtyDeltaStaleness)
var AgentGeoFailoverSleep     = 40000; // 40 seconds
exports.AgentGeoFailoverTimer = null;

exports.ClearAgentGeoFailoverTimer = function() {
  clearTimeout(exports.AgentGeoFailoverTimer);
  exports.AgentGeoFailoverTimer = null;
}

exports.DoAgentGeoFailover = function() {
  ZH.e('RUN: ZISL.DoAgentGeoFailover');
  exports.ClearAgentGeoFailoverTimer();
  for (var i = 0; i < exports.AgentGeoNodes.length; i++) {
    var gnode = exports.AgentGeoNodes[i];
    if (gnode.device_uuid === ZH.Agent.datacenter) {
      ZH.l('RECONNECT TO CONFIG DATACENTER'); ZH.p(gnode);
      var cnode = ZWss.DoGeoFailover(gnode);
      ZWss.RedirectWss(cnode, true);
      return;
    }
  }
}

exports.ShouldDoGeoFailover = function() {
  if (exports.AgentGeoFailoverTimer)           return false;
  if (!ZWss.RetryConfigDC)                     return false;
  if (ZH.MyDataCenter === ZH.Agent.datacenter) return false;
  for (var i = 0; i < exports.AgentGeoNodes.length; i++) {
    var gnode = exports.AgentGeoNodes[i];
    if (gnode.device_uuid === ZH.Agent.datacenter) return true;
  }
  return false;
}

function handle_geo_state_change_event(geo_nodes, next) {
  exports.AgentGeoNodes = ZH.clone(geo_nodes);
  ZH.l('handle_geo_state_change_event: RetryConfigDC: ' +
       ZWss.RetryConfigDC); ZH.p(exports.AgentGeoNodes);
  var sgf = exports.ShouldDoGeoFailover();
  if (sgf) {
    ZH.l('ZISL.DoAgentGeoFailover SLEEP: ' + AgentGeoFailoverSleep);
    exports.AgentGeoFailoverTimer = setTimeout(exports.DoAgentGeoFailover,
                                               AgentGeoFailoverSleep);
  }
  next(null, null);
}

exports.SetGeoNodes = function(net, geo_nodes, next) {
  var dkey    = ZS.AgentDataCenters;
  var d_entry = {geo_nodes : geo_nodes};
  net.plugin.do_set(net.collections.global_coll, dkey, d_entry,
  function(serr, sres) {
    if (serr) next(serr, null);
    else      handle_geo_state_change_event(geo_nodes, next);
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CLIENT GET CLUSTER INFO ---------------------------------------------------

exports.AgentGetClusterInfo = function(plugin, collections, hres, next) {
  hres.information = {cluster_node : ZH.CentralMaster,
                      datacenter   : ZH.MyDataCenter,
                      geo_nodes    : exports.AgentGeoNodes,
                      isolated     : ZH.Isolation,
                      connected    : ZH.Agent.cconn};
  next(null, hres);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT SEND CENTRAL DEVICE-ISOLATION ---------------------------------------

function set_agent_isolation(plugin, collections, val, next) {
  ZH.SetIsolation(val);
  var ikey = ZS.AgentIsolation;
  plugin.do_set_field(collections.admin_coll, ikey, "value", val, next);
}

// EXPORT:  SendCentralAgentIsolation()
// PURPOSE: Set to ON  -> notify central of offline-status
//          Set to OFF -> Drain any isolated modified to central
exports.SendCentralAgentIsolation = function(net, b, hres, next) {
  ZH.l('ZISL.SendCentralAgentIsolation: value: ' + b);
  if (ZH.Isolation === b) {
    ZH.l('Repeat Isolation set -> NO-OP');
    hres.isolation = ZH.Isolation; // Used in response
    next(null, hres);
  } else {
    if (b) ZH.e('START ISOLATION');
    else   ZH.e('END ISOLATION');
    set_agent_isolation(net.plugin, net.collections, b, function (ierr, ires) {
      if (ierr) next(ierr, hres);
      else {
        hres.isolation = ZH.Isolation; // Used in response
        if (ZH.Isolation) { // Send AgentOffline() to Central
          ZH.l('ISOLATION BEGINS');
          ZAio.SendCentralAgentOnline(net.plugin, net.collections, false,
          function(derr, dres) {
            if (derr) ZH.e(derr);
            else {
              ZH.IfDefinedTryBlock(ZS.EngineCallback.DisplayConnectionStatus);
            }
          });
          next(null, hres);
        } else { // No longer Isolated, Lets' Reconnect to central
          ZH.l('ISOLATION ENDS');
          ZWss.HandleIsolationOff(net, hres, next);
        }
      }
    });
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT HANDLE ACK-BACKOFF FROM CENTRAL -------------------------------------

exports.BackoffTimer = null;

exports.AgentDeIsolate = function() {
  ZH.l('AGENT DE-ISOLATE');
  exports.BackoffTimer = null;
  var net = ZH.CreateNetPerRequest(ZH.Agent);
  exports.SendCentralAgentIsolation(net, false, {}, function(serr, sres) {
    if (serr) ZH.e(serr);
  });
}

exports.HandleBackoff = function(net, csec, hres, next) {
  csec   = Number(csec);
  var ms = Math.floor(csec * 1000);
  ZH.l('ZISL.HandleBackoff: MS: ' + ms + ' ZH.Isolation: ' + ZH.Isolation);
  if (ZH.Isolation) next(null, hres); // already chilling
  else {
    var now  = ZH.GetMsTime();
    var endt = now + ms;
    set_agent_isolation(net.plugin, net.collections, endt,
    function (ierr, ires) {
      next(ierr, hres);
      if (!exports.BackoffTimer) {
        exports.BackoffTimer = setTimeout(exports.AgentDeIsolate, ms);
      }
    });
  }
}

exports.StartupOnBackoff = function() {
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  var ikey = ZS.AgentIsolation;
  net.plugin.do_get_field(net.collections.admin_coll, ikey, "value",
  function(gerr, gres) {
    if (gerr) ZH.e(gerr);
    else {
      var endt = Number(gres);
      var now  = ZH.GetMsTime();
      var to   = endt - now;
      if (!exports.BackoffTimer) {
        ZH.l('StartupOnBackoff: now: ' + now + ' endt: ' + endt + ' to: ' + to);
        exports.BackoffTimer = setTimeout(exports.AgentDeIsolate, to);
      }
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT GET AGENT-INFO FOR SEND-CENTRAL-AGENT-ONLINE-------------------------

function get_suser_perms_and_subs(plugin, collections, username, ainfo, next) {
  var pkey = ZS.GetUserChannelPermissions(username);
  plugin.do_get(collections.uperms_coll, pkey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      if (gres.length !== 0) {
        ainfo.permissions[username] = gres[0];
        delete(ainfo.permissions[username]._id)
      }
      var skey = ZS.GetUserChannelSubscriptions(username);
      plugin.do_get(collections.usubs_coll, skey, function (serr, sres) {
        if (serr) next(serr, null);
        else {
          if (sres.length !== 0) {
            ainfo.subscriptions[username] = sres[0];
            delete(ainfo.subscriptions[username]._id)
          }
          next(null, null);
        }
      });
    }
  });
}

function get_all_susers_perms_and_subs(plugin, collections, susers, ainfo,
                                       next) {
  var need = susers.length;
  if (need === 0) next(null, ainfo);
  else {
    var done = 0;
    var nerr = false;
    for (var i = 0; i < need; i++) {
      var username = susers[i];
      get_suser_perms_and_subs(plugin, collections, username, ainfo,
      function(gerr, gres) {
        if      (nerr) return;
        else if (gerr) {
          nerr = true;
          next(gerr, null);
        } else {
          done += 1;
          if (done === need) next(null, ainfo);
        }
      });
    }
  }
}

exports.GetAgentInfo = function(plugin, collections, next) {
  var sub    = ZH.CreateMySub();
  var su_key = ZS.GetAgentStationedUsers(sub.UUID);
  plugin.do_get(collections.su_coll, su_key, function (gerr, gres) {
    if      (gerr)              next(gerr, null);
    else if (gres.length === 0) next(null, null);
    else {
      var susers = [];
      var smap   = gres[0];
      delete(smap._id);
      for (var username in smap) susers.push(username);
      var ainfo  = {permissions    : {},
                    subscriptions  : {},
                    stationedusers : ZH.clone(susers)};
      get_all_susers_perms_and_subs(plugin, collections, susers, ainfo, next);
    }
  });
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZISL']={} : exports);

