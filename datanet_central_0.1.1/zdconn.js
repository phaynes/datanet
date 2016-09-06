"use strict";

require('./setImmediate');

var ZCloud   = require('./zcloud_server');
var ZPio     = require('./zpio');
var ZSumm    = require('./zsummary');
var ZChannel = require('./zchannel');
var ZCache   = require('./zcache');
var ZSU      = require('./zstationuser');
var ZUM      = require('./zuser_management');
var ZVote    = require('./zvote');
var ZCLS     = require('./zcluster');
var ZPart    = require('./zpartition');
var ZMDC     = require('./zmemory_data_cache');
var ZDS      = require('./zdatastore');
var ZDQ      = require('./zdata_queue');
var ZS       = require('./zshared');
var ZH       = require('./zhelper');


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL TO SYNC SETTINGS --------------------------------------------------

var MinNextToSyncInterval           = 10000;
var MaxNextToSyncInterval           = 15000;

var GeoNeedMergeKeysSleep           = 100;

var SignalCentralSyncedSleep = 1000;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DATACENTER SYNC RESEND SETTINGS -------------------------------------------

var RetrySendDataCenterOnlineSleep = 5000;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GLOBALS -------------------------------------------------------------------

var CentralToSyncDaemonInTimer    = null;
var CentralToSyncDaemonInProgress = false;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEVICE-ONLINE METHOD ------------------------------------------------------

function handle_device_init(plugin, collections, dvc, udata, next) {
  ZH.l('ZDConn: handle_device_init');
  var ndkey = ZS.NextDeviceUUID;
  plugin.do_increment(collections.global_coll, ndkey, "value", 1,
  function (ierr, ires) {
    if (ierr) next(ierr, null);
    else {
      var nuuid                      = Number(ires.value);
      ZH.l('NextDeviceUUID: ' + nuuid);
      udata.device.uuid              = nuuid;
      udata.device.initialize_device = true;
      next(null, null);
    }
  });
}

function set_isolation_state(plugin, collections, auuid, b, next) {
  ZH.l('set_isolation_state: U: ' + auuid + ' VALUE: ' + b);
  if (b) ZH.CentralIsolatedAgents[auuid] = true;
  else   delete(ZH.CentralIsolatedAgents[auuid]);
  var sub  = ZH.CreateSub(auuid);
  var okey = ZS.GetIsolatedState(sub);
  var skey = ZS.AllIsolatedDevices;
  if (b) {
    plugin.do_set_field(collections.global_coll, okey, "value", b,
    function (serr, sres) {
      if (serr) next(serr, null);
      else {
        plugin.do_set_field(collections.global_coll, skey, auuid, b, next);
      }
    });
  } else {
    plugin.do_remove(collections.global_coll, okey, function (rerr, rres) {
      if (rerr) next(rerr, null);
      else {
        plugin.do_unset_field(collections.global_coll, skey, auuid, next);
      }
    });
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT ONLINE --------------------------------------------------------------

exports.HandleStorageAgentOnline = function(net, b, agent, perms, schans,
                                            susers, created, hres, next) {
  hres.user_data = {}; // Used in response
  var auuid      = Number(agent.uuid);
  var dkey       = agent.key;
  var udata      = hres.user_data;
  udata.device   = {uuid : auuid};
  ZH.l('HandleStorageAgentOnline: U: ' + auuid);
  persist_device_key(net, auuid, dkey, function(aerr, ares) {
    if (aerr) next(aerr, hres);
    else {
      if (!b) { // OFFLINE
        udata.offline = true;
        set_isolation_state(net.plugin, net.collections, auuid, true,
        function(serr, sres) {
          next(serr, hres);
        });
      } else {
        udata.offline = false;
        if (auuid === -1) { // New Device -> initialize
          handle_device_init(net.plugin, net.collections, agent, udata,
          function (ierr, ires) {
            if (ierr) next(ierr, hres);
            else {
              auuid = udata.device.uuid; // set in handle_device_init()
              set_isolation_state(net.plugin, net.collections, auuid, false,
              function(serr, sres) {
                next(serr, hres);
              });
            }
          });
        } else {            // Device was OFFLINE -> SYNC IT
          set_isolation_state(net.plugin, net.collections, auuid, false,
          function(ierr, ires) {
            if (ierr) next(ierr, hres);
            else {
              ZSumm.SummarizeAgentChanges(net.plugin, net.collections,
                                          auuid, perms, schans, susers,
                                          created, false, udata,
              function(serr, sres) {
                next(serr, hres);
              });
            }
          });
        }
      }
    }
  });
}

function generate_device_key(net, device, auuid, hres, next) {
  if (device.key) next(null, hres);
  else {
    var dkey                  = ZH.CreateDeviceKey();
    hres.user_data.device.key = dkey;
    persist_device_key(net, auuid, dkey, function(serr, sres) {
      next(serr, hres);
    });
  }
}

exports.RouterHandleAgentOnline = function(net, b, device, created, perms,
                                           schans, susers, hres, next) {
  hres.original_created = created; // Used in response
  ZSumm.RouterAdjustLocalCreated(net, created, function(aerr, acreated) {
    if (aerr) next(aerr, hres);
    else {
      ZDQ.StorageQueueRequestAgentOnline(net.plugin, net.collections, b, device,
                                         perms, schans, susers, acreated, hres,
      function(serr, sres) {
        if (serr) next(serr, hres);
        else {
          var auuid  = hres.user_data.device.uuid;
          var istate = !b;
          set_isolation_state(net.plugin, net.collections, auuid, istate,
          function(ierr, ires) {
            if (ierr) next(ierr, hres);
            else      generate_device_key(net, device, auuid, hres, next);
          });
        }
      });
    }
  });
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GEO/BROADCAST DEVICE KEY --------------------------------------------------

exports.HandleBroadcastHttpsAgent = function(net, rguuid, auuid,
                                             akey, server, next) {
  ZH.l('ZDConn.HandleBroadcastHttpsAgent');
  persist_device_key(net, auuid, akey, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      var ok = true;
      if (rguuid !== ZH.MyDataCenter) ok = false;
      else {
        var cnode = ZPart.GetClusterNode(auuid);
        if (cnode.device_uuid !== ZH.MyUUID) ok = false;
      }
      if (!ok) ZCloud.RemoveHttpsMap(auuid, server);
      next(null, null);
    }
  });
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT PING ----------------------------------------------------------------

exports.HandleStorageAgentRecheck = function(plugin, collections,
                                             agent, created, hres, next) {
  hres.user_data = {}; // Used in response
  var auuid      = Number(agent.uuid);
  var udata      = hres.user_data;
  ZH.l('HandleStorageAgentRecheck: U: ' + auuid + ' C: ' + created);
  var mkey       = ZS.StorageQueueMinSync;
  plugin.do_get_field(collections.global_coll, mkey, "value",
  function(gerr, mins) {
    if (gerr) next(gerr, null);
    else {
      if (!ZH.AmBoth && (!mins || (mins < created))) {
        udata.not_ready = true;
        next(null, hres);
      } else {
        ZSumm.SummarizeAgentChanges(plugin, collections,
                                    auuid, null, null, null,
                                    created, true, udata,
        function(serr, sres) {
          next(serr, hres);
        });
      }
    }
  });
}

exports.CentralHandleAgentRecheck = function(net, agent, created, hres, next) {
  ZH.l('ZDConn.CentralHandleAgentRecheck: U: ' + agent.uuid);
  hres.original_created = created; // Used in response
  ZSumm.RouterAdjustLocalCreated(net, created, function(aerr, acreated) {
    if (aerr) next(aerr, hres);
    else {
      ZDQ.StorageQueueRequestAgentRecheck(net.plugin, net.collections,
                                          agent, acreated, hres, next);
    }
  });
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STORAGE GET AGENT KEYS ----------------------------------------------------

exports.HandleStorageGetAgentKeys = function(plugin, collections,
                                             agent, nkeys, minage, wonly,
                                             hres, next) {
  hres.user_data = {}; // Used in response
  var auuid      = Number(agent.uuid);
  var udata      = hres.user_data;
  ZH.l('HandleStorageGetAgentKeys: U: ' + auuid);
  ZSumm.StorageSummarizeAgentKeyChanges(plugin, collections,
                                        auuid, nkeys, minage, wonly, udata,
  function(serr, sres) {
    next(serr, hres);
  });
}

exports.CentralHandleAgentGetAgentKeys = function(plugin, collections,
                                                  agent, nkeys, minage, wonly,
                                                  hres, next) {
  ZSumm.GetTopKeysFromSubscriberRMKC(plugin, collections,
                                     agent, nkeys, minage, wonly, hres,
  function(gerr, hit) {
    if (gerr) next(gerr, hres);
    else {
      if (hit) next(null, hres); // CACHE-HIT
      else {                     // CACHE-MISS
        ZDQ.StorageQueueRequestGetAgentKeys(plugin, collections,
                                            agent, nkeys, minage, wonly,
                                            hres, next);
      }
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GEO-NEED-MERGE KEYS DAEMON ------------------------------------------------

function do_geo_need_merge_keys(net, kss, next) {
  if (kss.length === 0) next(null, null);
  else {
    var ks = kss.shift();
    ZH.l('do_geo_need_merge_keys: K: ' + ks.kqk);
    ZDQ.PushRouterGeoNeedMerge(net.plugin, net.collections, ks,
    function(perr, pres) {
      if (perr) next(perr, null);
      else {
        if (kss.length === 0) { // NOTE: early exit
          next(null, null);
        } else {
          ZH.l('do_geo_need_merge_keys: SLEEP: ' + GeoNeedMergeKeysSleep);
          setTimeout(function() {
            do_geo_need_merge_keys(net, kss, next);
          }, GeoNeedMergeKeysSleep);
        }
      }
    });
  }
}

function geo_need_merge_keys(net, kss, next) {
  if (ZH.ChaosMode === 15) {
    ZH.e('CHAOS-MODE: ' + ZH.ChaosDescriptions[ZH.ChaosMode]);
    return next(null, null);
  }
  if (kss.length === 0) {
    ZH.e('CENTRAL_TOSYNC_KEYS: ZERO KEYS');
    next(null, null);
  } else {
    ZH.e('CENTRAL_TOSYNC_KEYS: #Ks: ' + kss.length); ZH.p(kss);
    do_geo_need_merge_keys(net, kss, next);
  }
}

function __run_central_tosync_keys_daemon(next) {
  if (!ZH.AmStorage) throw(new Error("central_tosynckeys_daemon LOGIC ERROR"));
  if (ZH.CentralDisableCentralToSyncKeysDaemon) return next(null, null);
  if (ZH.ClusterNetworkPartitionMode)           return next(null, null);
  CentralToSyncDaemonInProgress = true;
  var net = ZH.CreateNetPerRequest(ZH.Central);
  ZMDC.GetAllCentralKeysToSync(net.plugin, net.collections,
  function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      if (gres.length === 0) {
        ZH.e('CENTRAL_TOSYNC_KEYS: ZERO KEYS');
        next(null, null);
      } else {
        var kqks = gres[0];
        delete(kqks._id);
        var kss  = [];
        for (var kqk in kqks) {
          var ks    = ZH.ParseKQK(kqk);
          var cnode = ZPart.GetKeyNode(ks);
          if (cnode && cnode.device_uuid === ZH.MyUUID) kss.push(ks);
        }
        geo_need_merge_keys(net, kss, next);
      }
    }
  });
}

function run_central_tosync_keys_daemon() {
  __run_central_tosync_keys_daemon(function(nerr, nres) {
    CentralToSyncDaemonInProgress = false;
    if (nerr) ZH.e('run_central_tosync_keys_daemon: ERROR: ' + nerr);
    next_run_central_tosync_keys_daemon();
  });
}

function signal_central_tosync_keys_daemon() {
  if (!ZH.AmStorage) return;
  if (CentralToSyncDaemonInProgress) return;
  if (CentralToSyncDaemonInTimer) {
    clearTimeout(CentralToSyncDaemonInTimer);
    CentralToSyncDaemonInTimer = null;
  }
  run_central_tosync_keys_daemon();
}

function next_run_central_tosync_keys_daemon() {
  var min = MinNextToSyncInterval;
  var max = MaxNextToSyncInterval;
  var to  = min + ((max - min) * Math.random());
  CentralToSyncDaemonInTimer = setTimeout(run_central_tosync_keys_daemon, to);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DATACENTER ONLINE (OTHER DATACENTER NEEDS SUMMARY) ------------------------

function persist_device_key(net, auuid, dkey, next) {
  if (auuid === -1) next(null, null);
  else {
    ZH.l('persist_device_key: U: ' + auuid + ' DKEY: ' + dkey);
    ZH.CentralDeviceKeyMap[auuid] = dkey;
    var kkey                      = ZS.GetDeviceKey(auuid);
    var dkentry                   = {device_id  : auuid,
                                     device_key : dkey};
    net.plugin.do_set(net.collections.dk_coll, kkey, dkentry, next);
  }
}

function do_persist_device_keys(net, adkeys, next) {
  if (adkeys.length == 0) next(null, null);
  else {
    var adkey = adkeys.shift();
    var auuid = adkey.auuid;
    var dkey  = adkey.dkey;
    persist_device_key(net, auuid, dkey, function(serr, sres) {
      if (serr) next(serr, null);
      else      setImmediate(do_persist_device_keys, net, adkeys, next);
    });
  }
}

function persist_device_keys(net, dkeys, next) {
  ZH.l('persist_device_keys: ' + JSON.stringify(dkeys));
  var adkeys = [];
  for (var auuid in dkeys) {
    adkeys.push({auuid : auuid, dkey : dkeys[auuid]});
  }
  do_persist_device_keys(net, adkeys, next);
}

exports.HandleStorageDeviceKeys = function(net, dkeys, next) {
  ZH.l('ZDConn.HandleStorageDeviceKeys');
  persist_device_keys(net, dkeys, next);
}

exports.CentralHandleGeoBroadcastDeviceKeys = function(net, dkeys, hres, next) {
  ZH.l('ZDConn.CentralHandleGeoBroadcastDeviceKeys');
  ZDQ.PushStorageDeviceKeys(net.plugin, net.collections, dkeys,
  function(perr, pres) {
    next(perr, hres);
  });
}

exports.CentralHandleGeoDataCenterOnline = function(net, guuid, dkeys,
                                                    hres, next) {
  if (!ZH.CentralSynced) next(new Error(ZS.Errors.InGeoSync), hres);
  else {
    // NOTE: ROUTER uses DeviceKeys in ZCloud.initialize_central_device_key_map
    persist_device_keys(net, dkeys, function(serr, sres) {
      if (serr) next(serr, hres);
      else {
        ZDQ.StorageQueueRequestGeoDataCenterOnline(net.plugin, net.collections,
                                                   guuid, dkeys, hres, next);
      }
    });
  }
}

exports.HandleStorageGeoDataCenterOnline = function(net, guuid, dkeys,
                                                    hres, next) {
  ZH.l('HandleStorageGeoDataCenterOnline: GU: ' + guuid);
  hres.dc_data       = {}; // Used in response
  hres.dc_data.guuid = guuid;
  //NOTE: persist FIRST, ZSumm.SummarizeDataCenterChanges() sends UNION
  persist_device_keys(net, dkeys, function(serr, sres) {
    if (serr) next(serr, hres);
    else {
      ZSumm.SummarizeDataCenterChanges(net, guuid, hres.dc_data,
      function(aerr, ares) {
        next(aerr, hres);
      });
    }
  });
}

function persist_key_sync_metadata(plugin, collections, kss, next) {
  var need = kss ? kss.length : 0;
  if (need === 0) next(null, null);
  else {
    var done = 0;
    var nerr = false;
    for (var i = 0; i < need; i++) {
      var ks   = kss[i];
      ZMDC.SetCentralKeysToSync(plugin, collections, ks, function(serr, sres) {
        if      (nerr) return;
        else if (serr) {
          nerr = true;
          next(serr, null);
        } else {
          done += 1;
          if (done === need) next(null, null);
        }
      });
    }
  }
}

function sync_geo_keys(plugin, collections, kss, next) {
  ZH.l('sync_geo_keys: #Ks: ' + kss.length);
  persist_key_sync_metadata(plugin, collections, kss, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      //NOTE: signal_central_tosync_keys_daemon() is ASYNC
      signal_central_tosync_keys_daemon();
      next(null, null);
    }
  });
}

function sync_dc_users(net, users, next) {
  var need = users.length;
  if (need === 0) next(null, null);
  else {
    var done = 0;
    var nerr = false;
    for (var i = 0; i < need; i++) {
      var user     = users[i];
      var username = user.username;
      var role     = user.role;
      var phash    = user.phash;
      ZUM.CentralHandleGeoAddUser(net.plugin, net.collections,
                                  username, phash, role, true, {},
      function(serr, sres) {
        if      (nerr) return;
        else if (serr) {
          nerr = true;
          next(serr, null);
        } else {
          done += 1;
          if (done === need) next(null, null);
        }
      });
    }
  }
}

function flatten_channels(perms) {
  var uperms = [];
  for (var i = 0; i < perms.length; i++) {
    var perm     = perms[i];
    var username = perm.username;
    var channels = perm.channels;
    for (var j = 0; j < channels.length; j++) {
      var channel = channels[j];
      uperms.push({username : username, id : channel.id, priv : channel.priv});
    }
  }
  return uperms;
}

function sync_dc_permissions(net, perms, next) {
  var uperms = flatten_channels(perms);
  var need   = uperms.length;
  if (need === 0) next(null, null);
  else {
    var done = 0;
    var nerr = false;
    for (var i = 0; i < need; i++) {
      var uperm    = uperms[i];
      var username = uperm.username;
      var schanid  = uperm.id;
      var priv     = uperm.priv;
      var fullpriv = (priv === 'R') ? 'READ' : 'WRITE';
      ZH.l('ZDConn.sync_dc_permissions: U: ' + username + ' R: ' + schanid);
      ZUM.CentralHandleGeoGrantUser(net.plugin, net.collections,
                                    username, schanid, fullpriv, false, {},
      function(serr, sres) {
        if      (nerr) return;
        else if (serr) {
          nerr = true;
          next(serr, null);
        } else {
          done += 1;
          if (done === need) next(null, null);
        }
      });
    }
  }
}
function sync_dc_subscriptions(net, subscriptions, next) {
  var usubs = flatten_channels(subscriptions);
  var need  = usubs.length;
  if (need === 0) next(null, null);
  else {
    var done = 0;
    var nerr = false;
    for (var i = 0; i < need; i++) {
      var usub     = usubs[i];
      var username = usub.username;
      var auth     = {username : username};
      var schanid  = usub.id;
      ZChannel.CentralHandleGeoSubscribe(net.plugin, net.collections, ZH.MyUUID,
                                         schanid, auth, {},
      function(serr, sres) {
        if      (nerr) return;
        else if (serr) {
          nerr = true;
          next(serr, null);
        } else {
          done += 1;
          if (done === need) next(null, null);
        }
      });
    }
  }
}

function sync_dc_stationed_users(net, susers, next) {
  var ususers = [];
  for (var i = 0; i < susers.length; i++) {
    var suser = susers[i];
    var auuid = suser.device.uuid;
    var users = suser.users;
    for (var j = 0; j < users.length; j++) {
      var user = users[j];
      ususers.push({auuid : auuid, username : user});
    }
  }
  var need  = ususers.length;
  if (need === 0) next(null, null);
  else {
    var done = 0;
    var nerr = false;
    for (var i = 0; i < need; i++) {
      var ususer   = ususers[i];
      var auuid    = ususer.auuid;
      var username = ususer.username;
      var auth     = {username : username};
      ZSU.CentralHandleGeoStationUser(net.plugin, net.collections,
                                      auuid, auth, {},
      function(serr, sres) {
        if      (nerr) return;
        else if (serr) {
          nerr = true;
          next(serr, null);
        } else {
          done += 1;
          if (done === need) next(null, null);
        }
      });
    }
  }
}

function sync_dc_cached(net, cached, next) {
  var ucached = [];
  for (var i = 0; i < cached.length; i++) {
    var cache = cached[i];
    var suuid = cache.agent_uuid;
    var kss   = cache.kss;
    for (var j = 0; j < kss.length; j++) {
      var ks = kss[j];
      ucached.push({agent_uuid : suuid, ks : ks});
    }
  }
  var need = ucached.length;
  if (need === 0) next(null, null);
  else {
    var done = 0;
    var nerr = false;
    for (var i = 0; i < need; i++) {
      var ucache = ucached[i];
      var suuid  = ucache.agent_uuid;
      var ks     = ucache.ks;
      var watch  = false;
      var agent  = {uuid : suuid};
      ZCache.CentralHandleGeoClusterCache(net, ks, watch, agent, {},
      function(serr, sres) {
        if      (nerr) return;
        else if (serr) {
          nerr = true;
          next(serr, null);
        } else {
          done += 1;
          if (done === need) next(null, null);
        }
      });
    }
  }
}

function sync_dc_online_response_kss(plugin, collections, data, next) {
  var kss  = data.kss;
  var rkss = data.rkss;
  for (var i = 0; i < rkss.length; i++) { // Sync RKSS[]
    kss.push(rkss[i]);
  }
  sync_geo_keys(plugin, collections, kss, next);
}

//TODO FIXLOG
function handle_data_center_online_response(net, data, next) {
  var cached        = data.cached;
  var users         = data.users;
  var perms         = data.permissions;
  var subscriptions = data.subscriptions;
  var susers        = data.stationed_users;
  var dkeys         = data.device_keys;
  sync_dc_online_response_kss(net.plugin, net.collections, data,
  function(kerr, kres) {
    if (kerr) next(kerr, null);
    else {
      sync_dc_cached(net, cached, function(cerr, cres) {
        if (cerr) next(cerr, null);
        else {
          sync_dc_users(net, users, function(perr, pres) {
            if (perr) next(perr, null);
            else {
              sync_dc_permissions(net, perms, function(serr, sres) {
                if (serr) next(serr, null);
                else {
                  sync_dc_subscriptions(net, subscriptions,
                  function(uerr, ures) {
                    if (uerr) next(uerr, null);
                    else {
                      sync_dc_stationed_users(net, susers, 
                      function(werr, wres) {
                        if (werr) next(werr, null);
                        else      persist_device_keys(net, dkeys, next);
                      });
                    }
                  });
                }
              });
            }
          });
        }
      });
    }
  });
}

//TODO FIXLOG
exports.HandleStorageDataCenterOnlineResponse = function(plugin, collections,
                                                         data, next) {
  ZH.CentralSynced = true; // Accept Deltas & NeedMerges
  ZH.e('ZDConn.HandleStorageDataCenterOnlineResponse: ' + 
       ' ZH.CentralSynced: ' + ZH.CentralSynced);
  sync_dc_online_response_kss(plugin, collections, data, function(kerr, kres) {
    if (kerr) next(kerr, null);
    else {
      // NOTE: sets CentralSynced to true STORAGE-cluster-wide
      ZPio.BroadcastClusterStatus(plugin, collections, ZH.CentralSynced, next);
    }
  });
}

var RetrySendDataCenterOnlineTimer = null;

function handle_datacenter_online_response(err, data) {
  if (ZH.CentralSynced) {
    ZH.l('handle_datacenter_online_response: ALREADY SYNCED -> NO-OP');
    if (RetrySendDataCenterOnlineTimer) {
      clearTimeout(RetrySendDataCenterOnlineTimer);
      RetrySendDataCenterOnlineTimer = null;
    }
    return;
  }
  if (err) { // NOTE: RetryTimer NOT cleared
    ZH.e('SendDataCenterOnline: ERROR: ' + err.message);
    return;
  }
  if (RetrySendDataCenterOnlineTimer) {
    clearTimeout(RetrySendDataCenterOnlineTimer);
    RetrySendDataCenterOnlineTimer = null;
  }

  ZH.l('handle_datacenter_online_response');
  var net = ZH.CreateNetPerRequest(ZH.Central);
  ZDQ.PushStorageDataCenterOnlineResponse(net.plugin, net.collections, data,
  function(perr, pres) {
    if (perr) ZH.e(perr);
    else {
      handle_data_center_online_response(net, data, function(serr, sres) {
        if (serr) ZH.e(serr);
        else {
          ZH.CentralSynced = true; // Accept Deltas & NeedMerges
          ZH.e('DC_Online_response: ZH.CentralSynced: ' + ZH.CentralSynced);
          // NOTE: sets CentralSynced to true ROUTER-cluster-wide
          ZPio.BroadcastClusterStatus(net.plugin, net.collections,
                                      ZH.CentralSynced, ZH.OnErrLog);
        }
      });
    }
  });
}

exports.SendDataCenterOnline = function() {
  if (ZH.CentralSynced) {
    ZH.l('ZDConn.SendDataCenterOnline: ALREADY SYNCED -> NO-OP');
    return;
  }
  var rssleep = RetrySendDataCenterOnlineSleep;
  var net     = ZH.CreateNetPerRequest(ZH.Central);
  var gnode   = ZCLS.GetRandomGeoNode(); // RANDOM -> NOT ME (COULD BE PRIMARY)
  if (!gnode) {
    ZH.l('RESEND DATACENTER-ONLINE: SLEEP: ' + rssleep);
    setTimeout(function() {
      exports.SendDataCenterOnline();
    }, rssleep);
  } else {
    var udata = {};
    ZSumm.DataCenterSyncDeviceKeys(net, udata, function(gerr, gres) {
      if (gerr) {
        ZH.e(gerr.message);
        ZH.l('DB-ERROR: RESEND DATACENTER-ONLINE: SLEEP: ' + rssleep);
        setTimeout(function() {
          exports.SendDataCenterOnline();
        }, rssleep);
      } else {
        var dkeys = udata.device_keys;
        ZPio.GeoSendDataCenterOnline(net.plugin, net.collections,
                                     gnode, dkeys,
                                     handle_datacenter_online_response);
        // NOTE: BELOW is ASYNC
        RetrySendDataCenterOnlineTimer = setTimeout(function() {
          ZH.l('DATACENTER-ONLINE TIMEOUT -> RESEND');
          exports.SendDataCenterOnline();
        }, rssleep);
      }
    });
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STORAGE FREEZE KEY --------------------------------------------------------

exports.StorageHandleFreezeKey = function(net, ks, next) {
  if (ZCLS.AmPrimaryDataCenter()) next(null, null);
  else {
    ZH.l('ZDConn.StorageHandleFreezeKey: K: ' + ks.kqk);
    ZMDC.SetCentralKeysToSync(net.plugin, net.collections, ks, 
    function(serr, sres) {
      if (serr) next(serr, null);
      else {
        // NOTE: signal_central_tosync_keys_daemon() is ASYNC
        signal_central_tosync_keys_daemon();
        next(null, null);
      }
    });
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SIGNAL HANDLER ------------------------------------------------------------

function broadcast_cluster_status(net, next) {
  // NOTE: sets CentralSynced to true cluster-wide
  ZPio.BroadcastClusterStatus(net.plugin, net.collections,
                              ZH.CentralSynced, next);
}

function __do_signal_central_synced() {
  ZH.e('SIGNAL ROUTER -> CENTRAL-SYNCED');
  var net  = ZH.CreateNetPerRequest(ZH.Central);
  var next = ZH.OnErrLog;
  ZDQ.RouterQueueRequestStorageOnline(net.plugin, net.collections,
  function(perr, pres) {
    if (perr) next(perr, null);
    else      broadcast_cluster_status(net, next);
  });
}

function do_signal_central_synced() {
  ZH.l('do_signal_central_synced: RouterNode: ' + ZCLS.RouterNode);
  if (ZCLS.RouterNode) {
   __do_signal_central_synced();
  } else {
    ZH.e('do_signal_central_synced: SLEEP ' + SignalCentralSyncedSleep);
    setTimeout(function() {
      do_signal_central_synced();
    }, SignalCentralSyncedSleep);
  }
}

exports.SignalCentralSynced = function() {
  if (!ZH.AmStorage) {
    ZH.e('ZDConn.SignalCentralSynced: ROUTER -> IGNORE -> NO-OP');
  } else {
    do_signal_central_synced();
  }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRAPPERS ------------------------------------------------------------------

// NOTE: Used by ZCentral.init_complete()
exports.StartCentralToSyncKeysDaemon = function() {
  next_run_central_tosync_keys_daemon();
}

// NOTE: Used by ZVote.central_post_cluster_state_commit();
//               ZSignal["SIGXCPU"]
exports.SignalCentralToSyncKeysDaemon = function() {
  signal_central_tosync_keys_daemon();
}


