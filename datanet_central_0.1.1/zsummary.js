"use strict";

require('./setImmediate');

var ZChannel = require('./zchannel');
var ZVote    = require('./zvote');
var ZDQ      = require('./zdata_queue');
var ZMDC     = require('./zmemory_data_cache');
var ZCLS     = require('./zcluster');
var ZS       = require('./zshared');
var ZH       = require('./zhelper');

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SUBSCRIBER RECENTLY MODIFIED KEY CACHE ------------------------------------

var SubscriberRMK = {cache         : {},
                     ReapThreshold : 35,
                     Trim          : 10,
                     Birth         : ZH.GetMsTime()};

function cmp_when(c1, c2) {
  return (c1.when === c2.when) ? 0 : (c1.when > c2.when) ? 1 : -1;
}

function create_ks_from_cache(kqk, centry) {
  var ks            = ZH.ParseKQK(kqk);
  ks.modification   = centry.when;
  ks.num_bytes      = centry.nbytes;
  ks.security_token = ZH.GenerateKeySecurityToken(ks);
  ZH.SummarizeKS(ks);
  return ks;
}

function create_sorted_storage_RMK_entry(scache) {
  var skqks  = [];
  if (scache)  {
    for (var kqk in scache) {
      var sentry = ZH.clone(scache[kqk]);
      sentry.kqk = kqk;
      skqks.push(sentry);
    }
    skqks.sort(cmp_when);
  }
  return skqks;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ADD TO RMKC ---------------------------------------------------------------

function trim_storage_RMK(duuid, scache, nc) {
  var want  = SubscriberRMK.ReapThreshold - SubscriberRMK.Trim;
  var diff  = nc - want;
  ZH.l('trim_storage_RMK: diff: ' + diff);
  var skqks = create_sorted_storage_RMK_entry(scache);
  for (var i = 0; i < diff; i++) {
    var centry = skqks[i];
    var kqk    = centry.kqk;
    delete(scache[kqk]);
  }
}

function add_to_subscriber_rmkc(ks, duuid, cache, mcreated, nbytes) {
  ZH.l('AddToStorageRecentlyModifiedKeyCache: U: ' + duuid + ' K: ' + ks.kqk);
  if (!SubscriberRMK.cache[duuid]) SubscriberRMK.cache[duuid] = {};
  var scache     = SubscriberRMK.cache[duuid];
  scache[ks.kqk] = {when   : mcreated,
                    nbytes : nbytes,
                    cache  : cache};
  var nc = Object.keys(scache).length;
  if (nc > SubscriberRMK.ReapThreshold) trim_storage_RMK(duuid, scache, nc);
}

exports.AddToRecentlyModifiedKeyCache = function(ks, dentry, suuids) {
  var mcreated = dentry.delta._meta.created[ZH.MyDataCenter];
  var nbytes   = dentry.delta._meta.num_bytes;
  for (var i = 0; i < suuids.length; i++) {
    var suuid = suuids[i].UUID;
    var cache = suuids[i].cache;
    add_to_subscriber_rmkc(ks, suuid, cache, mcreated, nbytes);
  }
  ZH.l('AddToRecentlyModifiedKeyCache: K: ' + ks.kqk + ' #S: ' + suuids.length);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ROUTER GET-AGENT-KEYS (FROM SUBSCRIBER-RMKC) ------------------------------

exports.GetTopKeysFromSubscriberRMKC = function(plugin, collections,
                                                agent, nkeys, minage, wonly,
                                                udata, next) {
  if (nkeys && nkeys > SubscriberRMK.ReapThreshold) {
    return next(null, false);
  }
  if (minage && ((minage < SubscriberRMK.Birth) || 
                 (minage < ZCLS.ClusterBorn))) {
    return next(null, false);
  }
  ZH.l('ZSumm.GetTopKeysFromSubscriberRMKC: HIT');
  udata.user_data        = {};  // Used in response
  udata.user_data.device = {uuid : duuid}
  var udata  = udata.user_data;
  var duuid  = Number(agent.uuid);
  var scache = SubscriberRMK.cache[duuid];
  var skqks  = create_sorted_storage_RMK_entry(scache);
  udata.kss  = [];
  var cnt    = 0;
  for (var i = (skqks.length - 1); i >= 0; i--) {
    var centry = skqks[i];
    var kqk    = centry.kqk;
    var ok     = minage ? (centry.when > minage) : true;
    if (ok && (wonly && !centry.cache)) ok = false;
    if (ok) {
      var ks  = create_ks_from_cache(kqk, centry);
      udata.kss.push(ks);
      cnt    += 1;
    }
    if (cnt === nkeys) break;
  }
  next(null, true);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GET DEVICE-INFO HELPERS----------------------------------------------------

function get_all_users_perms(plugin, collections, dinfo, users, next) {
  if (users.length === 0) next(null, null);
  else {
    var username = users.shift();
    var pkey     = ZS.GetUserChannelPermissions(username);
    plugin.do_get(collections.uperms_coll, pkey, function(gerr, gres) {
      if (gerr) next(gerr, null);
      else {
        if (gres.length !== 0) {
          dinfo.permissions[username] = gres[0];
          delete(dinfo.permissions[username]._id)
        }
        setImmediate(get_all_users_perms, plugin, collections,
                     dinfo, users, next);
      }
    });
  }
}

function get_all_users_subscriptions(plugin, collections, dinfo, users, next) {
  if (users.length === 0) next(null, null);
  else {
    var username = users.shift();
    var pkey     = ZS.GetUserChannelSubscriptions(username);
    plugin.do_get(collections.usubs_coll, pkey, function(gerr, gres) {
      if (gerr) next(gerr, null);
      else {
        if (gres.length !== 0) {
          dinfo.usersubscriptions[username] = gres[0];
          delete(dinfo.usersubscriptions[username]._id)
          for (var schanid in dinfo.usersubscriptions[username]) {
            dinfo.devicesubscriptions[schanid] = true;
          }
        }
        setImmediate(get_all_users_subscriptions, plugin, collections,
                     dinfo, users, next);
      }
    });
  }
}

function get_cached_key_kmod(plugin, collections, akqks, dinfo, next) {
  if (akqks.length === 0) next(null, null);
  else {
    var kqk  = akqks.shift();
    var kkey = ZS.GetKeyModification(kqk);
    plugin.do_get(collections.global_coll, kkey, function(gerr, gres) {
      if (gerr) next(gerr, null);
      else {
        if (gres.length !== 0) {
          var kmod        = gres[0];
          dinfo.ckss[kqk] = kmod;
        }
        setImmediate(get_cached_key_kmod, plugin, collections,
                     akqks, dinfo, next);
      }
    });
  }
}

function get_device_cached_keys(plugin, collections, suuid, dinfo, next) {
  var dk_key = ZS.GetDeviceToKeys(suuid);
  plugin.do_get(collections.dtok_coll, dk_key, function (gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      if (gres.length === 0) next(null, null);
      else {
        var kqks  = gres[0];
        var akqks = [];
        for (var kqk in kqks) akqks.push(kqk);
        get_cached_key_kmod(plugin, collections, akqks, dinfo, next);
      }
    }
  });
}

function get_device_info(plugin, collections, suuid, dinfo, next) {
  dinfo.ckss = {};
  get_device_cached_keys(plugin, collections, suuid, dinfo,
  function (gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      var su_key = ZS.GetAgentStationedUsers(suuid);
      plugin.do_get(collections.su_coll, su_key, function(uerr, ures) {
        if (uerr) next(uerr, null);
        else {
          if (ures.length !== 0) {
            dinfo.susers = ures[0];
            delete(dinfo.susers._id);
          }
          var susers = [];
          for (var user in dinfo.susers) susers.push(user);
          dinfo.usersubscriptions   = {};
          dinfo.devicesubscriptions = {};
          get_all_users_subscriptions(plugin, collections, dinfo, susers,
          function(serr, sres) {
            if (serr) next(serr, null);
            else {
              var pusers = [];
              for (var user in dinfo.susers) pusers.push(user);
              dinfo.permissions = {};
              get_all_users_perms(plugin, collections, dinfo, pusers, next);
            }
          });
        }
      });
    }
  });
}

function add_all_channels_pkss(plugin, collections, udata, next) {
  var csubs = udata.replication_channels;
  var need  = csubs.length;
  if (need === 0) next(null, udata);
  else {
    var done = 0;
    var nerr = false;
    for (var i = 0; i < need; i++) {
      var schanid = csubs[i];
      ZChannel.GetChannelKeys(plugin, collections, schanid, true,
      function(gerr, pkss) {
        if      (nerr) return;
        else if (gerr) {
          nerr = true;
          next(gerr, null);
        } else {
          if (pkss !== null) { //TODO UNIQUE pkss
            for (var i = 0; i < pkss.length; i++) {
              var pks = pkss[i];
              ZH.SummarizeKS(pks);
              udata.pkss.push(pks);
            }
          }
          done += 1;
          if (done === need) next(null, udata);
        }
      });
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SUMMARIZE-CHANNEL HELPERS -------------------------------------------------

function send_propogate_subscribe(plugin, collections, sub, schanid,
                                  auth, perms) {
  ZChannel.GetChannelKeys(plugin, collections, schanid, true,
  function(gerr, pkss) {
    if (gerr) ZH.e(gerr.message);
    else {
      ZDQ.AsyncPushRouterSubscriberPropogateSubscribe(plugin, collections,
                                                      sub, schanid, auth,
                                                      perms, pkss);
    }
  });
}

function summarize_channel_changes(plugin, collections, dinfo, suuid,
                                   perms, schans, next) {
  ZH.l('summarize_channel_changes');
  var sub = ZH.CreateSub(suuid);
  for (var username in schans) {
    var auth          = {username : username}
    var subscriptions = schans[username];
    var duser         = dinfo.usersubscriptions[username];
    if (duser) {
      for (var schanid in subscriptions) {
        var match = duser[schanid];
        if (match) {
          delete(duser[schanid]);
          delete(subscriptions[schanid]);
        }
      }
      for (var schanid in subscriptions) {
        var sperms = subscriptions[schanid];
        ZDQ.AsyncPushRouterSubscriberPropogateUnsubscribe(plugin, collections,
                                                          sub, schanid,
                                                          auth, sperms);
      }
    }
  }
  for (var username in dinfo.usersubscriptions) {
    var auth           = {username : username}
    var dsubscriptions = dinfo.usersubscriptions[username];
    for (var schanid in dsubscriptions) {
      var dperms = dsubscriptions[schanid];
      send_propogate_subscribe(plugin, collections, sub, schanid, auth, dperms);
    }
  }

  for (var username in perms) {
    var auth        = {username : username}
    var permissions = perms[username];
    var duser       = dinfo.permissions[username];
    if (duser) {
      for (var schanid in permissions) {
        var match = duser[schanid];
        if (match) {
          delete(duser[schanid]);
          delete(permissions[schanid]);
        }
      }
      for (var schanid in permissions) {
        ZDQ.AsyncPushRouterSubscriberPropogateGrantUser(plugin, collections,
                                                        sub, auth,
                                                        schanid, 'REVOKE');
      }
    }
  }
  for (var username in dinfo.permissions) {
    var auth         = {username : username}
    var dpermissions = dinfo.permissions[username];
    for (var schanid in dpermissions) {
      var dperms = dpermissions[schanid];
      var priv  = (dperms === "W") ? "WRITE" : "READ";
        ZDQ.AsyncPushRouterSubscriberPropogateGrantUser(plugin, collections,
                                                        sub, auth,
                                                        schanid, priv);
    }
  }
  next(null, null);
}

function summarize_removed_users(plugin, collections, dinfo, suuid, susers,
                                 next) {
  ZH.l('summarize_removed_users');
  var sub     = ZH.CreateSub(suuid);
  var msusers = {};
  if (susers) {
    for (var i = 0; i < susers.length; i++) msusers[susers[i]] = true;
  }
  for (var username in msusers) {
    if (dinfo.susers) {
      var match = dinfo.susers[username];
      if (match) {
        delete(msusers[username]);
        delete(dinfo.susers[username]);
      }
    }
  }
  for (var username in msusers) { // User exists only on AGENT -> REMOVED
    var auth = {username : username}
    ZDQ.AsyncPushRouterSubscriberPropogateRemoveUser(plugin, collections,
                                                     sub, auth);
  }
  next(null, null);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REMOVED KEYS HELPER -------------------------------------------------------

function add_all_removed_keys_to_response(plugin, collections, dsubs, is_agent,
                                          udata, next) {
  ZH.l('add_all_removed_keys_to_response');
  plugin.do_scan(collections.ldr_coll, function(serr, rldrs) {
    if (serr) next(serr, null);
    else {
      if (rldrs.length === 0) next(null, null);
      else {
        for (var i = 0; i < rldrs.length; i++) {
          var meta = rldrs[i].meta;
          var mod  = rldrs[i]["@"];
          if (!is_agent) { // CENTRAL
            var ks = ZH.CompositeQueueKey(meta.ns, meta.cn, meta._id);
            ks.modification = mod;
            if (!udata.kss[ks.kqk]) {
              ZH.SummarizeKS(ks);
              udata.rkss.push(ks);
            }
          } else {         // AGENT
            if (dsubs) {
              var rchans = meta.replication_channels;
              var match  = false;
              for (var j = 0; j < rchans.length; j++) {
                var rchan = rchans[j];
                if (dsubs[rchan]) { // Device Subscription HIT
                  match = true;
                  break;
                }
              }
              if (match) {
                var ks = ZH.CompositeQueueKey(meta.ns, meta.cn, meta._id);
                ks.modification = mod;
                if (!udata.pkss[ks.kqk]) {
                  ZH.SummarizeKS(ks);
                  udata.rkss.push(ks);
                }
              }
            }
          }
        }
        next(null, null);
      }
    }
  });
}

// KEY REMOVED BEFORE KEY INSERTED
function delete_invalid_agent_rkss(udata) {
  var kmods = {};
  for (var i = 0; i < udata.pkss.length; i++) {
    kmods[udata.pkss[i].kqk] = udata.pkss[i].modification;
  }
  for (var i = 0; i < udata.ckss.length; i++) {
    kmods[udata.ckss[i].kqk] = udata.ckss[i].modification;
  }
  var tor = [];
  for (var i = 0; i < udata.rkss.length; i++) {
    var rkss = udata.rkss[i];
    var kqk  = rkss.kqk;
    var kmod = kmods[kqk];
    if (kmod) {
      var rmod = rkss.modification;
      if (rmod <= kmod) {
        ZH.l('delete_invalid_agent_rkss: K: ' + kqk + ' rmod: ' + rmod +
             ' kmod: ' + kmod);
        tor.push(i);
      }
    }
  }
  for (var i = (tor.length - 1); i >= 0; i--) {
    udata.rkss.splice(tor[i], 1);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SUMMARIZE CHANGES FOR AGENT -----------------------------------------------

function do_watched_ckss(plugin, collections,
                         akqks, dckss, suuid, is_keep, next) {
  if (akqks.length === 0) next(null, null);
  else {
    var kqk = akqks.shift();
    var ks  = ZH.ParseKQK(kqk);
    ZMDC.GetAgentWatchKeys(plugin, collections, ks, suuid,
    function(gerr, watch) {
      if (gerr) next(gerr, null);
      else {
        ZH.l('watched_ckss: K: ' + ks.kqk + ' U: ' + suuid +
             ' K: ' + is_keep + ' W: ' + watch);
        if (is_keep) { // KEEP WATCHED
          if (!watch) delete(dckss[ks.kqk]);
        } else {       // REMOVE WATCHED
          if (watch) delete(dckss[ks.kqk]);
        }
        setImmediate(do_watched_ckss, plugin, collections,
                     akqks, dckss, suuid, is_keep, next);
      }
    });
  }
}

function remove_watched_ckss(plugin, collections, akqks, dckss, suuid, next) {
  do_watched_ckss(plugin, collections, akqks, dckss, suuid, false, next);
}

function keep_watched_ckss(plugin, collections, akqks, dckss, suuid, next) {
  do_watched_ckss(plugin, collections, akqks, dckss, suuid, true, next);
}

function add_ckss_from_dinfo(plugin, collections, dinfo, udata, next) {
  udata.ckss = [];
  for (var kqk in dinfo.ckss) {
    var cks            = ZH.ParseKQK(kqk);
    var kmod           = dinfo.ckss[kqk];
    cks.modification   = kmod.modification;
    cks.num_bytes      = kmod.num_bytes;
    cks.security_token = ZH.GenerateKeySecurityToken(cks);
    ZH.SummarizeKS(cks);
    udata.ckss.push(cks);
  }
  next(null, null);
}

function add_ckss(plugin, collections,
                  suuid, is_gk, wonly, dinfo, udata, next) {
  if (is_gk) { // GetAgentKeys include WATCHED keys
    if (!wonly) {
      add_ckss_from_dinfo(plugin, collections, dinfo, udata, next);
    } else {
      var akqks = []
      for (var kqk in dinfo.ckss) akqks.push(kqk);
      keep_watched_ckss(plugin, collections, akqks, dinfo.ckss, suuid,
      function(rerr, rres) {
        if (rerr) next(rerr, null);
        else      add_ckss_from_dinfo(plugin, collections, dinfo, udata, next);
      });
    }
  } else {
    var akqks = []
    for (var kqk in dinfo.ckss) akqks.push(kqk);
    remove_watched_ckss(plugin, collections, akqks, dinfo.ckss, suuid,
    function(rerr, rres) {
      if (rerr) next(rerr, null);
      else      add_ckss_from_dinfo(plugin, collections, dinfo, udata, next);
    });
  }
}

function get_agent_full_kss_info(plugin, collections,
                                 suuid, dinfo, is_gk, wonly, udata, next) {
  if (wonly) { // NOTE: next(null, null) -> dsubs - s null
    udata.replication_channels = []; // Used in response
    udata.pkss                 = []; // Used in response
    add_ckss(plugin, collections, suuid, is_gk, true, dinfo, udata, next);
  } else {
    var sub   = ZH.CreateSub(suuid);
    var dskey = ZS.GetDeviceSubscriptions(sub);
    plugin.do_get(collections.global_coll, dskey, function(gerr, gres) {
      if (gerr) next(gerr, null);
      else {
        var dsubs                 = null;
        udata.replication_channels = []; // Used in response
        if (gres.length !== 0) {
          dsubs = gres[0];
          delete(dsubs._id);
          for (var schanid in dsubs) udata.replication_channels.push(schanid);
        }
        udata.pkss = []; // Used in response
        add_all_channels_pkss(plugin, collections, udata, function(aerr, ares) {
          if (aerr) next(aerr, null);
          else {
            add_ckss(plugin, collections, suuid, is_gk, false, dinfo, udata,
            function(serr, sres) {
              next(serr, dsubs);
            });
          }
        });
      }
    });
  }
}

function get_agent_full_info(plugin, collections, suuid,
                             perms, schans, susers, is_ping, dinfo,
                             udata, next) {
  ZH.l('get_agent_full_info: U: ' + suuid);
  get_agent_full_kss_info(plugin, collections,
                          suuid, dinfo, false, false, udata,
  function(gerr, dsubs) {
    if (gerr) next(gerr, null);
    else {
      udata.rkss = []; // Used in response
      // NOTE: must come AFTER get_agent_full_kss_info()
      add_all_removed_keys_to_response(plugin, collections, dsubs, true, udata,
      function(kerr, kres) {
        if (kerr) next(kerr, null);
        else {
          delete_invalid_agent_rkss(udata);
          udata.pkss.sort(ZH.CmpModification);
          udata.rkss.sort(ZH.CmpModification);
          if (is_ping) next(null, null);
          else {
            summarize_channel_changes(plugin, collections, dinfo, suuid,
                                      perms, schans,
            function(serr, sres) {
              if (serr) next(serr, null);
              else {
                summarize_removed_users(plugin, collections, dinfo,
                                        suuid, susers, next);
              }
            });
          }
        }
      });
    }
  });
}

exports.SummarizeAgentChanges = function(plugin, collections, suuid,
                                         perms, schans, susers, created,
                                         is_ping, udata, next) {
  ZH.l('ZSumm.SummarizeAgentChanges: U: ' + suuid);
  var dinfo = {};
  get_device_info(plugin, collections, suuid, dinfo, function(aerr, ares) {
    if (aerr) next(aerr, null);
    else {
      get_agent_full_info(plugin, collections,
                          suuid, perms, schans, susers, is_ping, dinfo, udata,
      function(gerr, gres) {
        if (gerr) next(gerr, null);
        else {
          trim_younger_than_minimum_age(created, udata.pkss, "pkss");
          trim_younger_than_minimum_age(created, udata.ckss, "ckss");
          trim_younger_than_minimum_age(created, udata.rkss, "rkss");
          next(null, null);
        }
      });
    }
  });
}
  
// NOTE: if 'created' <  LastClusterState.cluster_born -> FULL-SYNC
//       if 'created' <  lowest LiveClusterNode PingTime -> adjust to MINTS
exports.RouterAdjustLocalCreated = function(net, created, next) {
  ZH.l('ZSumm.RouterAdjustLocalCreated: C: ' + created);
  if (!created) next(null, 0);
  else {
    var acreated = Number(created);
    var ckey     = ZS.ClusterState;
    net.plugin.do_get_field(net.collections.global_coll, ckey, "cluster_born",
    function(gerr, cborn) {
      if (gerr) next(gerr, null);
      else {
        if (cborn) {
          ZH.l('ADJUST created: CB: ' + cborn);
          if (acreated < cborn) {
            ZH.l('Created adjusted -> BEFORE CLUSTER_BORN -> FULL SYNC');
            return next(null, 0);
          }
        }
        // A ClusterNode may be down, but not yet Voted out
        ZVote.FetchClusterStatus(net.plugin, net.collections,
        function(cerr, cstat) {
          if (cerr) next(cerr, null);
          else {
            var mints = 0; // Earliest Ping of LiveClusterNodes
            for (var cuuid in cstat) {
              if (ZCLS.GetClusterNodeByUUID(cuuid)) {
                var cts = cstat[cuuid].ts * 1000;
                if      (mints === 0) mints = cts;
                else if (cts < mints) mints = cts;
              }
            }
            if (acreated > mints) {
              ZH.l('CREATED: C: ' + created +
                   ' SET TO Lowest-LiveClusterNode-PingTime: ' + mints);
              acreated = mints;
            }
            next(null, acreated);
          }
        });
      }
    });
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STORAGE GET AGENT KEYS ----------------------------------------------------

function cmp_ks_mod(k1, k2) {
  return (k1.modification === k2.modification) ? 0 :
         (k1.modification >   k2.modification) ? -1 : 1; // DESC
}

function combine_pkss_and_ckss_to_kss(udata) {
  var kss = [];
  for (var i = 0; i < udata.pkss.length; i++) {
    kss.push(udata.pkss[i]);
  }
  for (var i = 0; i < udata.ckss.length; i++) {
    kss.push(udata.ckss[i]);
  }
  return kss;
}

function get_top_keys(plugin, collections, nkeys, kss, udata, next) {
  if (!nkeys) {
    udata.kss = kss;
    next(null, null);
  } else {
    kss.sort(cmp_ks_mod);
    udata.kss = [];
    var end   = (nkeys <= kss.length) ? nkeys : kss.length;
    for (var i = 0; i < end; i++) {
      delete(kss[i].security_token);
      udata.kss.push(kss[i]);
    }
    next(null, null);
  }
}

function trim_younger_than_minimum_age(minage, kss, desc) {
  if (!minage) return;
  var tor = [];
  for (var i = 0; i < kss.length; i++) {
    var ks  = kss[i];
    var age = ks.modification;
    if (age <= minage) {
      ZH.l('TOO YOUNG: (' + desc + ') K: ' + ks.kqk +
           ' A: ' + age + ' MA: ' + minage);
      tor.push(i);
    }
  }
  for (var i = (tor.length - 1); i >= 0; i--) {
    kss.splice(tor[i], 1);
  }
}

exports.StorageSummarizeAgentKeyChanges = function(plugin, collections,
                                                   suuid, nkeys, minage, wonly,
                                                   udata, next) {
  var dinfo = {};
  ZH.l('ZSumm.StorageSummarizeAgentKeyChanges: U: ' + suuid + ' M: ' + minage);
  get_device_info(plugin, collections, suuid, dinfo, function(aerr, ares) {
    if (aerr) next(aerr, null);
    else {
      get_agent_full_kss_info(plugin, collections,
                              suuid, dinfo, true, wonly, udata,
      function(gerr, gres) {
        if (gerr) next(gerr, null);
        else {
          var kss = combine_pkss_and_ckss_to_kss(udata);
          delete(udata.pkss);
          delete(udata.ckss);
          trim_younger_than_minimum_age(minage, kss, "kss");
          get_top_keys(plugin, collections, nkeys, kss, udata, next);
        }
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STORAGE SPRINKLE ----------------------------------------------------------

exports.HandleStorageSprinkle = function(net, tname, when, next) {
  ZH.l('ZSumm.HandleStorageSprinkle: T: ' + tname + ' @: ' + when);
  var skey = ZS.StorageSprinkle;
  net.plugin.do_get(net.collections.global_coll, skey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      var sprinkles = gres[0];
      net.plugin.do_set_field(net.collections.global_coll, skey, tname, when,
      function(serr, sres) {
        if (serr) next(serr, null);
        else {
          if (!sprinkles) next(null, null);
          else {
            var omins = Number.MAX_VALUE;
            for (var t in sprinkles) {
              var ts = sprinkles[t];
              if (ts < omins) omins = ts;
            }
            sprinkles[tname] = when;
            var nmins        = Number.MAX_VALUE;
            for (var t in sprinkles) {
              var ts = sprinkles[t];
              if (ts < nmins) nmins = ts;
            }
	    if (omins === nmins) next(null, null);
            else {
              ZH.l('NEW: StorageQueueMinSync: ' + nmins);
              var mkey = ZS.StorageQueueMinSync;
              net.plugin.do_set_field(net.collections.global_coll,
                                      mkey, "value", nmins, next);
            }
          }
        }
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SUMMARIZE CHANGES FOR DATACENTER ------------------------------------------

function scan_ns_cn(plugin, collections, ns, cn, kss, next) {
  ZH.l('scan_ns_cn: NS: ' + ns + ' CN: ' + cn);
  var net = ZH.CreateNetPerRequest(ZH.Central);
  net.plugin.do_scan(net.collections.crdt_coll, function (serr, crdts) {
    if (serr) next(serr, null);
    else {
      for (var i = 0; i < crdts.length; i++) {
        var crdt = crdts[i];
        var meta = crdt._meta;
        var ks   = ZH.CompositeQueueKey(meta.ns, meta.cn, meta._id);
        kss.push(ks);
      }
      next(null, null);
    }
  });
}

function fetch_entire_database_keys(plugin, collections, next) {
  ZH.l('fetch_entire_database_keys');
  plugin.do_get(collections.global_coll, ZS.AllNamespaceCollections,
  function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      var kss = [];
      if (gres.length === 0) next(null, kss);
      else {
        var nscns = gres[0];
        delete(nscns._id);
        var anscns = [];
        for (var nscn in nscns) {
          var res = nscn.split('|');
          var ns  = res[0];
          var cn  = res[1];
          anscns.push({ns : ns, cn : cn});
        }
        var need = anscns.length;
        if (need === 0) next(null, kss);
        else {
          var done = 0;
          var nerr = false;
          for (var i = 0; i < need; i++) {
            var anscn = anscns[i];
            scan_ns_cn(plugin, collections, anscn.ns, anscn.cn, kss,
            function(serr, sres) {
              if      (nerr) return;
              else if (serr) {
                nerr = true;
                next(serr, null);
              } else {
                done += 1;
                if (done === need) next(null, kss);
              }
            });
          }
        }
      }
    }
  });
}

function parse_username(plugin, res) {
  var arr = res._id.split("_");
  return arr[1];
}

function parse_device_uuid(plugin, res) {
  var arr = res._id.split("_");
  return arr[1];
}

function summary_add_users(net, udata, next) {
  net.plugin.do_scan(net.collections.uauth_coll, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      udata.users = [];
      for (var i = 0; i < sres.length; i++) {
        var res      = sres[i];
        var username = parse_username(net.plugin, res);
        var role     = res.role;
        var phash    = res.phash;
        udata.users.push({username : username, role : role, phash : phash});
      }
      next(null, null);
    }
  });
}

function summary_add_permissions(net, udata, next) {
  net.plugin.do_scan(net.collections.uperms_coll, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      udata.permissions = [];
      for (var i = 0; i < sres.length; i++) {
        var res      = sres[i];
        var username = parse_username(net.plugin, res);
        var perm     = {username : username, channels : []};
        delete(res._id);
        for (var schanid in res) {
          perm.channels.push({id : schanid, priv : res[schanid]});
        }
        udata.permissions.push(perm);
      }
      next(null, null);
    }
  });
}

function summary_add_subscriptions(net, udata, next) {
  net.plugin.do_scan(net.collections.usubs_coll, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      udata.subscriptions = [];
      for (var i = 0; i < sres.length; i++) {
        var res          = sres[i];
        var username     = parse_username(net.plugin, res);
        var subscription = {username : username, channels : []};
        delete(res._id);
        for (var schanid in res) {
          subscription.channels.push({id : schanid, priv : res[schanid]});
        }
        udata.subscriptions.push(subscription);
      }
      next(null, null);
    }
  });
}

function summary_add_stationed_users(net, udata, next) {
  net.plugin.do_scan(net.collections.su_coll, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      udata.stationed_users = [];
      for (var i = 0; i < sres.length; i++) {
        var res   = sres[i];
        var duuid = parse_username(net.plugin, res);
        var suser = {device : {uuid : duuid}, users : []};
        delete(res._id);
        for (var user in res) {
          suser.users.push(user);
        }
        udata.stationed_users.push(suser);
      }
      next(null, null);
    }
  });
}

function summary_add_cached_keys(net, udata, next) {
  net.plugin.do_scan(net.collections.dtok_coll, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      udata.cached = [];
      for (var i = 0; i < sres.length; i++) {
        var res   = sres[i];
        var suuid = parse_device_uuid(net.plugin, res);
        var cache = {agent_uuid : suuid, kss : []};
        delete(res._id);
        for (var kqk in res) {
          var ks = ZH.ParseKQK(kqk);
          cache.kss.push(ks);
        }
        udata.cached.push(cache);
      }
      next(null, null);
    }
  });
}

function data_center_sync_user_data(net, udata, next) {
  summary_add_users(net, udata, function(uerr, ures) {
    if (uerr) next(uerr, null);
    else {
      summary_add_permissions(net, udata, function(perr, pres) {
        if (perr) next(perr, null);
        else {
          summary_add_subscriptions(net, udata, function(serr, sres) {
            if (serr) next(serr, null);
            else      summary_add_stationed_users(net, udata, next);
          });
        }
      });
    }
  });
}

exports.DataCenterSyncDeviceKeys = function(net, udata, next) {
  udata.device_keys = {};
  ZH.l('data_center_sync_device_keys');
  net.plugin.do_scan(net.collections.dk_coll, function(serr, dkinfos) {
    if (serr) next(serr, null);
    else {
      for (var i = 0; i < dkinfos.length; i++) {
        var dkinfo               = dkinfos[i];
        var duuid                = dkinfo.device_id;
        var dkey                 = dkinfo.device_key;
        udata.device_keys[duuid] = dkey;
      }
      next(null, null);
    }
  });
}

exports.SummarizeDataCenterChanges = function(net, guuid, udata, next) {
  ZH.l('ZSumm.SummarizeDataCenterChanges: GU: ' + guuid);
  udata.rkss    = [];                  // Used in response
  udata.created = ZCLS.LastGeoCreated; // Used in response
  fetch_entire_database_keys(net.plugin, net.collections, function(ferr, kss) {
    if (ferr) next(ferr, null);
    else {
      udata.kss  = ZH.clone(kss);
      udata.rkss = []; // Used in response
      // NOTE: must come AFTER fetch_entire_database_keys()
      add_all_removed_keys_to_response(net.plugin, net.collections,
                                       null, false, udata,
      function(aerr, ares) {
        if (aerr) next(aerr, null);
        else {
          summary_add_cached_keys(net, udata, function(cerr, cres) {
            if (cerr) next(cerr, null);
            else {
              data_center_sync_user_data(net, udata, function(serr, sres) {
                if (serr) next(serr, null);
                else      exports.DataCenterSyncDeviceKeys(net, udata, next);
              });
            }
          });
        }
      });
    }
  });
}


