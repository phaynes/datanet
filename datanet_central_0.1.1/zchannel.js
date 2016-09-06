"use strict";

var ZPio, ZAio, ZCache, ZSU, ZAS, ZAuth, ZFix, ZMDC, ZDS, ZDQ, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  ZPio   = require('./zpio');
  ZAio   = require('./zaio');
  ZCache = require('./zcache');
  ZSU    = require('./zstationuser');
  ZAS    = require('./zactivesync');
  ZAuth  = require('./zauth');
  ZFix   = require('./zfixlog');
  ZMDC   = require('./zmemory_data_cache');
  ZDS    = require('./zdatastore');
  ZDQ    = require('./zdata_queue');
  ZS     = require('./zshared');
  ZH     = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

function get_user_subscriptions(plugin, collections, auth, next) {
  var pkey = ZS.GetUserChannelSubscriptions(auth.username);
  plugin.do_get(collections.usubs_coll, pkey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      var subs = {};
      if (gres.length !== 0) {
        subs = gres[0];
        delete(subs._id);
      }
      next(null, subs);
    }
  });
}

function get_user_agents(plugin, collections, auth, next) {
  var us_key = ZS.GetUserStationedOnAgents(auth.username);
  plugin.do_get(collections.users_coll, us_key, function (gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      var subs = [];
      if (gres.length !== 0) {
        var gsubs = gres[0];
        delete(gsubs._id);
        for (var suuid in gsubs) {
          subs.push(suuid);
        }
      }
      next(null, subs);
    }
  });
}

function get_channel_keys(plugin, collections, schanid, sectok, next) {
  var ckey = ZS.GetChannelToKeys(schanid);
  plugin.do_get(collections.global_coll, ckey, function (gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      var kss = [];
      if (gres.length !== 0) {
        var kqks = gres[0];
        delete(kqks._id);
        for (var kqk in kqks) {
          var ks          = ZH.ParseKQK(kqk);
          var kmod        = kqks[kqk];
          ks.modification = kmod.modification;
          ks.num_bytes    = kmod.num_bytes;
          if (sectok) ks.security_token = ZH.GenerateKeySecurityToken(ks);
          ZH.SummarizeKS(ks);
          kss.push(ks);
        }
      }
      kss.sort(ZH.CmpModification);
      next(null, kss);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS (DEVICE SUBSCRIPTIONS) --------------------------------------------

function store_channel_device(plugin, collections, sub, schanid, hres, next) {
  ZH.l('store_channel_device: R: ' + schanid + ' U: ' + sub.UUID);
  ZMDC.IncrementChannelToDevice(plugin, collections, schanid, sub, 1,
  function (ierr, ires) {
    next(ierr, hres);
  });
}

function destation_remove_channel_device(plugin, collections,
                                         sub, schanid, hres, next) {
  ZH.l('destation_remove_channel_device: R: ' + schanid + ' U: ' + sub.UUID);
  ZMDC.IncrementChannelToDevice(plugin, collections, schanid, sub, -1,
  function (ierr, ires) {
    next(ierr, hres);
  });
}

function store_device_subscription(plugin, collections, sub, schanid,
                                   hres, next) {
  ZH.l('store_device_subscription: U: ' + sub.UUID + ' R: ' + schanid);
  var dskey = ZS.GetDeviceSubscriptions(sub); // AGENT-SUBS
  plugin.do_increment(collections.global_coll, dskey, schanid, 1,
  function (serr, sres) {
    next(serr, hres);
  });
}

function destation_remove_device_subscription(plugin, collections,
                                              sub, schanid, hres, next) {
  ZH.l('destation_remove_device_subscription: U: ' + sub.UUID +
       ' R: ' + schanid);
  var dskey = ZS.GetDeviceSubscriptions(sub); // AGENT-SUBS
  plugin.do_get_field(collections.global_coll, dskey, schanid,
  function (gerr, nsubs) {
    if (gerr) next(gerr, hres);
    else {
      if (nsubs === 1) { // AGENT also no longer subscribed
        plugin.do_unset_field(collections.global_coll, dskey, schanid,
        function (uerr, ures) {
          next(uerr, hres);
        });
      } else {
        plugin.do_increment(collections.global_coll, dskey, schanid, -1,
        function (derr, dres) {
          next(derr, hres);
        });
      }
    }
  });
}

function station_channel(plugin, collections, sub, schanid, hres, next) {
  ZH.l('station_channel: U: ' + sub.UUID + ' R: ' + schanid);
  store_channel_device(plugin, collections, sub, schanid, hres,
  function(serr, sres) {
    if (serr) next(serr, hres);
    else {
      store_device_subscription(plugin, collections, sub, schanid, hres, next);
    }
  });
}


function destation_channel(plugin, collections, sub, schanid, hres, next) {
  ZH.l('destation_channel: U: ' + sub.UUID + ' R: ' + schanid);
  destation_remove_channel_device(plugin, collections, sub, schanid, hres,
  function(rerr, rres) {
    if (rerr) next(rerr, hres);
    else {
      destation_remove_device_subscription(plugin, collections,
                                           sub, schanid, hres, next);
    }
  });
}

function store_user_subscription(plugin, collections, schanid, perm, auth,
                                 hres, next) {
  ZH.l('store_user_subscription: UN: ' + auth.username + ' R: ' + schanid);
  var pkey = ZS.GetUserChannelSubscriptions(auth.username);  // USER-SUBS
  plugin.do_set_field(collections.usubs_coll, pkey, schanid, perm,
  function(perr, pres) {
    next(perr, hres);
  });
}

function remove_user_subscription(plugin, collections, schanid, auth,
                                  hres, next) {
  ZH.l('remove_user_subscription: UN: ' + auth.username + ' R: ' + schanid);
  var pkey = ZS.GetUserChannelSubscriptions(auth.username);  // USER-SUBS
  plugin.do_unset_field(collections.usubs_coll, pkey, schanid,
  function(perr, pres) {
    next(perr, hres);
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS (MULTIPLE SUBSCRIPTIONS) ------------------------------------------

function store_subscribe(plugin, collections, schanid, perm, sub, auth,
                         hres, next) {
  ZH.l('store_subscribe: UN: ' + auth.username + ' R: ' + schanid +
       ' P: ' + perm);
  station_channel(plugin, collections, sub, schanid, hres,
  function(serr, sres) {
    if (serr) next(serr, hres);
    else {
      store_user_subscription(plugin, collections, schanid, perm, auth,
                              hres, next);
    }
  });
}

function store_unsubscribe(plugin, collections, schanid, sub, auth, hres, next){
  ZH.l('store_unsubscribe: UN: ' + auth.username + ' R: ' + schanid);
  destation_channel(plugin, collections, sub, schanid, hres,
  function(rerr, rres) {
    if (rerr) next(rerr, hres);
    else {
      remove_user_subscription(plugin, collections, schanid, auth, hres, next);
    }
  });
}

function store_subscriptions(plugin, collections, tos, sub, auth, hres, next) {
  var need = tos.length;
  if (need === 0) next(null, hres);
  else {
    var done = 0;
    var nerr = false;
    for (var i = 0; i < need; i++) {
      var e = tos[i];
      store_subscribe(plugin, collections, e.schanid, e.perm, sub, auth, {},
      function(serr, sres) {
        if      (nerr) return;
        else if (serr) {
          nerr = true;
          next(serr, hres);
        } else {
          done += 1;
          if (done === need) next(null, hres);
        }
      });
    }
  }
}

function agent_store_user_subscriptions(plugin, collections, csubs, sub, auth,
                                        hres, next) {
  var tostore = []; // Convert to array for recursive callback function
  for (var schanid in csubs) {
    tostore.push({schanid : schanid, perm : csubs[schanid]});
  }
  store_subscriptions(plugin, collections, tostore, sub, auth, hres, next)
}

function remove_subscriptions(plugin, collections, tor, sub, auth, hres, next) {
  var need = tor.length;
  if (need === 0) next(null, hres);
  else {
    var done = 0;
    var nerr = false;
    for (var i = 0; i < need; i++) {
      var e = tor[i];
      store_unsubscribe(plugin, collections, e.schanid, sub, auth, hres,
      function(serr, sres) {
        if      (nerr) return;
        else if (serr) {
          nerr = true;
          next(serr, hres);
        } else {
          done += 1;
          if (done === need) next(null, hres);
        }
      });
    }
  }
}

function agent_remove_user_subscriptions(plugin, collections, csubs, sub, auth,
                                         hres, next) {
  var toremove = []; // Convert to array for recursive callback function
  for (var schanid in csubs) {
    toremove.push({schanid : schanid, perm : csubs[schanid]});
  }
  remove_subscriptions(plugin, collections, toremove, sub, auth, hres, next)
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT HANDLE GET-SUBSCRIPTION ---------------------------------------------

function agent_add_subs_to_response(csubs, hres) {
  var subs = [];
  for (var r in csubs) {
    subs.push(r);
  }
  hres.subscriptions = subs;
}

exports.AgentGetAgentSubscriptions = function(plugin, collections, hres, next){
  ZH.l('ZChannel.AgentGetAgentSubscriptions');
  var sub   = ZH.CreateMySub();
  var dskey = ZS.GetDeviceSubscriptions(sub);
  plugin.do_get(collections.global_coll, dskey, function(gerr, gres) {
    if (gerr) next(gerr, hres);
    else {
      if (gres.length) {
        var csubs = gres[0];
        delete(csubs._id);
        agent_add_subs_to_response(csubs, hres);
      }
      next(null, hres);
    }
  });
}

exports.AgentGetUserSubscriptions = function(plugin, collections, auth,
                                             hres, next) {
  ZH.l('ZChannel.AgentGetUserSubscriptions: UN: ' + auth.username);
  ZAio.SendCentralGetUserSubscriptions(plugin, collections, auth,
  function(serr, dres) {
    if (serr) next(serr, hres);
    else {
      var subs         = dres.subscriptions;
      agent_add_subs_to_response(subs, hres);
      hres.permissions = dres.permissions;
      next(null, hres);
    }
  });
}

exports.AgentGetUserInfo = function(plugin, collections, auth, hres, next) {
  ZH.l('ZChannel.AgentGetUserInfo: UN: ' + auth.username);
  ZAio.SendCentralGetUserInfo(plugin, collections, auth,
  function(derr, dres) {
    if (derr) next(derr, hres);
    else {
      hres.information = {permissions   : dres.permissions,
                          subscriptions : dres.subscriptions,
                          devices       : dres.devices};
      next(null, hres);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT SEND SUBSCRIBE TO CENTRAL -------------------------------------------

exports.AgentSubscribe = function(plugin, collections, schanid, auth,
                                  hres, next) {
  ZH.l('ZChannel.AgentSubscribe: R: ' + schanid);
  ZAio.SendCentralSubscribe(plugin, collections, schanid, auth,
  function(aerr, dres) {
    if (aerr) next(aerr, hres);
    else {
      var perm = dres.permissions;
      store_user_subscription(plugin, collections, schanid, perm, auth,
                              hres, next);
    }
  });
}

exports.AgentUnsubscribe = function(plugin, collections, schanid, auth,
                                    hres, next) {
  ZH.l('ZChannel.AgentUnsubscribe: R: ' + schanid);
  ZAio.SendCentralUnsubscribe(plugin, collections, schanid, auth,
  function(aerr, dres) {
    if (aerr) next(aerr, hres);
    else {
      remove_user_subscription(plugin, collections, schanid, auth, hres, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT HANDLE PROPOGATE SUBSCRIBE ------------------------------------------

exports.HandlePropogateSubscribe = function(net, schanid, perm, pkss,
                                            auth, hres, next) {
  if (pkss === null) pkss = [];
  ZH.l('ZChannel.HandlePropogateSubscribe: UN: ' + auth.username +
       ' R: ' + schanid + ' P: ' + perm);
  var sub = ZH.CreateMySub();
  store_subscribe(net.plugin, net.collections, schanid, perm, sub, auth, hres,
  function(serr, sres) {
    if (serr) next(serr, hres);
    else      ZAS.AgentSyncChannelKss(net, pkss, true, hres, next);
  });
}

exports.HandlePropogateUnsubscribe = function(net, schanid, auth, hres, next) {
  ZH.l('ZChannel.HandlePropogateUnsubscribe: UN: ' + auth.username +
       ' R: ' + schanid);
  var sub = ZH.CreateMySub();
  store_unsubscribe(net.plugin, net.collections, schanid, sub, auth, hres,
  function(serr, sres) {
    if (serr) next(serr, hres);
    else {
      ZAS.AgentHandleUnsyncChannel(net.plugin, net.collections, schanid,
                                   hres, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL PROPOGATE TO AGENTS -----------------------------------------------

function send_propogate(plugin, collections, sub, schanid, pkss,
                        station_func, send_func, auth, hres, next) {
  station_func(plugin, collections, sub, schanid, hres, function(serr, sres) {
    if (serr) next(serr, hres);
    else {
      var offline = ZH.IsSubscriberOffline(sub.UUID);
      if (!offline) {
        var perm     = hres.permissions; // Used ONLY by PropogateSubscribe
        var suuid    = sub.UUID;
        var username = auth.username;
        send_func(plugin, collections, suuid, schanid, username, perm, pkss);
      }
      next(null, hres);
    }
  });
}

function send_propogate_subscribe(plugin, collections, sub, schanid,
                                  pkss, auth, hres, next) {
  ZH.l('send_propogate_subscribe: U: ' + sub.UUID + ' R: ' + schanid);
  send_propogate(plugin, collections, sub, schanid, pkss,
                 station_channel, ZPio.SendSubscriberPropogateSubscribe,
                 auth, hres, next);
}

function send_propogate_unsubscribe(plugin, collections, sub, schanid,
                                    auth, hres, next) {
  ZH.l('send_propogate_unsubscribe: U: ' + sub.UUID + ' R: ' + schanid);
  send_propogate(plugin, collections, sub, schanid, null,
                 destation_channel, ZPio.SendSubscriberPropogateUnsubscribe,
                 auth, hres, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL SYNC/PROPOGATE HELPERS AGENT --------------------------------------

function sync_subscribe(plugin, collections, schanid, sub, auth, hres, next) {
  ZH.l('sync_subscribe: U: ' + sub.UUID + ' R: ' + schanid);
  get_channel_keys(plugin, collections, schanid, true, function(gerr, pkss) {
    if (gerr) next(gerr, hres);
    else {
      hres.pkss = pkss; // Used in response
      send_propogate_subscribe(plugin, collections, sub, schanid, pkss,
                               auth, hres, next);
    }
  });
}

function sync_unsubscribe(plugin, collections, schanid, sub, auth, hres, next) {
  ZH.l('sync_unsubscribe: U: ' + sub.UUID + ' R: ' + schanid);
  send_propogate_unsubscribe(plugin, collections, sub, schanid, auth,
                             hres, next);
}

function sync_agents(plugin, collections, sfunc, schanid, subs, auth,
                     hres, next) {
  var need = subs.length;
  if (need === 0) next(null, hres);
  else {
    var done = 0;
    var nerr = false;
    for (var i = 0; i < need; i++) {
      var suuid = subs[i];
      var sub   = ZH.CreateSub(suuid);
      sfunc(plugin, collections, schanid, sub, auth, hres,
      function(serr, sres) {
        if      (nerr) return;
        else if (serr) {
          nerr = true;
          next(serr, hres);
        } else {
          done += 1;
          if (done === need) next(null, hres);
        }
      });
    }
  }
}

function central_sync_agents(plugin, collections, sfunc, schanid, auth,
                             hres, next) {
  get_user_agents(plugin, collections, auth, function(gerr, subs) {
    if (gerr) next(gerr, hres);
    else {
      sync_agents(plugin, collections, sfunc, schanid, subs, auth, hres, next);
    }
  });
}

function subscribe_action_pre_check(plugin, collections, issub, schanid,
                                    auth, next) {
  var skey = ZS.GetUserChannelSubscriptions(auth.username);
  plugin.do_get_field(collections.usubs_coll, skey, schanid, 
  function (cerr, cres) {
    if (cerr) next(cerr, null);
    else {
      if        (issub  && cres !== null) {
        next(new Error(ZS.Errors.AlreadySubscribed), null);
      } else if (!issub && cres === null) {
        next(new Error(ZS.Errors.NotSubscribed), null);
      } else {
        ZAuth.GetCentralSubscribePermissions(plugin, collections, auth, schanid,
        function(gerr, perm) {
          if (gerr) next(gerr, null);
          else      next(null, perm); // Used ONLY by SUBSCRIBE
        });
      }
    }
  });
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL HANDLE SUBSCRIBE FROM AGENT ---------------------------------------

exports.CentralHandleBroadcastSubscribe = function(plugin, collections,
                                                   schanid, perm,
                                                   auth, hres, next) {
  hres.permissions = perm; // Used in response AND send_propogate()
  ZMDC.InvalidateChannelToDevice(schanid);
  central_sync_agents(plugin, collections, sync_subscribe,
                      schanid, auth, hres, next);
}

function do_central_handle_geo_subscribe(plugin, collections,
                                         sub, schanid, perm, auth, hres, next) {
  store_subscribe(plugin, collections, schanid, perm, sub, auth, hres,
  function (serr, sres) {
    if (serr) next(serr, hres);
    else {
      //NOTE: ZPio.BroadcastSubscribe() is ASYNC
      ZPio.BroadcastSubscribe(plugin, collections, schanid, perm, auth);
      next(null, hres);
    }
  });
}

exports.HandleStorageGeoSubscribe = function(net, duuid, schanid, auth, next) {
  ZH.l('ZChannel.HandleStorageGeoSubscribe: U: ' + duuid + ' R: ' + schanid);
  ZAuth.GetCentralSubscribePermissions(net.plugin, net.collections,
                                       auth, schanid,
  function(gerr, perm) {
    if (gerr) next(gerr, hres);
    else {
      var sub = ZH.CreateSub(duuid);
      do_central_handle_geo_subscribe(net.plugin, net.collections,
                                      sub, schanid, perm, auth, {}, next);
    }
  });
}

exports.CentralHandleGeoSubscribe = function(plugin, collections, duuid,
                                             schanid, auth, hres, next) {
  ZH.l('ZChannel.CentralHandleGeoSubscribe: U: ' + duuid + ' R: ' + schanid);
  schanid = ZH.IfNumberConvert(schanid);
  var sub = ZH.CreateSub(duuid);
  ZAuth.GetCentralSubscribePermissions(plugin, collections, auth, schanid,
  function(gerr, perm) {
    if (gerr) next(gerr, hres);
    else {
      ZDQ.PushStorageGeoSubscribe(plugin, collections,
                                  duuid, schanid, perm, auth,
      function(perr, pres) {
        if (perr) next(perr, hres);
        else {
          do_central_handle_geo_subscribe(plugin, collections,
                                          sub, schanid, perm, auth, hres, next);
        }
      });
    }
  });
}

// NOTE: NO DB WRITES -> HAPPENS ONLY IN ROUTER LAYER
exports.CentralHandleAgentSubscribe = function(plugin, collections, duuid,
                                               schanid, auth, hres, next) {
  ZH.l('ZChannel.CentralHandleAgentSubscribe: U: ' + duuid + ' R: ' + schanid);
  schanid = ZH.IfNumberConvert(schanid);
  var sub = ZH.CreateSub(duuid);
  subscribe_action_pre_check(plugin, collections, true, schanid, auth,
  function(perr, perm) {
    if (perr) next(perr, hres);
    else {
      var pkey = ZS.GetUserChannelPermissions(auth.username);
      plugin.do_get_field(collections.uperms_coll, pkey, schanid,
      function(gerr, perms) {
        if (gerr) next(gerr, hres);
        else {
          hres.permissions = perms; // Used in response
          //NOTE: ZPio.GeoBroadcastSubscribe() is ASYNC
          ZPio.GeoBroadcastSubscribe(plugin, collections,
                                     duuid, schanid, perm, auth);
          next(null, hres);
        }
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL HANDLE UNSUBSCRIBE FROM AGENT -------------------------------------

exports.CentralHandleBroadcastUnsubscribe = function(plugin, collections,
                                                     schanid,
                                                     auth, hres, next) {
  ZMDC.InvalidateChannelToDevice(schanid);
  central_sync_agents(plugin, collections, sync_unsubscribe,
                      schanid, auth, hres, next);
}

function do_central_handle_geo_unsubscribe(plugin, collections,
                                           sub, schanid, auth, hres, next) {
  store_unsubscribe(plugin, collections, schanid, sub, auth, hres,
  function (uerr, ures) {
    if (uerr) next(uerr, hres);
    else { // Broadcast sync_unsubscribe() to OTHER Cluster-Nodes
      //NOTE: ZPio.BroadcastUnsubscribe() is ASYNC
      ZPio.BroadcastUnsubscribe(plugin, collections, schanid, auth);
      next(null, hres);
    }
  });
}

exports.HandleStorageGeoUnsubscribe = function(net, duuid, schanid,
                                               auth, next) {
  ZH.l('ZChannel.HandleStorageGeoUnsubscribe: U: ' + duuid + ' R: ' + schanid);
  var sub = ZH.CreateSub(duuid);
  do_central_handle_geo_unsubscribe(net.plugin, net.collections,
                                    sub, schanid, auth, {}, next);
}

exports.CentralHandleGeoUnsubscribe = function(plugin, collections, duuid,
                                               schanid, auth, hres, next) {
  schanid = ZH.IfNumberConvert(schanid);
  ZH.l('ZChannel.CentralHandleGeoUnsubscribe: U: ' + duuid + ' R: ' + schanid);
  var sub = ZH.CreateSub(duuid);
  ZDQ.PushStorageGeoUnsubscribe(plugin, collections, duuid, schanid, auth,
  function(perr, pres) {
    if (perr) next(perr, hres);
    else {
      do_central_handle_geo_unsubscribe(plugin, collections,
                                        sub, schanid, auth, hres, next);
    }
  });
}

exports.CentralHandleAgentUnsubscribe = function(plugin, collections, duuid,
                                                 schanid, auth, hres, next) {
  schanid    = ZH.IfNumberConvert(schanid);
  ZH.l('ZChannel.CentralHandleAgentUnsubscribe: U: ' + duuid + ' R: ' +schanid);
  hres.duuid = duuid;   // Used in response
  subscribe_action_pre_check(plugin, collections, false, schanid, auth,
  function(perr, perm) {
    if (perr) next(perr, hres);
    else {
      //NOTE: ZPio.GeoBroadcastUnsubscribe() is ASYNC
      ZPio.GeoBroadcastUnsubscribe(plugin, collections, duuid, schanid, auth);
      next(null, hres);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL HANDLE GET-USER-SUBSCRIPTIONS FROM AGENT --------------------------

exports.CentralHandleGetUserSubscriptions = function(plugin, collections, auth,
                                                     hres, next) {
  ZH.l('ZChannel.CentralHandleGetUserSubscriptions: N: ' + auth.username);
  ZAuth.GetAllUserChannelPermissions(plugin, collections, auth, {},
  function(perr, pres) {
    if (perr) next(perr, hres);
    else {
      get_user_subscriptions(plugin, collections, auth, function(gerr, subs) {
        if (gerr) next(gerr, hres);
        else {
          hres.permissions   = pres.permissions;
          hres.subscriptions = subs;
          next(null, hres);
        }
      });
    }
  });
}

exports.CentralHandleGetUserInfo = function(net, auth, hres, next) {
  ZH.l('ZChannel.CentralHandleGetUserInfo: N: ' + auth.username);
  var ukey = ZS.GetUserAuthentication(auth.username);
  net.plugin.do_get(net.collections.uauth_coll, ukey, function(gerr, gres) {
    if (gerr) next(gerr, hres);
    else {
      if (gres.length === 0) next(new Error(ZS.Errors.NonExistentUser), hres);
      else {
        ZAuth.GetAllUserChannelPermissions(net.plugin, net.collections,
                                           auth, {},
        function(perr, pres) {
          if (perr) next(perr, hres);
          else {
            hres.permissions = pres.permissions ? pres.permissions : {};
            get_user_subscriptions(net.plugin, net.collections, auth,
            function(ferr, subs) {
              if (ferr) next(ferr, hres);
              else {
                var asubs = [];
                for (var schanid in subs) asubs.push(schanid);
                hres.subscriptions = asubs;
                var us_key = ZS.GetUserStationedOnAgents(auth.username);
                net.plugin.do_get(net.collections.users_coll, us_key,
                function(gerr, gres) {
                  if (gerr) next(gerr, hres);
                  else {
                    var adevices = []
                    if (gres.length !== 0) {
                      var devices = gres[0];
                      delete(devices._id);
                      for (var uuid in devices) adevices.push(uuid);
                    }
                    hres.devices = adevices;
                    next(null, hres);
                  }
                });
              }
            });
          }
        });
      }
    }
  });
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRAPPERS ------------------------------------------------------------------

exports.GetUserAgents = function(plugin, collections, auth, next) {
  get_user_agents(plugin, collections, auth, next);
}

exports.AgentStoreUserSubscriptions = function(plugin, collections, csubs, sub,
                                               auth, hres, next) {
  agent_store_user_subscriptions(plugin, collections, csubs, sub, auth,
                                 hres, next);
}

exports.AgentRemoveUserSubscriptions = function(plugin, collections, csubs, sub,
                                                auth, hres, next) {
  agent_remove_user_subscriptions(plugin, collections, csubs, sub, auth,
                                  hres, next);
}

exports.GetUserSubscriptions = function(plugin, collections, auth, next) {
  get_user_subscriptions(plugin, collections, auth, next);
}

exports.StationChannel = function(plugin, collections, sub, schanid,
                                  hres, next) {
  station_channel(plugin, collections, sub, schanid, hres, next);
}

exports.DestationChannel = function(plugin, collections, sub, schanid,
                                    hres, next) {
  destation_channel(plugin, collections, sub, schanid, hres, next);
}

// NOTE: Used by ZSumm.add_all_channels_keys_to_response()
//               ZSU.add_channels_pkss_to_reponse()
exports.GetChannelKeys = function(plugin, collections, schanid, sectok, next) {
  get_channel_keys(plugin, collections, schanid, sectok, next);
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZChannel']={} : exports);

