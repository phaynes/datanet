"use strict";

var ZPio, ZAio, ZChannel, ZAS, ZADaem, ZAuth, ZDQ, ZQ, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  require('./setImmediate');
  ZPio     = require('./zpio');
  ZAio     = require('./zaio');
  ZChannel = require('./zchannel');
  ZAS      = require('./zactivesync');
  ZADaem   = require('./zagent_daemon');
  ZAuth    = require('./zauth');
  ZDQ      = require('./zdata_queue');
  ZQ       = require('./zqueue');
  ZS       = require('./zshared');
  ZH       = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

function store_permission(plugin, collections, schanid, perms, auth, next) {
  ZH.l('store_permission: UN: ' + auth.username + ' R: ' + schanid);
  var pkey = ZS.GetUserChannelPermissions(auth.username);
  plugin.do_set_field(collections.uperms_coll, pkey, schanid, perms, next);
}

function store_permissions(plugin, collections, tostore, auth, hres, next) {
  var need = tostore.length;
  if (need === 0) next(null, hres);
  else {
    var done = 0;
    var nerr = false;
    for (var i = 0; i < need; i++) {
      var entry = tostore[i];
      store_permission(plugin, collections, entry.schanid, entry.perms, auth,
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

function agent_store_permissions(plugin, collections, cperms, auth,
                                 hres, next) {
  var tostore = []; // Convert to array for recursive callback function
  for (var schanid in cperms) {
    var perms = cperms[schanid];
    tostore.push({schanid : schanid, perms : cperms[schanid]});
  }
  store_permissions(plugin, collections, tostore, auth, hres, next)
}

function agent_remove_permissions(plugin, collections, auth, hres, next) {
  ZH.l('agent_remove_permissions: UN: ' + auth.username);
  var pkey = ZS.GetUserChannelPermissions(auth.username);
  plugin.do_remove(collections.uperms_coll, pkey, function(rerr, rres) {
    if (rerr) next(rerr, hres);
    else      next(null, hres);
  });
}

function store_stationed_user(plugin, collections, sub, auth, hres, next) {
  ZH.l('store_stationed_user: UN: ' + auth.username + ' U: ' + sub.UUID);
  var su_key = ZS.GetAgentStationedUsers(sub.UUID);
  plugin.do_set_field(collections.su_coll, su_key, auth.username, true,
  function (serr, sres) {
    if (serr) next(serr, hres);
    else {
      var us_key = ZS.GetUserStationedOnAgents(auth.username);
      plugin.do_set_field(collections.users_coll, us_key, sub.UUID, true,
      function (uerr, ures) {
        if (uerr) next(uerr, hres);
        else      next(null, hres);
      });
    }
  });
}

function get_stationed_user(plugin, collections, sub, auth, next) {
  ZH.l('get_stationed_user: UN: ' + auth.username + ' U: ' + sub.UUID);
  var su_key = ZS.GetAgentStationedUsers(sub.UUID);
  plugin.do_get_field(collections.su_coll, su_key, auth.username, next);
}

function do_remove_stationed_user(plugin, collections, sub, auth, hres, next) {
  ZH.l('do_remove_stationed_user: UN: ' + auth.username + ' U: ' + sub.UUID);
  var su_key = ZS.GetAgentStationedUsers(sub.UUID);
  plugin.do_unset_field(collections.su_coll, su_key, auth.username, 
  function (uerr, ures) {
    if (uerr) next(uerr, hres);
    else {
      var us_key = ZS.GetUserStationedOnAgents(auth.username);
      plugin.do_unset_field(collections.users_coll, us_key, sub.UUID, 
      function (uerr, ures) {
        if (uerr) next(uerr, hres);
        else      next(null, hres);
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT HANDLE STATION ------------------------------------------------------

function agent_is_user_stationed(plugin, collections, auth, next) {
  var sub    = ZH.CreateMySub();
  var su_key = ZS.GetAgentStationedUsers(sub.UUID);
  plugin.do_get_field(collections.su_coll, su_key, auth.username, next);
}

exports.AgentIsUserStationed = function(plugin, collections, auth, next) {
  agent_is_user_stationed(plugin, collections, auth, next);
}

function agent_station_user(plugin, collections, csubs, cperms,
                            auth, hres, next) {
  var sub = ZH.CreateMySub();
  store_stationed_user(plugin, collections, sub, auth, hres,
  function(aerr, ares) {
    if (aerr) next(aerr, hres);
    else {
      agent_store_permissions(plugin, collections, cperms, auth, hres,
      function(perr, pres) {
        if (perr) next(perr, hres);
        else {
          ZChannel.AgentStoreUserSubscriptions(plugin, collections, csubs, sub,
                                               auth, hres, next);
        }
      });
    }
  });
}

exports.AgentStationUser = function(net, auth, hres, next ) {
  ZH.l('ZSU.AgentStationUser: UN: ' + auth.username);
  var sub = ZH.CreateMySub();
  agent_is_user_stationed(net.plugin, net.collections, auth,
  function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (ok)   next(new Error(ZS.Errors.AlreadyStationed), hres);
    else {
      ZAio.SendCentralStationUser(net.plugin, net.collections, auth,
      function(cerr, dres) {
        if (cerr) next(cerr, hres);
        else {
          var csubs  = dres.subscriptions;
          var cperms = dres.permissions
          agent_station_user(net.plugin, net.collections,
                             csubs, cperms, auth, hres,
          function(uerr, ures) {
            if (uerr) next(uerr, hres);
            else {
              var pkss = dres.pkss;
              if (!pkss) next(null, hres);
              else       ZAS.AgentSyncChannelKss(net, pkss, false, hres, next);
            }
          });
        }
      });
    }
  });
}

function agent_destation_user(plugin, collections, csubs, auth, hres, next) {
  var sub = ZH.CreateMySub();
  ZChannel.AgentRemoveUserSubscriptions(plugin, collections, csubs, sub,
                                        auth, hres,
  function(rerr, rres) {
    if (rerr) next(rerr, hres);
    else {
      agent_remove_permissions(plugin, collections, auth, hres,
      function(aerr, ares) {
        if (aerr) next(aerr, hres);
        else {
          do_remove_stationed_user(plugin, collections, sub, auth, hres, next);
        }
      });
    }
  });
}

// NOTE: Used by ZUM.HandlePropogateRemoveUser()
exports.AgentProcessDestationUser = function(plugin, collections, auth,
                                             hres, next) {
  ZH.l('ZSU.AgentProcessDestationUser: UN: ' + auth.username);
  var sub = ZH.CreateMySub();
  ZChannel.GetUserSubscriptions(plugin, collections, auth,
  function(gerr, csubs) {
    if (gerr) next(gerr, hres);
    else {
      hres.subscriptions = csubs;
      agent_destation_user(plugin, collections, csubs, auth, hres, next);
    }
  });
}

exports.AgentDestationUser = function(plugin, collections, auth, hres, next ) {
  ZH.l('ZSU.AgentDestationUser: UN: ' + auth.username);
  agent_is_user_stationed(plugin, collections, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.NotStationed), hres);
    else {
      ZAio.SendCentralDestationUser(plugin, collections, auth,
      function(serr, dres) {
        if (serr) next(serr, hres);
        else {
          exports.AgentProcessDestationUser(plugin, collections, auth,
                                            hres, next);
        }
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GET-STATIONED-USERS -------------------------------------------------------

exports.AgentGetStationedUsers = function(plugin, collections, hres, next) {
  ZH.l('ZSU.AgentGetStationedUsers');
  var sub    = ZH.CreateMySub();
  var su_key = ZS.GetAgentStationedUsers(sub.UUID);
  plugin.do_get(collections.su_coll, su_key, function (gerr, gres) {
    if (gerr) next(gerr, hres);
    else {
      hres.users = []; // Used in response
      if (gres.length !== 0) {
        var users  = gres[0];
        delete(users._id);
        for (var user in users) { hres.users.push(user); }
      }
      next(null, hres);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL STATION-USER HELPERS ----------------------------------------------

function sync_channel(plugin, collections, sub, schanid, hres, next) {
  ZH.l('sync_channel: U: ' + sub.UUID + ' R: ' + schanid);
  ZChannel.StationChannel(plugin, collections, sub, schanid, hres, next);
}

function unsync_channel(plugin, collections, sub, schanid, hres, next) {
  ZH.l('unsync_channel: U: ' + sub.UUID + ' R: ' + schanid);
  ZChannel.DestationChannel(plugin, collections, sub, schanid, hres, next);
}

function sync_channels(plugin, collections, sfunc, sub, usubs, auth,
                      hres, next) {
  var need = usubs.length;
  if (need === 0) next(null, hres);
  else {
    var done = 0;
    var nerr = false;
    for (var i = 0; i < need; i++) {
      var us = usubs[i];
      sfunc(plugin, collections, sub, us.schanid, hres, function(serr, sres) {
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

// FUNCTION: central_sync_channels()
// PURPOSE:  sync single agent with all subscriptions of a single user
//
function central_sync_channels(plugin, collections, sfunc, sub, csubs, auth,
                               hres, next) {
  var usubs = [];
  for (var schanid in csubs) {
    usubs.push({schanid : schanid, perms : csubs[schanid]});
  }
  sync_channels(plugin, collections, sfunc, sub, usubs, auth, hres, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL HANDLE STATION-USER FROM AGENT ------------------------------------

function add_channels_pkss_to_reponse(plugin, collections, usubs, hres, next) {
  var apkss = {};
  var need  = usubs.length;
  if (need === 0) next(null, hres);
  else {
    var done = 0;
    var nerr = false;
    for (var i = 0; i < need; i++) {
      var schanid = usubs[i];
      ZChannel.GetChannelKeys(plugin, collections, schanid, true,
      function(gerr, pkss) {
        if      (nerr) return;
        else if (gerr) {
          nerr = true;
          next(gerr, hres);
        } else {
          for (var i = 0; i < pkss.length; i++) {
            var kqk    = pkss[i].kqk;
            apkss[kqk] = pkss[i];
          }
          done += 1;
          if (done === need) {
            for (var kqk in apkss) {
              var ks = apkss[kqk];
              hres.pkss.push(ks); // Used in response
            }
            next(null, hres);
          }
        }
      });
    }
  }
}

function add_pkss_to_response(plugin, collections, csubs, hres, next) {
  var usubs = [];
  for (var schanid in csubs) { usubs.push(schanid); }
  add_channels_pkss_to_reponse(plugin, collections, usubs, hres, next);
}

function central_station_user(plugin, collections, sub, auth, hres, next) {
  store_stationed_user(plugin, collections, sub, auth, hres,
  function (kerr, kres) {
    if (kerr) next(kerr, hres);
    else {
      ZChannel.GetUserSubscriptions(plugin, collections, auth,
      function(gerr, csubs) {
        if (gerr) next(gerr, hres);
        else {
          central_sync_channels(plugin, collections, sync_channel,
                                sub, csubs, auth, hres, next);
        }
      });
    }
  });
}

function station_user_response(plugin, collections, sub, auth, hres, next) {
  ZChannel.GetUserSubscriptions(plugin, collections, auth,
  function(gerr, csubs) {
    if (gerr) next(gerr, hres);
    else { // GetAllUserChannelPermissions() pops hres.permissions
      hres.subscriptions = csubs;
      ZAuth.GetAllUserChannelPermissions(plugin, collections, auth, hres,
      function(perr, pres) {
        if (perr) next(perr, hres);
        else {
          hres.pkss = []; // Used in response
          add_pkss_to_response(plugin, collections, csubs, hres, next);
        }
      });
    }
  });
}

exports.HandleStorageGeoStationUser = function(net, duuid, auth, next) {
  ZH.l('StorageGeoBroadcastStationUser: U: ' + duuid + ' UN: ' + auth.username);
  var sub = ZH.CreateSub(duuid);
  central_station_user(net.plugin, net.collections, sub, auth, {}, next);
}

exports.CentralHandleGeoStationUser = function(plugin, collections,
                                               duuid, auth, hres, next) {
  ZH.l('ZSU.CentralHandleGeoStationUser: U: ' + duuid +
       ' UN: ' + auth.username);
  ZDQ.PushStorageGeoStationUser(plugin, collections, duuid, auth,
  function(perr, pres) {
    if (perr) next(perr, hres);
    else {
      var sub = ZH.CreateSub(duuid);
      central_station_user(plugin, collections, sub, auth, hres, next);
    }
  });
}

// NOTE: NO DB WRITES -> HAPPENS ONLY IN ROUTER LAYER
exports.CentralHandleAgentStationUser = function(plugin, collections,
                                                 duuid, auth, hres, next) {
  ZH.l('ZSU.CentralHandleAgentStationUser: UN: ' + auth.username);
  var sub = ZH.CreateSub(duuid);
  get_stationed_user(plugin, collections, sub, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (ok)   next(new Error(ZS.Errors.AlreadyStationed), hres);
    else {
      //NOTE: ZPio.GeoBroadcastStationUser() is ASYNC
      ZPio.GeoBroadcastStationUser(plugin, collections, duuid, auth);
      station_user_response(plugin, collections, sub, auth, hres, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL HANDLE DESTATION-USER FROM AGENT ----------------------------------

function central_destation_user(plugin, collections, sub, auth, hres, next) {
  do_remove_stationed_user(plugin, collections, sub, auth, hres,
  function(rerr, rres) {
    if (rerr) next(rerr, hres);
    else {
      ZChannel.GetUserSubscriptions(plugin, collections, auth,
      function(gerr, csubs) {
        if (gerr) next(gerr, hres);
        else {
          hres.subscriptions = csubs;
          central_sync_channels(plugin, collections, unsync_channel,
                                sub, csubs, auth, hres, next);
        }
      });
    }
  });
}

exports.HandleStorageGeoDestationUser = function(net, duuid, auth, next) {
  ZH.l('StorageGeoBroadcastDestationUser: U: ' + duuid +
       ' UN: ' + auth.username);
  var sub = ZH.CreateSub(duuid);
  central_destation_user(net.plugin, net.collections, sub, auth, {}, next);
}

exports.CentralHandleGeoDestationUser = function(plugin, collections,
                                                 duuid, auth, hres, next) {
  ZH.l('ZSU.CentralHandleGeoDestationUser: U: ' + duuid +
       ' UN: ' + auth.username);
  var sub = ZH.CreateSub(duuid);
  ZDQ.PushStorageGeoDestationUser(plugin, collections, duuid, auth,
  function(perr, pres) {
    if (perr) next(perr, hres);
    else {
      central_destation_user(plugin, collections, sub, auth, hres, next);
    }
  });
}

exports.CentralHandleAgentDestationUser = function(plugin, collections,
                                                   duuid, auth, hres, next) {
  ZH.l('ZSU.CentralHandleAgentDestationUser: UN: ' + auth.username);
  var sub = ZH.CreateSub(duuid);
  get_stationed_user(plugin, collections, sub, auth, function(aerr, ok) {
    if      (aerr) next(aerr, hres);
    else if (!ok)  next(new Error(ZS.Errors.NotStationed), hres);
    else {
      //NOTE: ZPio.GeoBroadcastDestationUser() is ASYNC
      ZPio.GeoBroadcastDestationUser(plugin, collections, duuid, auth);
      next(null, hres);
    }
  });
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZSU']={} : exports);

