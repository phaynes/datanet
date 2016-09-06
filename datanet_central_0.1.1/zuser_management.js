"use strict";

var ZPio, ZChannel, ZSU, ZAio, ZAuth, ZDQ, ZQ, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  require('./setImmediate');
  ZPio     = require('./zpio');
  ZChannel = require('./zchannel');
  ZSU      = require('./zstationuser');
  ZAio     = require('./zaio');
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

function agents_action(plugin, collections, afunc, suuids, auth, hres, next) {
  var need = suuids.length;
  if (need === 0) next(null, hres);
  else {
    var done = 0;
    var nerr = false;
    for (var i = 0; i < need; i++) {
      var suuid = suuids[i];
      afunc(plugin, collections, suuid, auth, hres, function(serr, sres) {
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

function agents_grant_action(plugin, collections, afunc, do_unsub, suuids,
                             username, schanid, priv, hres, next) {
  var need = suuids.length;
  if (need === 0) next(null, hres);
  else {
    var done = 0;
    var nerr = false;
    for (var i = 0; i < need; i++) {
      var suuid = suuids[i];
      afunc(plugin, collections, do_unsub, suuid, username, schanid, priv, hres,
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

function is_user_subscribed(plugin, collections, schanid, username, next) {
  var skey = ZS.GetUserChannelSubscriptions(username);
  plugin.do_get_field(collections.usubs_coll, skey, schanid,
  function (cerr, cres) {
    if      (cerr)          next(gerr, null);
    else if (cres === null) next(null, false);
    else                    next(null, true);
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GRANT USER DATABASE CALL --------------------------------------------------

exports.GrantUser = function(plugin, collections,
                             username, schanid, priv, next) {
  var pkey = ZS.GetUserChannelPermissions(username);
  if        (priv === 'REVOKE') {
    plugin.do_unset_field(collections.uperms_coll, pkey, schanid, next);
  } else if (priv === 'READ') {
    plugin.do_set_field(collections.uperms_coll,   pkey, schanid, 'R', next);
  } else if (priv === 'WRITE') {
    plugin.do_set_field(collections.uperms_coll,   pkey, schanid, 'W', next);
  } else {
    next(new Error(ZS.Errors.BadPrivilegeDesc), null);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REMOVE USER INFO ----------------------------------------------------------

function remove_user(plugin, collections, username, hres, next) {
  delete(ZH.UserPasswordMap[username]); // First DELETE from in-memory map
  var ukey = ZS.GetUserAuthentication(username);
  plugin.do_remove(collections.uauth_coll, ukey, function(rerr, rres) {
    if (rerr) next(rerr, hres);
    else {
      var pkey = ZS.GetUserChannelPermissions(username);
      plugin.do_remove(collections.uperms_coll, pkey, function(perr, pres) {
        next(perr, hres);
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT CALL CENTRAL REMOVE-USER --------------------------------------------

exports.AgentAddUser = function(plugin, collections, username, password, role,
                                auth, hres, next) {
  ZH.l('ZUM.AgentAddUser: UN: ' + username);
  ZAio.SendCentralAddUser(plugin, collections, username, password, role,
                          auth, hres,
  function(aerr, dres) {
    next(aerr, hres);
  });
}

exports.AgentRemoveUser = function(plugin, collections, username, auth,
                                   hres, next) {
  ZH.l('ZUM.AgentRemoveUser: UN: ' + username);
  ZAio.SendCentralRemoveUser(plugin, collections, username, auth, hres,
  function(aerr, dres) {
    next(aerr, hres);
  });
}

exports.AgentGrantUser = function(plugin, collections, username, schanid, priv,
                                  auth, hres, next) {
  ZH.l('ZUM.AgentGrantUser: UN: ' + username + ' R: ' + schanid);
  ZAio.SendCentralGrantUser(plugin, collections, username, schanid, priv,
                            auth, hres,
  function(aerr, dres) {
    next(aerr, hres);
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT HANDLE PROPOGATE REMOVE USER ----------------------------------------

exports.HandlePropogateRemoveUser = function(net, auth, hres, next) {
  ZH.l('ZUM.HandlePropogateRemoveUser: UN: ' + auth.username);
  remove_user(net.plugin, net.collections, auth.username, hres,
  function(rerr, rres) {
    if (rerr) next(rerr, hres);
    else {
      ZSU.AgentProcessDestationUser(net.plugin, net.collections,
                                    auth, hres, next);
    }
  });
}

exports.HandlePropogateGrantUser = function(net, username, schanid, priv,
                                            hres, next) {
  ZH.l('ZUM.HandlePropogateGrantUser: UN: ' + username + ' R: ' + schanid);
 exports.GrantUser(net.plugin, net.collections, username, schanid, priv,
  function(gerr, gres){
    next(gerr, hres);
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL HANDLE ADD USER ---------------------------------------------------

exports.DoAddUser = function(plugin, collections, username, role, phash,
                             hres, next) {
  //TODO disallow adding "*" user
  var ukey    = ZS.GetUserAuthentication(username);
  var u_entry = { _id : ukey, role: role, phash : phash};
  ZH.l('ZUM.CentralHandleAddUser: ' + JSON.stringify(u_entry));
  plugin.do_set(collections.uauth_coll, ukey, u_entry, function(serr, sres) {
    next(serr, hres);
  });
}

function do_central_handle_geo_add_user(plugin, collections,
                                           username, password, role,
                                           internal, hres, next) {
  if (internal) { // password is already hashed
    var phash = password;
    exports.DoAddUser(plugin, collections, username, role, phash, hres, next);
  } else {
    ZH.HashPassword(password, function(perr, phash) {
      if (perr) next(perr, hres);
      else {
        exports.DoAddUser(plugin, collections,
                          username, role, phash, hres, next);
      }
    });
  }
}

exports.HandleStorageGeoAddUser = function(net, username, password, role,
                                           internal, next) {
  ZH.l('ZUM.HandleStorageGeoAddUser: UN: ' + username);
  do_central_handle_geo_add_user(net.plugin, net.collections,
                                 username, password, role, internal, {}, next);
}

exports.CentralHandleGeoAddUser = function(plugin, collections,
                                           username, password, role,
                                           internal, hres, next) {
  ZH.l('ZUM.CentralHandleGeoAddUser: UN: ' + username);
  ZDQ.PushStorageGeoAddUser(plugin, collections,
                            username, password, role, internal,
  function(perr, pres) {
    if (perr) next(perr, hres);
    else {
      do_central_handle_geo_add_user(plugin, collections,
                                     username, password, role, internal,
                                     hres, next);
    }
  });
}

exports.CentralHandleAdminAddUser = function(plugin, collections,
                                             username, password, role,
                                             hres, next) {
  ZH.l('ZUM.CentralHandleAdminAddUser: UN: ' + username);
  hres.username = username; // Used in response
  // NOTE: ZPio.GeoBroadcastAddUser() is ASYNC
  ZPio.GeoBroadcastAddUser(plugin, collections, username, password, role);
  next(null, hres);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL HANDLE REMOVE USER ------------------------------------------------

function central_remove_user_propogate(plugin, collections, suuid, auth,
                                       hres, next) {
  var offline = ZH.IsSubscriberOffline(suuid);
  if (!offline) {
    var username = auth.username;
    ZPio.SendSubscriberPropogateRemoveUser(plugin, collections,
                                           suuid, username);
  }
  next(null, hres);
}

exports.CentralHandleBroadcastRemoveUser = function(plugin, collections,
                                                    suuids, auth, hres, next) {
  agents_action(plugin, collections, central_remove_user_propogate,
                suuids, auth, hres, next);
}

function central_remove_user_metadata(plugin, collections, suuid, auth,
                                      hres, next) {
  ZSU.CentralHandleGeoDestationUser(plugin, collections, suuid, auth,
                                    hres, next);
}

function storage_remove_user_metadata(plugin, collections, suuid, auth,
                                      hres, next) {
  var net = ZH.CreateNetPerRequest(ZH.Central); //TODO pass 'net' to this func
  ZSU.HandleStorageGeoDestationUser(net, suuid, auth, next);
}

function do_central_handle_geo_remove_user(plugin, collections,
                                           auth, suuids, am_router,
                                           hres, next) {
  remove_user(plugin, collections, auth.username, hres, function(rerr, rres) {
    if (rerr) next(rerr, hres);
    else {
      // NOTE: ZPio.BroadcastRemoveUser is ASYNC
      ZPio.BroadcastRemoveUser(plugin, collections, suuids, auth);
      var rfunc = am_router ? central_remove_user_metadata :
                              storage_remove_user_metadata;
      agents_action(plugin, collections, rfunc, suuids, auth, hres, next);
    }
  });
}

exports.HandleStorageGeoRemoveUser = function(net, username, next) {
  ZH.l('ZUM.HandleStorageGeoRemoveUser: UN: ' + username);
  var auth = {username : username};
  ZChannel.GetUserAgents(net.plugin, net.collections, auth,
  function(uerr, suuids) {
    if (uerr) next(uerr, hres);
    else {
      do_central_handle_geo_remove_user(net.plugin, net.collections,
                                        auth, suuids, false, {}, next)
    }
  });
}

exports.CentralHandleGeoRemoveUser = function(plugin, collections,
                                              username, hres, next) {
  ZH.l('ZUM.CentralHandleGeoRemoveUser: UN: ' + username);
  ZDQ.PushStorageGeoRemoveUser(plugin, collections, username, 
  function(perr, pres) {
    if (perr) next(perr, hres);
    else {
      var auth = {username : username};
      ZChannel.GetUserAgents(plugin, collections, auth, function(uerr, suuids) {
        if (uerr) next(uerr, hres);
        else {
          do_central_handle_geo_remove_user(plugin, collections,
                                            auth, suuids, true, hres, next)
        }
      });
    }
  });
}

exports.CentralHandleRemoveUser = function(plugin, collections,
                                           username, hres, next) {
  ZH.l('ZUM.CentralHandleRemoveUser: UN: ' + username);
  //TODO disallow removing "*" user
  hres.username = username; // Used in response
  var auth = {username : username};
  var ukey = ZS.GetUserAuthentication(auth.username);
  plugin.do_get_field(collections.uauth_coll, ukey, "role",
  function(gerr, role) {
    if (gerr) next(gerr, hres);
    else {
      if (role === null) next(new Error(ZS.Errors.NonExistentUser), hres);
      else {
        // NOTE: ZPio.GeoBroadcastRemoveUser() is ASYNC
        ZPio.GeoBroadcastRemoveUser(plugin, collections, username);
        next(null, hres);
      }
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL GRANT USER --------------------------------------------------------

function central_grant_user_propogate(plugin, collections, do_unsub, suuid,
                                      auth, schanid, priv, hres, next) {
  var sub     = ZH.CreateSub(suuid);
  var offline = ZH.IsSubscriberOffline(suuid);
  if (!offline) {
    var username = auth.username;
    ZPio.SendSubscriberPropogateGrantUser(plugin, collections, suuid,
                                          username, schanid, priv);
    if (do_unsub) {
      ZPio.SendSubscriberPropogateUnsubscribe(plugin, collections, suuid,
                                              schanid, username, null);
    }
  }
  next(null, hres);
}

exports.CentralHandleBroadcastGrantUser = function(plugin, collections,
                                                   schanid, priv, do_unsub,
                                                   suuids, auth, hres, next) {
  agents_grant_action(plugin, collections, central_grant_user_propogate,
                      do_unsub, suuids, auth, schanid, priv, hres, next);
}

function do_central_handle_geo_grant_user(plugin, collections,
                                          username, schanid, priv, do_unsub,
                                          suuids, hres, next) {
 exports.GrantUser(plugin, collections, username, schanid, priv,
  function(uerr, ures) {
    if (uerr) next(uerr, hres);
    else {
      var auth = {username : username};
      // NOTE: ZPio.BroadcastGrantUser() is ASYNC
      ZPio.BroadcastGrantUser(plugin, collections,
                              do_unsub, schanid, priv, suuids, auth);
      next(null, hres);
    }
  });
}

exports.HandleStorageGeoGrantUser = function(net, username, schanid, priv,
                                             do_unsub, next) {
  var auth = {username : username};
  ZChannel.GetUserAgents(net.plugin, net.collections, auth,
  function(gerr, suuids) {
    if (gerr) next(gerr, null);
    else {
      do_central_handle_geo_grant_user(net.plugin, net.collections,
                                       username, schanid, priv, do_unsub,
                                       suuids, {}, next);
    }
  });
}

exports.CentralHandleGeoGrantUser = function(plugin, collections,
                                             username, schanid, priv, do_unsub,
                                             hres, next) {
  ZH.l('ZUM.CentralHandleGeoGrantUser: UN: ' + username + ' R: ' + schanid +
       ' P: ' + priv);
  ZDQ.PushStorageGeoGrantUser(plugin, collections,
                              username, schanid, priv, do_unsub,
  function(perr, pres) {
    if (perr) next(perr, hres);
    else {
      var auth = {username : username};
      ZChannel.GetUserAgents(plugin, collections, auth, function(gerr, suuids) {
        if (gerr) next(gerr, hres);
        else {
          do_central_handle_geo_grant_user(plugin, collections,
                                           username, schanid, priv, do_unsub,
                                           suuids, hres, next);
        }
      });
    }
  });
}

function get_user_exists(plugin, collections, username, next) {
  if (username === ZH.WildCardUser) next(null, true);
  else {
    var ukey = ZS.GetUserAuthentication(username);
    plugin.do_get(collections.uauth_coll, ukey, function(gerr, gres) {
      if (gerr) next(gerr, hres);
      else {
        var ok = (gres.length !== 0);
        next(null, ok);
      }
    });
  }
}

// NOTE: NO DB WRITES -> HAPPENS ONLY IN ROUTER LAYER
exports.CentralHandleGrantUser = function(plugin, collections,
                                          username, schanid, priv, hres, next) {
  ZH.l('ZUM.CentralHandleGrantUser: UN: ' + username + ' R: ' + schanid);
  hres.username = username; // Used in response
  get_user_exists(plugin, collections, username, function(gerr, ok) {
    if (gerr) next(gerr, hres);
    else {
      if (!ok) next(new Error(ZS.Errors.NonExistentUser), hres);
      else {
        is_user_subscribed(plugin, collections, schanid, username,
        function(ierr, issub) {
          if (ierr) next(ierr, hres);
          else {
            var do_unsub = (issub && priv === 'REVOKE');
            var auth     = {username : username};
            ZChannel.GetUserAgents(plugin, collections, auth,
            function(gerr, suuids) {
              if (gerr) next(gerr, hres);
              else {
                // NOTE: ZPio.GeoBroadcastGrantUser() is ASYNC
                ZPio.GeoBroadcastGrantUser(plugin, collections,
                                           do_unsub, schanid, priv, username);
                next(null, hres);
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
// CENTRAL GET USER CHANNEL PERMISSIONS --------------------------------------

function create_user_chan_perm(rchan, username, priv) {
  return {username : username,
          channel  : {id          : rchan,
                      permissions : priv}
         };
}

function do_get_user_chan_perms(plugin, collections,
                                rchans, username, hres, next) {
  if (rchans.length === 0) next(null, hres);
  else {
    var rchan = rchans.shift();
    var pkey  = ZS.GetUserChannelPermissions(username);
    plugin.do_get_field(collections.uperms_coll, pkey, rchan,
    function(gerr, priv) {
      if (gerr) next(gerr, hres);
      else {
        if (priv) {
          var pe = create_user_chan_perm(rchan, username, priv);
          hres.permissions.push(pe);
          setImmediate(do_get_user_chan_perms, plugin, collections,
                       rchans, username, hres, next);
        } else { // NO PRIVILEGES -> CHECK WILDCARD
          var wkey = ZS.GetUserChannelPermissions(ZH.WildCardUser);
          plugin.do_get_field(collections.uperms_coll, wkey, rchan,
          function(ferr, wpriv) {
            if (ferr) next(ferr, hres);
            else {
              if (wpriv) {
                var pe = create_user_chan_perm(rchan, ZH.WildCardUser, wpriv);
                hres.permissions.push(pe);
              }
              setImmediate(do_get_user_chan_perms, plugin, collections,
                           rchans, username, hres, next);
            }
          });
        }
      }
    });
  }
}

exports.CentralHandleGetUserChannelPermissions = function(plugin, collections,
                                                          rchans, auth,
                                                          hres, next) {
  hres.permissions = []; // Used in response
  var username     = auth.username;
  ZH.l('ZUM.CentralHandleGetUserChannelPermissions: U: ' + username);
  var crchans      = ZH.clone(rchans);
  do_get_user_chan_perms(plugin, collections, crchans, username, hres, next);
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZUM']={} : exports);

