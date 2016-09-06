"use strict";

var ZISL, ZDelt, ZDoc, ZHB, ZCache, ZChannel, ZSU, ZUM, ZLat;
var ZMessage, ZAuth, ZDS, ZQ, ZS, ZH, ZDBP;
if (typeof(exports) !== 'undefined') {
  ZISL     = require('./zisolated');
  ZDelt    = require('./zdeltas');
  ZDoc     = require('./zdoc');
  ZHB      = require('./zheartbeat');
  ZCache   = require('./zcache');
  ZChannel = require('./zchannel');
  ZSU      = require('./zstationuser');
  ZUM      = require('./zuser_management');
  ZLat     = require('./zlatency');
  ZMessage = require('./zmessage');
  ZAuth    = require('./zauth');
  ZDS      = require('./zdatastore');
  ZQ       = require('./zqueue');
  ZS       = require('./zshared');
  ZH       = require('./zhelper');
  ZDBP     = require('./zdb_plugin');
} 

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SUBSCRIBE -----------------------------------------------------------------

function ZyncSubscribe(schanid, next) {
  schanid = ZH.IfNumberConvert(schanid);
  ZH.l('Zync.Subscribe: R: ' + schanid);
  if (!ZH.Agent.cconn || ZH.Isolation) { // reject if no ZH.Agent.cconn
    next(new Error(ZS.Errors.NoCentralConn), null);
    return;
  }
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  var auth = ZH.BrowserAuth;
  ZAuth.HasAgentSubscribePermissions(net, auth, schanid, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.SubscribePermsFail), null);
    else {
      ZChannel.AgentSubscribe(net.plugin, net.collections,
                              schanid, auth, {}, next);
    }
  });
}

function ZyncUnsubscribe(schanid, next) {
  schanid = ZH.IfNumberConvert(schanid);
  ZH.l('Zync.Unsubscribe: R: ' + schanid);
  if (!ZH.Agent.cconn || ZH.Isolation) { // reject if no ZH.Agent.cconn
    next(new Error(ZS.Errors.NoCentralConn), null);
    return;
  }
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  var auth = ZH.BrowserAuth;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), null);
    else {
      ZChannel.AgentUnsubscribe(net.plugin, net.collections,
                                schanid, auth, {}, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STORE ---------------------------------------------------------------------

function ZyncStore(ns, cn, key, json, next) {
  json._id = key; // Document will contain key
  var err  = ZH.ValidateJson(json);
  if (err) next(err, null);
  else {
    var net     = ZH.CreateNetPerRequest(ZH.Agent);
    var auth    = ZH.BrowserAuth;
    var rchans  = json._channels;
    var ks      = ZH.CompositeQueueKey(ns, cn, key);
    ZAuth.HasAgentStorePermissions(net, auth, ks, rchans, function(aerr, ok) {
      if      (aerr) next(aerr, null);
      else if (!ok)  next(new Error(ZS.Errors.WritePermsFail), null);
      else {
        var sep = false;
        var ex  = 0;
        ZDelt.AgentStore(net, ks, sep, ex, json, auth, {},
        function(serr, sres) {
          if (serr) next(serr, null);
          else {
            var ncrdt = sres.applied.crdt;
            var njson = ZH.CreatePrettyJson(ncrdt);
            var nzdoc = ZDoc.Create(ns, cn, njson, ncrdt);
            next(null, nzdoc);
          }
        });
      }
    });
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FETCH ---------------------------------------------------------------------

function ZyncFetch(ns, cn, key, next) {
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  var auth = ZH.BrowserAuth;
  var ks   = ZH.CompositeQueueKey(ns, cn, key);
  ZH.l('ZyncFetch: K: ' + ks.kqk);
  ZAuth.HasReadPermissions(net, auth, ks, ZH.MyUUID, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.NoDataFound), null);
    else {
      ZDelt.AgentFetch(net, ks, {}, function(serr, sres) {
        if (serr) next(serr, null);
        else {
          var ncrdt = sres.applied.crdt;
          var njson = ZH.CreatePrettyJson(ncrdt);
          var nzdoc = ZDoc.Create(ns, cn, njson, ncrdt);
          next(null, nzdoc);
        }
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// COMMIT --------------------------------------------------------------------

exports.ZyncCommit = function(ns, cn, key, crdt, oplog, next) {
  var net     = ZH.CreateNetPerRequest(ZH.Agent);
  var auth    = ZH.BrowserAuth;
  var rchans  = ZH.GetReplicationChannels(crdt._meta);
  var ks      = ZH.CompositeQueueKey(ns, cn, key);
  ZH.l('ZyncCommit: K: ' + ks.kqk);
  ZAuth.HasWritePermissions(net, auth, ks, ZH.MyUUID, rchans,
  function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.WritePermsFail), null);
    else {
      var ex = 0;
      ZDelt.AgentCommit(net, ks, crdt, oplog, ex, auth, {},
      function(serr, sres) {
        if (serr) next(serr, null);
        else {
          var ncrdt = sres.applied.crdt;
          var njson = ZH.CreatePrettyJson(ncrdt);
          var nzdoc = ZDoc.Create(ns, cn, njson, ncrdt);
          next(null, nzdoc);
        }
      });
    }
  });
}

exports.ZyncStatelessCommit = function(ns, cn, key, oplog, rchans, next) {
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  var auth = ZH.BrowserAuth;
  var ks   = ZH.CompositeQueueKey(ns, cn, key);
  ZH.l('ZyncStatelessCommit: K: ' + ks.kqk);
  ZAuth.HasWritePermissions(net, auth, ks, ZH.MyUUID, rchans,
  function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.WritePermsFail), null);
    else {
      ZDelt.AgentStatelessCommit(net, ks, oplog, rchans, auth, {}, next);
    }
  });
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REMOVE --------------------------------------------------------------------

function ZyncRemove(ns, cn, key, next) {
  ZH.l('Zync.Remove: key: ' + key);
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  var auth = ZH.BrowserAuth;
  var ks   = ZH.CompositeQueueKey(ns, cn, key);
  ZAuth.HasWritePermissionsOnKey(net, auth, ks, ZH.MyUUID, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.WritePermsFail), null);
    else {
      ZDelt.AgentRemove(net, ks, auth, {}, function(aerr, ares) {
        next(aerr, null);
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PULL ----------------------------------------------------------------------

exports.ZyncPull = function(ns, cn, key, crdt, oplog, next) {
  var net     = ZH.CreateNetPerRequest(ZH.Agent);
  var auth    = ZH.BrowserAuth;
  var rchans  = ZH.GetReplicationChannels(crdt._meta);
  var ks      = ZH.CompositeQueueKey(ns, cn, key);
  ZAuth.HasWritePermissions(net, auth, ks, ZH.MyUUID, rchans,
  function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.WritePermsFail), null);
    else {
      ZDelt.AgentPull(net, ks, crdt, oplog, auth, {}, function(err, sres) {
        if (err) next(err, null);
        else {
          var ncrdt  = sres.applied.crdt;
          var mjson  = sres.applied.json;
          var noplog = sres.applied.oplog;
          ZDoc.CreateZDocFromPullResponse(ns, cn, key, ncrdt, mjson, noplog,
                                          next);
        }
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FIND ----------------------------------------------------------------------

function ZyncFind(ns, cn, query, next) {
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  var auth = ZH.BrowserAuth;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), null);
    else {
      var hres = {};
      ZDS.FindPermsData(net, ns, cn, query, auth, hres, function(aerr, ares) {
        if (aerr) next(aerr, null);
        else      next(null, hres.jsons);
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SCAN ----------------------------------------------------------------------

function ZyncScan(ns, cn, next) {
  ZH.l('Zync.Scan');
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  var auth = ZH.BrowserAuth;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), null);
    else {
      var hres = {};
      ZDS.AgentScanReadPermsData(net.plugin, net.collections, auth, hres,
      function(aerr, ares) {
        if (aerr) next(aerr, null);
        else      ZDoc.CreateZDocs(ns, cn, hres.crdts, next);
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CACHE ---------------------------------------------------------------------

// NOTE: ZH.Agent.cconn/ZH.Isolation check done in ZAio.SendCentralCache()
function ZyncCache(ns, cn, key, pin, watch, sticky, next) {
  ZH.l('ZyncCache: K: ' + key + ' P: ' + pin + ' W: ' + watch +
       ' S: ' + sticky);
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  var auth = ZH.BrowserAuth;
  var ks   = ZH.CompositeQueueKey(ns, cn, key);
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), null);
    else {
      var force    = false;
      var internal = false;
      ZCache.AgentCache(net, ks, pin, watch, sticky, force, internal, auth, {},
      function(serr, sres) {
        if (serr) next(serr, null);
        else {
          var ncrdt = sres.applied.crdt;
          var njson = ZH.CreatePrettyJson(ncrdt);
          var nzdoc = ZDoc.Create(ns, cn, njson, ncrdt);
          next(null, nzdoc);
        }
      });
    }
  });
}

function ZyncEvict(ns, cn, key, next) {
  ZH.l('Zync.Evict: K: ' + key);
  if (!ZH.Agent.cconn || ZH.Isolation) { // reject if no ZH.Agent.cconn
    next(new Error(ZS.Errors.NoCentralConn), null);
    return;
  }
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  var auth = ZH.BrowserAuth;
  var ks   = ZH.CompositeQueueKey(ns, cn, key);
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), null);
    else { // NOTE: no 'auth' argument for ZCache.AgentEvict()
      ZCache.AgentEvict(net, ks, false, {}, next);
    }
  });
}

function ZyncLocalEvict(ns, cn, key, next) {
  ZH.l('Zync.LocalEvict: K: ' + key);
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  var auth = ZH.BrowserAuth;
  var ks   = ZH.CompositeQueueKey(ns, cn, key);
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), null);
    else {
      ZCache.AgentLocalEvict(net, ks, false, {}, next);
    }
  });
}

function ZyncIsCached(ks, next) {
  ZH.l('Zync.IsCached: K: ' + ks.kqk);
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  var auth = ZH.BrowserAuth;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), null);
    else {
      ZCache.AgentIsCached(net.plugin, net.collections, ks, auth, {}, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SET ISOLATION -------------------------------------------------------------

function ZyncIsolation(b, next) {
  ZH.l('Zync.Isolation: value: ' + b);
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  var auth = ZH.BrowserAuth;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), null);
    else {
      ZISL.SendCentralAgentIsolation(net, b, {}, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SET NOTIFY & SHUTDOWN -----------------------------------------------------

function ZyncSetNotify(cmd, url, next) {
  ZH.l('Zync.SetNotify: cmd: ' + cmd + ' url: ' + url);
  next(new Error(ZS.Errors.BrowserNotify), {});
}

function ZyncShutdown(next) {
  ZH.l('Zync.Shutdown');
  next(new Error(ZS.Errors.BrowserShutdown), {});
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STATION USER ON CURRENT AGENT ---------------------------------------------

function ZyncStationUser(auth, next) {
  ZH.l('Zync.StationUser: UN: ' + auth.username);
  if (!ZH.Agent.cconn || ZH.Isolation) { // reject if no ZH.Agent.cconn
    next(new Error(ZS.Errors.NoCentralConn), null);
    return;
  }
  exports.SetClientAuthentication(auth.username, auth.password);
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), null);
    else {
      ZSU.AgentStationUser(net, auth, {}, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DESTATION USER FROM CURRENT AGENT -----------------------------------------

function ZyncDestationUser(next) {
  ZH.l('Zync.DestationUser: UN: ' + ZH.BrowserAuth.username);
  if (!ZH.Agent.cconn || ZH.Isolation) { // reject if no ZH.Agent.cconn
    next(new Error(ZS.Errors.NoCentralConn), null);
    return;
  }
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  var auth = ZH.BrowserAuth;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), null);
    else {
      ZSU.AgentDestationUser(net.plugin, net.collections, auth, {}, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GET STATIONED USERS (ON AGENT) --------------------------------------------

exports.ZyncGetStationedUsers = function(next) {
  ZH.l('Zync.GetStationedUsers');
  var net = ZH.CreateNetPerRequest(ZH.Agent);
  ZSU.AgentGetStationedUsers(net.plugin, net.collections, {}, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SWITCH USER ---------------------------------------------------------------

function ZyncSwitchUser(auth, next) {
  ZH.l('Zync.SwitchUser: UN: ' + auth.username);
  exports.SetClientAuthentication(auth.username, auth.password);
  var net = ZH.CreateNetPerRequest(ZH.Agent);
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), null);
    else           next(null, {user : auth.username});
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INFO CALLS ----------------------------------------------------------------

exports.ZyncGetAgentSubscriptions = function(next) {
  ZH.l('Zync.GetAgentSubscriptions');
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  var auth = ZH.BrowserAuth;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), null);
    else {
      ZChannel.AgentGetAgentSubscriptions(net.plugin, net.collections,
                                          {}, next);
    }
  });
}

function ZyncGetUserSubscriptions(next) {
  ZH.l('Zync.GetUserSubscriptions');
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  var auth = ZH.BrowserAuth;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), null);
    else {
      ZChannel.AgentGetUserSubscriptions(net.plugin, net.collections,
                                         auth, {}, next);
    }
  });
}

function ZyncGetUserInfo(auth, next) {
  ZH.l('Zync.GetUserInfo');
  var net = ZH.CreateNetPerRequest(ZH.Agent);
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), null);
    else {
      ZChannel.AgentGetUserInfo(net.plugin, net.collections, auth, {}, next);
    }
  });
}

function ZyncGetClusterInfo(next) {
  ZH.l('Zync.GetClusterInfo');
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  var auth = ZH.BrowserAuth;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), null);
    else {
      ZISL.AgentGetClusterInfo(net.plugin, net.collections, {}, next);
    }
  });
}

function ZyncGetLatencies(next) {
  ZH.l('Zync.GetLatencies');
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  var auth = ZH.BrowserAuth;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), null);
    else {
      ZLat.AgentGetLatencies(net.plugin, net.collections, {}, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HEARTBEAT -----------------------------------------------------------------

function ZyncHeartbeat(cmd, field, uuid, mlen, trim, isi, isa, next) {
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  var auth = ZH.BrowserAuth;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), null);
    else {
      ZHB.AgentDataHeartbeat(net, cmd, field, uuid, mlen, trim, isi, isa,
                             auth, {}, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// MESSAGES ------------------------------------------------------------------

function ZyncMessage(mtxt, next) {
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  var auth = ZH.BrowserAuth;
  ZMessage.HandleClientMessage(net, mtxt, auth, {}, next);
}

function ZyncRequest(mtxt, next) {
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  var auth = ZH.BrowserAuth;
  ZMessage.HandleClientRequest(net, mtxt, auth, {}, next);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ADMIN ---------------------------------------------------------------------

function ZyncAddUser(username, password, role, next) {
  ZH.l('Zync.AddUser');
  if (!ZH.Agent.cconn || ZH.Isolation) { // reject if no ZH.Agent.cconn
    next(new Error(ZS.Errors.NoCentralConn), null);
    return;
  }
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  var auth = ZH.BrowserAuth;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), null);
    else {
      ZUM.AgentAddUser(net.plugin, net.collections,
                       username, password, role, auth, {}, next);
    }
  });
}

function ZyncRemoveUser(username, next) {
  ZH.l('Zync.RemoveUser');
  if (!ZH.Agent.cconn || ZH.Isolation) { // reject if no ZH.Agent.cconn
    next(new Error(ZS.Errors.NoCentralConn), null);
    return;
  }
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  var auth = ZH.BrowserAuth;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), null);
    else {
      ZUM.AgentRemoveUser(net.plugin, net.collections,
                          username, auth, {}, next);
    }
  });
}

function ZyncGrantUser(username, schanid, priv, next) {
  ZH.l('Zync.GrantUser');
  schanid = ZH.IfNumberConvert(schanid);
  if (priv !== 'WRITE' && priv !== 'READ' && priv !== 'REVOKE') {
    next(new Error(ZS.Errors.BadPrivilegeDesc), null);
    return;
  }
  if (!ZH.Agent.cconn || ZH.Isolation) { // reject if no ZH.Agent.cconn
    next(new Error(ZS.Errors.NoCentralConn), null);
    return;
  }
  var net  = ZH.CreateNetPerRequest(ZH.Agent);
  var auth = ZH.BrowserAuth;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), null);
    else {
      ZUM.AgentGrantUser(net.plugin, net.collections,
                         username, schanid, priv, auth, {}, next);
    }
  });
}

function ZyncRemoveDataCenter(dcuuid, next) {
  ZH.l('Zync.RemoveDataCenter');
  if (!ZH.Agent.cconn || ZH.Isolation) { // reject if no ZH.Agent.cconn
    next(new Error(ZS.Errors.NoCentralConn), null);
    return;
  }
  var net   = ZH.CreateNetPerRequest(ZH.Agent);
  var auth  = ZH.BrowserAuth;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), null);
    else {
      ZAio.SendCentralRemoveDataCenter(net.plugin, net.collections,
                                       dcuuid, auth, {}, next);
    }
  });
}

function ZyncGetAgentKeys(duuid, nkeys, minage, next) {
  ZH.l('Zync.GetAgentKeys');
  if (!ZH.Agent.cconn || ZH.Isolation) { // reject if no ZH.Agent.cconn
    next(new Error(ZS.Errors.NoCentralConn), null);
    return;
  }
  var duuid  = Number(duuid);
  var nkeys  = Number(nkeys);
  var minage = Number(minage);
  var net    = ZH.CreateNetPerRequest(ZH.Agent);
  var auth   = ZH.BrowserAuth;
  ZAuth.BasicAuthentication(net, auth, function(aerr, ok) {
    if      (aerr) next(aerr, null);
    else if (!ok)  next(new Error(ZS.Errors.BasicAuthFail), null);
    else {
      ZAio.SendCentralAgentGetAgentKeys(net.plugin, net.collections,
                                        duuid, nkeys, minage, auth, {}, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INFO CALLS ----------------------------------------------------------------

function ZyncIsSubscribed(schanid, next) {
  ZH.l('Zync.IsSubscribed: ' + schanid);
  exports.ZyncGetAgentSubscriptions(function(gerr, gres) {
    ZH.HandleIsSubscribed(gerr, gres, schanid, next);
  });
}

function ZyncIsUserStationed(username, next) {
  ZH.l('Zync.IsUserStationed: ' + username);
  exports.ZyncGetStationedUsers(function(gerr, susers) {
    ZH.HandleIsUserStationed(gerr, susers, username, next);
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// COLLECTION CLASS ----------------------------------------------------------

// FUNCTION: BrowserCollection()
// PURPOSE:  Constructor: stores necessary data/handles to operate independently
//
function BrowserCollection(client, namespace, cname) {
  this.client = client;
  this.ns     = namespace;
  this.cn     = cname;
}

BrowserCollection.prototype.store = function(key, json, next) {
  ZyncStore(this.ns, this.cn, key, json, next);
}

BrowserCollection.prototype.fetch = function(key, next) {
  ZyncFetch(this.ns, this.cn, key, next);
}

BrowserCollection.prototype.find = function(query, next) {
  ZyncFind(this.ns, this.cn, query, next);
}

BrowserCollection.prototype.cache = function(key, next) {
  ZyncCache(this.ns, this.cn, key, false, false, false, next);
}

BrowserCollection.prototype.pin = function(key, next) {
  ZyncCache(this.ns, this.cn, key, true, false, false, next);
}

BrowserCollection.prototype.watch = function(key, next) {
  ZyncCache(this.ns, this.cn, key, false, true, false, next);
}

BrowserCollection.prototype.sticky_cache = function(key, next) {
  ZyncCache(this.ns, this.cn, key, false, false, true, next);
}

BrowserCollection.prototype.evict = function(key, next) {
  ZyncEvict(this.ns, this.cn, key, next);
}

BrowserCollection.prototype.local_evict = function(key, next) {
  ZyncLocalEvict(this.ns, this.cn, key, next);
}

BrowserCollection.prototype.remove = function(key, next) {
  ZyncRemove(this.ns, this.cn, key, next);
}

BrowserCollection.prototype.scan = function(next) {
  ZyncScan(this.ns, this.cn, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CLIENT SETUP CALLS --------------------------------------------------------

exports.ConstructCmdClient = function(name) {
  this.name = name;
  return this;
}

exports.SetAgentAddress = function(ip, port) {
}

// NOTE: Used by STATION-USER & USER command
exports.SetClientAuthentication = function(username, password) {
  this.ClientAuth = {username : username, password : password};
  ZH.BrowserAuth  = this.ClientAuth;
  if (typeof(ZS.EngineCallback.ChangeUserFunc) !== 'undefined') {
    ZS.EngineCallback.ChangeUserFunc(ZH.BrowserAuth.username);
  }
}

exports.SetNamespace = function(ns) {
  ZH.l('SetNamespace: NS: ' + ns);
  this.ns = ns;
  if (this.collection) this.collection.ns = ns;
}

exports.Collection = function(cname) { // Used in SetNamespace()
  this.cn         = cname;
  this.collection = new BrowserCollection(this, this.ns, cname);
  return this.collection;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ADMIN SETTINGS ------------------------------------------------------------

exports.AddUser = function(username, password, role, next) {
  ZyncAddUser(username, password, role, next);
}

exports.RemoveUser = function(username, next) {
  ZyncRemoveUser(username, next);
}

exports.GrantUser = function(username, schanid, priv, next) {
  ZyncGrantUser(username, schanid, priv, next);
}

exports.RemoveDataCenter = function(guuid, next) {
  ZyncRemoveDataCenter(guuid, next);
}

exports.GetAgentKeys = function(duuid, nkeys, minage, next) {
  ZyncGetAgentKeys(duuid, nkeys, minage, next);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT SETTINGS ------------------------------------------------------------

exports.StationUser = function(auth, next) {
  ZyncStationUser(auth, next);
}

exports.DestationUser = function(username, next) {
  if (username !== ZH.BrowserAuth.username) {
    next(new Error(ZS.Errors.OpNotOnSelf), null);
  } else {
    ZyncDestationUser(next);
  }
}

exports.SwitchUser = function(auth, next) {
  ZyncSwitchUser(auth, next);
}

exports.Subscribe = function(schanid, next) {
  ZyncSubscribe(schanid, next);
}

exports.Unsubscribe = function(schanid, next) {
  ZyncUnsubscribe(schanid, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INFO CALLS ----------------------------------------------------------------

exports.GetAgentSubscriptions = function(next) {
  exports.ZyncGetAgentSubscriptions(next);
}

exports.GetUserSubscriptions = function(next) {
  ZyncGetUserSubscriptions(next);
}

exports.GetStationedUsers = function(next) {
  exports.ZyncGetStationedUsers(next);
}

exports.GetUserInfo = function(auth, next) {
  ZyncGetUserInfo(auth, next);
}

exports.GetClusterInfo = function(next) {
  ZyncGetClusterInfo(next);
}

exports.GetLatencies = function(next) {
  ZyncGetLatencies(next);
}

exports.Heartbeat = function(cmd, field, uuid, mlen, trim, isi, isa, next) {
  ZyncHeartbeat(cmd, field, uuid, mlen, trim, isi, isa, next);
}

exports.Message = function(mtxt, next) {
  ZyncMessage(mtxt, next);
}

exports.Request = function(mtxt, next) {
  ZyncRequest(mtxt, next);
}

exports.IsUserStationed = function(username, next) {
  ZyncIsUserStationed(username, next);
}

exports.IsSubscribed = function(schanid, next) {
  ZyncIsSubscribed(schanid, next);
}

exports.IsCached = function(ks, next) {
  ZyncIsCached(ks, function(ierr, hres) {
    if (ierr) next(ierr, null);
    else {
      var hit = hres.cached;
      next(null, hit);
    }
  });
}

exports.GetCurrentUsername = function() {
  return ZH.BrowserAuth.username;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CONNECTION CALLS ----------------------------------------------------------

exports.Isolation = function(b, next) {
  ZyncIsolation(b, next);
}

exports.SetNotify = function(cmd, url, next) {
  ZyncSetNotify(cmd, url, next);
}

// BROWSER-ONLY (Agent uses config file)
exports.SetCacheConfig = function(conf, next) {
  if (!conf.policy || !conf.max_bytes) {
    var etxt = 'Cache Configuration must contain sections: [policy,max_bytes]';
    next(new Error(etxt), null);
  } else {
    ZH.Agent.CacheConfig = conf;
    next(null, null);
  }
}

exports.Shutdown = function(next) {
  ZyncShutdown(next);
}

exports.Close = function() {
  ZDBP.Close(ZH.OnErrLog);
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZBrowserClient']={} : exports);

