"use strict";

var ZDoc, ZCio, ZMCG, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  ZDoc = require('./zdoc');
  ZCio = require('./zcio');
  ZMCG = require('./zmemcache_get');
  ZS   = require('./zshared');
  ZH   = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// COLLECTION CLASS ----------------------------------------------------------

// FUNCTION: CmdCollection()
// PURPOSE:  Constructor: stores necessary data/handles to operate independently
//
function CmdCollection(client, namespace, cname) {
  this.client = client;
  this.ns     = namespace;
  this.cn     = cname;
}

CmdCollection.prototype.store = function(key, json, next) {
  var sep = false;
  var ex  = 0;
  this.client.Store(this.ns, this.cn, key, sep, ex, json, next);
}

CmdCollection.prototype.fetch = function(key, next) {
  this.client.Fetch(this.ns, this.cn, key, next);
}

CmdCollection.prototype.find = function(query, next) {
  this.client.Find(this.ns, this.cn, query, next);
}

CmdCollection.prototype.cache = function(key, next) {
  this.client.Cache(this.ns, this.cn, key, false, false, false, next);
}

CmdCollection.prototype.pin = function(key, next) {
  this.client.Cache(this.ns, this.cn, key, true, false, false, next);
}

CmdCollection.prototype.watch = function(key, next) {
  this.client.Cache(this.ns, this.cn, key, false, true, false, next);
}

CmdCollection.prototype.sticky_cache = function(key, next) {
  this.client.Cache(this.ns, this.cn, key, false, false, true, next);
}

CmdCollection.prototype.evict = function(key, next) {
  this.client.Evict(this.ns, this.cn, key, next);
}

CmdCollection.prototype.local_evict = function(key, next) {
  this.client.LocalEvict(this.ns, this.cn, key, next);
}

CmdCollection.prototype.expire = function(key, expire, next) {
  this.client.Expire(this.ns, this.cn, key, expire, next);
}

CmdCollection.prototype.remove = function(key, next) {
  this.client.Remove(this.ns, this.cn, key, next);
}

CmdCollection.prototype.scan = function(next) {
  this.client.Scan(this.ns, this.cn, next);
}

CmdCollection.prototype.stateless_create = function(key, next) {
  this.client.StatelessCreate(this.ns, this.cn, key, next);
}

CmdCollection.prototype.memcache_store = function(key, sep, ex, json, next) {
  this.client.MemcacheStore(this.ns, this.cn, key, sep, ex, json, next);
}

CmdCollection.prototype.memcache_get = function(key, root, next) {
  this.client.MemcacheGet(this.ns, this.cn, key, root, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// COMMAND CLIENT CLASS ------------------------------------------------------

// FUNCTION: CmdClient()
// PURPOSE:  Constructor
//
function CmdClient(name) {
  this.name = name;
}

exports.ConstructCmdClient = function(name) {
  return new CmdClient(name);
}

CmdClient.prototype.SetAgentAddress = function(ip, port) {
  this.ip   = ip;
  this.port = port;
}

// NOTE: Used by STATION-USER & USER command
CmdClient.prototype.SetClientAuthentication = function(username, password) {
  var auth        = {username : username, password : password};
  this.ClientAuth = auth;
  if (typeof(ZS.EngineCallback.ChangeUserFunc) !== 'undefined') {
    ZS.EngineCallback.ChangeUserFunc(username);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// LOCAL CALLS ---------------------------------------------------------------

CmdClient.prototype.SetNamespace = function(ns) {
  ZH.l('SetNamespace: NS: ' + ns);
  this.ns = ns;
  if (this.collection) this.collection.ns = ns;
}

CmdClient.prototype.Collection = function(cname) {
  this.cn         = cname;
  this.collection = new CmdCollection(this, this.ns, this.cn);
  return this.collection;
}

CmdClient.prototype.GetMyAgentReqOpts = function() {
  return { hostname : this.ip,
           port     : this.port,
           path     : '/agent',
           method   : 'POST',
           //TODO next 3 lines: HACK to ignore self signed ssl certs
           rejectUnauthorized : false,
           requestCert        : true,
           agent              : false
         };
}

CmdClient.prototype.StatelessCreate = function(ns, cn, key, next) {
  ZH.l('ZCmdClient.StatelessCreate: K: ' + key);
  ZDoc.CreateStatelessZDoc(this, ns, cn, key, next);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REMOTE CALLS --------------------------------------------------------------

CmdClient.prototype.Subscribe = function(schanid, next) {
  schanid = ZH.IfNumberConvert(schanid);
  ZH.l('ZCmdClient.Subscribe: R: ' + schanid);
  ZCio.SendAgentSubscribe(this, schanid, next);
}

CmdClient.prototype.Unsubscribe = function(schanid, next) {
  schanid = ZH.IfNumberConvert(schanid);
  ZH.l('ZCmdClient.Unsubscribe: R: ' + schanid);
  ZCio.SendAgentUnsubscribe(this, schanid, next);
}

CmdClient.prototype.Fetch = function(ns, cn, key, next) {
  ZCio.SendAgentFetch(this, ns, cn, key, next);
}

CmdClient.prototype.Find = function(ns, cn, query, next) {
  ZCio.SendAgentFind(this, ns, cn, query, next);
}

function do_store(cli, ns, cn, key, sep, ex, json, ismc, next) {
  json._id = key; // Document will contain key
  var err  = ZH.ValidateJson(json);
  if (err) next(err, null);
  else {
    ZCio.SendAgentStore(cli, ns, cn, key, sep, ex, json, ismc, next);
  }
}

CmdClient.prototype.Store = function(ns, cn, key, sep, ex, json, next) {
  do_store(this, ns, cn, key, sep, ex, json, false, next);
}

CmdClient.prototype.Commit = function(ns, cn, key, crdt, oplog, next) {
  ZH.l('ZCmdClient.Commit: K: ' + key);
  ZCio.SendAgentCommit(this, ns, cn, key, crdt, oplog, next);
}

CmdClient.prototype.Pull = function(ns, cn, key, crdt, oplog, next) {
  ZH.l('ZCmdClient.Pull: K: ' + key);
  ZCio.SendAgentPull(this, ns, cn, key, crdt, oplog, next);
}

CmdClient.prototype.Remove = function(ns, cn, key, next) {
  ZH.l('ZCmdClient.Remove: key: ' + key);
  ZCio.SendAgentRemove(this, ns, cn, key, next);
}

CmdClient.prototype.StatelessCommit = function(ns, cn, key,
                                               oplog, rchans, next) {
  ZH.l('ZCmdClient.StatelessCommit: K: ' + key);
  ZCio.SendAgentStatelessCommit(this, ns, cn, key, oplog, rchans, next);
}

CmdClient.prototype.Scan = function(ns, cn, next) {
  ZH.l('ZCmdClient.Scan');
  ZCio.SendAgentScan(this, ns, cn, next);
}

CmdClient.prototype.Cache = function(ns, cn, key, pin, watch, sticky, next) {
  ZH.l('ZCmdClient.Cache: key: ' + key);
  ZCio.SendAgentCache(this, ns, cn, key, pin, watch, sticky, next);
}

CmdClient.prototype.Evict = function(ns, cn, key, next) {
  ZH.l('ZCmdClient.Evict: key: ' + key);
  ZCio.SendAgentEvict(this, ns, cn, key, next);
}

CmdClient.prototype.LocalEvict = function(ns, cn, key, next) {
  ZH.l('ZCmdClient.LocalEvict: key: ' + key);
  ZCio.SendAgentLocalEvict(this, ns, cn, key, next);
}

CmdClient.prototype.Expire = function(ns, cn, key, expire, next) {
  ZH.l('ZCmdClient.Expire: key: ' + key);
  ZCio.SendAgentExpire(this, ns, cn, key, expire, next);
}

CmdClient.prototype.Isolation = function(b, next) {
  ZH.l('ZCmdClient.Isolation: value: ' + b);
  ZCio.SendAgentClientIsolation(this, b, next);
}

CmdClient.prototype.SetNotify = function(cmd, url, next) {
  ZH.l('ZCmdClient.SetNotify: cmd: ' + cmd + ' url: ' + url);
  ZCio.SendAgentSetNotify(this, cmd, url, next);
}

CmdClient.prototype.StationUser = function(auth, next) {
  ZH.l('ZCmdClient.StationUser: UN: ' + auth.username);
  this.SetClientAuthentication(auth.username, auth.password);
  ZCio.SendAgentStationUser(this, auth, next);
}

CmdClient.prototype.DestationUser = function(next) {
  ZH.l('ZCmdClient.DestationUser: UN: ' + this.ClientAuth.username);
  ZCio.SendAgentDestationUser(this, next);
}

function get_stationed_users(cli, next) {
  ZCio.SendAgentGetStationedUsers(cli, next);
}
CmdClient.prototype.GetStationedUsers = function(next) {
  ZH.l('ZCmdClient.GetStationedUsers');
  get_stationed_users(this, next);
}

CmdClient.prototype.SwitchUser = function(auth, next) {
  ZH.l('ZCmdClient.SwitchUser: UN: ' + auth.username);
  this.SetClientAuthentication(auth.username, auth.password);
  ZCio.SendAgentSwitchUser(this, auth, next);
}

function get_agent_subscriptions(cli, next) {
  ZCio.SendAgentGetAgentSubscriptions(cli, next);
}

CmdClient.prototype.GetAgentSubscriptions = function(next) {
  ZH.l('ZCmdClient.GetAgentSubscriptions');
  get_agent_subscriptions(this, next);
}

CmdClient.prototype.GetUserSubscriptions = function(next) {
  ZH.l('ZCmdClient.GetUserSubscriptions');
  ZCio.SendAgentGetUserSubscriptions(this, next);
}

CmdClient.prototype.GetUserInfo = function(auth, next) {
  ZH.l('ZCmdClient.GetUserInfo');
  ZCio.SendAgentGetUserInfo(this, auth, next);
}

CmdClient.prototype.GetClusterInfo = function(next) {
  ZH.l('ZCmdClient.GetClusterInfo');
  ZCio.SendAgentGetClusterInfo(this, next);
}

CmdClient.prototype.GetLatencies = function(next) {
  ZH.l('ZCmdClient.GetLatencies');
  ZCio.SendAgentGetLatencies(this, next);
}

CmdClient.prototype.Heartbeat = function(cmd, field, uuid,
                                         mlen, trim, isi, isa, next) {
  ZCio.SendAgentHeartbeat(this, cmd, field, uuid, mlen, trim, isi, isa, next);
}

CmdClient.prototype.Message = function(mtxt, next) {
  ZCio.SendAgentMessage(this, mtxt, next);
}

CmdClient.prototype.Request = function(mtxt, next) {
  ZCio.SendAgentRequest(this, mtxt, next);
}

CmdClient.prototype.AddUser = function(username, password, role, next) {
  ZH.l('ZCmdClient.AddUser');
  ZCio.SendAgentAddUser(this, username, password, role, next);
}

CmdClient.prototype.RemoveUser = function(username, next) {
  ZH.l('ZCmdClient.RemoveUser');
  ZCio.SendAgentRemoveUser(this, username, next);
}

CmdClient.prototype.GrantUser = function(username, schanid, priv, next) {
  ZH.l('ZCmdClient.GrantUser');
  schanid = ZH.IfNumberConvert(schanid);
  if (priv !== 'WRITE' && priv !== 'READ' && priv !== 'REVOKE') {
    next(new Error(ZS.Errors.BadPrivilegeDesc), null);
    return;
  }
  ZCio.SendAgentGrantUser(this, username, schanid, priv, next);
}

CmdClient.prototype.RemoveDataCenter = function(dcuuid, next) {
  ZH.l('ZCmdClient.RemoveDataCenter');
  ZCio.SendAgentRemoveDataCenter(this, dcuuid, next);
}

CmdClient.prototype.GetAgentKeys = function(duuid, nkeys, minage, wonly, next) {
  ZH.l('ZCmdClient.GetAgentKeys');
  var duuid  = Number(duuid);
  var nkeys  = Number(nkeys);
  var minage = Number(minage);
  ZCio.SendAgentGetAgentKeys(this, duuid, nkeys, minage, wonly, next);
}

CmdClient.prototype.IsUserStationed = function(username, next) {
  ZH.l('ZCmdClient.IsUserStationed: ' + username);
  get_stationed_users(this, function(gerr, susers) {
    ZH.HandleIsUserStationed(gerr, susers, username, next);
  });
}

CmdClient.prototype.IsSubscribed = function(schanid, next) {
  ZH.l('ZCmdClient.IsSubscribed: ' + schanid);
  get_agent_subscriptions(this, function(gerr, gres) {
    ZH.HandleIsSubscribed(gerr, gres, schanid, next);
  });
}

CmdClient.prototype.IsCached = function(ks, next) {
  ZH.l('ZCmdClient.IsCached');
  ZCio.SendAgentIsCached(this, ks, function(ierr, hres) {
    if (ierr) next(ierr, null);
    else {
      var hit = hres.cached;
      next(null, hit);
    }
  });
}

CmdClient.prototype.Shutdown = function(next) {
  ZH.l('ZCmdClient.Shutdown');
  ZCio.SendAgentShutdown(this, next);
}

CmdClient.prototype.Close = function() {
  ZH.l('ZCmdClient.Close()');
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REMOTE MEMCACHE CALLS -----------------------------------------------------

var RetryErrorMessages                            = {};
RetryErrorMessages[ZS.Errors.MemcacheKeyNotLocal] = true;
RetryErrorMessages[ZS.Errors.HttpsSocketClose]    = true;
RetryErrorMessages[ZS.Errors.NetConnectRefused]   = true;

function retry_memcache_store(me, ns, cn, key, sep, ex, json, next) {
  var mcname = me.MemcacheClusterName;
  me.GetMemcacheClusterState(mcname, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      ZMCG.GetMemcacheAgentClient(me, ns, cn, key, function(gerr, mcli) {
        if (gerr) next(gerr, null);
        else {
          ZH.l('RETRYING: do_store');
          do_store(mcli, ns, cn, key, sep, ex, json, true, next);
        }
      });
    }
  });
}

CmdClient.prototype.MemcacheStore = function(ns, cn, key, sep, ex, json, next) {
  var me = this;
  ZMCG.GetMemcacheAgentClient(me, ns, cn, key, function(gerr, mcli) {
    if (gerr) next(gerr, null);
    else {
      do_store(mcli, ns, cn, key, sep, ex, json, true, function(serr, sres) {
        if (!serr) next(null, sres);
        else {
          var retry = RetryErrorMessages[serr.message];
          if (!retry) next(serr, null);
          else {
            retry_memcache_store(me, ns, cn, key, sep, ex, json, next);
          }
        }
      });
    }
  });
}

function retry_memcache_commit(me, ns, cn, key, oplog, rchans, sep, ex, next) {
  var mcname = me.MemcacheClusterName;
  me.GetMemcacheClusterState(mcname, function(serr, sres) {
    if (serr) next(serr, null);
    else {
      ZMCG.GetMemcacheAgentClient(me, ns, cn, key, function(gerr, mcli) {
        if (gerr) next(gerr, null);
        else {
          ZH.l('RETRYING: ZCio.SendAgentMemcacheCommit');
          ZCio.SendAgentMemcacheCommit(mcli, ns, cn, key, oplog,
                                       rchans, sep, ex, next);
        }
      });
    }
  });
}

CmdClient.prototype.MemcacheCommit = function(ns, cn, key, oplog, rchans,
                                              sep, ex, next) {
  var me = this;
  ZMCG.GetMemcacheAgentClient(me, ns, cn, key, function(gerr, mcli) {
    if (gerr) next(gerr, null);
    else {
      ZCio.SendAgentMemcacheCommit(mcli, ns, cn, key, oplog, rchans, sep, ex,
      function(serr, sres) {
        if (!serr) next(null, sres);
        else {
          var retry = RetryErrorMessages[serr.message];
          if (!retry) next(serr, null);
          else {
            retry_memcache_commit(me, ns, cn, key, oplog,
                                  rchans, sep, ex, next);
          }
        }
      });
    }
  });
}

CmdClient.prototype.MemcacheGet = function(ns, cn, key, root, next) {
  ZH.l('ZCmdClient.MemcacheGet: K: ' + key + ' F: ' + root);
  ZMCG.MemcacheGet(ns, cn, key, root, next);
}

CmdClient.prototype.MemcacheFetch = function(ns, cn, key, root, next) {
  ZH.l('ZCmdClient.MemcacheFetch: K: ' + key + ' F: ' + root);
  var me = this;
  ZMCG.MemcacheGet(ns, cn, key, root, function(ferr, fres) {
    if (ferr) next(ferr, null);
    else {
      if (ZH.IsDefined(fres) && fres !== null) { // MACHETE DONT PERSIST NULLS
        next(null, fres);
      } else {
        ZMCG.GetMemcacheAgentClient(me, ns, cn, key, function(gerr, mcli) {
          if (gerr) next(gerr, null);
          else {
            var pin    = false;
            var watch  = false;
            var sticky = true;
            ZCio.SendAgentCache(mcli, ns, cn, key, pin, watch, sticky,
            function(cerr, zdoc) {
              if (cerr) next(cerr, null);
              else {
                var json = zdoc._;
                if (!root) next(null, json);
                else       next(null, json[root]);
              }
            });
          }
        });
      }
    }
  });
}

CmdClient.prototype.GetMemcacheClusterState = function(mcname, next) {
  var me = this;
  me.MemcacheClusterName = mcname;
  ZCio.SendAgentGetMemcacheClusterState(this, mcname, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      var mstate = gres.state;
      ZMCG.SetCientMemcacheDaemonClusterState(mstate);
      ZMCG.CreateMemcacheAgentClientPool(mcname);
      next(null, mstate);
    }
  });
}

CmdClient.prototype.GetMemcacheDaemonClusterState = function(mcname, next) {
  var me = this;
  me.GetMemcacheClusterState(mcname, function(gerr, mstate) {
    if (gerr) next(gerr, null);
    else {
      var dstate = ZMCG.SetCientMemcacheDaemonClusterState(mstate);
      next(null, dstate);
    }
  });
}

CmdClient.prototype.SetCientMemcacheDaemonClusterState = function(mstate) {
  ZMCG.SetCientMemcacheDaemonClusterState(mstate);
}

// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZCmdClient']={} : exports);

