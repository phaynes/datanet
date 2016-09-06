"use strict"
var Memcached = require('memcached');
var msgpack   = require('msgpack-js');
var lz4       = require('lz4')

require('./setImmediate');

var ZConv      = require('./zconvert');
var ZCmdClient = require('./zcmd_client');
var ZCMP       = require('./zdcompress');
var ZS         = require('./zshared');
var ZH         = require('./zhelper');

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// MEMCACHE CLUSTER HELPERS --------------------------------------------------

function mc_failure(details) {
  ZH.e('mc_failure');
  ZH.e("Server " + details.server + "went down due to: " +
       details.messages.join( '' ));
}

function mc_issue(details) {
  ZH.e('mc_issue');
}

var ClientMemcacheClusterState       = null;
var ClientMemcacheDaemonClusterState = null;

var MCconns = {};
var MacPool = {};

exports.CreateMemcacheAgentClientPool = function(mcname) {
  var mcs    = ClientMemcacheClusterState;
  if (!mcs || !mcs.cluster_nodes) return;
  var cnodes = mcs.cluster_nodes;
  for (var i = 0; i < cnodes.length; i++) {
    var cnode = cnodes[i];
    var duuid = cnode.device_uuid;
    var mcli  = ZCmdClient.ConstructCmdClient(duuid);
    mcli.SetAgentAddress(cnode.fip, cnode.fport);
    mcli.MemcacheClusterName = mcname;
    MacPool[duuid] = mcli;
    ZH.l('CreatePool: D: ' + duuid + ' I: ' + cnode.fip + ' P: ' + cnode.fport);
  }
}

exports.GetMemcacheAgentClient = function(me, ns, cn, key, next) {
  var mcs = ClientMemcacheClusterState;
  if (!mcs) {
    return next(new Error(ZS.Errors.NoClientMemcacheClusterState), null);
  }
  var ks    = ZH.CompositeQueueKey(ns, cn, key);
  var hval  = ZH.GetKeyHash(ks.kqk);
  var slot  = hval % mcs.cluster_nodes.length;
  var cnode = mcs.cluster_nodes[slot];
  var duuid = cnode.device_uuid;
  ZH.l('GetMemcacheAgentClient: K: ' + ks.kqk + ' D: ' + duuid);
  var mcli  = MacPool[duuid];
  if (!mcli) next(new Error(ZS.Errors.MemcacheClusterNoClientFound), null); 
  else {
    mcli.ClientAuth          = me.ClientAuth;
    mcli.MemcacheClusterName = me.MemcacheClusterName;
    next(null, mcli);
  }
}

function get_memcache_daemon_connection(ks) {
  var mdcs  = ClientMemcacheDaemonClusterState;
  var hval  = ZH.GetKeyHash(ks.kqk);
  var slot  = hval % mdcs.daemon_nodes.length;
  var dnode = mdcs.daemon_nodes[slot];
  var url   = dnode.ip + ":" + dnode.port;
  ZH.l('get_memcache_daemon_connection: K: ' + ks.kqk + ' U: ' + url);
  if (MCconns[url]) return MCconns[url];
  else {
    ZH.l('CREATING MEMCACHE CONNECTION: U: ' + url);
    var mc       = new Memcached(url);
    mc.on('failure', mc_failure);
    mc.on('issue',   mc_issue);
    MCconns[url] = mc;
    return MCconns[url];
  }
}

exports.SetCientMemcacheDaemonClusterState = function(mstate) {
  if (!mstate || !mstate.cluster_nodes) return null;
  ClientMemcacheClusterState = mstate;
  var dstate = {daemon_nodes : []};
  for (var i = 0; i < mstate.cluster_nodes.length; i++) {
    var cnode = mstate.cluster_nodes[i];
    var mc    = cnode.memcached;
    dstate.daemon_nodes.push({ip : mc.ip, port : mc.port});
  }
  //ZH.l('ZMCG.SetCientMemcacheDaemonClusterState'); ZH.p(dstate);
  ClientMemcacheDaemonClusterState = dstate;
  return dstate;
}

exports.AgentCheckMemcacheKeyIsLocal = function(net, ks, next) {
  ZH.l('ZMCG.AgentCheckMemcacheKeyIsLocal: K: ' + ks.kqk);
  var mcname = ZH.Agent.MemcacheClusterConfig.name
  var skey   = ZS.GetMemcacheClusterState(mcname);
  net.plugin.do_get(net.collections.global_coll, skey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      if (gres.length === 0) {
        next(new Error(ZS.Errors.MemcacheKeyNotLocal), null);
      } else {
        var mstate = gres[0];
        var hval   = ZH.GetKeyHash(ks.kqk);
        var slot   = hval % mstate.cluster_nodes.length;
        var dnode  = mstate.cluster_nodes[slot];
        var mcac   = ZH.Agent.MemcacheAgentConfig;
        //ZH.e('dnode'); ZH.e(dnode); ZH.e('mcac'); ZH.e(mcac);
        if (dnode.fip === mcac.request_ip && dnode.fport === mcac.port) {
          next(null, null);
        } else {
          next(new Error(ZS.Errors.MemcacheKeyNotLocal), null);
        }
      }
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// MEMCACHE GET --------------------------------------------------------------

function get_separate_crdt_fields(mc, mkey, crdt, mbrs, next) {
  if (mbrs.length === 0) next(null, crdt);
  else {
    var root = mbrs.shift();
    var fkey = mkey + "|" + root;
    mc.get(fkey, function(gerr, zval) {
      if (gerr) next(gerr, null);
      else {
        var uncompressed   = lz4.decode(zval);
        var decoded        = msgpack.decode(uncompressed);
        var member         = ZCMP.DecompressCrdtMember(decoded);
        crdt._data.V[root] = member;
        setImmediate(get_separate_crdt_fields, mc, mkey, crdt, mbrs, next);
      }
    });
  }
}

function get_separate_crdt(mc, mkey, crdt, next) {
  var mcrdt   = ZH.clone(crdt);
  mcrdt._data = ZConv.AddORSetMember({}, "O", crdt._meta);
  var dkey    = ZH.GetMemcacheDirectoryMembers(mkey);
  mc.get(dkey, function(gerr, mbrs) {
    if (gerr) next(gerr, null);
    else {
      get_separate_crdt_fields(mc, mkey, mcrdt, mbrs, function(ferr, scrdt) {
        if (ferr) next(ferr, null);
        else {
          var json = ZH.CreatePrettyJson(scrdt);
          next(null, json);
        }
      });
    }
  });
}

// TODO mc.get() can hang forever -> Bug in node.js 'memcached' library
exports.MemcacheGet = function(ns, cn, key, root, next) {
  var mdcs  = ClientMemcacheDaemonClusterState;
  if (!mdcs) {
    return next(new Error(ZS.Errors.NoClientMemcacheClusterState), null);
  }
  var ks    = ZH.CompositeQueueKey(ns, cn, key);
  var mc    = get_memcache_daemon_connection(ks);
  var mkey  = ks.kqk;
  mc.get(mkey, function(gerr, zval) {
    if (gerr) next(gerr, null);
    else {
      if (ZH.IsUndefined(zval)) next(null, null);
      else {
        var uncompressed = lz4.decode(zval);
        var decoded      = msgpack.decode(uncompressed);
        var crdt         = ZCMP.DecompressCrdt(decoded);
        if (crdt._data) {
          var json = ZH.CreatePrettyJson(crdt);
          if (!root) next(null, json);
          else {
            var found = ZH.LookupByDotNotation(json, root);
            next(null, found);
          }
        } else {
          if (!root) get_separate_crdt(mc, mkey, crdt, next);
          else {
            var fkey = mkey + "|" + root;
            mc.get(fkey, function(ferr, zval) {
              if (ferr) next(ferr, null)
              else {
                var uncompressed = lz4.decode(zval);
                var decoded      = msgpack.decode(uncompressed);
                var member       = ZCMP.DecompressCrdtMember(decoded);
                var val          = ZConv.CrdtElementToJson(member);
                next(null, val);
              }
            });
          }
        }
      }
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EXTERNAL SCRIPT MEMCACHE-GET HELPERS --------------------------------------

exports.End = function() {
  for (var url in MCconns) {
    var mc = MCconns[url];
    mc.end();
  }
  for (var duuid in MacPool) {
    var mcli = MacPool[duuid];
    mcli.Close();
  }
}

// ZMCG.SetClientMemcacheClusterState() is for external scripts 
exports.SetClientMemcacheClusterState = function(mcname, dnodes) {
  if (!Array.isArray(dnodes)) {
    throw(new Error(ZS.Errors.MemcacheClusterStateInvalid));
  }
  var mstate = {cluster_nodes : []}
  for (var i = 0; i < dnodes.length; i++) {
    var dnode = dnodes[i];
    if (!dnode.ip || !dnode.aport || !dnode.mport || !dnode.device_uuid) {
      throw(new Error(ZS.Errors.MemcacheClusterStateInvalid));
    }
    var mcd                 = {ip : dnode.ip, port: dnode.mport};
    mstate.cluster_nodes[i] = {device_uuid : dnode.device_uuid,
                               fip         : dnode.ip,
                               fport       : dnode.aport,
                               memcached   : mcd};
  }
  exports.SetCientMemcacheDaemonClusterState(mstate);
  exports.CreateMemcacheAgentClientPool(mcname);
}

