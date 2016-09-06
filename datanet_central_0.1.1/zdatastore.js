"use strict";

var ZGack, ZNM, ZAS, ZConv, ZDelt, ZAuth, ZGC, ZDack, ZMDC, ZQ, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  require('./setImmediate');
  ZNM   = require('./zneedmerge');
  ZGack = require('./zgack');
  ZAS   = require('./zactivesync');
  ZConv = require('./zconvert');
  ZDelt = require('./zdeltas');
  ZAuth = require('./zauth');
  ZGC   = require('./zgc');
  ZDack = require('./zdack');
  ZMDC  = require('./zmemory_data_cache');
  ZQ    = require('./zqueue');
  ZS    = require('./zshared');
  ZH    = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DATA-STORE HELPERS --------------------------------------------------------

function get_data_collection(net, ks) {
  net.plugin.set_namespace(ks.ns);
  net.plugin.set_collection_name(ks.cn);
  var dname;
  if (ZH.AmCentral) dname = ZH.AmRouter ? "RGLOBAL" : "SGLOBAL";
  else              dname = ZH.Agent.device_uuid;
  var d_cname   = ZH.CreateDeviceCollname(dname, ks.cn);
  var dfunc     = net.plugin.do_create_collection;
  var data_coll = dfunc(net.db_handle, d_cname, false);
  return data_coll;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FIND (Client, External) ---------------------------------------------------

function find_read_perms_data(net, ns, cn, jsons, auth, hres, next) {
  var need = jsons.length;
  if (jsons.length === 0) next(null, hres);
  else {
    var json   = jsons.shift();
    var ks     = ZH.CompositeQueueKey(ns, cn, json._id);
    var rchans = json._channels;
    ZAuth.GetAgentDataReadPermissions(net.plugin, net.collections,
                                      ks, rchans, auth,
    function(aerr, perms) {
      if (aerr) next(aerr, hres);
      else {
        if (perms) {
          hres.jsons.push(json);
        }
        setImmediate(find_read_perms_data, net,
                     ns, cn, jsons, auth, hres, next);
      }
    });
  }
}

exports.FindPermsData = function(net, ns, cn, query, auth, hres, next) {
  ZH.l('ZDS.FindPermsData: NS: ' + ns + ' CN: ' + cn + ' Q: ' + query);
  var ks        = ZH.CompositeQueueKey(ns, cn, null);
  var data_coll = get_data_collection(net, ks);
  net.plugin.do_find_json(data_coll, query, function (serr, jsons) {
    if (serr) next(serr, hres);
    else { // REMOVE crdts outside current users SUBSCRIPTIONs
      var ajsons = [];
      for (var key in jsons) ajsons.push(jsons[key]);
      hres.jsons = []; // Used in response
      find_read_perms_data(net, ns, cn, ajsons, auth, hres, next);
    }
  });
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT SCAN WITH PERMISSIONS -----------------------------------------------

//TODO return JSONs not CRDTs
function scan_read_perms_data(plugin, collections, crdts, auth, hres, next) {
  if (crdts.length === 0) next(null, hres);
  else {
    var crdt   = crdts.shift();
    var kqk    = crdt._id;
    var ks     = ZH.ParseKQK(kqk);
    var rchans = ZH.GetReplicationChannels(crdt._meta);
    ZAuth.GetAgentDataReadPermissions(plugin, collections, ks, rchans, auth,
    function(aerr, perms) {
      if (aerr) next(aerr, hres);
      else {
        if (perms !== null) hres.crdts[ks.kqk] = crdt;
        setImmediate(scan_read_perms_data, plugin, collections,
                     crdts, auth, hres, next);
      }
    });
  }
}

exports.AgentScanReadPermsData = function(plugin, collections, auth,
                                          hres, next) {
  if (plugin.name === "Memcache") {
    return next(new Error("MEMCACHE DOES NOT SUPPORT SCANS"), hres);
  }
  ZH.l('ZDS.AgentScanReadPermsData');
  plugin.do_scan(collections.crdt_coll, function(serr, crdts) {
    if (serr) next(serr, hres);
    else { // REMOVE crdts outside current users SUBSCRIPTIONs
      hres.crdts = {};
      scan_read_perms_data(plugin, collections, crdts, auth, hres, next);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STORE DELTA HELPERS -------------------------------------------------------

exports.StoreLastRemoveDelta = function(plugin, collections, ks, meta, next) {
  var lkey = ZS.GetLastRemoveDelta(ks);
  var ts   = meta["@"];
  var lval = {meta : meta,
              "@"  : ts,
              "_"  : meta.author.agent_uuid};
  plugin.do_set(collections.ldr_coll, lkey, lval, next);
}

function store_delta_last_removed(plugin, collections, ks, dentry, next) {
  var meta = dentry.delta._meta;
  if (!meta.remove) next(null, null);
  else { // STORE REMOVE-DELTA METADATA
    var meta = dentry.delta._meta;
    exports.StoreLastRemoveDelta(plugin, collections, ks, meta, next)
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT-SIDE AGENT DELTA ----------------------------------------------------

exports.RemoveAgentDelta = function(plugin, collections, ks, avrsn, next) {
  ZH.l('ZDS.RemoveAgentDelta: K: ' + ks.kqk + ' AV: ' + avrsn);
  var pkey = ZS.GetAgentPersistDelta(ks.kqk, ZH.MyUUID, avrsn);
  plugin.do_remove(collections.delta_coll, pkey, next);
}

exports.StoreAgentDelta = function(plugin, collections,
                                   ks, dentry, auth, next) {
  ZH.l('ZDS.StoreAgentDelta: ' + ZH.SummarizeDelta(ks.key, dentry));
  var meta    = dentry.delta._meta;
  var avrsn   = meta.author.agent_version
  var pkey    = ZS.GetAgentPersistDelta(ks.kqk, ZH.MyUUID, avrsn);
  var p_entry = {dentry : dentry, auth : auth};
  plugin.do_set(collections.delta_coll, pkey, p_entry, function (perr, pres) {
    if (perr) next(perr, null);
    else      store_delta_last_removed(plugin, collections, ks, dentry, next);
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT-SIDE SUBSCRIBER DELTA -----------------------------------------------

exports.RemoveSubscriberDelta = function(plugin, collections,
                                         ks, author, next) {
  var auuid = author.agent_uuid;
  var avrsn = author.agent_version;
  ZH.l('ZDS.RemoveSubscriberDelta: AU: ' + auuid + ' AV: ' + avrsn);
  var pkey  = ZS.GetAgentPersistDelta(ks.kqk, auuid, avrsn);
  plugin.do_remove(collections.delta_coll, pkey, next);
}

// NOTE: ZDS.StoreSubscriberDelta UPDATES AgentDeltas (auth NOT overwritten)
exports.StoreSubscriberDelta = function(plugin, collections, ks, dentry, next) {
  var author = dentry.delta._meta.author;
  var auuid  = author.agent_uuid;
  var avrsn  = author.agent_version;
  ZH.l('ZDS.StoreSubscriberDelta: AU: ' + auuid + ' AV: ' + avrsn);
  var pkey   = ZS.GetAgentPersistDelta(ks.kqk, auuid, avrsn);
  plugin.do_set_field(collections.delta_coll, pkey, "dentry", dentry,
  function (perr, pres) {
    if (perr) next(perr, null);
    else      store_delta_last_removed(plugin, collections, ks, dentry, next);
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL-SIDE AGENT DELTA --------------------------------------------------

exports.CentralRemoveDelta = function(plugin, collections, ks, author, next) {
  var auuid = author.agent_uuid;
  var avrsn = author.agent_version;
  ZH.l('ZDS.CentralRemoveDelta: K: ' + ks.kqk + ' AU: ' + auuid +
       ' AV: ' + avrsn);
  var pkey  = ZS.GetCentralPersistDelta(ks.kqk, auuid, avrsn);
  plugin.do_remove(collections.delta_coll, pkey, next);
}

exports.CentralStoreDelta = function(plugin, collections, ks, dentry, next) {
  var meta  = dentry.delta._meta;
  var auuid = meta.author.agent_uuid;
  var avrsn = meta.author.agent_version;
  ZH.l('ZDS.CentralStoreDelta: K: ' + ks.kqk + ' AU: ' + auuid +
       ' AV: ' + avrsn);
  var pkey    = ZS.GetCentralPersistDelta(ks.kqk, auuid, avrsn);
  var p_entry = {_id : pkey, dentry : dentry}; // do NOT store AUTH
  plugin.do_set(collections.delta_coll, pkey, p_entry, function (perr, pres) {
    if (perr) next(perr, null);
    else      store_delta_last_removed(plugin, collections, ks, dentry, next);
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CRDTS ---------------------------------------------------------------------

function remove_rchans(dest, src) {
  var dexists = {}
  for (var i = 0; i < dest.length; i++) {
    dexists[dest[i]] = true;
  }
  for (var i = 0; i < src.length; i++) {
    if (dexists[src[i]]) delete(dexists[src[i]]);
  }
  var res = [];
  for (var rchan in dexists) {
    res.push(ZH.IfNumberConvert(rchan));
  }
  return res;
}

// NOTE: Used by ZSM.check_subscriber_delta_removed_channels()
exports.RemoveChannels = function(meta) {
  var res = remove_rchans(meta.replication_channels, meta.removed_channels);
  meta.replication_channels = ZH.clone(res);
  delete(meta.removed_channels);
}

var DebugOnStoreCrdt     = false;
var DebugOnRetrieveCrdt  = false;
var DeepDebugOnStoreCrdt = false;

function conditional_debug_retrieve_crdt(plugin, collections, ks, crdt, next) {
  if (!DebugOnRetrieveCrdt) next(null, crdt);
  else {
    ZH.DebugCrdt(plugin, collections, ks, crdt, 'RetrieveCrdt: ', true,
    function(cerr, cres) {
      if (cerr) ZH.e(cerr);
      next(null, crdt);
    });
  }
}

function do_retrieve_crdt(plugin, collections, ks, crdt, next) {
  var expiration = crdt ? crdt._meta.expiration : null;
  if (!expiration) {
    conditional_debug_retrieve_crdt(plugin, collections, ks, crdt, next);
  } else {
    var nows = ZH.GetMsTime() / 1000;
    if (expiration > nows) { // NOT YET EXPIRED -> OK
      conditional_debug_retrieve_crdt(plugin, collections, ks, crdt, next);
    } else {
      ZH.e('LOCAL-RETRIEVE-EXPIRATION: K: ' + ks.kqk + ' E: ' + expiration +
           ' NOW: ' + nows);
      next(null, null);
    }
  }
}

// NOTE: SKIPS DEBUG & LOCAL-RETRIEVE-EXPIRATION
exports.ForceRetrieveCrdt = function(plugin, collections, ks, next) {
  plugin.do_get_crdt(collections.crdt_coll, ks.kqk, function (gerr, gres) {
    if (gerr) next(gerr,  null);
    else {
      if (gres.length === 0) next(null, null);
      else                   next(null, gres[0]);
    }
  });
}

exports.RetrieveCrdt = function(plugin, collections, ks, next) {
  exports.ForceRetrieveCrdt(plugin, collections, ks, function(gerr, fcrdt) {
    if (gerr) next(gerr,  null);
    else      do_retrieve_crdt(plugin, collections, ks, fcrdt, next);
  });
}

function cleanup_crdt_before_store(ccrdt, ks) {
  var meta = ccrdt._meta;
  delete(meta.last_member_count);
  delete(meta.remove);
  delete(meta.expire);
  delete(meta.auto_cache);
  delete(meta.author);
  delete(meta.dependencies);
  delete(meta.delta_bytes);
  delete(meta.xaction);
  delete(meta.OOO_GCV);
  delete(meta.from_central);
  delete(meta.dirty_central);
  delete(meta.DO_GC);
  delete(meta.DO_DS);
  delete(meta.DO_REORDER);
  delete(meta.DO_REAP);
  delete(meta.reap_gc_version);
  delete(meta.DO_IGNORE);
  delete(meta.AUTO);
  delete(meta.reference_uuid);
  delete(meta.reference_version);
  delete(meta.reference_GC_version);
  delete(meta.reference_ignore);
  if (meta.removed_channels) exports.RemoveChannels(meta);
  ccrdt._id = ks.kqk; // NOTE json._id !== crdt._id
}

// NOTE: ZERO checks done, used externally for ROLLBACKs
exports.InternalStoreCrdt = function(net, ks, crdt, next) {
  if (!crdt) next(null, null); // Can happen on ROLLBACK
  else {
    var sep   = false;
    var locex = false;
    var delta = null;
    var ccrdt  = ZH.clone(crdt);
    cleanup_crdt_before_store(ccrdt, ks);
    var rchans = ccrdt._meta.replication_channels;
    ZMDC.CasKeyRepChans(net.plugin, net.collections, ks, rchans,
    function(serr, sres) {
      if (serr) next(serr, null);
      else {
        if (DeepDebugOnStoreCrdt) ZH.p(ccrdt);
        net.plugin.do_store_crdt(net.collections.crdt_coll,
                                 ks.kqk, ccrdt, sep, locex, delta,
        function (cerr, cres) {
          if (cerr) next(cerr, null);
          else {
            var json      = ZH.CreatePrettyJson(ccrdt);
            var data_coll = get_data_collection(net, ks);
            net.plugin.do_store_json(data_coll, ks.key, json, sep, locex, delta,
            function(jerr, jres) {
              if (jerr) next(jerr, null);
              else {
                var skey = ZS.AllNamespaceCollections;
                var sval = ZS.GetAllNamespaceCollectionsValue(ks);
                net.plugin.do_set_field(net.collections.global_coll,
                                        skey, sval, true, next);
              }
            });
          }
        });
      }
    });
  }
}

exports.StoreCrdt = function(net, ks, crdt, next) {
  if (crdt) ZH.l('ZDS.StoreCrdt: ' + ZH.SummarizeCrdt(ks.key, crdt._meta));
  if (!DebugOnStoreCrdt) exports.InternalStoreCrdt(net, ks, crdt, next);
  else {
    ZH.DebugCrdt(net.plugin, net.collections, ks, crdt, 'StoreCrdt: ', true,
    function(cerr, cres) {
      if (cerr) ZH.e(cerr);
      exports.InternalStoreCrdt(net, ks, crdt, next);
    });
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// REMOVE KEY ----------------------------------------------------------------

exports.RemoveKey = function(net, ks, sep, next) {
  ZH.l('ZDS.RemoveKeyFromStorage: K: ' + ks.kqk);
  var kkey = ZS.GetKeyInfo(ks);
  net.plugin.do_remove(net.collections.kinfo_coll, kkey, function(kerr, kres) {
    if (kerr) next(kerr, null);
    else {
      net.plugin.do_remove_crdt(net.collections.crdt_coll, ks.kqk, sep,
      function(serr, sres) {
        if (serr) next(serr, null);
        else {
          var data_coll = get_data_collection(net, ks);
          net.plugin.do_remove_json(data_coll, ks.key, sep,
          function(rerr, rres) {
            if (rerr) next(rerr, null);
            else      ZGC.RemoveGCVSummaries(net, ks, next);
          });
        }
      });
    }
  });
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZDS']={} : exports);

