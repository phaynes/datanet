"use strict";

require('./setImmediate');

var ZPub   = require('./zpublisher');
var ZRAD   = require('./zremote_apply_delta');
var ZGack  = require('./zgack');
var ZPio   = require('./zpio');
var ZAD    = require('./zapply_delta');
var ZOOR   = require('./zooo_replay');
var ZGC    = require('./zgc');
var ZMDC   = require('./zmemory_data_cache');
var ZDS    = require('./zdatastore');
var ZFix   = require('./zfixlog');
var ZCLS   = require('./zcluster');
var ZDQ    = require('./zdata_queue');
var ZQ     = require('./zqueue');
var ZS     = require('./zshared');
var ZH     = require('./zhelper');


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

function summarize_get_ether_delta(ks, auuid, avrsn, ngacks, ndcs, is_sub) {
  ZH.l('get_ether_delta: K: ' + ks.kqk + ' AU: ' + auuid + ' AV: ' + avrsn +
       ' ngacks: ' + ngacks + ' #DC: ' + ndcs + ' is_sub: ' + is_sub);
}

// GeoNeedMerge EtherDeltas only produce GC-ACKs (ShellDeltas)
function convert_to_shell_delta(dentry) {
  var dmeta                    = dentry.delta._meta;
  var ndentry                  = {};
  ndentry.is_shell             = true;
  ndentry.author               = ZH.clone(dmeta.author);
  ndentry.replication_channels = ZH.clone(dmeta.replication_channels);
  ndentry.dirty_central        = dmeta.dirty_central;
  return ndentry;
}

function get_ether_delta(net, ks, auuid, avrsn, ether, is_sub, next) {
  var author = {agent_uuid : auuid, agent_version : avrsn};
  ZGack.GetNumGacks(net.plugin, net.collections, ks, author,
  function(aerr, ngacks) {
    if (aerr) next(aerr, null);
    else {
      var ndcs = ZCLS.GeoNodes.length;
      summarize_get_ether_delta(ks, auuid, avrsn, ngacks, ndcs, is_sub);
      if (ndcs === ngacks) { // GeoClusterReOrg shrink -> AlreadyAcked
        next(null, null);
      } else {
        ZRAD.FetchStorageDelta(net.plugin, net.collections, ks, author,
        function(ferr, fdentry) {
          if (ferr) next(ferr, null);
          else {
            if (fdentry) {
              var ksafe  = false;
              if (is_sub && ZH.CentralSubscriberEtherKSafetyTwo) {
                var is_geo = fdentry.delta._meta.is_geo;
                if (is_geo || (ngacks > 1)) ksafe = true;
              }
              if (!ksafe) { // Delta NOT K-SAFE -> include in Ether
                var ekey    = ZS.GetEtherKey(fdentry.delta._meta.author);
                if (!is_sub) fdentry = convert_to_shell_delta(fdentry);
                ether[ekey] = fdentry;
              }
            }
            next(null, null);
          }
        });
      }
    }
  });
}

function populate_ether(net, ks, ether, is_sub, next) {
  var gkey = ZS.GetCentralKeyAgentVersions(ks.kqk);
  net.plugin.do_get_array_values(net.collections.delta_coll, gkey, "aversions",
  function (gerr, avrsns) {
    if (gerr) next(gerr, null);
    else {
      var need  = avrsns ? avrsns.length : 0;
      if (need === 0) next(null, null);
      else {
        var done = 0;
        var nerr = false;
        for (var i = 0; i < need; i++) {
          var avrsn = avrsns[i];
          var auuid = ZH.GetAvUUID(avrsn);
          get_ether_delta(net, ks, auuid, avrsn, ether, is_sub,
          function(ferr, fres) {
            if      (nerr) return;
            else if (ferr) {
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
  });
}

function get_need_merge_body(net, ks, ether, is_sub, next) {
  ZDS.ForceRetrieveCrdt(net.plugin, net.collections, ks, function(ferr, fcrdt) {
    if (ferr) next(ferr, null);
    else {
      populate_ether(net, ks, ether, is_sub, function(perr, pres) {
        var ne = Object.keys(ether).length;
        ZH.l('get_need_merge_body: #ether: ' + ne + ' is_sub: ' + is_sub);
        next(perr, fcrdt);
      });
    }
  });
}

function get_datacenter_removed_crdt(net, ks, next) {
  var lkey = ZS.GetLastRemoveDelta(ks);
  net.plugin.do_get_field(net.collections.ldr_coll, lkey, "meta", 
  function(gerr, rmeta) {
    if (gerr) next(gerr, null);
    else {
      if (!rmeta) next(null, null); // KEY NEVER EXISTED
      else { // Used LastDentry's META to propogate REMOVED KEY
        var crdt          = {_meta : ZH.clone(rmeta)};
        crdt._meta.remove = true;
        next(null, crdt);
      }
    }
  });
}

function get_datacenter_xcrdt(net, ks, b, crdt, next) {
  if (crdt) {
    b.remove = false;
    b.crdt   = ZH.clone(crdt);
    next(null, null);
  } else {
    b.remove = true;
    get_datacenter_removed_crdt(net, ks, function(gerr, rcrdt) {
      if (gerr) next(gerr, null);
      else {
        b.crdt = rcrdt;
        next(null, null);
      }
    });
  }
}

function get_geo_need_merge_body(net, ks, sgcv, is_sub, next) {
  var b = {ether : {}};
  ZRAD.GetCentralAgentVersions(net, ks, function(cerr, cavrsns) {
    if (cerr) next(cerr, null);
    else {
      b.cavrsns = cavrsns;
      get_need_merge_body(net, ks, b.ether, is_sub, function(gerr, fcrdt) {
        if (gerr) next(gerr, null);
        else {
          get_datacenter_xcrdt(net, ks, b, fcrdt, function(aerr, ares) {
            if (aerr) next(aerr, null);
            else {
              ZMDC.GetGCVersion(net.plugin, net.collections, ks,
              function(verr, gcv) {
                if (verr) next(verr, null);
                else {
                  ZGC.GetGCVSummaryRange(net.plugin, net.collections,
                                         ks, sgcv, gcv,
                  function(serr, gcsumms) {
                    if (serr) next(serr, null);
                    else {
                      b.gcsumms = gcsumms;
                      next(null, b);
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


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL HANDLE CLUSTER_STORAGE_NEED_MERGE ---------------------------------

function summarize_merge(sub, ks, cavrsns, crdt, ether, gcsumms, remove,
                         hres, next) {
  ks.security_token = ZH.GenerateKeySecurityToken(ks);
  var mdata = {ks                     : ks,
               device                 : {uuid : ZH.MyUUID},
               central_agent_versions : cavrsns,
               crdt                   : crdt,
               gc_summary             : gcsumms,
               ether                  : ether,
               remove                 : remove,
               subscriber             : sub};
  return mdata;
}

exports.FlowHandleStorageNeedMerge = function(net, ks, sgcv, rid, agent,
                                              hres, next) {
  hres.ks    = ks;         // Used in response
  hres.duuid = agent.uuid; // Used in response
  var suuid  = agent.uuid;
  var sub    = ZH.CreateSub(suuid);
  ZH.l('FlowHandleStorageNeedMerge: U: ' + suuid + ' K: ' + ks.kqk +
       ' (S)GCV: ' + sgcv);
  ZMDC.GetCentralKeysToSync(net.plugin, net.collections, ks,
  function(gerr, frozen) {
    if (gerr) next(gerr, hres);
    else {
      if (frozen) { // KEY IS FROZEN
        ZH.l('FlowHandleStorageNeedMerge: FROZEN: K: ' + ks.kqk);
        next(new Error(ZS.Errors.InGeoSync), hres);
      } else {
        get_geo_need_merge_body(net, ks, sgcv, true, function(merr, b) {
          if (merr) next(merr, hres);
          else {
            var md = summarize_merge(sub, ks, b.cavrsns, b.crdt, b.ether,
                                     b.gcsumms, b.remove);
            hres.merge_data            = md;
            hres.merge_data.request_id = rid;
            next(null, hres);
          }
        });
      }
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL HANDLE GEO-NEED-MERGE ---------------------------------------------

function summarize_geo_need_merge(guuid, device, ks, crdt, cavrsns,
                                  gcsumms, ether, remove, hres, next) {
  ks.security_token = ZH.GenerateKeySecurityToken(ks);
  var mdata = {ks                     : ks,
               device                 : {uuid : ZH.MyUUID},
               datacenter             : guuid,
               crdt                   : crdt,
               central_agent_versions : cavrsns,
               gc_summary             : gcsumms,
               ether                  : ether,
               remove                 : remove};
  return mdata;
}

exports.FlowHandleStorageGeoNeedMerge = function(net, ks, guuid, agent,
                                                 hres, next) {
  hres.ks    = ks;    // Used in response
  hres.guuid = guuid; // Used in response
  ZH.l('FlowHandleStorageGeoNeedMerge: GU: ' + guuid + ' K: ' + ks.kqk);
  ZMDC.GetCentralKeysToSync(net.plugin, net.collections, ks,
  function(gerr, frozen) {
    if (gerr) next(gerr, hres);
    else {
      if (frozen) { // KEY IS FROZEN
        ZH.l('FlowHandleStorageGeoNeedMerge: FROZEN: K: ' + ks.kqk);
        next(new Error(ZS.Errors.InGeoSync), hres);
      } else {
        get_geo_need_merge_body(net, ks, 0, false, function(merr, b) {
          if (merr) next(merr, hres);
          else {
            var md = summarize_geo_need_merge(guuid, agent, ks, b.crdt,
                                              b.cavrsns, b.gcsumms, b.ether,
                                              b.remove);
            hres.merge_data = md;
            next(null, hres);
          }
        });
      }
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL HANDLE DATACENTER-MERGE -------------------------------------------

function do_send_ether_geo_acks(net, ks, dentries, next) {
  if (dentries.length === 0) next(null, null);
  else {
    var dentry        = dentries.shift();
    var rguuid        = dentry.author.datacenter; // ShellDelta
    var author        = dentry.author;
    var rchans        = dentry.replication_channels;
    var dirty_central = dentry.dirty_central;
    ZDQ.PushRouterSendAckGeoDelta(net.plugin, net.collections,
                                  rguuid, ks, author, rchans, dirty_central,
    function(serr, sres) {
      if (serr) next(serr, null);
      else      setImmediate(do_send_ether_geo_acks, net, ks, dentries, next);
    });
  }
}

function send_ether_geo_acks(net, ks, ether, next) {
  var dentries = [];
  for (var ekey in ether) {
    dentries.push(ether[ekey]);
  }
  do_send_ether_geo_acks(net, ks, dentries, next);
}

// NOTE: MUST come after apply_store_ack_GNM()
function push_router_cluster_subscriber_merge(net, pc, next) {
  var ks      = pc.ks;
  var xcrdt   = pc.xcrdt;
  var cavrsns = pc.extra_data.cavrsns;
  var gcsumms = pc.extra_data.gcsumms;
  var ether   = {}; // NOTE: GNM ether is 100% ShellDeltas
  var remove   = pc.extra_data.remove;
  ZDQ.PushRouterClusterSubscriberMerge(net.plugin, net.collections,
                                       ks, xcrdt, cavrsns,
                                       gcsumms, ether, remove, next);
}

function dc_merge_set_agent_versions(net, ks, authors, next) {
  if (authors.length === 0) next(null, null);
  else {
    var author = authors.shift();
    var auuid  = author.agent_uuid;
    var avrsn  = author.agent_version;
    ZH.l('ZNM.set_agent_version: K: ' + ks.kqk + ' U: ' + auuid +
         ' AV: ' + avrsn);
    ZMDC.SetDeviceToCentralAgentVersion(net.plugin, net.collections,
                                        auuid, ks, avrsn,
    function(serr, sres) {
      if (serr) next(serr, null);
      else {
        setImmediate(dc_merge_set_agent_versions, net, ks, authors, next);
      }
    });
  }
}

function storage_update_versions_GNM(net, pc, next) {
  var ks      = pc.ks;
  var cavrsns = pc.extra_data.cavrsns;
  var kkey    = ZS.GetKeyToCentralAgentVersion(ks.kqk);
  net.plugin.do_set(net.collections.global_coll, kkey, cavrsns,
  function(kerr, kres) {
    if (kerr) next(kerr, null);
    else {
      var authors = [];
      for (var auuid in cavrsns) {
        authors.push({agent_uuid : auuid, agent_version : cavrsns[auuid]});
      }
      dc_merge_set_agent_versions(net, ks, authors, next);
    }
  });
}

function remove_GNM_crdt(net, pc, next) {
  var ks  = pc.ks;
  var md  = pc.extra_data.md;
  var gcv = md.gcv;
  get_datacenter_removed_crdt(net, ks, function(gerr, rcrdt) {
    if (gerr) next(gerr, null);
    else {
      if (rcrdt) pc.xcrdt = rcrdt; // Make SubscriberMerge(s) UP-TO-DATE
      var xmeta  = pc.xcrdt._meta;
      pc.orchans = ZH.GetReplicationChannels(xmeta);
      ZMDC.SetGCVersion(net.plugin, net.collections, ks, gcv,
      function(serr, sres) {
        if (serr) next(serr, null);
        else {
          ZAD.RemoveCentralKey(net, pc, function(rerr, rres) {
            if (rerr) next(rerr, null);
            else {
              ZDS.StoreLastRemoveDelta(net.plugin, net.collections,
                                       ks, xmeta, next);
            }
          });
        }
      });
    }
  });
}

function store_GNM_crdt(net, pc, next) {
  var ks    = pc.ks;
  var md    = pc.extra_data.md;
  var ncrdt = pc.ncrdt;
  var gcv   = md.gcv;
  pc.xcrdt  = pc.ncrdt; // Make SubscriberMerge(s) UP-TO-DATE
  ZH.l('store_GNM_crdt: K: ' + ks.kqk + ' SET-GCV: ' + gcv);
  ZMDC.SetGCVersion(net.plugin, net.collections, ks, gcv, function(serr, sres) {
    if (serr) next(serr, null);
    else      ZDS.StoreCrdt(net, ks, ncrdt, next);
  });
}

function apply_store_ack_GNM(net, pc, next) {
  var ks = pc.ks;
  ZRAD.UpdateChannelsMetadata(net.plugin, net.collections, pc, true,
  function(uerr, ures) {
    if (uerr) next(uerr, null);
    else {
      ZOOR.ReplayMergeDeltas(net, ks, pc, false, function(aerr, ares) {
        if (aerr) next(aerr, null);
        else {
          ZRAD.GetCentralAgentVersions(net, ks, function(gerr, cavrsns) {
            if (gerr) next(gerr, null);
            else { // Make SubscriberMerge(s) UP-TO-DATE
              pc.extra_data.cavrsns = cavrsns;
              if (!pc.ncrdt) remove_GNM_crdt(net, pc, next);
              else           store_GNM_crdt (net, pc, next);
            }
          });
        }
      });
    }
  });
}

function finalize_ack_GNM(net, pc, next) {
  var ks    = pc.ks;
  var ether = pc.extra_data.ether;
  push_router_cluster_subscriber_merge(net, pc, function(serr, sres) {
    if (serr) next(serr, null);
    else      send_ether_geo_acks(net, ks, ether, next);
  });
}

function commit_ack_GNM(net, pc, next) {
  var ks       = pc.ks;
  var createds = pc.xcrdt._meta.created;
  var gcsumms  = pc.extra_data.gcsumms;
  storage_update_versions_GNM(net, pc, function(uerr, ures) {
    if (uerr) next(uerr, null);
    else {
      apply_store_ack_GNM(net, pc, function(merr, mres) {
        if(merr) next(merr, null);
        else {
          if (ZH.ChaosMode === 31) { // NOTE: DC-MERGE-STEP-1
            return next(new Error(ZH.ChaosDescriptions[31]), null);
          }
          ZCLS.SaveLastGeoCreateds(net.plugin, net.collections, createds,
          function(serr, sres) {
            if (serr) next(serr, null);
            else {
              ZGC.SaveGCVSummaries(net.plugin, net.collections, ks, gcsumms,
              function(aerr, ares) {
                if (aerr) next(aerr, null);
                else {
                  ZMDC.UnsetCentralKeysToSync(net.plugin, net.collections, ks,
                  function(kerr, kres) {
                    if (kerr) next(kerr, null);
                    else      finalize_ack_GNM(net, pc, next);
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

exports.RetryAckGeoNeedMerge = function(plugin, collections, pc, rerr, next) {
  var ks  = pc.ks;
  var net = ZH.CreateNetPerRequest(ZH.Central);
  ZH.l('ZNM.RetryAckGeoNeedMerge: K: ' + ks.kqk);
  var internal = rerr ? false : true;
  commit_ack_GNM(net, pc, function(serr, sres) {
    if (serr) {
      if (internal) next(serr, null);
      else          ZFix.ActivateFixLog(serr, next);
    } else {
      next(null, null);
    }
  });
}

function add_fixlog_dc_merge(net, pc, next) {
  var ks   = pc.ks;
  var udid = ks.kqk;
  ZH.l('add_fixlog_dc_merge: K: ' + ks.kqk + ' UD: ' + udid);
  var ukey = ZS.FixLog;
  net.plugin.do_set_field(net.collections.global_coll, ukey, udid, pc, next);
}

function start_ack_geo_need_merge(net, ks, xcrdt, cavrsns, gcsumms,
                                  ether, remove, md, next) {
  var op    = 'AckGeoNeedMerge'
  var edata = {cavrsns : cavrsns,
               gcsumms : gcsumms,
               ether   : ether,
               remove  : remove,
               md      : md};    // MD needed for ZOOR
  var pc    = ZH.InitPreCommitInfo(op, ks, null, xcrdt, edata);
  ZH.CalculateRchanMatch(pc, md.ocrdt, xcrdt._meta);
  add_fixlog_dc_merge(net, pc, function(uerr, ures) {
    if (uerr) next(uerr, null);
    else {
      var rfunc = exports.RetryAckGeoNeedMerge;
      commit_ack_GNM(net, pc, function(cerr, cres) {
        if (cerr) rfunc(net.plugin, net.collections, pc, cerr, next);
        else      ZFix.RemoveFromFixLog(net.plugin, net.collections, pc, next);
      });
    }
  });
}

function fetch_ack_GNM_metadata(net, ks, xcrdt, next) {
  var gcv    = ZH.GetCrdtGCVersion(xcrdt._meta)
  var rchans = xcrdt._meta.replication_channels;
  ZPub.GetSubs(net.plugin, net.collections, ks, rchans, function(serr, subs) {
    if (serr) next(serr, null);
    else {
      var md = {subs  : subs,
                ocrdt : xcrdt,
                gcv   : gcv};
      next(null, md);
    }
  });
}

exports.FlowHandleStorageAckGeoNeedMerge = function(net, ks, xcrdt,
                                                    cavrsns, gcsumms, ether,
                                                    remove, next) {
  ZH.l('ZNM.FlowHandleStorageAckGeoNeedMerge: K: ' + ks.kqk);
  ZMDC.GetCentralKeysToSync(net.plugin, net.collections, ks,
  function(ferr, frozen) {
    if (ferr) next(ferr, null);
    else {
      if (!frozen) { // KEY is UNFROZEN (ALREADY MERGED)
        ZH.l('ZNM.FlowStorageAckGeoNeedMerge ON UNFROZEN KEY -> NO-OP');
        next(null, null);
      } else {       // FROZEN -> NEEDS MERGE
        fetch_ack_GNM_metadata(net, ks, xcrdt, function(gerr, md) {
          if (gerr) next(gerr, null);
          else {
            start_ack_geo_need_merge(net, ks, xcrdt, cavrsns, gcsumms,
                                     ether, remove, md, next);
          }
        });
      }
    }
  });
}

exports.HandleStorageAckGeoNeedMerge = function(net, ks, md, next) {
  ZH.l('ZNM.HandleStorageAckGeoNeedMerge: K: ' + ks.kqk);
  var crdt     = md.crdt;
  var cavrsns  = md.central_agent_versions;
  var gcsumms  = md.gc_summary;
  var ether    = md.ether;
  var remove   = md.remove;
  var data     = {crdt  : crdt,  cavrsns : cavrsns, gcsumms  : gcsumms,
                  ether : ether, remove  : remove};
  ZQ.AddToKeySerializationFlow(ZQ.StorageKeySerializationQueue, ks,
                               'STORAGE_ACK_GEO_NEED_MERGE',
                               net, data, ZH.NobodyAuth, {}, next);
  var flow = {k : ks.kqk,
              q : ZQ.StorageKeySerializationQueue,
              m : ZQ.StorageKeySerialization,
              f : ZH.Central.FlowCentralKeySerialization};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ROUTER-GEO-NEED-MERGE -----------------------------------------------------

//TODO FIXLOG
exports.FlowHandleRouterGeoNeedMergeResponse = function(plugin, collections,
                                                        qe, next) {
  var net  = qe.net;
  var ks   = qe.ks;
  ZH.l('ZNM.FlowHandleRouterGeoNeedMergeResponse: K: ' + ks.kqk);
  var data = qe.data;
  var md   = data.merge_data;
  if (!md) {
    ZH.e('ERROR: FlowHandleRouterGeoNeedMergeResponse: MERGE_DATA'); ZH.p(hres);
    next(null, null);
  } else {
    var crdt = md.crdt;
    ZPub.RouterUpdateKeyChannels(net, ks, null, crdt, function(serr, sres) {
      if (serr) next(serr, null);
      else {
        ZDQ.PushStorageAckGeoNeedMerge(net.plugin, net.collections,
                                       ks, md, next);
      }
    });
  }
}

function geo_need_merge_processed(err, hres) {
  if (err) {
    ZH.e('ERROR: ZNM.geo_need_merge_processed: ' + err);
  } else {
    var net = ZH.CreateNetPerRequest(ZH.Central);
    var md  = hres.merge_data;
    if (!md) {
      ZH.e('ERROR: geo_need_merge_processed -> NO MERGE_DATA'); ZH.p(hres);
    } else {
      var ks   = md.ks;
      var data = {ks : ks, merge_data : md};
      ZQ.AddToKeySerializationFlow(ZQ.RouterKeySerializationQueue, ks,
                                   'ROUTER_GEO_NEED_MERGE_RESPONSE',
                                   net, data, ZH.NobodyAuth, {}, ZH.OnErrLog);
      var flow = {k : ks.kqk,
                  q : ZQ.RouterKeySerializationQueue,
                  m : ZQ.RouterKeySerialization,
                  f : ZH.Central.FlowCentralKeySerialization};
      ZQ.StartFlow(net.plugin, net.collections, flow);
    }
  }
}

function geo_send_merge(net, ks) {
  var gnode = ZCLS.AmPrimaryDataCenter() ? ZCLS.GetRandomGeoNode() :
                                           ZCLS.GetPrimaryDataCenter();
  if (gnode) { // NOTE: ZPio.GeoSendNeedMerge() is ASYNC
    ZPio.GeoSendNeedMerge(net.plugin, net.collections,
                          gnode, ks, geo_need_merge_processed);
  }
}

exports.HandleRouterGeoNeedMerge = function(net, ks, next) {
  // NOTE: geo_send_merge() is ASYNC
  geo_send_merge(net, ks);
  next(null, null);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HANDLE ANNOUNCE NEW GEO CLUSTER -------------------------------------------

function do_send_reorder_delta(net, ks, author, next) {
  var avrsn = author.agent_version;
  ZH.e('do_send_reorder_delta: K: ' + ks.kqk + ' AV: ' + avrsn);
  ZRAD.FetchStorageKeyMetadata(net, ks, author, function(gerr, md) {
    if (gerr) next(gerr, null);
    else {
      if (!md.ocrdt) next(null, null);
      else {
        ZRAD.FetchStorageDelta(net.plugin, net.collections, ks, author,
        function(ferr, odentry) {
          if (ferr) next(ferr, null);
          else {
            if (!odentry) next(null, null);
            else {
              ZRAD.PrimaryFixOOOGCVDelta(net, ks, odentry, md, next);
            }
          }
        });
      }
    }
  });
}

exports.NewGeoClusterSendReorderDelta = function(plugin, collections,
                                                 qe, next) {
  var net    = qe.net;
  var qnext  = qe.next;
  var qhres  = qe.hres
  var ks     = qe.ks;
  var data   = qe.data;
  var author = data.author;
  ZH.l('ZNM.NewGeoClusterSendReorderDelta: K: ' + ks.kqk);
  do_send_reorder_delta(net, ks, author, function(serr, sres) {
    qnext(serr, qhres);
    next (null, null);
  });
}

function send_new_geo_cluster_reorder_delta(net, ks, author, next) {
  var data = {author : author};
  ZQ.AddToKeySerializationFlow(ZQ.StorageKeySerializationQueue, ks,
                               'STORAGE_NEW_GEO_CLUSTER_SEND_REORDER_DELTA',
                               net, data, ZH.NobodyAuth, {}, next);
  var flow = {k : ks.kqk,
              q : ZQ.StorageKeySerializationQueue,
              m : ZQ.StorageKeySerialization,
              f : ZH.Central.FlowCentralKeySerialization};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}

function send_new_geo_cluster_reorder_deltas(net, kentries, next) {
  if (kentries.length === 0) next(null, null);
  else {
    var kentry = kentries.shift();
    var ks     = kentry.ks;
    var author = kentry.author;
    send_new_geo_cluster_reorder_delta(net, ks, author, function(serr, sres) {
      if (serr) next(serr, null);
      else {
        setImmediate(send_new_geo_cluster_reorder_deltas, net, kentries, next);
      }
    });
  }
}

function send_reorder_deltas_for_ooo_gcv_deltas(net, next) {
  net.plugin.do_scan(net.collections.oood_coll, function(gerr, kentries) {
    if (gerr) next(gerr, null);
    else      send_new_geo_cluster_reorder_deltas(net, kentries, next);
  });
}

exports.StoragePrimaryHandleAnnounceNewGeoCluster = function(net, next) {
  if (!ZCLS.AmPrimaryDataCenter()) next(null, null);
  else {
    if (!ZH.CentralSynced) {
      ZH.e('ZNM.StoragePrimaryHandleAnnounceNewGeoCluster:' +
           ' CENTRAL NOT SYNCED -> NO-OP');
      next(null, null);
    } else {
      ZH.e('ZNM.StoragePrimaryHandleAnnounceNewGeoCluster');
      ZMDC.RemoveAllCentralKeysToSync(net.plugin, net.collections,
      function(serr, sres) {
        if (serr) next(serr, null);
        else      send_reorder_deltas_for_ooo_gcv_deltas(net, next);
      });
    }
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FIND ----------------------------------------------------------------------

exports.HandleStorageFind = function(net, ns, cn, query, auth, hres, next) {
  hres.f_data = {}; // Used in response
  ZDS.FindPermsData(net, ns, cn, query, auth, hres.f_data,
  function(ferr, fres) {
    next(null, hres);
  });
}

exports.CentralHandleFind = function(plugin, collections,
                                     ns, cn, query, auth, hres, next) {
  ZDQ.StorageQueueRequestFind(plugin, collections,
                              ns, cn, query, auth, hres, next);
}

