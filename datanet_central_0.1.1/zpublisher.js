
var T = require('./ztrace'); // TRACE (before strict)
"use strict";

require('./setImmediate');

var ZRAD     = require('./zremote_apply_delta');
var ZPio     = require('./zpio');
var ZMerge   = require('./zmerge');
var ZChannel = require('./zchannel');
var ZCache   = require('./zcache');
var ZCLS     = require('./zcluster');
var ZMDC     = require('./zmemory_data_cache');
var ZPart    = require('./zpartition');
var ZDQ      = require('./zdata_queue');
var ZFilter  = require('./zfilter');
var ZS       = require('./zshared');
var ZH       = require('./zhelper');


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPER --------------------------------------------------------------------

function cleanup_geo_delta_before_send(pdentry) {
  pdentry.delta._meta.is_geo = true;
  delete(pdentry.delta._meta.geo_sent);
  delete(pdentry.delta._meta.geo_received);
  delete(pdentry.delta._meta.subscriber_received);
}

function cleanup_cluster_subscriber_delta_before_send(pdentry) {
  delete(pdentry.delta._meta.is_geo); // Fail-safe
  delete(pdentry.delta._meta.subscriber_received);
}


//----------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SEND CLUSTER SUBSCRIBER DENTRIES ------------------------------------------

function get_cnodes_from_subs(subs) {
  var cnodes = {};
  var suuids = subs.uuids;
  for (var i = 0; i < suuids.length; i++) {
    var cnode = ZPart.GetClusterNode(suuids[i].UUID);
    if (cnode) {
      cnodes[cnode.device_uuid] = cnode;
    }
  }
  return cnodes;
}

function send_cluster_subscriber_dentries(net, ks, agent, dentries, is_geo,
                                          subs, next) {
  var cnodes    = get_cnodes_from_subs(subs);
  var ncnodes   = Object.keys(cnodes).length;
  var pdentries = ZH.clone(dentries);
  for (var i = 0; i < pdentries.length; i++) {
    var pdentry = pdentries[i];
    cleanup_cluster_subscriber_delta_before_send(pdentry);
  }
  if (ncnodes) {
    for (var cuuid in cnodes) {
      var cnode = cnodes[cuuid];
      // NOTE: ZPio.SendClusterSubscriberDelta() is ASYNC
      ZPio.SendClusterSubscriberDentries(net.plugin, net.collections,
                                         cnode, ks, agent, pdentries);
    }
  }
  if (!is_geo) {
    ZPio.GeoBroadcastDentries(net.plugin, net.collections,
                              ks, agent, pdentries);
  }
  next(null, null);
}


//----------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PUBLISH CLUSTER-DELTA -----------------------------------------------------

function geo_broadcast_delta(net, ks, dentry, auth, next) {
  ZH.l('geo_broadcast_delta: ' + ZH.SummarizeDelta(ks.key, dentry));
  var pdentry = ZH.clone(dentry);
  cleanup_geo_delta_before_send(pdentry);
  // NOTE: ZPio.GeoBroadcastDelta() is ASYNc
  ZPio.GeoBroadcastDelta(net, ks, pdentry, auth);
  next(null, null);
}

function send_cluster_subscriber_deltas(net, ks, dentry, subs, do_watch, next) {
  var cnodes  = get_cnodes_from_subs(subs);
  var ncnodes = Object.keys(cnodes).length;
  if (ncnodes) {
    var pdentry = ZH.clone(dentry);
    cleanup_cluster_subscriber_delta_before_send(pdentry);
    for (var cuuid in cnodes) {
      var cnode = cnodes[cuuid];
      // NOTE: ZPio.SendClusterSubscriberDelta() is ASYNC
      ZPio.SendClusterSubscriberDelta(net.plugin, net.collections,
                                      cnode, ks, pdentry, do_watch);
    }
  }
  next(null, null);
}

function central_broadcast_delta(net, ks, dentry, subs,
                                 do_watch, username, next) {
  ZH.l('central_broadcast_delta: ' + ZH.SummarizeDelta(ks.key, dentry));
  var is_geo = dentry.delta._meta.is_geo;
  if (is_geo) {
    send_cluster_subscriber_deltas(net, ks, dentry, subs, do_watch, next);
  } else {
    var auth = {username : username};
    geo_broadcast_delta(net, ks, dentry, auth, function(serr, sres) {
      if (serr) next(serr, null);
      else {
        send_cluster_subscriber_deltas(net, ks, dentry, subs, do_watch, next);
      }
    });
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// READ DB FOR CLUSTER-DELTA -------------------------------------------------

function get_rchan_subs(plugin, collections, psubs, ks, rchans, next) {
  var need = rchans.length;
  if (need === 0) next(null, null);
  else {
    var done = 0;
    var nerr = false;
    for (var i = 0; i < need; i++) {
      var rchanid = rchans[i];
      ZMDC.GetAllChannelToDevice(plugin, collections, rchanid,
      function(gerr, gres) {
        if      (nerr) return;
        else if (gerr) {
          nerr = true;
          next(gerr, null);
        } else {
          if (gres.length !== 0) {
            var subs = gres[0];
            delete(subs._id);
            for (var suuid in subs) psubs.push({uuid : suuid});
          }
          done += 1;
          if (done === need) next(null, null);
        }
      });
    }
  }
}

function get_watched_cached_keys(plugin, collections, ks, subs, csubs, next) {
  if (subs.length === 0) next(null, null);
  else {
    var suuid = subs.shift();
    ZMDC.GetAgentWatchKeys(plugin, collections, ks, suuid,
    function(gerr, watch) {
      if (gerr) next(gerr, null);
      else {
        csubs.push({uuid : suuid, watch : watch});
        setImmediate(get_watched_cached_keys, plugin, collections,
                     ks, subs, csubs, next);
      }
    });
  }
}

function get_cache_subs(plugin, collections, csubs, ks, next) {
  ZMDC.GetAllKeyToDevices(plugin, collections, ks, function (cerr, cres) {
    if (cerr) next(cerr, null);
    else {
      if (cres.length === 0) next(null, null);
      else {
        var cs   = cres[0];
        delete(cs._id);
        var subs = [];
        for (var suuid in cs) subs.push(suuid);
        get_watched_cached_keys(plugin, collections, ks, subs, csubs, next);
      }
    }
  });
}

function union_subs(psubs, csubs) {
  var usubs = {};
  var subs  = [];
  for (var i = 0; i < psubs.length; i++) {
    var suuid    = psubs[i].uuid;
    var sub      = ZH.CreateSub(suuid);
    sub.cache    = false;
    sub.watch    = false;
    usubs[suuid] = sub;
  }
  var has_watch = false;
  for (var i = 0; i < csubs.length; i++) {
    var suuid    = csubs[i].uuid;
    var sub      = ZH.CreateSub(suuid);
    sub.cache    = true;
    sub.watch    = csubs[i].watch ? true : false;
    if (sub.watch) has_watch = true;
    usubs[suuid] = sub;
  }
  for (var k in usubs) {
    subs.push(usubs[k]);
  }
  return {uuids : subs, has_watch : has_watch};
}

function get_subs(plugin, collections, ks, rchans, next) {
  var psubs = [];
  get_rchan_subs(plugin, collections, psubs, ks, rchans, function(serr, sres) {
    if (serr) sext(serr, null);
    else {
      var csubs = [];
      get_cache_subs(plugin, collections, csubs, ks, function (cerr, cres) {
        if (cerr) next(cerr, null);
        else {
          var subs = union_subs(psubs, csubs);
          next(null, subs);
        }
      });
    }
  });
}

function populate_xaction(dentry, created, username) {
  var meta = dentry.delta._meta;
  if (!meta.xaction) {
    var author        = meta.author;
    author.datacenter = ZH.MyDataCenter
    author.created    = created;
    author.username   = username;
    meta.xaction      = ZH.GenerateDeltaSecurityToken(dentry);
  }
  meta.created         = {};
  var mguuid           = ZH.MyDataCenter;
  meta.created[mguuid] = created;
}

function generate_delta_xaction_id(net, ks, dentry, username) {
  var created = ZH.GetMsTime();
  populate_xaction(dentry, created, username);
  ZH.l('generate_clusterdelta_xaction_id: K: ' + ks.kqk + ' C: ' + created);
  return created;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// QUICK CLUSTER DELTA COMMIT ------------------------------------------------

function push_delta_to_storage(net, ks, dentry, rguuid, next) {
  ZH.l('push_delta_to_storage: K: ' + ks.kqk);
  ZDQ.PushStorageClusterDelta(net, ks, dentry, rguuid, next);
}

//TODO router_update_key_channels()'s code path is pure SPAGHETTI-code
//     it is called on Agent,Router,Subscribe
//     for AgentDelta, SubscriberDelta, SubscriberMerge, GeoNeedMergeResponse
//     code needs to be refactored -> LESS DENSE + MORE READABLE
//     -> put this code plus ZRAD code in ZAD
//
function router_update_key_channels(net, ks, dentry, crdt, next) {
  var is_merge = crdt ? true : false;
  var pc       = {ks : ks, dentry : dentry, xcrdt : crdt}; //TODO orchans[]
  var nrchans  = is_merge ? crdt._meta.replication_channels :
                            dentry.delta._meta.replication_channels;
  ZMDC.CasKeyRepChans(net.plugin, net.collections, ks, nrchans,
  function(aerr, ares) {
    if (aerr) next(aerr, null);
    else {
      ZRAD.UpdateChannelsMetadata(net.plugin, net.collections,
                                  pc, is_merge, next);
    }
  });
}

function router_central_delta_commit(net, ks, dentry, rguuid, next) {
  router_update_key_channels(net, ks, dentry, null, function(serr, sres) {
    if (serr) next(serr, null);
    else      push_delta_to_storage(net, ks, dentry, rguuid, next);
  });
}

function process_cluster_delta(net, ks, dentry, orchans, rguuid, username,
                               hres, next) {
  var created = generate_delta_xaction_id(net, ks, dentry, username);
  var rchans  = ZH.GetReplicationChannels(dentry.delta._meta);
  get_subs(net.plugin, net.collections, ks, rchans, function (perr, subs) {
    if (perr) next(perr, hres);
    else {
      central_broadcast_delta(net, ks, dentry, subs, false, username,
      function(berr, bres) {
        if (berr) next(berr, hres);
        else {
          hres.dentry = dentry; // Used in response
          router_central_delta_commit(net, ks, dentry, rguuid,
          function(aerr, ares) {
            next(aerr, hres);
          });
        }
      });
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FILTER FAIL REPLY ---------------------------------------------------------

function send_subscriber_server_filter_failed(ks, dentry, filter_fail, next) {
  var cavrsn         = dentry.delta._meta.author.agent_version;
  var ff             = filter_fail.message;
  ks.security_token  = ZH.GenerateKeySecurityToken(ks);
  var meta           = dentry.delta._meta;
  var error_details  = ZRAD.CreateErrorDetails(ks, meta, cavrsn, null, ff);
  error_details.filter_fail = filter_fail;
  //TODO ZPio.SendSubscriberServerFilterFailed()
  next(new Error(ZS.Errors.ServerFilterFailed), null);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL HANDLE CLUSTER-DELTA ----------------------------------------------

function check_server_filter(net, ks, dentry, next) {
  var is_geo = dentry.delta._meta.is_geo;
  if (is_geo) next(null, null);
  else {
    var sfname = dentry.delta._meta.server_filter;
    if (!sfname) next(null, null);
    else {
      var sfunc  = ZFilter.Filters[sfname];
      if (!sfunc) next(new Error(ZS.Errors.ServerFilterNotFound), null);
      else {
        var pc     = {ks : ks, dentry : dentry};
        var merge  = ZMerge.CreateBestEffortPostMergeDeltas(pc);
        var pmd    = merge.post_merge_deltas
        ZFilter.RunFilter(net, ks, pmd, sfunc, function(aerr, ferr) {
          if (aerr) next(aerr, null);
          else {
            if (ferr) {
              var ff = ferr;
              ZH.l('FAIL: FILTER-FUNC(' + sfname + ') K: ' + ks.kqk);
              send_subscriber_server_filter_failed(ks, dentry, ff, next);
            } else {
              ZH.l('PASS: FILTER-FUNC(' + sfname + ') K: ' + ks.kqk);
              next(null, null);
            }
          }
        });
      }
    }
  }
}

function conditional_send_update_subscriber_map(net, ks, dentry,
                                                username, orchans, next) {
  var store    = dentry.delta._meta.initial_delta;
  var acache   = dentry.delta._meta.auto_cache;
  var oschanid = orchans ? orchans[0] : null;
  var dschanid = dentry.delta._meta.replication_channels[0];
  var match    = (oschanid === dschanid);
  ZH.l('conditional_send_update_subscriber_map: K: ' + ks.kqk +
       ' STORE: ' + store + ' ACACHE: ' + acache + ' MATCH: ' + match);
  if (!ZH.DisableBroadcastUpdateSubscriberMap) {
    if (store && !acache && !match) {
      var rchans = ZH.GetReplicationChannels(dentry.delta._meta);
      // NOTE: ZPio.BroadcastUpdateSubscriberMap() is ASYNC
      ZPio.BroadcastUpdateSubscriberMap(net, ks, rchans, username,
                                        null, false, false);
    }
  }
  next(null, null);
}

function handle_cluster_delta(net, ks, dentry, rguuid, username,
                              is_geo, is_dentry, hres, next) {
  var auuid   = dentry._;
  ZH.l('handle_cluster_delta: ' + ZH.SummarizePublish(auuid, ks.key, dentry));
  var meta    = dentry.delta._meta;
  meta.is_geo = is_geo;
  ZMDC.GetKeyRepChans(net.plugin, net.collections, ks, function(ferr, orchans) {
    if (ferr) next(ferr, hres);
    else {
      check_server_filter(net, ks, dentry, function(cerr, cres) {
        if (cerr) next(cerr, hres);
        else {
          if (is_dentry) next(null, hres);
          else {
            process_cluster_delta(net, ks, dentry, orchans,
                                  rguuid, username, hres,
            function(serr, sres) {
              if (serr) next(serr, hres);
              else {
                next(null, hres);
                // NOTE: conditional_send_update_subscriber_map() is ASYNC
                conditional_send_update_subscriber_map(net, ks, dentry,
                                                       username, orchans,
                                                       ZH.OnErrLog);
              }
            });
          }
        }
      });
    }
  });
}

exports.HandleClusterDelta = function(net, ks, dentry, rguuid, username, is_geo,
                                      hres, next) {
  hres.ks     = ks;     // Used in response
  hres.dentry = dentry; // Used in response
  handle_cluster_delta(net, ks, dentry, rguuid, username, is_geo, false,
                       hres,  next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HANDLE CLUSTER DENTRIES ---------------------------------------------------

function add_dentry_ack(hres, dentry, auth, rpt) {
  var meta = dentry.delta._meta;
  var ga   = {authorization : auth,
              author        : meta.author,
              repeat        : rpt,
              delta_info    : ZH.CreateDeltaInfo(meta)};
  hres.acks.push(ga); // Used in response
}

function add_dentry_error(hres, dentry, derr, details) {
  var emsg = {message       : derr.message,
              details       : details,
              delta_version : dentry.delta._meta.delta_version,
              am_primary    : ZCLS.AmPrimaryDataCenter()
              }
  ZH.l('add_dentry_error'); ZH.p(emsg);
  hres.errors.push(emsg); // Used in response
}

function handle_single_dentry(net, ks, auuid, dentry, rguuid, is_geo,
                              hres, next) {
  var dauuid  = dentry.delta._meta.author.agent_uuid;
  var is_sync = !is_geo && (dauuid !== auuid);
  ZH.l('handle_single_dentry: GEO: ' + is_geo + ' DU: ' + dauuid + 
       ' AU: ' + auuid + ' SYNC: ' + is_sync);
  if (is_geo) { // GEO are pre-AUTHed
    handle_cluster_delta(net, ks, dentry, rguuid, null, is_geo, true,
                         hres, next);
  } else if (is_sync) {
    var tkn     = ZH.GenerateDeltaSecurityToken(dentry);
    var xaction = dentry.delta._meta.xaction;
    if (tkn !== xaction) {
      ZH.l('SECURITY VIOLATION: tkn: ' + tkn + ' X: '+ xaction);
      next(new Error(ZS.Errors.SecurityViolation), hres);
    } else {
      handle_cluster_delta(net, ks, dentry, rguuid, null, is_geo, true,
                           hres, next);
    }
  } else {
    var auth   = dentry.authorization;  // AUTH: AuthorDelta
    var rchans = ZH.GetReplicationChannels(dentry.delta._meta);
    ZH.Central.HasClusterDeltaPermissions(net, auth, ks, auuid, rchans,
    function(aerr, ok) {
      if      (aerr) next(aerr, hres);
      else if (!ok)  next(new Error(ZS.Errors.WritePermsFail), hres);
      else {
        var username = auth.username;
        handle_cluster_delta(net, ks, dentry, rguuid, username, is_geo, true,
                             hres, next);
      }
    });
  }
}

function handle_cluster_dentries(net, ks, auuid, dentries, rguuid, is_geo,
                                 hres, next) {
  if (dentries.length === 0) next(null, hres);
  else {
    var dentry = dentries.shift();
    ZH.l('handle_cluster_dentries: #Ds: ' + dentries.length +
         ZH.SummarizeDelta(ks.key, dentry));
    var auth  = dentry.authorization;
    var dhres = {auth : auth, dentry : dentry};
    handle_single_dentry(net, ks, auuid, dentry, rguuid, is_geo, dhres,
    function(cerr, cres) {
      if (!cerr) {
        var adentry = dhres.dentry;
        add_dentry_ack(hres, adentry, auth, false);
        setImmediate(handle_cluster_dentries, net,
                     ks, auuid, dentries, rguuid, is_geo, hres, next);
      } else { // ERROR: [SecurityViolation]
        add_dentry_error(hres, dentry, cerr, dhres.error_details);
        next(null, hres); // -> STOP PROCESSING DENTRIES[]
      }
    });
  }
}

function handle_ok_dentries(net, ks, agent, dentries, rguuid,
                            username, is_geo, next) {
  var fdentry = dentries[(dentries.length - 1)];
  var rchans  = ZH.GetReplicationChannels(fdentry.delta._meta);
  get_subs(net.plugin, net.collections, ks, rchans,
  function (perr, subs) {
    if (perr) next(perr, hres);
    else {
      for (var i = 0; i < dentries.length; i++) {
        var dentry = dentries[i];
        generate_delta_xaction_id(net, ks, dentry, username);
      }
      send_cluster_subscriber_dentries(net, ks, agent, dentries, is_geo, subs,
      function(serr, sres) {
        if (serr) next(serr, null);
        else {
          ZDQ.PushStorageClusterDentries(net, ks, dentries, rguuid, next);
        }
      });
    }
  });
}

exports.HandleClusterDentries = function(net, ks, agent, dentries,
                                         freeze, rguuid, username, is_geo,
                                         hres, next) {
  var mname = is_geo ? "HandleClusterGeoDentries" : "HandleClusterDentries";
  ZH.e(mname + ': K: ' + ks.kqk + ' R: ' + rguuid +
       ' #Ds: ' + dentries.length + ' FREEZE: ' + freeze);
  hres.ks = ks; // Used in response
  if (!freeze && (!dentries || dentries.length === 0)) {
    return next(new Error(ZS.Errors.MalformedDentries), hres);
  }
  if (freeze) {
    ZDQ.PushStorageFreezeKey(net, ks, function(serr, sres) {
      next(serr, hres);
    });
  } else {
    hres.replication_channels = dentries[0].delta._meta.replication_channels;
    hres.acks                 = []; // Used in response
    hres.errors               = []; // Used in response
    var auuid                 = agent.uuid;
    var cdentries = ZH.clone(dentries); // NOTE: gets modified
    handle_cluster_dentries(net, ks, auuid, cdentries, rguuid, is_geo, hres,
    function(serr, sres) {
      if (serr) next(serr, hres);
      else {
        if (hres.errors.length !== 0) next(null, hres); // DENTRIES[] ERRORS
        else {
          handle_ok_dentries(net, ks, agent, dentries, rguuid, username, is_geo,
          function(perr, pres) {
            next(perr, hres);
          });
        }
      }
    });
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ROUTER BROADCAST AUTO DELTA -----------------------------------------------

function do_router_broadcast_auto_delta(net, ks, dentry, next) {
  var rchans = dentry.delta._meta.replication_channels;
  get_subs(net.plugin, net.collections, ks, rchans, function(rerr, subs) {
    if (rerr) next(rerr, null);
    else      central_broadcast_delta(net, ks, dentry, subs, false, null, next);
  });
}

exports.HandleRouterBroadcastAutoDelta = function(net, ks, dentry, next) {
  if (dentry.delta._meta.DO_DS || dentry.delta._meta.DO_GC) {
    ZH.l('router_broadcast_auto_delta: K: ' + ks.kqk);
    do_router_broadcast_auto_delta(net, ks, dentry, next);
  } else { // UN-COMMITTED AgentDelta
    router_update_key_channels(net, ks, dentry, null, function(aerr, ares) {
      if (aerr) next(aerr, null);
      else      do_router_broadcast_auto_delta(net, ks, dentry, next);
    });
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SUBSCRIBER-MAPS -----------------------------------------------------------

function update_subscriber_map_cached(net, ks, watch, agent, next) {
  var suuid = agent.uuid;
  ZH.l('update_subscriber_map_cached: K: ' + ks.kqk + ' U: ' + suuid);
  ZCache.StoreCacheKeyMetadata(net, ks, watch, suuid, {}, next);
}

function update_subscriber_map_subscribed(net, ks, rchans, username, next) {
  ZH.l('update_subscriber_map_subscribed: K: ' + ks.kqk + ' UN: ' + username);
  ZMDC.CasKeyRepChans(net.plugin, net.collections, ks, rchans,
  function(serr, sres) {
    if (serr) next(serr, null);
    else {
      var perms   = "W";
      var schanid = rchans[0];
      var auth    = {username : username};
      ZChannel.CentralHandleBroadcastSubscribe(net.plugin, net.collections,
                                               schanid, perms, auth, {}, next)
    }
  });
}

exports.HandleUpdateSubscriberMap = function(net, ks, rchans, username, agent,
                                             watch, is_cache, next) {
  ZH.l('ZPub.HandleUpdateSubscriberMap: K: ' + ks.kqk);
  if (is_cache) {
    update_subscriber_map_cached(net, ks, watch, agent, next);
  } else {
    update_subscriber_map_subscribed(net, ks, rchans, username, next);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRAPPERS ------------------------------------------------------------------

// NOTE: Used by ZNM.create_cluster_subscriber_DC_merge()
//               ZCSub.HandleClusterSubscriberDelta()
//               ZCSub.HandleClusterSubscriberCommitDelta()
exports.GetSubs = function(plugin, collections, ks, rchans, next) {
  get_subs(plugin, collections, ks, rchans, next);
}

// NOTE: Used by ZGDD.send_geo_delta_unacked_DC()
exports.CleanupGeoDeltaBeforeSend = function(pdentry) {
  cleanup_geo_delta_before_send(pdentry);
}

// NOTE: Used by ZXact.CreateAutoDentry()
exports.GenerateDeltaXactionID = function(net, ks, dentry, username) {
  return generate_delta_xaction_id(net, ks, dentry, username);
}

// NOTE: Used by ZDQ.handle_router_watch_delta()
exports.SendClusterSubscriberDelta = function(net, ks, dentry, subs, do_watch,
                                              next) {
  send_cluster_subscriber_deltas(net, ks, dentry, subs, do_watch, next);
}

// NOTE: Used by ZNM.FlowHandleRouterGeoNeedMergeResponse()
exports.RouterUpdateKeyChannels = function(net, ks, dentry, crdt, next) {
  router_update_key_channels(net, ks, dentry, crdt, next);
}

