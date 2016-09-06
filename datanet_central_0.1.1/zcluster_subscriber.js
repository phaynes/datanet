"use strict";

var ZPub     = require('./zpublisher');
var ZSumm    = require('./zsummary');
var ZPio     = require('./zpio');
var ZPart    = require('./zpartition');
var ZChannel = require('./zchannel');
var ZQ       = require('./zqueue');
var ZS       = require('./zshared');
var ZH       = require('./zhelper');

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEBUG ---------------------------------------------------------------------

function debug_skip(reason, suuid, key, dentry) {
  ZH.l('--: SKIP - ' + reason + ': ' + ZH.SummarizePublish(suuid, key, dentry));
}
function debug_send(reason, suuid, key, dentry) {
  ZH.l('++: SEND - ' + reason + ': ' + ZH.SummarizePublish(suuid, key, dentry));
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PUBSUB FLOW HANDLERS ------------------------------------------------------

//TODO ZQ.PubSubQueue[] serves no purpose -> DEPRECATE

function ps_flow_send_subscriber_merge(plugin, collections, qe, next) {
  var suuid   = qe.duuid;
  var data    = qe.data;
  var ks      = data.ks;
  var gcv     = data.gcv;
  var cavrsns = data.cavrsns;
  var crdt    = data.crdt;
  var gcsumms = data.gcsumms;
  var ether   = data.ether;
  var remove  = data.remove;
  var sub     = ZH.CreateSub(suuid);
  ZPio.SendSubscriberMerge(plugin, collections, sub,
                           ks, gcv, cavrsns, crdt, gcsumms, ether, remove);
  next (null, null);
}

function ps_flow_send_subscriber_commit(plugin, collections, qe, next) {
  var suuid  = qe.duuid;
  var data   = qe.data;
  var ks     = data.ks;
  var author = data.author;
  var sub    = ZH.CreateSub(suuid);
  ZPio.SendSubscriberCommitDelta(plugin, collections, sub, ks, author);
  next (null, null);
}

function ps_flow_send_subscriber_delta(plugin, collections, qe, next){
  var suuid         = qe.duuid;
  var data          = qe.data;
  var ks            = data.ks;
  ks.security_token = ZH.GenerateKeySecurityToken(ks);
  var dentry        = data.dentry;
  var sub           = ZH.CreateSub(suuid);
  var hres          = data.hres;
  debug_send('DELTA', suuid, ks.key, dentry);
  ZPio.SendSubscriberDelta(plugin, collections, sub, ks, dentry, hres);
  next (null, null);
}

function ps_flow_send_subscriber_dentries(plugin, collections, qe, next){
  var suuid         = qe.duuid;
  var data          = qe.data;
  var ks            = data.ks;
  ks.security_token = ZH.GenerateKeySecurityToken(ks);
  var dentries      = data.dentries;
  var sub           = ZH.CreateSub(suuid);
  var hres          = data.hres;
  ZPio.SendSubscriberDentries(plugin, collections, sub, ks, dentries, hres);
  next (null, null);
}

function ps_flow_send_subscriber(plugin, collections, qe, next) {
  var op = qe.op;
  var pfunc;
  if      (op === 'PUBLISH_DELTA')     pfunc = ps_flow_send_subscriber_delta;
  else if (op === 'PUBLISH_DENTRIES')  pfunc = ps_flow_send_subscriber_dentries;
  else if (op === 'SUBSCRIBER_MERGE')  pfunc = ps_flow_send_subscriber_merge;
  else if (op === 'SUBSCRIBER_COMMIT') pfunc = ps_flow_send_subscriber_commit;
  else {
    ZH.e('FLOW_SEND_SUBSCRIBER: OP: ' + op);
    throw(new Error('PROGRAM ERROR(FLOW_SEND_SUBSCRIBER)'));
  }
  pfunc(plugin, collections, qe, next);
}

exports.GetFlowSendSubscriberId = function(qe) {
  var op = qe.op;
  var id;
  if (qe.data && qe.data.ks && qe.data.ks.kqk) {
    id = qe.data.ks.kqk;
  } else {
    ZH.e('FLOW_SEND_SUBSCRIBER_ID: OP: ' + op);
    throw(new Error('PROGRAM ERROR(FLOW_SEND_SUBSCRIBER_ID)'));
  }
  return id;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL KEY READ QUEUE FLOW -----------------------------------------------

function ckr_flow_central_key_read(plugin, collections, qe, next) {
  var op   = qe.op;
  var pfunc;
  switch(op) {
    case 'SUBSCRIBER_DELTA'                :
      pfunc = ckr_flow_handle_cluster_subscriber_delta;
      break;
    case 'SUBSCRIBER_DENTRIES'             :
      pfunc = ckr_flow_handle_cluster_subscriber_dentries;
      break;
    case 'ROUTER_CLUSTER_SUBSCRIBER_MERGE' :
      pfunc = ckr_flow_handle_router_cluster_subscriber_merge;
      break;
    case 'CLUSTER_SUBSCRIBER_MERGE'        :
      pfunc = ckr_flow_handle_cluster_subscriber_merge;
      break;
    case 'GEO_SUBSCRIBER_COMMIT_DELTA'     :
      pfunc = ckr_flow_handle_geo_subscriber_commit_delta;
      break;
    case 'CLUSTER_SUBSCRIBER_COMMIT_DELTA' :
      pfunc = ckr_flow_handle_cluster_subscriber_commit_delta;
      break;
    default                                :
      throw(new Error('PROGRAM ERROR(flow_central_key_read)'));
  }
  pfunc(plugin, collections, qe.ks, qe.data, next);
}

exports.GetFlowCentralKeyReadId = function(qe) {
  var op = qe.op;
  var id;
  switch(op) {
    case 'SUBSCRIBER_DELTA'                :
    case 'SUBSCRIBER_DENTRIES'             :
    case 'ROUTER_CLUSTER_SUBSCRIBER_MERGE' :
    case 'CLUSTER_SUBSCRIBER_MERGE'        :
    case 'GEO_SUBSCRIBER_COMMIT_DELTA'     :
    case 'CLUSTER_SUBSCRIBER_COMMIT_DELTA' :
      id = qe.ks.kqk;
      break;
    default                                :
      throw(new Error('PROGRAM ERROR(flow_central_key_read)'));
  }
  return id;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL USER READ QUEUE FLOW ----------------------------------------------

function flow_central_user_read(plugin, collections, qe, next) {
  var op   = qe.op;
  var pfunc;
  switch(op) {
    case 'CLUSTER_USER_MESSAGE' :
      pfunc = flow_handle_cluster_user_message;
      break;
    default                     :
      throw(new Error('PROGRAM ERROR(flow_central_user_read)'));
  }
  pfunc(plugin, collections, qe.username, qe.data, next);
}

exports.GetFlowCentralUserReadId = function(qe) {
  var op = qe.op;
  var id;
  switch(op) {
    case 'CLUSTER_USER_MESSAGE' :
      id = qe.data.username;
      break;
    default                     :
      throw(new Error('PROGRAM ERROR(flow_central_user_read)'));
  }
  return id;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HANDLE CLUSTER_SUBSCRIBER_DELTA -------------------------------------------

function do_publish_delta(plugin, collections, suuid, ks, dentry, hres) {
  var offline = ZH.IsSubscriberOffline(suuid);
  if (offline) debug_skip('OFFLINE', suuid, ks.key, dentry);
  else {
    ZH.l('do_publish_delta: U: ' + suuid + ' K: ' + ks.kqk);
    var data = {ks : ks, dentry : dentry, hres : hres};
    ZQ.AddToSubscriberFlow(ZQ.PubSubQueue, suuid, 'PUBLISH_DELTA', data);
    var flow = {k : suuid,     q : ZQ.PubSubQueue,
                m : ZQ.PubSub, f : ps_flow_send_subscriber};
    ZQ.StartFlow(plugin, collections, flow);
  }
}

function publish_delta_to_sub(plugin, collections, sub,
                              ks, dentry, do_watch, hres) {
  var suuid    = sub.UUID;
  var cnode    = ZPart.GetClusterNode(suuid);
  if (!cnode || cnode.device_uuid !== ZH.MyUUID) return;
  var meta     = dentry.delta._meta;
  var selfie   = (suuid === meta.author.agent_uuid);
  var no_watch = do_watch ? !sub.watch : sub.watch;
  ZH.l('watch: NO: ' + no_watch + ' DO: ' + do_watch + ' S.W: ' + sub.watch);
  if (selfie) { // Delta Author  -> NO-OP
    debug_skip('AUTHOR', suuid, ks.key, dentry);
  } else if (no_watch) { // WATCH keys Deltas from StorageApplyDelta
    debug_skip('NO_WATCH', suuid, ks.key, dentry);
  } else {
    do_publish_delta(plugin, collections, suuid, ks, dentry, hres);
  }
}

function do_cluster_subscriber_delta(plugin, collections, ks, data, next) {
  var dentry   = data.dentry;
  var do_watch = data.do_watch;
  var hres     = data.hres;
  var rchans   = dentry.delta._meta.replication_channels;
  ZPub.GetSubs(plugin, collections, ks, rchans, function(rerr, subs) {
    if (rerr) next(rerr, null);
    else {
      if (!subs) next(null, null);
      else {
        var suuids = subs.uuids;
        if (suuids.length === 0) next(null, null);
        else {
          ZSumm.AddToRecentlyModifiedKeyCache(ks, dentry, suuids);
          ZH.l('pub_delta_to_subs: #s: ' + suuids.length);
          for (var i = 0; i < suuids.length; i++) {
            publish_delta_to_sub(plugin, collections,
                                 suuids[i], ks, dentry, do_watch, hres);
          }
          next(null, null);
        }
      }
    }
  });
}

function ckr_flow_handle_cluster_subscriber_delta(plugin, collections,
                                                  ks, data, next) {
  do_cluster_subscriber_delta(plugin, collections, ks, data,
  function(aerr, ares) {
    if (aerr) ZH.e(aerr); // NOTE: Just LOG, no THROWs in FLOWs
    next(null, null);
  });
}

exports.HandleClusterSubscriberDelta = function(net, ks, dentry,
                                                do_watch, hres) {
  ZH.l('ZCSub.HandleClusterSubscriberDelta: K: ' + ks.kqk);
  var data   = {dentry   : dentry,
                do_watch : do_watch,
                hres     : hres};
  ZQ.AddToCentralKeyReadFlow(ZQ.CentralKeyReadQueue, ks,
                             'SUBSCRIBER_DELTA', net, data);
  var flow  = {k : ks.kqk,            q : ZQ.CentralKeyReadQueue,
               m : ZQ.CentralKeyRead, f : ckr_flow_central_key_read};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HANDLE CLUSTER_SUBSCRIBER_DENTRIES ----------------------------------------

function do_publish_dentries(plugin, collections, suuid, ks, dentries, hres) {
  var offline = ZH.IsSubscriberOffline(suuid);
  if (!offline) {
    ZH.l('do_publish_dentries: U: ' + suuid + ' K: ' + ks.kqk);
    var data = {ks : ks, dentries : dentries, hres : hres};
    ZQ.AddToSubscriberFlow(ZQ.PubSubQueue, suuid, 'PUBLISH_DENTRIES', data);
    var flow = {k : suuid,     q : ZQ.PubSubQueue,
                m : ZQ.PubSub, f : ps_flow_send_subscriber};
    ZQ.StartFlow(plugin, collections, flow);
  }
}

function publish_dentries_to_sub(plugin, collections, sub, ks, dentries, hres) {
  var suuid    = sub.UUID;
  var cnode    = ZPart.GetClusterNode(suuid);
  if (!cnode || cnode.device_uuid !== ZH.MyUUID) return;
  do_publish_dentries(plugin, collections, suuid, ks, dentries, hres);
}

function do_cluster_subscriber_dentries(plugin, collections, ks, data, next) {
  var agent    = data.agent;
  var dentries = data.dentries;
  var hres     = data.hres;
  var auuid    = agent.uuid;
  var fdentry  = dentries[(dentries.length - 1)];
  var rchans   = ZH.GetReplicationChannels(fdentry.delta._meta);
  ZPub.GetSubs(plugin, collections, ks, rchans, function(rerr, subs) {
    if (rerr) next(rerr, null);
    else {
      if (!subs) next(null, null);
      else {
        var suuids = subs.uuids;
        if (suuids.length === 0) next(null, null);
        else {
          for (var i = 0; i < dentries.length; i++) {
            var dentry = dentries[i];
            ZSumm.AddToRecentlyModifiedKeyCache(ks, dentry, suuids);
          }
          ZH.l('pub_dentries_to_subs: #s: ' + suuids.length);
          for (var i = 0; i < suuids.length; i++) {
            var suuid = suuids[i];
            if (suuid.UUID === auuid) {
              ZH.l('subscriber_dentries: K: ' + ks.kqk + ' SKIP AUTHOR');
              continue;
            }
            publish_dentries_to_sub(plugin, collections,
                                    suuid, ks, dentries, hres);
          }
          next(null, null);
        }
      }
    }
  });
}

function ckr_flow_handle_cluster_subscriber_dentries(plugin, collections,
                                                     ks, data, next) {
  do_cluster_subscriber_dentries(plugin, collections, ks, data,
  function(aerr, ares) {
    if (aerr) ZH.e(aerr); // NOTE: Just LOG, no THROWs in FLOWs
    next(null, null);
  });
}

exports.HandleClusterSubscriberDentries = function(net, ks, agent,
                                                   dentries, hres) {
  ZH.l('ZCSub.HandleClusterSubscriberDentries: K: ' + ks.kqk);
  var data   = {agent    : agent,
                dentries : dentries,
                hres     : hres};
  ZQ.AddToCentralKeyReadFlow(ZQ.CentralKeyReadQueue, ks,
                             'SUBSCRIBER_DENTRIES', net, data);
  var flow  = {k : ks.kqk,            q : ZQ.CentralKeyReadQueue,
               m : ZQ.CentralKeyRead, f : ckr_flow_central_key_read};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HANDLE CLUSTER_SUBSCRIBER_COMMIT_DELTA ------------------------------------

function do_commit_to_sub(plugin, collections, suuid, ks, author) {
  var offline = ZH.IsSubscriberOffline(suuid);
  if (offline) ZH.l('--: SKIP - OFFLINE: U: ' + suuid + ' K: ' + ks.kqk);
  else {
    var data = {ks: ks, author : author};
    ZQ.AddToSubscriberFlow(ZQ.PubSubQueue, suuid, 'SUBSCRIBER_COMMIT', data);
    var flow = {k : suuid,     q : ZQ.PubSubQueue,
                m : ZQ.PubSub, f : ps_flow_send_subscriber};
    ZQ.StartFlow(plugin, collections, flow);
  }
}

function commit_to_sub(plugin, collections, sub, ks, author) {
  var suuid = sub.UUID;
  var cnode = ZPart.GetClusterNode(suuid);
  if (!cnode || cnode.device_uuid !== ZH.MyUUID) return;
  do_commit_to_sub(plugin, collections, suuid, ks, author);
}

function do_cluster_subscriber_commit_delta(plugin, collections,
                                            ks, data, next) {
  var author = data.author;
  var rchans = data.rchans;
  ZPub.GetSubs(plugin, collections, ks, rchans, function(rerr, subs) {
    if (rerr) next(rerr, null);
    else {
      if (!subs) next(null, null);
      else {
        var suuids = subs.uuids;
        if (suuids.length === 0) next(null, null);
        else {
          ZH.l('commit_to_subs: #s: ' + suuids.length);
          for (var i = 0; i < suuids.length; i++) {
            commit_to_sub(plugin, collections, suuids[i], ks, author);
          }
          next(null, null);
        }
      }
    }
  });
}

function ckr_flow_handle_cluster_subscriber_commit_delta(plugin, collections,
                                                         ks, data, next) {
  do_cluster_subscriber_commit_delta(plugin, collections, ks, data,
  function(aerr, ares) {
    if (aerr) ZH.e(aerr); // NOTE: Just LOG, no THROWs in FLOWs
    next(null, null);
  });
}

exports.HandleClusterSubscriberCommitDelta = function(net, ks, author, rchans) {
  ZH.l('ZCSub.HandleClusterSubscriberCommitDelta: K: ' + ks.kqk);
  var data   = {author : author, rchans : rchans};
  ZQ.AddToCentralKeyReadFlow(ZQ.CentralKeyReadQueue, ks,
                             'CLUSTER_SUBSCRIBER_COMMIT_DELTA', net, data);
  var flow  = {k : ks.kqk,            q : ZQ.CentralKeyReadQueue,
               m : ZQ.CentralKeyRead, f : ckr_flow_central_key_read};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}



// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HANDLE GEO-SUBSCRIBER-COMMIT-DELTA ----------------------------------------

function do_geo_subscriber_commit_delta(plugin, collections, ks, data, next) {
  var author = data.author;
  var rchans = data.rchans;
  ZPub.GetSubs(plugin, collections, ks, rchans, function(rerr, subs) {
    if (rerr) next(rerr, null);
    else {
      var suuids = subs.uuids;
      var cnodes = {};
      for (var i = 0; i < suuids.length; i++) {
        var cnode = ZPart.GetClusterNode(suuids[i].UUID);
        if (cnode) {
          cnodes[cnode.device_uuid] = cnode;
        }
      }
      for (var cuuid in cnodes) {
        var cnode = cnodes[cuuid];
        //NOTE: ZPio.SendClusterSubscriberCommitDelta() is ASYNC
        ZPio.SendClusterSubscriberCommitDelta(plugin, collections, cnode,
                                              ks, author, rchans);
      }
      next(null, null);
    }
  });
}

function ckr_flow_handle_geo_subscriber_commit_delta(plugin, collections,
                                                     ks, data, next) {
  do_geo_subscriber_commit_delta(plugin, collections, ks, data,
  function(aerr, ares) {
    if (aerr) ZH.e(aerr); // NOTE: Just LOG, no THROWs in FLOWs
    next(null, null);
  });
}

exports.HandleGeoSubscriberCommitDelta = function(net, ks, author, rchans) {
  ZH.l('ZCSub.HandleGeoSubscriberCommitDelta');
  var data   = {author : author, rchans : rchans};
  ZQ.AddToCentralKeyReadFlow(ZQ.CentralKeyReadQueue, ks,
                             'GEO_SUBSCRIBER_COMMIT_DELTA', net, data);
  var flow  = {k : ks.kqk,            q : ZQ.CentralKeyReadQueue,
               m : ZQ.CentralKeyRead, f : ckr_flow_central_key_read};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HANDLE ROUTER_CLUSTER_SUBSCRIBER_MERGE ------------------------------------

function do_router_cluster_subscriber_merge(plugin, collections,
                                            ks, data, next) {
  var crdt    = data.crdt;
  var cavrsns = data.cavrsns;
  var gcsumms = data.gcsumms;
  var ether   = data.ether;
  var remove  = data.remove;
  var rchans  = crdt._meta.replication_channels;
  ZPub.GetSubs(plugin, collections, ks, rchans, function(rerr, subs) {
    if (rerr) next(rerr, null);
    else {
      if (!subs) next(null, null);
      else {
        var suuids = subs.uuids;
        if (suuids.length === 0) next(null, null);
        else {
          var cnodes = {};
          for (var i = 0; i < suuids.length; i++) {
            var cnode = ZPart.GetClusterNode(suuids[i].UUID);
            cnodes[cnode.device_uuid] = cnode;
          }
          delete(cavrsns._id);
          for (var cuuid in cnodes) {
            var cnode = cnodes[cuuid];
            var mdata = {ks                     : ks,
                         device                 : {uuid : ZH.MyUUID},
                         crdt                   : crdt,
                         central_agent_versions : cavrsns,
                         gc_summary             : gcsumms,
                         ether                  : ether,
                         remove                 : remove};
            ZPio.SendClusterSubscriberMerge(plugin, collections, cnode, mdata);
          }
          next(null, null);
        }
      }
    }
  });
}

function ckr_flow_handle_router_cluster_subscriber_merge(plugin, collections,
                                                         ks, data, next) {
  do_router_cluster_subscriber_merge(plugin, collections, ks, data,
  function(aerr, ares) {
    if (aerr) ZH.e(aerr); // NOTE: Just LOG, no THROWs in FLOWs
    next(null, null);
  });
}

exports.HandleRouterClusterSubscriberMerge = function(net, ks, crdt, cavrsns,
                                                      gcsumms, ether, remove) {
  ZH.l('ZCSub.HandleRouterClusterSubscriberMerge');
  var data   = {crdt : crdt, cavrsns : cavrsns, gcsumms : gcsumms,
                ether : ether, remove : remove};
  ZQ.AddToCentralKeyReadFlow(ZQ.CentralKeyReadQueue, ks,
                             'ROUTER_CLUSTER_SUBSCRIBER_MERGE', net, data);
  var flow  = {k : ks.kqk,            q : ZQ.CentralKeyReadQueue,
               m : ZQ.CentralKeyRead, f : ckr_flow_central_key_read};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HANDLE CLUSTER_SUBSCRIBER_MERGE -------------------------------------------

function do_publish_subscriber_merge(plugin, collections, suuid,
                                     ks, crdt, cavrsns, gcsumms, ether) {
  var offline = ZH.IsSubscriberOffline(suuid);
  if (offline) ZH.l('--: SKIP - OFFLINE: U: ' + suuid + ' K: ' + ks.kqk);
  else {
    var data   = {ks : ks, crdt : crdt, cavrsns : cavrsns,
                  gcsumms : gcsumms, ether : ether};
    ZQ.AddToSubscriberFlow(ZQ.PubSubQueue, suuid, 'SUBSCRIBER_MERGE', data);
    var flow = {k : suuid,     q : ZQ.PubSubQueue,
                m : ZQ.PubSub, f : ps_flow_send_subscriber};
    ZQ.StartFlow(plugin, collections, flow);
  }
}

function publish_subscriber_merge(plugin, collections, sub,
                                  ks, crdt, cavrsns, gcsumms, ether) {
  var suuid = sub.UUID;
  var cnode = ZPart.GetClusterNode(suuid);
  if (!cnode || cnode.device_uuid !== ZH.MyUUID) return;
  do_publish_subscriber_merge(plugin, collections, suuid,
                              ks, crdt, cavrsns, gcsumms, ether);
}

function ckr_flow_handle_cluster_subscriber_merge(plugin, collections,
                                                  ks, data, next) {
  var crdt    = data.crdt;
  var cavrsns = data.cavrsns;
  var gcsumms = data.gcsumms;
  var ether   = data.ether;
  var rchans  = crdt._meta.replication_channels;
  ZPub.GetSubs(plugin, collections, ks, rchans, function(rerr, subs) {
    if (rerr) next(rerr, null);
    else {
      if (!subs) next(null, null);
      else {
        var suuids = subs.uuids;
        if (suuids.length === 0) next(null, null);
        else {
          ZH.l('SEND SubscriberMerge: #s: ' + suuids.length);
          for (var i = 0; i < suuids.length; i++) {
            publish_subscriber_merge(plugin, collections, suuids[i],
                                     ks, crdt, cavrsns, gcsumms, ether);
          }
          next(null, null);
        }
      }
    }
  });
}

exports.HandleClusterSubscriberMerge = function(net, ks, crdt, cavrsns, gcsumms,
                                                ether, remove) {
  ZH.l('ZCSub.HandleClusterSubscriberMerge');
  var data   = {crdt : crdt, cavrsns : cavrsns, gcsumms : gcsumms,
                ether : ether, remove : remove};
  ZQ.AddToCentralKeyReadFlow(ZQ.CentralKeyReadQueue, ks,
                             'CLUSTER_SUBSCRIBER_MERGE', net, data);
  var flow  = {k : ks.kqk,            q : ZQ.CentralKeyReadQueue,
               m : ZQ.CentralKeyRead, f : ckr_flow_central_key_read};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HANDLE CLUSTER_SUBSCRIBER_MESSAGE -----------------------------------------

exports.HandleClusterSubscriberMessage = function(net, suuid, msg) {
  ZH.l('ZCSub.HandleClusterSubscriberMessage: U: ' + suuid);
  var offline = ZH.IsSubscriberOffline(suuid);
  if (offline) ZH.l('--: SKIP - OFFLINE: U: ' + suuid);
  else {
    var sub = ZH.CreateSub(suuid);
    ZPio.SendSubscriberMessage(net.plugin, net.collections, sub, msg);
  }
}

function flow_handle_cluster_user_message(plugin, collections,
                                          username, data, next) {
  var msg  = data.message;
  var auth = {username : username};
  ZChannel.GetUserAgents(plugin, collections, auth, function(uerr, suuids) {
    if (uerr) next(uerr, null);
    else {
      if (suuids.length === 0) next(null, null);
      else {
        for (var i = 0; i < suuids.length; i++) {
          var suuid   = suuids[i];
          var offline = ZH.IsSubscriberOffline(suuid);
          if (offline) ZH.l('--: SKIP - OFFLINE: U: ' + suuid);
          else {
            var sub = ZH.CreateSub(suuid);
            ZPio.SendSubscriberMessage(plugin, collections, sub, msg);
          }
        }
        next(null, null);
      }
    }
  });
}

exports.HandleClusterUserMessage = function(net, username, msg, next) {
  ZH.l('ZCSub.HandleClusterUserMessage: UN: ' + username);
  var data  = {username : username, message : msg};
  ZQ.AddToCentralUserReadFlow(ZQ.CentralUserReadQueue, username,
                             'CLUSTER_USER_MESSAGE', net, data);
  var flow  = {k : username,           q : ZQ.CentralUserReadQueue,
               m : ZQ.CentralUserRead, f : flow_central_user_read};
  ZQ.StartFlow(net.plugin, net.collections, flow);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HANDLE AGENT DELTA/DENTRIES ERROR -----------------------------------------

exports.HandleClusterSubscriberAgentDeltaError = function(net, ks,
                                                          agent, author,
                                                          rchans, error) {
  var suuid   = agent.uuid;
  var offline = ZH.IsSubscriberOffline(suuid);
  if (!offline) {
    var sub = ZH.CreateSub(suuid);
    ZPio.SendSubscriberAgentDeltaError(net.plugin, net.collections,
                                       sub, ks, author, rchans, error);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HANDLE BROADCAST-ANNOUNCE-NEW-MEMCACHE-CLUSTER ----------------------------

function do_announce_new_memcache_cluster(net, mstate, asc_state, next) {
  var asc_cnodes = asc_state.cluster_nodes;
  for (var i = 0; i < asc_cnodes.length; i++) {
    var asc_cnode = asc_cnodes[i];
    var asc_duuid = asc_cnode.device_uuid;
    var offline   = ZH.IsSubscriberOffline(asc_duuid);
    if (!offline) {
      var sub = ZH.CreateSub(asc_duuid);
      // NOTE: ZPio.SendSubscriberAnnounceNewMemcacheCluster() is ASYNC
      ZPio.SendSubscriberAnnounceNewMemcacheCluster(net.plugin, net.collections,
                                                    sub, mstate);
    }
  }
  next(null, null);
}

exports.HandleBroadcastAnnounceNewMemcacheCluster = function(net, mstate,
                                                             next) {
  ZH.l('ZCSub.HandleBroadcastAnnounceNewMemcacheCluster'); ZH.p(mstate);
  var ascname = mstate.app_server_cluster_name;
  var skey    = ZS.GetAppServerClusterState(ascname);
  net.plugin.do_get(net.collections.global_coll, skey, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      if (gres.length === 0) next(null, null);
      else {
        var asc_state = gres[0];
        do_announce_new_memcache_cluster(net, mstate, asc_state, next);
      }
    }
  });
}

