"use strict";

var https  = require('https');

var ZNM    = require('./zneedmerge');
var ZDelt  = require('./zdeltas');
var ZCLS   = require('./zcluster');
var ZPart  = require('./zpartition');
var ZCMP   = require('./zdcompress');
var ZQ     = require('./zqueue');
var ZS     = require('./zshared');
var ZH     = require('./zhelper');

// NOTE: this is an EVENT-FREE file
//       Algorithms rely on ALL functions in this file being SYNCHRONOUS

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

function wss_send_to_subscriber(wsconn, jrpc_body, next) {
  var cbody = ZH.NetworkSerialize(jrpc_body);
  wsconn.send(cbody, next);
}

function sock_send_to_subscriber(sock, jrpc_body, next) {
  var cbody = ZH.NetworkSerialize(jrpc_body);
  var cbuf  = ZH.ConvertBodyToBinaryNetwork(cbody);
  sock.write(cbuf, next);
}

function https_send_to_subscriber(hso, jrpc_body, next) {
  var options   = hso;
  var cbu       = options.hostname + ':' + options.port
  ZH.l('https_send_to_subscriber: SERVER: ' + cbu);
  var cbody     = ZH.NetworkSerialize(jrpc_body);
  options.agent = new https.Agent(options);
  var req = https.request(options, function(res) {
    res.on('data', function(d) {
      ZH.l("https_send_to_subscriber: RESPONSE-DATA: " + d);
    });
  });
  req.on('error', function(e) {
    ZH.e('ERROR: https_send_to_subscriber: ' + e.message);
  });
  req.write(cbody);
  req.end();
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL SEND DELTA TO SUBSCRIBER ------------------------------------------

function send_subscriber_data(plugin, collections, method, sub, data, hres) {
  var id       = ZDelt.GetNextSubscriberRpcID(plugin, collections, sub);
  if (hres) ZH.l('CALL_TRACE: FROM: ' + hres.id + ' TO: ' + id);
  var suuid    = sub.UUID;
  var isolated = ZH.CentralIsolatedAgents[suuid];
  var wsconn   = ZH.CentralWsconnMap     [suuid];
  var sock     = ZH.CentralSocketMap     [suuid];
  var hso      = ZH.CentralHttpsMap      [suuid];
  if (isolated) {
    ZH.l(method + ': U: ' + suuid + ' is ISOLATED');
  } else {
    if (hso) { // HTTPS -> ADD CALLBACK-KEY
      data.callback_key = ZH.CentralHttpsKeyMap[suuid];
    }
    var jrpc_body = ZH.CreateJsonRpcBody(method, ZH.NobodyAuth, data, id);
    if (hso) {           // HTTPS
      https_send_to_subscriber(hso, jrpc_body, function(err) {
        if (err) ZH.l(method + ': U: ' + suuid + ' is OFFLINE');
      });
    } else if (wsconn) { // WSS
      wss_send_to_subscriber(wsconn, jrpc_body, function(err) {
        if (err) ZH.l(method + ': U: ' + suuid + ' is OFFLINE');
      });
    } else if (sock) {   // TCP
      sock_send_to_subscriber(sock, jrpc_body, function(err) {
        if (err) ZH.l(method + ': U: ' + suuid + ' is OFFLINE');
      });
    } else {
      ZH.l(method + ': U: ' + suuid + ' is OFFLINE');
    }
  }
}

exports.SendSubscriberAgentDeltaError = function(plugin, collections,
                                                 sub, ks, author,
                                                 rchans, error) {
  var method = 'AgentDeltaError'
  ZH.l('|->(S): AgentDeltaError: K: ' + ks.kqk);
  var data   = {device               : {uuid : ZH.MyUUID},
                datacenter           : ZH.MyDataCenter,
                ks                   : ks,
                author               : author,
                replication_channels : rchans,
                error                : error};
  send_subscriber_data(plugin, collections, method, sub, data, {});
}

exports.SendSubscriberCommitDelta = function(plugin, collections, sub,
                                             ks, author) {
  var method = 'SubscriberCommitDelta';
  ZH.l('|->(S): SubscriberCommitDelta: K: ' + ks.kqk);
  var data   = {device     : {uuid : ZH.MyUUID},
                datacenter : ZH.MyDataCenter,
                ks         : ks,
                author     : author};
  send_subscriber_data(plugin, collections, method, sub, data, {});
}

exports.SendSubscriberDelta = function(plugin, collections, sub,
                                       ks, dentry, hres) {
  var method = 'SubscriberDelta';
  ZH.l('|->(S): SubscriberDelta: ' +
       ZH.SummarizePublish(sub.UUID, ks.key, dentry));
  delete(dentry.delta._meta.subscriber_received);
  if (ZH.CentralExtendedTimestampsInDeltas) {
    dentry.delta._meta.subscriber_sent = ZH.GetMsTime();
  }
  var data   = {device     : {uuid : ZH.MyUUID},
                datacenter : ZH.MyDataCenter,
                ks         : ks,
                dentry     : dentry};
  if (ZH.ChaosMode === 33) {
    ZH.e('CHAOS-MODE: ' + ZH.ChaosDescriptions[ZH.ChaosMode]);
    setTimeout(function() {
      send_subscriber_data(plugin, collections, method, sub, data, hres);
    }, 10000);
  } else {
    send_subscriber_data(plugin, collections, method, sub, data, hres);
  }
}

exports.SendSubscriberDentries = function(plugin, collections, sub,
                                          ks, dentries, hres) {
  for (var i = 0; i < dentries.length; i++) {
    var dentry = dentries[i];
    delete(dentry.delta._meta.subscriber_received);
    if (ZH.CentralExtendedTimestampsInDeltas) {
      dentry.delta._meta.subscriber_sent = ZH.GetMsTime();
    }
  }
  var method = 'SubscriberDentries';
  ZH.l('|->(S): SubscriberDentries: ' + ks.kqk);
  var data   = {device     : {uuid : ZH.MyUUID},
                datacenter : ZH.MyDataCenter,
                ks         : ks,
                dentries   : dentries};
  send_subscriber_data(plugin, collections, method, sub, data, hres);
}

exports.SendSubscriberMergeData = function(plugin, collections, sub, data) {
  var method = 'SubscriberMerge';
  ZH.l('|->(S): SubscriberMerge: K: ' + data.ks.kqk);
  if (!data.zcrdt) {
    data.zcrdt = ZCMP.CompressCrdt(data.crdt);
  }
  delete(data.crdt);
  send_subscriber_data(plugin, collections, method, sub, data, {});
}

exports.SendSubscriberMerge = function(plugin, collections, sub,
                                       ks, gcv, cavrsns, crdt,
                                       gcsumms, ether, remove) {
  var method = 'SubscriberMerge';
  ZH.l('|->(S): SubscriberMerge: K: ' + ks.kqk);
  var data   = {device                 : {uuid : ZH.MyUUID},
                datacenter             : ZH.MyDataCenter,
                ks                     : ks,
                gc_version             : gcv,
                central_agent_versions : cavrsns,
                zcrdt                  : ZCMP.CompressCrdt(crdt),
                gc_summary             : gcsumms,
                ether                  : ether,
                remove                 : remove};
  send_subscriber_data(plugin, collections, method, sub, data, {});
}

exports.SendSubscriberMessage = function(plugin, collections, sub, msg) {
  var method = 'SubscriberMessage';
  ZH.l('|->(S): SubscriberMessage: U: ' + sub.UUID);
  var data   = {device     : {uuid : ZH.MyUUID},
                datacenter : ZH.MyDataCenter,
                message    : msg};
  send_subscriber_data(plugin, collections, method, sub, data, {});
}

exports.SendSubscriberReconnect = function(plugin, collections, sub) {
  var method = 'SubscriberReconnect';
  ZH.l('|->(S): SubscriberReconnect: U: ' + sub.UUID);
  var data   = {device     : {uuid : ZH.MyUUID},
                datacenter : ZH.MyDataCenter};
  send_subscriber_data(plugin, collections, method, sub, data, {});
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL SEND GEO STATE CHANGE ---------------------------------------------

exports.SendSubscriberGeoStateChange = function(plugin, collections,
                                                sub, gnodes) {
  var method = 'SubscriberGeoStateChange';
  ZH.l('|->(S): SubscriberGeoStateChange');
  var data   = {device     : {uuid : ZH.MyUUID},
                datacenter : ZH.MyDataCenter,
                geo_nodes  : gnodes};
  send_subscriber_data(plugin, collections, method, sub, data, {});
}


exports.SendSubscriberAnnounceNewMemcacheCluster = function(plugin, collections,
                                                            sub, mstate) {
  var method = 'SubscriberAnnounceNewMemcacheCluster';
  ZH.l('|->(S): SubscriberAnnounceNewMemcacheCluster');
  var data   = {device        : {uuid : ZH.MyUUID},
                datacenter    : ZH.MyDataCenter,
                cluster_state : mstate};
  send_subscriber_data(plugin, collections, method, sub, data, {});
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL SEND PROPOGATE-SUBSCRIBE ------------------------------------------

function send_subscriber_propogate_channel(plugin, collections, method,
                                           suuid, schanid, username,
                                           perm, pkss) {
  ZH.l('|->(S): ' + method + ': U: ' + suuid + ' R: ' + schanid);
  var sub       = ZH.CreateSub(suuid);
  var data      = {device   : {uuid : ZH.MyUUID},
                   channel  : {id          : schanid,
                               permissions : perm},
                   pkss     : pkss,
                   username : username};
  send_subscriber_data(plugin, collections, method, sub, data, {});
}

exports.SendSubscriberPropogateSubscribe = function(plugin, collections,
                                                    suuid, schanid,
                                                    username, perm, pkss) {
  var method = 'PropogateSubscribe';
  send_subscriber_propogate_channel(plugin, collections, method, suuid, schanid,
                                    username, perm, pkss);
}

exports.SendSubscriberPropogateUnsubscribe = function(plugin, collections,
                                                      suuid, schanid,
                                                      username, perm) {
  var method = 'PropogateUnsubscribe';
  send_subscriber_propogate_channel(plugin, collections, method, suuid, schanid,
                                    username, perm, null);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL SEND PROPOGATE-CACHE ----------------------------------------------

exports.SendSubscriberPropogateRemoveUser = function(plugin, collections,
                                                     suuid, username) {
  ZH.l('|->(S): PropogateRemoveUser: U: ' + suuid + ' UN: ' + username);
  var method    = 'PropogateRemoveUser';
  var sub       = ZH.CreateSub(suuid);
  var data      = {device   : {uuid : ZH.MyUUID},
                   username : username};
  send_subscriber_data(plugin, collections, method, sub, data, {});
}

exports.SendSubscriberPropogateGrantUser = function(plugin, collections, suuid,
                                                    username, schanid, priv) {
  ZH.l('|->(S): PropogateGrantUser: U: ' + suuid + ' UN: ' + username +
       ' R: ' + schanid + ' P: ' + priv);
  var method    = 'PropogateGrantUser';
  var sub       = ZH.CreateSub(suuid);
  var data      = {device    : {uuid : ZH.MyUUID},
                   username  : username,
                   channel   : {id : schanid},
                   privilege : priv};
  send_subscriber_data(plugin, collections, method, sub, data, {});
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CENTRAL SEND BACKOF -------------------------------------------------------

exports.SendAgentBackoff = function(plugin, collections, sub, csec, hres) {
  ZH.l('|->(S): AgentBackoff: U: ' + sub.UUID + ' SEC: ' + csec);
  var method    = 'AgentBackoff';
  var data      = {device : {uuid : ZH.MyUUID}, seconds : csec};
  send_subscriber_data(plugin, collections, method, sub, data, {});
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SEND CLUSTER DELTA --------------------------------------------------------

function send_cluster(plugin, collections, method, cnode, data, auth,
                      hres, next) {
  if (!ZH.AmRouter) throw(new Error("ZPio.send_cluster() LOGIC ERROR"));
  if (!cnode) {
    ZH.e('ERROR: send_cluster NO CNODE');
    ZH.l('ZCLS.ClusterNodes'); ZH.p(ZCLS.ClusterNodes);
    next(new Error(ZS.Errors.TlsClientNotReady), hres);
  } else {
    ZH.l('|->(I): ' + method + ': CU: ' + cnode.device_uuid);
    var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
    if (hres) ZH.l('CALL_TRACE: FROM: ' + hres.id + ' TO: ' + id);
    var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
    ZCLS.ClusterMessage(cnode, jrpc_body, hres, next);
  }
}

exports.SendClusterSubscriberDelta = function(plugin, collections, cnode,
                                              ks, dentry, do_watch) {
  var method = 'ClusterSubscriberDelta';
  var data   = {ks       : ks,
                device   : {uuid : ZH.MyUUID},
                dentry   : dentry,
                do_watch : do_watch};
  send_cluster(plugin, collections, method, cnode, data,
               ZH.NobodyAuth, {}, ZH.OnErrLog);
}

exports.SendClusterSubscriberDentries = function(plugin, collections, cnode,
                                                 ks, agent, dentries) {
  var method = 'ClusterSubscriberDentries';
  var data   = {ks       : ks,
                device   : {uuid : ZH.MyUUID},
                agent    : agent,
                dentries : dentries};
  send_cluster(plugin, collections, method, cnode, data,
               ZH.NobodyAuth, {}, ZH.OnErrLog);
}

exports.SendClusterDelta = function(plugin, collections, ks, dentry, rguuid,
                                    auth, hres, next) {
  var method = 'ClusterDelta';
  var cnode  = ZPart.GetKeyNode(ks);
  var data   = {ks         : ks,
                device     : {uuid : ZH.MyUUID},
                datacenter : rguuid,
                dentry     : dentry};
  send_cluster(plugin, collections, method, cnode, data, auth, hres, next);
}

exports.SendClusterDentries = function(plugin, collections,
                                       ks, agent, dentries, rguuid,
                                       auth, hres, next) {
  var method = 'ClusterDentries';
  var cnode  = ZPart.GetKeyNode(ks);
  var data   = {ks         : ks,
                device     : {uuid : ZH.MyUUID},
                datacenter : rguuid,
                agent      : agent,
                dentries   : dentries,
                freeze     : false};
  send_cluster(plugin, collections, method, cnode, data, auth, hres, next);
}

exports.SendClusterGeoDentries = function(plugin, collections,
                                          ks, agent, dentries, freeze, rguuid,
                                          auth, hres, next) {
  var method = 'ClusterGeoDentries';
  var cnode  = ZPart.GetKeyNode(ks);
  var data   = {ks         : ks,
                device     : {uuid : ZH.MyUUID},
                datacenter : rguuid,
                agent      : agent,
                dentries   : dentries,
                freeze     : freeze};
  send_cluster(plugin, collections, method, cnode, data, auth, hres, next);
}

exports.SendClusterGeoDelta = function(plugin, collections,
                                       ks, dentry, rguuid, auth, hres, next) {
  var method = 'ClusterGeoDelta';
  var cnode  = ZPart.GetKeyNode(ks);
  var data   = {ks         : ks,
                device     : {uuid : ZH.MyUUID},
                datacenter : rguuid,
                dentry     : dentry};
  send_cluster(plugin, collections, method, cnode, data, auth, hres, next);
}

exports.SendClusterAckGeoDelta = function(plugin, collections,
                                          ks, author, rchans) {
  var method = 'ClusterAckGeoDelta';
  var cnode  = ZPart.GetKeyNode(ks);
  var data   = {ks                   : ks,
                device               : {uuid : ZH.MyUUID},
                author               : author,
                replication_channels : rchans};
  send_cluster(plugin, collections, method, cnode, data,
               ZH.NobodyAuth, {}, ZH.OnErrLog);
}

exports.SendClusterGeoSubscriberCommitDelta = function(net, ks,
                                                       author, rchans) {
  var method = 'ClusterGeoSubscriberCommitDelta';
  var cnode  = ZPart.GetKeyNode(ks);
  var data   = {ks                   : ks,
                device               : {uuid : ZH.MyUUID},
                author               : author,
                replication_channels : rchans};
  send_cluster(net.plugin, net.collections, method, cnode, data,
               ZH.NobodyAuth, {}, ZH.OnErrLog);
}




exports.SendClusterClientStore = function(plugin, collections,
                                          ks, json, auth, hres, next) {
  var method = 'ClusterClientStore';
  var cnode  = ZPart.GetKeyNode(ks);
  var data   = {ks     : ks,
                device : {uuid : ZH.MyUUID},
                json   : json};
  send_cluster(plugin, collections, method, cnode, data, auth, hres, next);
}

exports.SendClusterClientFetch = function(plugin, collections,
                                          ks, auth, hres, next) {
  var method = 'ClusterClientFetch';
  var cnode  = ZPart.GetKeyNode(ks);
  var data   = {ks     : ks,
                device : {uuid : ZH.MyUUID}};
  send_cluster(plugin, collections, method, cnode, data, auth, hres, next);
}

exports.SendClusterClientCommit = function(plugin, collections,
                                           ks, crdt, oplog, auth, hres, next) {
  var method = 'ClusterClientCommit';
  var cnode  = ZPart.GetKeyNode(ks);
  var data   = {ks     : ks,
                device : {uuid : ZH.MyUUID},
                crdt   : crdt,
                oplog  : oplog};
  send_cluster(plugin, collections, method, cnode, data, auth, hres, next);
}

exports.SendClusterClientRemove = function(plugin, collections,
                                           ks, auth, hres, next) {
  var method = 'ClusterClientRemove';
  var cnode  = ZPart.GetKeyNode(ks);
  var data   = {ks     : ks,
                device : {uuid : ZH.MyUUID}};
  send_cluster(plugin, collections, method, cnode, data, auth, hres, next);
}

exports.SendClusterClientStatelessCommit = function(plugin, collections,
                                                    ks, rchans, oplog,
                                                    auth, hres, next) {
  var method = 'ClusterClientStatelessCommit';
  var cnode  = ZPart.GetKeyNode(ks);
  var data   = {ks                   : ks,
                device               : {uuid : ZH.MyUUID},
                replication_channels : rchans,
                oplog                : oplog};
  send_cluster(plugin, collections, method, cnode, data, auth, hres, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SEND CLUSTER DELTA COMMIT -------------------------------------------------

exports.SendClusterSubscriberAgentDeltaError = function(plugin, collections,
                                                        ks, author, rchans,
                                                        error, agent) {
  ZH.l('ZPio.SendClusterSubscriberAgentDeltaError: K: ' + ks.kqk);
  var method = 'ClusterSubscriberAgentDeltaError';
  var suuid  = agent.uuid;
  var cnode  = ZPart.GetClusterNode(suuid);
  var data   = {ks                   : ks,
                device               : {uuid : ZH.MyUUID},
                agent                : agent,
                author               : author,
                replication_channels : rchans,
                error                : error};
  send_cluster(plugin, collections, method, cnode, data,
               ZH.NobodyAuth, {}, ZH.OnErrLog);
}

exports.SendClusterSubscriberCommitDelta = function(plugin, collections, cnode,
                                                    ks, author, rchans) {
  ZH.l('ZPio.SendClusterSubscriberCommitDelta: K: ' + ks.kqk);
  var method = 'ClusterSubscriberCommitDelta';
  var data   = {ks                   : ks,
                device               : {uuid : ZH.MyUUID},
                author               : author,
                replication_channels : rchans};
  send_cluster(plugin, collections, method, cnode, data,
               ZH.NobodyAuth, {}, ZH.OnErrLog);
}

exports.SendClusterGeoCommitDelta = function(plugin, collections,
                                             ks, author, rchans,
                                             rguuid, hres, next) {
  ZH.l('ZPio.SendClusterGeoCommitDelta: K: ' + ks.kqk);
  var method = 'ClusterGeoCommitDelta';
  var cnode  = ZPart.GetKeyNode(ks);
  var data   = {ks                   : ks,
                device               : {uuid : ZH.MyUUID},
                author               : author,
                replication_channels : rchans,
                datacenter           : rguuid};
  send_cluster(plugin, collections, method, cnode, data, ZH.NobodyAuth,
               hres, next);
}

exports.SendClusterAckGeoCommitDelta = function(plugin, collections,
                                                ks, author, rchans, rguuid,
                                                hres, next) {
  ZH.l('ZPio.SendClusterAckGeoCommitDelta: K: ' + ks.kqk);
  var method = 'ClusterAckGeoCommitDelta';
  var cnode  = ZPart.GetKeyNode(ks);
  var data   = {ks                   : ks,
                device               : {uuid : ZH.MyUUID},
                author               : author,
                replication_channels : rchans,
                datacenter           : rguuid};
  send_cluster(plugin, collections, method, cnode, data, ZH.NobodyAuth,
               hres, next);
}

exports.SendClusterSingleSubcriberCommitDelta = function(plugin, collections,
                                                         suuid, ks, author) {
  var method = 'ClusterSingleSubscriberCommitDelta';
  var cnode  = ZPart.GetClusterNode(suuid);
  var data   = {device     : {uuid : ZH.MyUUID},
                ks         : ks,
                subscriber : suuid,
                author     : author};
  send_cluster(plugin, collections, method, cnode, data,
               ZH.NobodyAuth, {}, ZH.OnErrLog);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SEND CLUSTER-SUBSCRIBER MERGE ---------------------------------------------

exports.SendClusterSubscriberMerge = function(plugin, collections,
                                              cnode, data) {
  var method = 'ClusterSubscriberMerge';
  send_cluster(plugin, collections, method, cnode, data,
               ZH.NobodyAuth, {}, ZH.OnErrLog);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SEND CLUSTER-SUBSCRIBER MESSAGE -------------------------------------------

exports.SendClusterSubscriberMessage = function(plugin, collections,
                                                suuid, msg) {
  var method = 'ClusterSubscriberMessage';
  var cnode  = ZPart.GetClusterNode(suuid);
  var data   = {device     : {uuid : ZH.MyUUID},
                subscriber : suuid,
                message    : msg};
  send_cluster(plugin, collections, method, cnode, data,
               ZH.NobodyAuth, {}, ZH.OnErrLog);
}

exports.SendClusterUserMessage = function(plugin, collections, cnode,
                                          username, msg) {
  var method = 'ClusterUserMessage';
  var data   = {device   : {uuid : ZH.MyUUID},
                username : username,
                message  : msg};
  send_cluster(plugin, collections, method, cnode, data,
               ZH.NobodyAuth, {}, ZH.OnErrLog);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SEND CLUSTER CACHE/EVICT --------------------------------------------------

exports.SendClusterCache = function(plugin, collections,
                                    ks, watch, agent, auth, hres, next) {
  var method = 'ClusterCache';
  var cnode  = ZPart.GetKeyNode(ks);
  var data   = {ks     : ks,
                device : {uuid : ZH.MyUUID},
                watch  : watch,
                agent  : agent};
  send_cluster(plugin, collections, method, cnode, data, auth, hres, next);
}

exports.SendClusterGeoCache = function(plugin, collections,
                                       ks, watch, agent, auth, hres, next) {
  var method = 'ClusterGeoCache';
  var cnode  = ZPart.GetKeyNode(ks);
  var data   = {ks     : ks,
                device : {uuid : ZH.MyUUID},
                watch  : watch,
                agent  : agent};
  send_cluster(plugin, collections, method, cnode, data, auth, hres, next);
}

exports.SendClusterEvict = function(plugin, collections, ks, agent, auth,
                                    hres, next) {
  var method = 'ClusterEvict';
  var cnode  = ZPart.GetKeyNode(ks);
  var data   = {ks     : ks,
                device : {uuid : ZH.MyUUID},
                agent  : agent};
  send_cluster(plugin, collections, method, cnode, data, auth, hres, next);
}

function send_cluster_geo_evict_method(plugin, collections, method,
                                       ks, agent, auth, hres, next) {
  var cnode  = ZPart.GetKeyNode(ks);
  var data   = {ks     : ks,
                device : {uuid : ZH.MyUUID},
                agent  : agent};
  send_cluster(plugin, collections, method, cnode, data, auth, hres, next);
}

exports.SendClusterGeoEvict = function(plugin, collections,
                                       ks, agent, auth, hres, next) {
  var method = 'ClusterGeoEvict';
  send_cluster_geo_evict_method(plugin, collections, method,
                                ks, agent, auth, hres, next);
}

exports.SendClusterGeoLocalEvict = function(plugin, collections,
                                            ks, agent, auth, hres, next) {
  var method = 'ClusterGeoLocalEvict';
  send_cluster_geo_evict_method(plugin, collections, method,
                                ks, agent, auth, hres, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// BROADCAST CLUSTER ---------------------------------------------------------

function broadcast(plugin, collections, method, data, auth, next) {
  var cnodes = ZCLS.GetClusterNodes();
  for (var i = 0; i < cnodes.length; i++) {
    var cnode     = cnodes[i];
    var id        = ZDelt.GetNextRpcID(plugin, collections, cnode);
    ZH.l('ClusterBroadcast: ' + method + ' CU: ' + cnode.device_uuid);
    var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
    ZCLS.ClusterMessage(cnode, jrpc_body, {}, next);
  }
}

exports.BroadcastHttpsAgent = function(plugin, collections,
                                       rguuid, server, duuid, dkey) {
  var method = 'BroadcastHttpsAgent';
  var data   = {device     : {uuid : ZH.MyUUID},
                datacenter : ZH.MyDataCenter,
                agent      : {uuid       : duuid,
                              datacenter : rguuid,
                              key        : dkey,
                              server     : server}};
  broadcast(plugin, collections, method, data, ZH.NobodyAuth, ZH.OnErrLog);
}

exports.BroadcastSubscribe = function(plugin, collections,
                                      schanid, perm, auth) {
  var method = 'BroadcastSubscribe';
  var data   = {device  : {uuid        : ZH.MyUUID},
                channel : {id          : schanid,
                           permissions : perm}};
  broadcast(plugin, collections, method, data, auth, ZH.OnErrLog);
}

exports.BroadcastUnsubscribe = function(plugin, collections, schanid, auth) {
  var method = 'BroadcastUnsubscribe';
  var data   = {device  : {uuid : ZH.MyUUID},
                channel : {id   : schanid}};
  broadcast(plugin, collections, method, data, auth, ZH.OnErrLog);
}

exports.BroadcastGrantUser = function(plugin, collections,
                                      do_unsub, schanid, priv, subs, auth) {
  var method = 'BroadcastGrantUser';
  var data   = {device         : {uuid        : ZH.MyUUID},
                channel        : {id          : schanid,
                                  privilege   : priv},
                subscribers    : subs,
                do_unsubscribe : do_unsub};
  broadcast(plugin, collections, method, data, auth, ZH.OnErrLog);
}

exports.BroadcastRemoveUser = function(plugin, collections, subs, auth) {
  var method = 'BroadcastRemoveUser';
  var data   = {device      : {uuid : ZH.MyUUID},
                subscribers : subs};
  broadcast(plugin, collections, method, data, auth, ZH.OnErrLog);
}

exports.BroadcastClusterStateChangeVote = function(plugin, collections,
                                                   term, vid, cnodes,
                                                   csynced, cnetpart, next) {
  var method = 'BroadcastClusterStateChangeVote';
  var data   = {device                    : {uuid : ZH.MyUUID},
                term_number               : term,
                vote_id                   : vid,
                cluster_nodes             : cnodes,
                cluster_synced            : csynced,
                cluster_network_partition : cnetpart};
  broadcast(plugin, collections, method, data, ZH.NobodyAuth, next);
}

exports.BroadcastClusterStateChangeCommit = function(plugin, collections,
                                                     term, vid, cnodes, csynced,
                                                     cnetpart, ptbl, next) {
  var method = 'BroadcastClusterStateChangeCommit';
  var data   = {device                    : {uuid : ZH.MyUUID},
                term_number               : term,
                vote_id                   : vid,
                cluster_nodes             : cnodes,
                cluster_synced            : csynced,
                cluster_network_partition : cnetpart,
                partition_table           : ptbl};
  broadcast(plugin, collections, method, data, ZH.NobodyAuth, next);
}

exports.BroadcastAnnounceNewGeoCluster = function(plugin, collections,
                                                  gterm, gnodes, gnetpart,
                                                  gmaj, csynced, send_dco) {
  var method = 'BroadcastAnnounceNewGeoCluster';
  var data   = {device                 : {uuid : ZH.MyUUID},
                geo_term_number        : gterm,
                geo_nodes              : gnodes,
                cluster_synced         : csynced,
                geo_network_partition  : gnetpart,
                geo_majority           : gmaj,
                send_datacenter_online : send_dco};
  broadcast(plugin, collections, method, data, ZH.NobodyAuth, ZH.OnErrLog);
}

exports.BroadcastClusterStatus = function(plugin, collections, csynced, next) {
  var method = 'BroadcastClusterStatus';
  var data   = {device         : {uuid : ZH.MyUUID},
                cluster_synced : csynced};
  broadcast(plugin, collections, method, data, ZH.NobodyAuth, next);
}

exports.BroadcastAnnounceNewMemcacheCluster = function(plugin, collections,
                                                       state) {
  var method = 'BroadcastAnnounceNewMemcacheCluster';
  var data   = {device        : {uuid : ZH.MyUUID},
                cluster_state : state};
  broadcast(plugin, collections, method, data, ZH.NobodyAuth, ZH.OnErrLog);
}

exports.BroadcastShutdown = function(plugin, collections) {
  var method = 'Shutdown';
  var data   = {device : {uuid : ZH.MyUUID}}
  broadcast(plugin, collections, method, data, ZH.NobodyAuth, ZH.OnErrLog);
}

exports.BroadcastUpdateSubscriberMap = function(net, ks, rchans, username,
                                                agent, watch, is_cache) {
  var method = 'BroadcastUpdateSubscriberMap';
  var data   = {device               : {uuid : ZH.MyUUID},
                ks                   : ks,
                replication_channels : rchans,   // SUBSCRIBE
                username             : username, // SUBSCRIBE
                agent                : agent,    // CACHE
                watch                : watch,    // CACHE
                is_cache             : is_cache};
  broadcast(net.plugin, net.collections, method, data,
            ZH.NobodyAuth, ZH.OnErrLog);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GEO SEND ------------------------------------------------------------------

function geo_send(plugin, collections, gnode, method, data, auth, hres, next) {
  if (!ZH.AmRouter) throw(new Error("ZPio.geo_send() LOGIC ERROR"));
  ZH.l('GeoSend: ' + method + ' U: ' + gnode.device_uuid);
  var id        = ZDelt.GetNextRpcID(plugin, collections, gnode);
  var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
  ZCLS.GeoMessage(gnode, jrpc_body, hres, next);
}

exports.GeoSendAckGeoDelta = function(plugin, collections, gnode,
                                      ks, author, rchans) {
  var method = 'AckGeoDelta';
  var data   = {device               : {uuid : ZH.MyUUID},
                datacenter           : ZH.MyDataCenter,
                destination          : gnode.device_uuid,
                ks                   : ks,
                author               : author,
                replication_channels : rchans};
  geo_send(plugin, collections, gnode, method, data,
           ZH.NobodyAuth, {}, ZH.OnErrLog);
}

exports.GeoSendAckGeoDeltaError = function(plugin, collections, gnode,
                                           ks, author, rchans, error) {
  var method = 'AckGeoDeltaError';
  var data   = {device               : {uuid : ZH.MyUUID},
                datacenter           : ZH.MyDataCenter,
                destination          : gnode.device_uuid,
                ks                   : ks,
                author               : author,
                replication_channels : rchans,
                error                : error};
  geo_send(plugin, collections, gnode, method, data,
           ZH.NobodyAuth, {}, ZH.OnErrLog);
}

exports.GeoSendAckGeoCommitDelta = function(plugin, collections,
                                            gnode, ks, author, rchans) {
  var method = 'AckGeoCommitDelta';
  var data   = {device               : {uuid : ZH.MyUUID},
                datacenter           : ZH.MyDataCenter,
                destination          : gnode.device_uuid,
                ks                   : ks,
                author               : author,
                replication_channels : rchans};
  geo_send(plugin, collections, gnode, method, data,
           ZH.NobodyAuth, {}, ZH.OnErrLog);
}

exports.GeoSendDataCenterOnline = function (plugin, collections,
                                            gnode, dkeys, next) {
  var method = 'GeoDataCenterOnline';
  var data   = {device      : {uuid : ZH.MyUUID},
                datacenter  : ZH.MyDataCenter,
                destination : gnode.device_uuid,
                device_keys : dkeys};
  geo_send(plugin, collections, gnode, method, data, ZH.NobodyAuth, {}, next);
}

function do_geo_send_need_merge(plugin, collections, gnode, ks, next) {
  var method = 'GeoNeedMerge';
  var data   = {device      : {uuid : ZH.MyUUID},
                datacenter  : ZH.MyDataCenter,
                destination : gnode.device_uuid,
                ks          : ks};
  geo_send(plugin, collections, gnode, method, data, ZH.NobodyAuth, {}, next);
}

exports.GeoSendNeedMerge = function (plugin, collections, gnode, ks, next) {
  if (ZH.ChaosMode === 24) {
    ZH.e('CHAOS-MODE: 24: Sleeping 30 seconds BEFORE ZPio.GeoSendNeedMerge()');
    setTimeout(function() {
      do_geo_send_need_merge(plugin, collections, gnode, ks, next);
    }, 30000);
  } else {
    do_geo_send_need_merge(plugin, collections, gnode, ks, next);
  }
}

exports.GeoSendDentries = function(plugin, collections,
                                   ks, dentries, freeze, gnode, next) {
  if (ZH.MyDataCenter === gnode.device_uuid) {
    ZH.l('ZPio.GeoSendDentries: -> SELF DATACENTER -> SKIPPING');
    next(null, null);
  } else {
    var dlen = dentries.length;
    ZH.l('ZPio.GeoSendDentries: U: ' + gnode.device_uuid + ' #Ds: ' + dlen);
    var method = 'GeoDentries';
    if (ZH.CentralExtendedTimestampsInDeltas) {
      var now = ZH.GetMsTime();
      for (var i = 0; i < dentries.length; i++) {
        var dentry = dentries[i];
        dentry.delta._meta.geo_sent = now;
      }
    }
    var data = {device      : {uuid : ZH.MyUUID},
                agent       : {uuid : ZH.MyUUID},
                datacenter  : ZH.MyDataCenter,
                destination : gnode.device_uuid,
                ks          : ks,
                dentries    : dentries,
                freeze      : freeze};
    geo_send(plugin, collections, gnode, method, data, ZH.NobodyAuth, {}, next);
  }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GEO BROADCAST -------------------------------------------------------------

function geo_broadcast(plugin, collections, method, data, auth, hres, next) {
  if (!ZH.AmRouter) throw(new Error("ZPio.geo_broadcast() LOGIC ERROR"));
  var gnodes = ZCLS.GetGeoNodes();
  for (var i = 0; i < gnodes.length; i++) {
    var gnode = gnodes[i];
    if (method === 'GeoDelta' || method == 'GeoDentries' ||
        method === 'AckGeoDelta') { // REDUNDANT TO SELF SEND
      if (ZH.MyDataCenter === gnode.device_uuid) {
        continue;
      }
    }
    var id = ZDelt.GetNextRpcID(plugin, collections, gnode);
    if (method === 'GeoDelta' && ZH.CentralExtendedTimestampsInDeltas) {
      data.dentry.delta._meta.geo_sent = ZH.GetMsTime();
    }
    if (method !== 'GeoDataPing') {
      ZH.l('GeoBroadcast[' + i + ']: ' + method + ' GU: ' +
            gnode.device_uuid + ' #DC: ' + gnodes.length);
      if (hres) ZH.l('CALL_TRACE: FROM: ' + hres.id + ' TO: ' + id);
    } 
    var jrpc_body = ZH.CreateJsonRpcBody(method, auth, data, id);
    ZCLS.GeoMessage(gnode, jrpc_body, hres, next);
  }
}

exports.GeoBroadcastDelta = function(net, ks, dentry, auth) {
  ZH.l('ZPio.GeoBroadcastDelta: ' + ZH.SummarizeDelta(ks.key, dentry));
  var method = 'GeoDelta';
  var data   = {device     : {uuid : ZH.MyUUID},
                datacenter : ZH.MyDataCenter,
                ks         : ks,
                dentry     : dentry};
  geo_broadcast(net.plugin, net.collections, method, data, auth,
                {}, ZH.OnErrLog);
}

exports.GeoBroadcastDentries = function(plugin, collections,
                                        ks, agent, dentries) {
  ZH.l('ZPio.GeoBroadcastDentries: K: ' + ks.kqk);
  var method = 'GeoDentries';
  var data   = {device     : {uuid : ZH.MyUUID},
                datacenter : ZH.MyDataCenter,
                ks         : ks,
                agent      : agent,
                dentries   : dentries};
  geo_broadcast(plugin, collections, method, data,
                ZH.NobodyAuth, {}, ZH.OnErrLog);
}

exports.GeoBroadcastSubscriberCommitDelta = function(net, ks, author, rchans) {
  var method = 'GeoSubscriberCommitDelta';
  var data   = {device               : {uuid : ZH.MyUUID},
                datacenter           : ZH.MyDataCenter,
                ks                   : ks,
                author               : author,
                replication_channels : rchans};
  geo_broadcast(net.plugin, net.collections, method, data,
                ZH.NobodyAuth, {}, ZH.OnErrLog);
}

exports.GeoBroadcastCommitDelta = function(plugin, collections,
                                           ks, author, rchans) {
  var method = 'GeoCommitDelta';
  var data   = {device               : {uuid : ZH.MyUUID},
                datacenter           : ZH.MyDataCenter,
                ks                   : ks,
                author               : author,
                replication_channels : rchans};
  geo_broadcast(plugin, collections, method, data,
                ZH.NobodyAuth, {}, ZH.OnErrLog);
}

exports.GeoBroadcastCache = function(plugin, collections, ks, watch, agent) {
  var method = 'GeoBroadcastCache';
  var data   = {device     : {uuid : ZH.MyUUID},
                datacenter : ZH.MyDataCenter,
                ks         : ks,
                watch      : watch,
                agent      : agent};
  geo_broadcast(plugin, collections, method, data,
                ZH.NobodyAuth, {}, ZH.OnErrLog);
}

function do_geo_broadcast_evict_method(plugin, collections, method, ks, agent) {
  var data   = {device     : {uuid : ZH.MyUUID},
                datacenter : ZH.MyDataCenter,
                ks         : ks,
                agent      : agent};
  geo_broadcast(plugin, collections, method, data,
                ZH.NobodyAuth, {}, ZH.OnErrLog);
}

exports.GeoBroadcastEvict = function(plugin, collections, ks, agent) {
  var method = 'GeoBroadcastEvict';
  do_geo_broadcast_evict_method(plugin, collections, method, ks, agent)
}

exports.GeoBroadcastLocalEvict = function(plugin, collections, ks, agent) {
  var method = 'GeoBroadcastLocalEvict';
  do_geo_broadcast_evict_method(plugin, collections, method, ks, agent)
}

exports.GeoBroadcastSubscribe = function(plugin, collections,
                                         duuid, schanid, perm, auth) {
  var method = 'GeoBroadcastSubscribe';
  var data   = {device     : {uuid        : ZH.MyUUID},
                datacenter : ZH.MyDataCenter,
                agent      : {uuid        : duuid},
                channel    : {id          : schanid,
                              permissions : perm}};
  geo_broadcast(plugin, collections, method, data, auth, {}, ZH.OnErrLog);
}

exports.GeoBroadcastUnsubscribe = function(plugin, collections,
                                           duuid, schanid, auth) {
  var method = 'GeoBroadcastUnsubscribe';
  var data   = {device     : {uuid        : ZH.MyUUID},
                datacenter : ZH.MyDataCenter,
                agent      : {uuid        : duuid},
                channel    : {id          : schanid}};
  geo_broadcast(plugin, collections, method, data, auth, {}, ZH.OnErrLog);
}

exports.GeoBroadcastStationUser = function(plugin, collections, duuid, auth) {
  var method = 'GeoBroadcastStationUser';
  var data   = {device     : {uuid : ZH.MyUUID},
                datacenter : ZH.MyDataCenter,
                agent      : {uuid : duuid}};
  geo_broadcast(plugin, collections, method, data, auth, {}, ZH.OnErrLog);
}

exports.GeoBroadcastDestationUser = function(plugin, collections, duuid, auth) {
  var method = 'GeoBroadcastDestationUser';
  var data   = {device     : {uuid : ZH.MyUUID},
                datacenter : ZH.MyDataCenter,
                agent      : {uuid : duuid}};
  geo_broadcast(plugin, collections, method, data, auth, {}, ZH.OnErrLog);
}

exports.GeoBroadcastRemoveUser = function(plugin, collections, username) {
  var method = 'GeoBroadcastRemoveUser';
  var data   = {device     : {uuid : ZH.MyUUID},
                datacenter : ZH.MyDataCenter,
                username   : username};
  geo_broadcast(plugin, collections, method, data,
                ZH.NobodyAuth, {}, ZH.OnErrLog);
}

exports.GeoBroadcastGrantUser = function(plugin, collections,
                                         do_unsub, schanid, priv, username) {
  var method = 'GeoBroadcastGrantUser';
  var data   = {device         : {uuid        : ZH.MyUUID},
                datacenter     : ZH.MyDataCenter,
                username       : username,
                channel        : {id          : schanid,
                                  privilege   : priv},
                do_unsubscribe : do_unsub};
  geo_broadcast(plugin, collections, method, data,
                ZH.NobodyAuth, {}, ZH.OnErrLog);
}

exports.GeoBroadcastAddUser = function(plugin, collections,
                                       username, password, role) {
  var method = 'GeoBroadcastAddUser';
  var data   = {device     : {uuid : ZH.MyUUID},
                datacenter : ZH.MyDataCenter,
                username   : username,
                password   : password,
                role       : role};
  geo_broadcast(plugin, collections, method, data,
                ZH.NobodyAuth, {}, ZH.OnErrLog);
}

exports.GeoBroadcastRemoveDataCenter = function(plugin, collections, dcuuid) {
  var method = 'GeoRemoveDataCenter';
  var data   = {device            : {uuid : ZH.MyUUID},
                datacenter        : ZH.MyDataCenter,
                remove_datacenter : dcuuid};
  geo_broadcast(plugin, collections, method, data,
                ZH.NobodyAuth, {}, ZH.OnErrLog);
}

exports.GeoBroadcastDataPing = function(plugin, collections, mgnode) {
  //ZH.l('ZPio.GeoBroadcastDataPing: MGU: ' + mgnode.device_uuid);
  var method = 'GeoDataPing';
  var data   = {device     : {uuid : ZH.MyUUID},
                datacenter : ZH.MyDataCenter,
                geo_node   : mgnode};
  geo_broadcast(plugin, collections, method, data,
                ZH.NobodyAuth, {}, ZH.OnErrLog);
}

exports.GeoBroadcastMessage = function(plugin, collections, msg) {
  var method = 'GeoMessage';
  var data   = {device     : {uuid : ZH.MyUUID},
                datacenter : ZH.MyDataCenter,
                message    : msg};
  geo_broadcast(plugin, collections, method, data,
                ZH.NobodyAuth, {}, ZH.OnErrLog);
}

exports.GeoBroadcastHttpsAgent = function(plugin, collections,
                                          server, duuid, dkey) {
  var method = 'GeoBroadcastHttpsAgent';
  var data   = {device     : {uuid : ZH.MyUUID},
                datacenter : ZH.MyDataCenter,
                agent      : {uuid   : duuid,
                              key    : dkey,
                              server : server}};
  geo_broadcast(plugin, collections, method, data,
                ZH.NobodyAuth, {}, ZH.OnErrLog);
}

exports.GeoBroadcastDeviceKeys = function(plugin, collections, dkeys) {
  var method = 'GeoBroadcastDeviceKeys';
  var data   = {device      : {uuid : ZH.MyUUID},
                datacenter  : ZH.MyDataCenter,
                device_keys : dkeys};
  geo_broadcast(plugin, collections, method, data,
                ZH.NobodyAuth, {}, ZH.OnErrLog);
}

exports.GeoBroadcastAckGeoDelta = function(plugin, collections, 
                                           ks, author, rchans) {
  var method = 'AckGeoDelta';
  var data   = {device               : {uuid : ZH.MyUUID},
                datacenter           : ZH.MyDataCenter,
                ks                   : ks,
                author               : author,
                replication_channels : rchans};
  geo_broadcast(plugin, collections, method, data,
                ZH.NobodyAuth, {}, ZH.OnErrLog);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ALL GNODES BROADCAST ------------------------------------------------------

function geo_leader_broadcast(plugin, collections, gnodes,
                              method, data, hres, next) {
  if (!ZH.AmRouter) throw(new Error("ZPio.geo_leader_broadcast() LOGIC ERROR"));
  for (var i = 0; i < gnodes.length; i++) {
    var gnode     = gnodes[i];
    var id        = ZDelt.GetNextRpcID(plugin, collections, gnode);
    var guuid     = gnode.device_uuid;
    if (method === 'GeoLeaderPing') {
      if (!ZCLS.GetGeoClient(guuid, true)) {
        ZH.l('SKIPPING: GeoLeaderPing to GU: ' + guuid);
        continue;
      }
    } else {
      ZH.l('GeoBroadcast: ' + method + ' GU: ' + guuid);
    }
    var jrpc_body = ZH.CreateJsonRpcBody(method, ZH.NobodyAuth, data, id);
    ZCLS.GeoLeaderMessage(gnode, jrpc_body, hres, next);
  }
}

exports.GeoBroadcastGeoStateChangeVote = function(plugin, collections, gnodes,
                                                  term, vid, gnetpart, gmaj,
                                                  next) {
  var method = 'GeoStateChangeVote';
  var data   = {device                : {uuid : ZH.MyUUID},
                datacenter            : ZH.MyDataCenter,
                term_number           : term,
                vote_id               : vid,
                geo_nodes             : gnodes,
                geo_network_partition : gnetpart,
                geo_majority          : gmaj};
  geo_leader_broadcast(plugin, collections, gnodes, method, data, {}, next);
}

exports.GeoBroadcastGeoStateChangeCommit = function(plugin, collections, gnodes,
                                                    term, vid, gnetpart, gmaj,
                                                    next) {
  var method = 'GeoStateChangeCommit';
  var data   = {device                : {uuid : ZH.MyUUID},
                datacenter            : ZH.MyDataCenter,
                term_number           : term,
                vote_id               : vid,
                geo_nodes             : gnodes,
                geo_network_partition : gnetpart,
                geo_majority          : gmaj};
  geo_leader_broadcast(plugin, collections, gnodes, method, data, {}, next);
}

exports.GeoBroadcastLeaderPing = function(plugin, collections, gnodes, mgnode) {
  //ZH.l('ZPio.GeoBroadcastLeaderPing: MGU: ' + mgnode.device_uuid);
  var method = 'GeoLeaderPing';
  var data   = {device     : {uuid : ZH.MyUUID},
                datacenter : ZH.MyDataCenter,
                geo_node   : mgnode};
  geo_leader_broadcast(plugin, collections, gnodes,
                       method, data, {}, ZH.OnErrLog);
}

exports.GeoBroadcastAnnounceNewCluster = function(plugin, collections,
                                                  gnodes, cnodes) {
  var method = 'AnnounceNewCluster';
  var data   = {device        : {uuid : ZH.MyUUID},
                datacenter    : ZH.MyDataCenter,
                cluster_nodes : cnodes};
  geo_leader_broadcast(plugin, collections, gnodes,
                       method, data, {}, ZH.OnErrLog);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DISCOVERY -----------------------------------------------------------------

exports.SendDiscovery = function(plugin, collections, gnode, mgnode, next) {
  if (!ZH.AmRouter) throw(new Error("ZPio.SendDiscovery() LOGIC ERROR"));
  var method = 'DataCenterDiscovery';
  var data   = {device      : {uuid : ZH.MyUUID},
                datacenter  : ZH.MyDataCenter,
                destination : gnode.device_uuid,
                geo_node    : mgnode};
  geo_send(plugin, collections, gnode, method, data, ZH.NobodyAuth, {}, next);
}


