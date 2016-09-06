
var ZCloud   = require('./zcloud_server');
var ZVote    = require('./zvote');
var ZH       = require('./zhelper');

function cluster_state_change_vote_processed(err, hres) {
  if (ZCloud.HandleCallbackError('BroadcastClusterStateChangeVote',
                                  err, hres)) {
    return;
  }
  ZH.l('>>>>(I): cluster_state_change_vote_processed');
  var data = {term_number               : hres.term_number,
              vote_id                   : hres.vote_id,
              cluster_nodes             : hres.cluster_nodes,
              cluster_synced            : hres.cluster_synced,
              cluster_network_partition : hres.cluster_network_partition,
              status            : "OK"};
  ZCloud.Respond(hres, data);
}

function cluster_state_change_commit_processed(err, hres) {
  if (ZCloud.HandleCallbackError('BroadcastClusterStateChangeCommit',
                                 err, hres)) {
    return;
  }
  ZH.l('>>>>(I): cluster_state_change_commit_processed');
  var data = {term_number : hres.term_number,
              vote_id     : hres.vote_id,
              status      : "OK"};
  ZCloud.Respond(hres, data);
}

function handle_cluster_state_change_vote(conn, params, id, wid, net) {
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var auth     = params.authentication;
  var hres     = ZCloud.CreateHres(conn, params, id, wid, auth);
  var next     = cluster_state_change_vote_processed;
  var term     = params.data.term_number;
  var vid      = params.data.vote_id;
  ZH.l('<-|(I): BroadcastClusterStateChangeVote: ' + term + ' V: ' + vid);
  var cnodes   = params.data.cluster_nodes;
  var csynced  = params.data.cluster_synced;
  var cnetpart = params.data.cluster_network_partition;
  ZVote.HandleClusterStateChangeVote(net, term, vid, cnodes, csynced, cnetpart,
                                     hres, next);
}

function handle_cluster_state_change_commit(conn, params, id, wid, net) {
  if (ZH.ChaosMode === 6) return ZCloud.ChaosDropResponse(6);
  if (params.cluster_method_key !== ZH.ClusterMethodKey) return;
  var auth     = params.authentication;
  var hres     = ZCloud.CreateHres(conn, params, id, wid, auth);
  var next     = cluster_state_change_commit_processed;
  var term     = params.data.term_number;
  var vid      = params.data.vote_id;
  ZH.l('<-|(I): BroadcastClusterStateChangeCommit: T: ' + term + ' V: ' + vid);
  var cnodes   = params.data.cluster_nodes;
  var csynced  = params.data.cluster_synced;
  var cnetpart = params.data.cluster_network_partition;
  var ptbl     = params.data.partition_table;
  ZVote.HandleClusterStateChangeCommit(net, term, vid, cnodes, csynced,
                                       cnetpart, ptbl, hres, next);
}

exports.Methods = {
// CLUSTER VOTE METHODS ------------------------------------------------------
                'BroadcastClusterStateChangeVote'   :
                  {handler : handle_cluster_state_change_vote,
                   router                    : true,
                   storage                   : true,
                   cluster_network_partition : true,
                   geo_network_partition     : true,
                   no_sync                   : true,
                   fixlog                    : true},
                'BroadcastClusterStateChangeCommit' :
                  {handler : handle_cluster_state_change_commit,
                   router                    : true,
                   storage                   : true,
                   cluster_network_partition : true,
                   geo_network_partition     : true,
                   no_sync                   : true,
                   fixlog                    : true},
};


