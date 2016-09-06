"use strict";

require('./setImmediate');

var ZRAD   = require('./zremote_apply_delta');
var ZDelt  = require('./zdeltas');
var ZXact  = require('./zxaction');
var ZDQ    = require('./zdata_queue');
var ZDS    = require('./zdatastore');
var ZQ     = require('./zqueue');
var ZH     = require('./zhelper');

function create_and_process_auto_dentry(net, ks, cdentry, hres, next) {
  //TODO FIXME DENTRY NO LONGER HAS CRDT -> THIS IS BROKEN
  hres.key_data = {crdt : cdentry.crdt}; // Used in response (BROKEN)
  var cmeta     = cdentry.delta._meta;
  var ncnt      = cmeta.member_count;
  ZXact.CreateAutoDentry(net, ks, cmeta, ncnt, false, function(aerr, dentry) {
    if (aerr) next(aerr, null);
    else {
      for (var i = 0; i < ZH.DeltaFields.length; i++) {
        var f           = ZH.DeltaFields[i];
        dentry.delta[f] = ZH.clone(cdentry.delta[f]);
      }
      ZRAD.FetchApplyStorageDeltaMetadata(net, ks, dentry, function(serr, md) {
        if (serr) next(serr, hres);
        else {
          ZRAD.ProcessAutoDelta(net, ks, md, dentry, function(perr, pres) {
            next(perr, hres);
          });
        }
      });
    }
  });
}

function create_and_process_auto_dentries(net, ks, dentries, hres, next) {
  if (dentries.length === 0) next(null, hres);
  else {
    var dentry = dentries.shift();
    create_and_process_auto_dentry(net, ks, dentry, hres, function(serr, sres) {
      if (serr) next(serr, hres);
      else {
        setImmediate(create_and_process_auto_dentries,
                     net, ks, dentries, hres, next);
      }
    });
  }
}

function do_flow_handle_client_store(net, ks, json, hres, next) {
  ZDelt.StorageCreateClientStoreDelta(net, ks, json, function(serr, sres) {
    if (serr) next(serr, hres);
    else {
      var cdentry = sres.dentry;
      create_and_process_auto_dentry(net, ks, cdentry, hres, next)
    }
  });
}

function do_flow_handle_client_commit(net, ks, crdt, oplog, hres, next) {
  ZDelt.StorageCreateClientCommitDelta(net, ks, crdt, oplog,
  function(serr, sres) {
    if (serr) next(serr, hres);
    else {
      var cdentry = sres.dentry;
      create_and_process_auto_dentry(net, ks, cdentry, hres, next)
    }
  });
}

function do_flow_handle_client_remove(net, ks, hres, next) {
  ZDelt.StorageCreateClientRemoveDelta(net, ks, function(serr, sres) {
    if (serr) next(serr, hres);
    else {
      var cdentry = sres.dentry;
      create_and_process_auto_dentry(net, ks, cdentry, hres, next)
    }
  });
}

function do_flow_handle_client_stateless_commit(net, ks, rchans, oplog,
                                                hres, next) {
  ZDelt.StorageCreateClientStatelessCommitDelta(net, ks, rchans, oplog,
  function(serr, sres) {
    if (serr) next(serr, hres);
    else {
      var cdentries = sres.dentries;
      create_and_process_auto_dentries(net, ks, cdentries, hres, next)
    }
  });
}

function do_flow_handle_client_fetch(net, ks, hres, next) {
  ZDS.RetrieveCrdt(net.plugin, net.collections, ks, function (ferr, ocrdt) {
    if (ferr) next(ferr, hres);
    else {
      if (!ocrdt) next(new Error(ZS.Errors.NoDataFound), hres);
      else {
        hres.key_data = {crdt : ocrdt}; // Used in response
        next(null, hres);
      }
    }
  });
}

function do_flow_handle_client_call(net, fetch, store, commit, scommit, remove,
                                    ks, json, crdt, oplog, rchans, hres, next) {
  if (fetch) {
    do_flow_handle_client_fetch(net, ks, hres, next);
  } else if (store) {
    do_flow_handle_client_store(net, ks, json, hres, next);
  } else if (commit) {
    do_flow_handle_client_commit(net, ks, crdt, oplog, hres, next);
  } else if (scommit) {
    do_flow_handle_client_stateless_commit(net, ks, rchans, oplog, hres, next);
  } else { // REMOVE
    do_flow_handle_client_remove(net, ks, hres, next);
  }
}

exports.FlowHandleStorageClusterClientCall = function(plugin, collections,
                                                      qe, next) {
  var net     = qe.net;
  var qnext   = qe.next;
  var qhres   = qe.hres;
  var ks      = qe.ks;
  var data    = qe.data;
  var fetch   = data.fetch;
  var store   = data.store;
  var commit  = data.commit;
  var scommit = data.scommit;
  var remove  = data.remove;
  var json    = data.json;
  var crdt    = data.crdt;
  var oplog   = data.oplog;
  var rchans  = data.rchans;
  ZH.l('ZDirect.FlowHandleStorageClusterClientCall: K: ' + ks.kqk);
  do_flow_handle_client_call(net, fetch, store, commit, scommit, remove,
                             ks, json, crdt, oplog, rchans, qhres,
  function(perr, pres) {
    qnext(perr, qhres);
    next (null, null);
  });
}

function handle_cluster_client_call(net, fetch, store, commit, scommit, remove,
                                    ks, json, crdt, oplog, rchans, hres, next) {
  hres.ks = ks; // Used in response
  ZDQ.StorageRequestClusterClientCall(net.plugin, net.collections,
                                      fetch, store, commit, scommit, remove,
                                      ks, json, crdt, oplog, rchans, hres,
  function(serr, sres) {
    if (serr) next(serr, hres);
    else {
      hres.crdt = hres.key_data.crdt;
      next(null, hres);
    }
  });
}

exports.CentralHandleClusterClientFetch = function(net, ks, hres, next) {
  handle_cluster_client_call(net, true, false, false, false, false,
                             ks, null, null, null, null, hres, next);
}

exports.CentralHandleClusterClientStore = function(net, ks, json, hres, next) {
  handle_cluster_client_call(net, false, true, false, false, false,
                             ks, json, null, null, null, hres, next);
}

exports.CentralHandleClusterClientCommit = function(net, ks, crdt, oplog,
                                                    hres, next) {
  handle_cluster_client_call(net, false, false, true, false, false,
                             ks, null, crdt, oplog, null, hres, next);
}

exports.CentralHandleClusterStatelessClientCommit =
                                                  function(net, ks, rchans,
                                                           oplog, hres, next) {
  handle_cluster_client_call(net, false, false, false, true, false,
                             ks, null, null, oplog, rchans, hres, next);
}

exports.CentralHandleClusterClientRemove = function(net, ks, hres, next) {
  handle_cluster_client_call(net, false, false, false, false, true,
                             ks, null, null, null, null, hres, next);
}

