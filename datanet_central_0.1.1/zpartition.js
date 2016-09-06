"use strict";

var ZCLS     = require('./zcluster');
var ZS       = require('./zshared');
var ZH       = require('./zhelper');


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SETTINGS ------------------------------------------------------------------

var NumberClusterPartitions = 23; //11; //97; //37;
exports.PartitionTable      = [];


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

function shuffle(arr) {
  var cslot = arr.length;
  while (cslot !== 0) {
    var rslot   = Math.floor(Math.random() * cslot); // pick random element
    cslot      -= 1;
    var tval    = arr[cslot]; // swap with the current element
    arr[cslot]  = arr[rslot];
    arr[rslot]  = tval;
  }
  return arr;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CLUSTER ACCESSORS ---------------------------------------------------------

exports.GetClusterNode = function(suuid) {
  var ptid  = exports.GetClusterPartition(suuid);
  if (ptid === null) return null;
  var cnid  = exports.PartitionTable[ptid].master;
  var cnode = ZCLS.ClusterNodes[cnid];
  //ZH.l('ZPart.GetClusterNode: CN: ' + cnid); ZH.p(cnode);
  return cnode;
}

exports.GetRandomPartition = function() {
  return Math.floor(Math.random() * NumberClusterPartitions);
}

function get_partition_agent(suuid) {
  var ptid;
  //ZH.l('get_partition_agent U: ' + suuid);
  var i = parseInt(suuid, 10);
  if (isNaN(i)) {
    ptid = ZH.GetKeyHash(suuid) % NumberClusterPartitions;
  } else {
    ptid = Math.abs(suuid % NumberClusterPartitions);
  }
  //ZH.l('get_partition_agent U: ' + suuid + ' P: ' + ptid);
  return ptid;
}

exports.GetClusterPartition = function(suuid) {
  if (ZCLS.ClusterNodes.length      === 0) return null;
  if (exports.PartitionTable.length === 0) return null;
  return get_partition_agent(suuid);
}

exports.GetPartitionFromKqk = function(kqk) {
  var hval = ZH.GetKeyHash(kqk);
  //ZH.l('ZPart.GetPartitionFromKqk: K: ' + kqk + ' H: ' + hval);
  return hval % NumberClusterPartitions;
}

exports.GetMyPartitions = function() {
  var prts    = [];
  if (exports.PartitionTable.length === 0) return prts;
  var my_slot = -1;
  for (var i = 0; i < ZCLS.ClusterNodes.length; i++) {
    var cnode = ZCLS.ClusterNodes[i];
    if (cnode.device_uuid === ZH.MyUUID) {
      my_slot = i;
      break;
    }
  }
  if (my_slot === -1) return prts;
  for (var i = 0; i < NumberClusterPartitions; i++) {
    var cnid = exports.PartitionTable[i].master;
    if (cnid === my_slot) prts.push(i);
  }
  return prts;
}

exports.GetKeyPartition = function(ks) {
  if (ZCLS.ClusterNodes.length   === 0) return null;
  if (exports.PartitionTable.length === 0) return null;
  return exports.GetPartitionFromKqk(ks.kqk);
}

exports.GetKeyNode = function(ks) {
  var ptid  = exports.GetKeyPartition(ks);
  if (ptid === null) return null;
  var cnid  = exports.PartitionTable[ptid].master;
  var cnode = ZCLS.ClusterNodes[cnid];
  //ZH.l('ZPart.GetKeyNode: CN: ' + cnid); //ZH.p(cnode);
  return cnode;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SAVE PARTITION TABLE --------------------------------------------------------

function persist_partition_table(net, ptbl, next) {
  var skey = ZS.PartitionTable;
  net.plugin.do_set_field(net.collections.global_coll, skey, "value", ptbl,
  function(serr, sres) { 
    if (serr) next(serr, null);
    else {
      exports.PartitionTable = ZH.clone(ptbl); // PARTITION-TABLE COMMIT
      next(null, null);
    }
  });
}

function simple_compute_partition_table(cnodes) {
  var ptbl = [];
  cnodes.sort(ZCLS.CmpDeviceUuid);
  var nnodes = cnodes.length;
  for (var i = 0; i < NumberClusterPartitions; i++) {
    var n          = nnodes ? (i % nnodes) : 0;
    ptbl[i]        = {};
    ptbl[i].master = n;
  }
  return ptbl;
}

exports.SavePartitionTable = function(net, cnodes, ptbl, next) {
  //ZH.e('ZPart.SavePartitionTable'); ZH.e(ptbl);
  if (!ptbl) ptbl = simple_compute_partition_table(cnodes);
  var am_cluster_leader = false;
  for (var i = 0; i < cnodes.length; i++) {
    var cnode = cnodes[i];
    if (cnode.leader) {
      if (cnode.device_uuid === ZH.MyUUID) am_cluster_leader = true;
      break;
    }
  }
  if (am_cluster_leader) persist_partition_table(net, ptbl, next);
  else {
    exports.PartitionTable = ZH.clone(ptbl); // PARTITION-TABLE COMMIT
    next(null, null);
  }
}

exports.CreatePartitionTable = function(net, next) {
  var cnodes = ZCLS.ClusterNodes;
  var ptbl   = simple_compute_partition_table(cnodes);
  exports.SavePartitionTable(net, cnodes, ptbl, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CLUSTER BRAIN -------------------------------------------------------------

var DebugCreateDiffPartitionTable = false;
var DebugFinalDiffPartitionTable  = false;

function debug_create_diff_ptbl(odiff, ocnodes, ncnodes, optbl,
                                nptbl, phist, nlen, per) {
  if (odiff < 1) ZH.l('GAINED ' + Math.abs(odiff) + ' NODES');
  else           ZH.l('LOST '   + odiff           + ' NODES');
  ZH.e('ocnodes'); ZH.e(ocnodes); ZH.e('ncnodes'); ZH.e(ncnodes);
  ZH.e('optbl');   ZH.e(optbl);
  if (odiff > 0) { ZH.e('nptbl'); ZH.e(nptbl); }
  ZH.e('phist');   ZH.e(phist);
}

function deep_debug_final_diff_ptbl(nptbl) {
  var phist = create_partiton_histogram(nptbl);
  ZH.e('FINAL: nptbl'); ZH.e(nptbl);
  ZH.e('FINAL: phist'); ZH.e(phist);
}

function debug_final_diff_ptbl(cnptbl, nptbl) {
  if (DebugFinalDiffPartitionTable) deep_debug_final_diff_ptbl(nptbl);
  var hits = 0;
  for (var i = 0; i < nptbl.length; i++) {
    var nn = nptbl[i].master;
    var cn = cnptbl[i].master;
    if (cn === nn) hits += 1;
  }
  var hitrate = Math.floor((hits * 100) / nptbl.length);
  ZH.e('NEW PARTITION-TABLE: HIT-RATE: ' + hitrate);
}

function cmp_partition_histogram(h1, h2) {
  return (h1.cnt === h2.cnt) ? 0 : (h1.cnt > h2.cnt) ? -1 : 1; // DESC
}

function create_uuid_dict(cnodes) {
  var d = {};
  for (var i = 0; i < cnodes.length; i++) {
    var uuid = cnodes[i].device_uuid;
    d[uuid]  = i;
  }
  return d;
}

function create_slot_dict(cnodes) {
  var d = {};
  for (var i = 0; i < cnodes.length; i++) {
    var uuid = cnodes[i].device_uuid;
    d[i]     = uuid;
  }
  return d;
}

function reorder_old_partition_table(optbl, ncnodes, ocnodes) {
  var nud   = create_uuid_dict(ncnodes);
  var osd   = create_slot_dict(ocnodes);
  var nptbl = ZH.clone(optbl);
  for (var i = 0; i < optbl.length; i++) {
    var on    = optbl[i].master;
    var ouuid = osd[on];
    var nn    = nud[ouuid];
    //ZH.l('i: ' + i + ' on: ' + on + ' ouuid: ' + ouuid + ' nn: ' + nn);
    if (on !== nn) nptbl[i].master = nn;
  }
  return nptbl;
}

function find_diff_cluster_node_slot(ncnodes, ocnodes) {
  var nud    = create_uuid_dict(ncnodes);
  var oud    = create_uuid_dict(ocnodes);
  var odiff  = ocnodes.length - ncnodes.length;
  var bigd   = (odiff < 0) ? nud : oud;
  var smalld = (odiff < 0) ? oud : nud;
  var xn     = [];
  for (var uuid in bigd) {
    if (ZH.IsUndefined(smalld[uuid])) xn.push(bigd[uuid]);
  }
  return xn;
}

function create_partiton_histogram(ptbl) {
  var hist = [];
  for (var i = 0; i < ptbl.length; i++) {
    var n = ptbl[i].master;
    if (ZH.IsDefined(n)) {
      var cnt  = hist[n] ? hist[n].cnt : 0;
      cnt     += 1;
      hist[n]  = {n : n, cnt : cnt};
    }
  }
  for (var i = 0; i < hist.length; i++) {
    if (!hist[i]) hist[i] = {n : i, cnt : 0};
  }
  hist.sort(cmp_partition_histogram);
  return hist;
}

function create_diff_partition_table(ncnodes, ocnodes, optbl, next) {
  var xn     = find_diff_cluster_node_slot(ncnodes, ocnodes);
  var nptbl  = reorder_old_partition_table(optbl, ncnodes, ocnodes);
  var cnptbl = ZH.clone(nptbl);
  var odiff  = ocnodes.length - ncnodes.length;
  ZH.l('create_diff_partition_table: odiff: ' + odiff); ZH.l('XN'); ZH.p(xn);
  var phist  = create_partiton_histogram(nptbl);
  if (phist.length === 0) next(null, optbl);
  else {
    if (DebugCreateDiffPartitionTable) {
      debug_create_diff_ptbl(odiff, ocnodes, ncnodes, optbl, nptbl, phist);
    }
    if (odiff < 0) { // GAINED NODE(S)
      var iarr = [];
      for (var i = 0; i < nptbl.length; i++) {
        iarr[i] = i;
      }
      shuffle(iarr); // Traverse nptbl[] in a "random" order
      var nlen = ncnodes.length;
      var per  = (Math.floor(nptbl.length / nlen) * xn.length) + 1;
      var xnc  = 0;
      ZH.e('nlen: ' + nlen + ' per: ' + per);
      while (per > 0) {
        var head  = phist.shift();
        var tn    = head.n;
        head.cnt -= 1;
        phist.push(head);
        phist.sort(cmp_partition_histogram);
        for (var j = 0; j < iarr.length; j++) {
          var i = iarr[j];
          var n = nptbl[i].master;
          if (tn === n) {
            var xnm          = xn[xnc];
            xnc             += 1;
            if (xnc === xn.length) xnc = 0;
            if (xnm != n) {
              nptbl[i].master  = xnm;
              per             -= 1;
              break;
            }
          }
          if (per === 0) break;
        }
      }
      debug_final_diff_ptbl(cnptbl, nptbl);
      next(null, nptbl);
    } else {        // LOST NODE(S)
      for (var i = 0; i < nptbl.length; i++) {
        if (ZH.IsUndefined(nptbl[i].master)) {
          var tail         = phist.pop();
          var tn           = tail.n;
          nptbl[i].master  = tn;
          tail.cnt        += 1;
          phist.push(tail);
          phist.sort(cmp_partition_histogram);
        }
      }
      debug_final_diff_ptbl(cnptbl, nptbl);
      next(null, nptbl);
    }
  }
}

exports.ComputeNewFromOldPartitionTable = function(plugin, collections,
                                                   cnodes, next) {
  var ckey = ZS.ClusterState;
  plugin.do_get_field(collections.global_coll, ckey, "cluster_nodes",
  function(ferr, ocnodes) {
    if (ferr) next(ferr, null);
    else {
      if (!ocnodes) next(null, null);
      else {
        plugin.do_get_field(collections.global_coll, ZS.PartitionTable, "value",
        function(gerr, optbl) {
          if (gerr) next(gerr, null);
          else {
            if (!optbl) next(null, null);
            else {
              create_diff_partition_table(cnodes, ocnodes, optbl, next);
            }
          }
        });
      }
    }
  });
}

// NOTE: Used by test script unit_tests/cluster_rebalance_tester.js
exports.TestCreatePartitionTable = function(cnodes, ocnodes, optbl, next) {
  DebugCreateDiffPartitionTable = true;
  DebugFinalDiffPartitionTable  = true;
  var ncp                       = optbl.length;
  NumberClusterPartitions       = ncp;
  ZH.l('ZPart.TestCreatePartitionTable: NumberClusterPartitions: ' + ncp);
  create_diff_partition_table(cnodes, ocnodes, optbl, next);
}
