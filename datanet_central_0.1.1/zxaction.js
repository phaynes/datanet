"use strict";

var ZPub, ZRAD, ZMerge, ZConv, ZOplog, ZDelt, ZMDC, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  ZPub   = require('./zpublisher');
  ZRAD   = require('./zremote_apply_delta');
  ZMerge = require('./zmerge');
  ZConv  = require('./zconvert');
  ZOplog = require('./zoplog');
  ZDelt  = require('./zdeltas');
  ZMDC   = require('./zmemory_data_cache');
  ZS     = require('./zshared');
  ZH     = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// COMMIT CRDT  --------------------------------------------------------------

function count_new_members(cdata) {
  var nmm = 0;
  if (cdata["#"] === -1) nmm += 1;
  var cval   = cdata.V;
  var ctype  = cdata.T;
  if        (ctype === "A") {
    for (var i = 0; i < cval.length; i++) {
      nmm += count_new_members(cval[i]);
    }
  } else if (ctype === "O") {
    for (var ckey in cval) {
      nmm += count_new_members(cval[key]);
    }
  }
  return nmm;
}

function count_new_modified_members(delta) {
  var nmm = 0;
  for (var i = 0; i < delta.modified.length; i++) {
    var cval  = delta.modified[i].V;
    nmm      += count_new_members(cval);
  }
  return nmm;
}

function count_json_elements(val) {
  var cnt = 1; // Count data structure itself
  if (typeof(val) === "object") {
    if (Array.isArray(val)) {
      for (var i = 0; i < val.length; i++) {
        cnt += count_json_elements(val[i]);
      }
    } else { // Object
      for (var k in val) {
        cnt += count_json_elements(val[k]);
      }
    }
  }
  return cnt;
}

function get_oplog_new_member_count(oplog) {
  if (!oplog) return 0;
  var cnt = oplog.length; // OK to over-estimate
  for (var i = 0; i < oplog.length; i++) {
    var op = oplog[i];
    var name = op.name;
    if (name === "set") {
      var val = op.args[0];
      cnt += count_json_elements(val);
    } else if (name === "insert") {
      var val = op.args[1];
      cnt += count_json_elements(val);
    }
  }
  return cnt;
}

exports.GetOperationCount = function(ocrdt, oplog, remove, expire, delta) {
  if (remove) return 1;
  if (expire) return 1;
  var meta = ocrdt._meta;
  var ncnt = 0;
  if (meta.member_count && meta.last_member_count) {
    ncnt = (meta.member_count - meta.last_member_count);
  }
  ncnt += get_oplog_new_member_count(oplog); // PULL & MEMCACHE_COMMIT
  ncnt += count_new_modified_members(delta);
  return ncnt;
}

exports.ReserveOpIdRange = function(plugin, collections, duuid, cnt, next) {
  var sub  = ZH.CreateSub(duuid);
  var dkey = ZS.GetAdminDevices(sub)
  plugin.do_increment(collections.admin_coll, dkey, "NextOpID", cnt,
  function (err, res) {
    if (err) next(err, null);
    else { // New version is start of ID-range
      var dvrsn = Number(res.NextOpID) - cnt;
      ZH.l('ReserveOpIdRange: U: ' + duuid + ' cnt: ' + cnt + ' DV: ' + dvrsn);
      next(null, dvrsn);
    }
  });
}

function assign_document_creation(meta, dvrsn, ts) {
  // AGENT-Author Metadata -> SET ONLY ONCE
  if (ZH.IsUndefined(meta.document_creation)) {
    meta.document_creation = { "_" : ZH.MyUUID,
                               "#" : dvrsn,
                               "@" : ts};
    ZH.l('ADDING META.DOCUMENT_CREATION'); ZH.p(meta.document_creation);
  }
}

function assign_member_version(cdata, dvrsn, duuid, ts, isa) {
  if (cdata["#"] === -1) {
    cdata["#"]  = dvrsn;
    dvrsn      += 1;
  }
  if (cdata["_"] === -1) cdata["_"] = duuid;
  if (cdata["@"] === -1) cdata["@"] = ts;
  if (isa) {
    if (!cdata["S"]) cdata["S"] = [ts];
  }
  var cval   = cdata.V;
  var ctype  = cdata.T;
  if        (ctype === "A") {
    for (var i = 0; i < cval.length; i++) {
      dvrsn = assign_member_version(cval[i],    dvrsn, duuid, ts, true);
    }
  } else if (ctype === "O") {
    for (var ckey in cval) {
      dvrsn = assign_member_version(cval[ckey], dvrsn, duuid, ts, false);
    }
  }
  return dvrsn;
}

function assign_array_left_neighbor(cdata) {
  var cval  = cdata.V;
  var ctype = cdata.T;
  if        (ctype === "O") { // Recurse to look for nested Arrays
    for (var ckey in cval) {
      assign_array_left_neighbor(cval[ckey]);
    }
  } else if (ctype === "A") {
    var oa = ZH.IsOrderedArray(cdata);
    for (var i = 0; i < cval.length; i++) {
      if (!oa) {
        // NOTE: First element: {<:{ _:0,#:0}}
        var left = (i === 0) ? 0 : i - 1;
        if (ZH.IsUndefined(cval[i]["<"])) { // needs a proper "<"
          var el_underscore = (i === 0) ? 0 : cval[left]["_"];
          var el_hashsign   = (i === 0) ? 0 : cval[left]["#"];
          cval[i]["<"] = {"_" : el_underscore, "#" : el_hashsign};
        }
      }
      assign_array_left_neighbor(cval[i]);
    }
  }
}

exports.UpdateCrdtMetaAndAdded = function(crdt, duuid, cnt, dvrsn, ts) {
  crdt._meta.author.agent_uuid = duuid;
  crdt._meta.op_count          = cnt;
  crdt._meta.delta_version     = dvrsn;
  assign_document_creation(crdt._meta, dvrsn, ts);
  if (crdt._data) { // Remove: MEMCACHE (separate) NO crdt._data
    // Add [_, #] to new members
    dvrsn = assign_member_version(crdt._data, dvrsn, ZH.MyUUID, ts, false);
    // Add [<] to new ARRAY members
    assign_array_left_neighbor(crdt._data);
  }
  return dvrsn;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ZDOC COMMIT ---------------------------------------------------------------

function populate_delta_added(cdata, added, op_path) {
  var cval  = cdata.V;
  var ctype = cdata.T;
  if (cdata["+"] === true) {
    added.push(ZOplog.CreateDeltaEntry(op_path, cdata));
    return; // Adding highest member of tree -> adds all descendants
  }
  if        (ctype === "A") {
    for (var i = 0; i < cval.length; i++) {
      op_path.push(ZOplog.CreateOpPathEntry(i, cval[i]));
      populate_delta_added(cval[i], added, op_path);
      op_path.pop();
    }
  } else if (ctype === "O") {
    for (var ckey in cval) {
      op_path.push(ZOplog.CreateOpPathEntry(ckey, cval[ckey]));
      populate_delta_added(cval[ckey], added, op_path);
      op_path.pop();
    }
  } 
}

function remove_crdt_plus_entries(crdt) {
  var cval  = crdt.V;
  var ctype = crdt.T;
  if (crdt["+"] === true) delete(crdt["+"]);
  if        (ctype === "A") {
    for (var i = 0; i < cval.length; i++) remove_crdt_plus_entries(cval[i]);
  } else if (ctype === "O") {
    for (var ckey in cval)                remove_crdt_plus_entries(cval[ckey]);
  }
}

function truncate_lhn_entries(cdata) {
  var cval  = cdata.V;
  var ctype = cdata.T;
  if (ZH.IsDefined(cdata["<"])) { // truncate LHN
    var duuid  = cdata["<"]["_"];
    var step   = cdata["<"]["#"];
    cdata["<"] = { "_" : duuid, "#" : step};
  }
  if        (ctype === "A") {
    for (var i = 0; i < cval.length; i++) truncate_lhn_entries(cval[i]);
  } else if (ctype === "O") {
    for (var ckey in cval)                truncate_lhn_entries(cval[ckey]);
  }
}

function cleanup_delta_fields(delta) {
  for (var i = 0; i < ZH.DeltaFields.length; i++) {
    var f = ZH.DeltaFields[i];
    for (var j = 0; j < delta[f].length; j++) {
      var dlt = delta[f][j];
      if (ZH.IsDefined(dlt.V["<"])) { // truncate LHN
        var duuid  = dlt.V["<"]["_"];
        var step   = dlt.V["<"]["#"];
        dlt.V["<"] = { "_" : duuid, "#" : step};
      }
    }
  }
  for (var i = 0; i < delta.added.length; i++) {
    remove_crdt_plus_entries(delta.added[i].V);
  }
}

function populate_nested_modified(crdt, delta, dvrsn, ts) {
  for (var i = 0; i < delta.modified.length; i++) {
    var cval    = delta.modified[i].V;
    var op_path = delta.modified[i].op_path;
    var clm     = ZMerge.FindNestedElementBase(crdt._data.V, op_path);
    var isa     = Array.isArray(clm);
    dvrsn       = assign_member_version(cval, dvrsn, ZH.MyUUID, ts, isa);
    assign_array_left_neighbor(cval);
  }
  return dvrsn;
}

function assign_delta_meta(crdt, delta, ts) {
  delta._meta      = ZH.clone(crdt._meta); // NOTE: CLONE META
  delta._meta["@"] = ts;                   // SET Delta Timestamp
}

function calculate_delta_size(delta) {
  var dmeta = delta._meta;
  var size  = ZConv.CalculateMetaSize(dmeta);
  ZH.l('calculate_delta_size: META-SIZE: ' + size);
  for (var i = 0; i < delta.modified.length; i++) {
    var cval  = delta.modified[i].V;
    size     += ZConv.CalculateCrdtMemberSize(cval);
  }
  for (var i = 0; i < delta.added.length; i++) {
    var cval  = delta.added[i].V;
    size     += ZConv.CalculateCrdtMemberSize(cval);
  }
  ZH.l('calculate_delta_size: #B: ' + size);
  delta._meta.delta_bytes = size;
}

exports.FinalizeDeltaForCommit = function(crdt, delta, dvrsn, ts) {
  if (crdt._data) { // Remove: MEMCACHE (separate) NO crdt._data
    populate_delta_added(crdt._data, delta.added, []);
    remove_crdt_plus_entries(crdt._data);
    truncate_lhn_entries(crdt._data);
  }
  cleanup_delta_fields(delta);
  dvrsn = populate_nested_modified(crdt, delta, dvrsn, ts);
  assign_delta_meta(crdt, delta, ts);
  calculate_delta_size(delta);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// COUNT TOMBSTONES ----------------------------------------------------------

function count_tombstones(x, cdata, is_oa) {
  var cval  = cdata.V;
  var ctype = cdata.T;
  if (!is_oa && cdata["X"]) x.num_tombstones += 1;
  if        (ctype === "A") {
    var oa = ZH.IsOrderedArray(cdata);
    for (var i = 0; i < cval.length; i++) {
      count_tombstones(x, cval[i], oa);
    }
  } else if (ctype === "O") {
    for (var ckey in cval) {
      count_tombstones(x, cval[ckey], false);
    }
  }
}

exports.CountTombstones = function(cdata) {
  var x = {num_tombstones : 0};
  count_tombstones(x, cdata, false);
  ZH.l('ZXact.CountTombstones: ' + x.num_tombstones);
  return x.num_tombstones;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AUTOMATIC DELTAS ----------------------------------------------------------

function cleanup_auto_dentry(dentry) {
  var meta = dentry.delta._meta;
  delete(meta.overwrite);
  delete(meta.initial_delta);
  delete(meta.remove);
  delete(meta.removed_channels);
  delete(meta.xaction);
  delete(meta.dirty_central);
  delete(meta.from_central);
  delete(meta.is_geo);
  delete(meta.OOO_GCV);
  delete(meta.DO_GC);
  delete(meta.DO_DS);
  delete(meta.DO_REORDER);
  delete(meta.DO_REAP);
  delete(meta.reap_gc_version);
  delete(meta.DO_IGNORE);
  delete(meta.DO_AUTO);
  delete(meta.reference_uuid);
  delete(meta.reference_version);
  delete(meta.reference_GC_version);
  delete(meta.reference_ignore);
  ZDelt.DeleteMetaTimings(dentry);
}

function get_current_agent_version(net, ks, is_agent, next) {
  if (is_agent) {
    var akey = ZS.GetKeyAgentVersion(ks.kqk);
    net.plugin.do_get_field(net.collections.key_coll, akey, "value", next);
  } else {
    ZMDC.GetDeviceToCentralAgentVersion(net.plugin, net.collections,
                                        ZH.MyUUID, ks, next);
  }
}

exports.CreateAutoDentry = function(net, ks, cmeta, new_count, is_agent, next) {
  var duuid = ZH.MyUUID;
  get_current_agent_version(net, ks, is_agent, function(gerr, avrsn) {
    if (gerr) next(gerr, null);
    else {
      var oavnum = ZH.GetAvnum(avrsn);
      var navnum = oavnum + 1;
      var navrsn = ZH.CreateAvrsn(ZH.MyUUID, navnum);
      ZH.l('CreateAutoDentry: K: ' + ks.kqk + ' New AgentVersion: ' + navrsn);
      exports.ReserveOpIdRange(net.plugin, net.collections, duuid, new_count,
      function(uerr, dvrsn) {
        if (uerr) next(uerr, null);
        else {
          ZRAD.GetCentralAgentVersions(net, ks, function(ferr, deps) {
            if (ferr) next(ferr, null);
            else {
              var meta           = ZH.clone(cmeta);
              ZH.InitAuthor(meta, deps);
              meta.author.agent_version = navrsn;
              meta["@"]          = ZH.GetMsTime();
              meta.delta_version = dvrsn;
              var ndelta         = ZH.InitDelta(meta);
              var ndentry        = ZH.CreateDentry(ndelta, ks);
              cleanup_auto_dentry(ndentry);
              if (is_agent) next(null, ndentry);
              else {
                meta.from_central = true;
                var username      = ZH.NobodyAuth.username;
                ZPub.GenerateDeltaXactionID(net, ks, ndentry, username);
                next(null, ndentry);
              }
            }
          });
        }
      });
    }
  });
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZXact']={} : exports);

