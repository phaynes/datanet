
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DEPRECATED ----------------------------------------------------------------

exports.Parents_SubPathMatch = function(ps, pmpath) {
  var pmdots  = pmpath.split('.');
  var match   = false;
  var parents = [];
  var pfield  = null;
  for (var pname in ps) {
    var paths = ps[pname];
    for (var i = 0; i < paths.length; i++) {
      var ppath = paths[i];
      var pdots = ppath.split('.');
      var miss  = false;
      for (var j = 0; j < pdots.length; j++) {
        if (pdots[j] !== pmdots[j]) {
          miss = true;
          break;
        }
      }
      if (!miss) {
        pfield = ppath;
        match  = match ? true : (pdots.length === pmdots.length);
        parents.push(pname);
      }
    }
  }
  if (parents.length === 0) return null;
  else {
    return {match        : match,
            parent_field : pfield,
            parents      : parents};
  }
}


function calc_channel_mismatch(crdt, delta) {
  delta._meta.removed_channels = [];
  var crchans = crdt ? crdt._meta.replication_channels : [];
  var crc     = {};
  for (var i = 0; i < crchans.length; i++) {
    crc[crchans[i]] = true;
  }
  var drchans = delta._meta.replication_channels;
  var drc     = {}
  for (var i = 0; i < drchans.length; i++) {
    drc[drchans[i]] = true;
  }
  for (var rchan in crc) {
    if (drc[rchan]) {
      delete(crc[rchan]);
      delete(drc[rchan]);
    }
  }
  //ZH.l('crc'); ZH.p(crc); ZH.l('drc'); ZH.p(drc);
  var ncrc = Object.keys(crc).length;
  if (ncrc) {
    for (var rchan in crc) {
      delta._meta.removed_channels.push(ZH.IfNumberConvert(rchan));
    }
  }
  var ndrc = Object.keys(drc).length;
  return ncrc + ndrc;
}

function union_rchans(dest, src) {
  if (dest && src) {
    var dexists = {}
    for (var i = 0; i < dest.length; i++) {
      dexists[dest[i]] = true;
    }
    for (var i = 0; i < src.length; i++) {
      if (!dexists[src[i]]) dest.push(ZH.IfNumberConvert(src[i]));
    }
  }
}


