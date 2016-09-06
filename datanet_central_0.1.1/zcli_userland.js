"use strict"

var ZH;
if (typeof(exports) !== 'undefined') {
  ZH   = require('./zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

exports.TypeCheckIsArray = function(arr) {
  return (typeof(arr) !== 'object' || !Array.isArray(arr)) ? false : true;
}

function type_check_list(list, op_name) {
  if (!exports.TypeCheckIsArray(list)) {
    return new Error(op_name + ' only possible on an array');
  }
}

function parse_int_or_quotedstring_value(value) {
  if (value[0] === "'" || value[0] === '"') {
    var quote = value[0];
    if (value[value.length - 1] !== quote) {
      return new Error("LREM <int> quoted-string");
    }
    return value.slice(1, value.length - 1); // remove the quotes
  } else {
    return Number(value);
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// LIST FUNCTIONS ------------------------------------------------------------

function do_lpush(zdoc, lname, json) {
  var list = ZH.LookupByDotNotation(zdoc._, lname);
  return zdoc.insert(lname, 0, json);
}
function do_rpush(zdoc, lname, json) {
  var list = ZH.LookupByDotNotation(zdoc._, lname);
  var spot = list.length ? list.length : 0;
  return zdoc.insert(lname, spot, json);
}

function list_pusher(zdoc, lname, json, xfunc, opname) {
  var list = ZH.LookupByDotNotation(zdoc._, lname);
  if (list) return xfunc(zdoc, lname, json);
  else {
    var par = ZH.ChopLastDot(lname);
    if (par.length > 0) {
      var pval = ZH.LookupByDotNotation(zdoc._, par);
      if (typeof(pval) !== 'object') {
        return new Error(opname + ' on incorrect type');
      }
    }
    return zdoc.s(lname, [json]);
  }
}
exports.do_lpush = function(zdoc, lname, json) {
  return list_pusher(zdoc, lname, json, do_lpush, 'LPUSH');
}
exports.do_rpush = function(zdoc, lname, json) {
  return list_pusher(zdoc, lname, json, do_rpush, 'RPUSH');
}

exports.do_lindex = function(zdoc, lname, index) {
  var list = ZH.LookupByDotNotation(zdoc._, lname);
  var err  = type_check_list(list, 'LINDEX');
  if (err) return err;
  return list[index];
}

exports.do_llen = function(zdoc, lname) {
  var list = ZH.LookupByDotNotation(zdoc._, lname);
  var err  = type_check_list(list, 'LLEN');
  if (err) return err;
  return list.length;
}

exports.do_lrange = function(zdoc, lname, start, stop) {
  var list = ZH.LookupByDotNotation(zdoc._, lname);
  var err  = type_check_list(list, 'LRANGE');
  if (err) return err;
  start = Number(start);
  stop  = Number(stop);
  if (!ZH.IsJsonElementInt(start)) return new Error("LRANGE key <int> <int>");
  if (!ZH.IsJsonElementInt(stop))  return new Error("LRANGE key <int> <int>");
  if (start < 0) start = list.length + start;
  if (stop  < 0) stop  = list.length + stop;
  var rlist = ZH.clone(list);
  return rlist.splice(start, stop);
}

exports.do_lpop = function(zdoc, lname) {
  var list = ZH.LookupByDotNotation(zdoc._, lname);
  var err  = type_check_list(list, 'LPOP');
  if (err) return err;
  var ret = list[0];
  zdoc.d(lname + '.0');
  return ret;
}

exports.do_rpop = function(zdoc, lname) {
  var list = ZH.LookupByDotNotation(zdoc._, lname);
  var err  = type_check_list(list, 'RPOP');
  if (err) return err;
  var last = list.length - 1;
  var ret = list[last];
  zdoc.d(lname + '.' + last);
  return ret;
}

exports.do_ltrim = function(zdoc, lname, start, stop) {
  var list = ZH.LookupByDotNotation(zdoc._, lname);
  var err  = type_check_list(list, 'LTRIM');
  if (err) return err;
  start = Number(start);
  stop  = Number(stop);
  if (!ZH.IsJsonElementInt(start)) return new Error("LTRIM key <int> <int>");
  if (!ZH.IsJsonElementInt(stop))  return new Error("LTRIM key <int> <int>");
  if (start <  0)           start = list.length + start;
  if (start <  0)           start = 0;                    // UNDERFLOW
  if (stop  <  0)           stop  = list.length + stop - 1;
  if (stop  >= list.length) stop  = list.length - 1;      // OVERFLOW
  for (var i = stop; i >= start; i--) {
    err = zdoc.d(lname + '.' + i);
    if (err) return err;
  }
}

exports.do_lrem = function(zdoc, lname, count, value) {
  var list = ZH.LookupByDotNotation(zdoc._, lname);
  var err  = type_check_list(list, 'LREM');
  if (err) return err;
  count = Number(count);
  if (!ZH.IsJsonElementInt(count)) return new Error("LREM key <int> value");
  value = parse_int_or_quotedstring_value(value);
  if (isNaN(value)) {
    return new Error("LREM key <int> <int>|<quoted-string>");
  }
  var torem = [];
  var all   = (count === 0);
  if (count < 0) { // reverse iteration
    for (var i = (list.length - 1); i >= 0; i--) {
      if (list[i] === value) {
        torem.push(i);
        count++;
        if (count === 0) break;
      }
    }
  } else {           // forward iteration
    for (var i = 0; i < list.length; i++) {
      if (list[i] === value) {
        torem.push(i);
        if (!all) {
          count--;
          if (count === 0) break;
        }
      }
    }
  }
  for (var i = torem.length - 1; i >= 0; i--) {
    err = zdoc.d(lname + '.' + torem[i]);
    if (err) return err;
  }
}

exports.do_linsert = function(zdoc, lname, befaft, pivot, value) {
  var list = ZH.LookupByDotNotation(zdoc._, lname);
  var err  = type_check_list(list, 'LINSERT');
  if (err) return err;
  var uba  = befaft.toUpperCase();
  var before;
  if      (uba === 'BEFORE') before = true;
  else if (uba === 'AFTER')  before = false;
  else return new Error("LINSERT key BEFORE|AFTER pivot value");
  pivot = parse_int_or_quotedstring_value(pivot);
  if (isNaN(pivot)) {
    return new Error("LINSERT key BEFORE|AFTER <int>|<quoted-string> value");
  }
  value = parse_int_or_quotedstring_value(value);
  if (isNaN(value)) {
    return new Error("LINSERT key BEFORE|AFTER pivot <int>|<quoted-string>");
  }
  for (var i = 0; i < list.length; i++) {
    if (list[i] === pivot) {
      if (before) { // BEFORE
        zdoc.insert(lname,  i, value);
      } else {      // AFTER
        zdoc.insert(lname, (i + 1), value);
      }
    }
  }
}

// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZCUL']={} : exports);

