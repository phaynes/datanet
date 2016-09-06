"use strict";

var ZS, ZH;
if (typeof(exports) !== 'undefined') {
  ZS = require('../zshared');
  ZH = require('../zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

function get_coll_prefix(coll) {
  return coll.name + '_';
}

exports.GetCollPrefix = function(coll) {
  return get_coll_prefix(coll);
}

exports.DatabaseDisabled = function(next) {
  next(new Error(ZS.Errors.DatabaseDisabled, null));
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SKELETON FOR STORAGE OBJECTS ----------------------------------------------

function generic_populate(coll, duuid) { }
function generic_shutdown(collection)  { }


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ADMIN OPERATIONS ----------------------------------------------------------

function set_namespace(ns) {
  exports.Plugin.namespace = ns;
}

function set_datacenter(dc) {
  exports.Plugin.datacenter = dc;
}

function LS_open(db_ip, db_port, ns, xname, next) {
  console.log('LS_open: NS: ' + ns);
  exports.Plugin.namespace = ns;
  next(null, null);
}

function LS_create_collection(db, cname, is_info) {
  var icname     = is_info ? ZS.InfoNamespace         + '_' + cname : 
                             exports.Plugin.namespace + '_' + cname;
  //console.log('LS_collection: icname: ' + icname);
  var collection = {name : icname};
  return collection;
}

function set_collection_name(cname) {
  ZH.l('Memory.set_collection_name: ' + cname);
  exports.Plugin.cname = cname;
}

function LS_close(next) { // NO-OP
  ZH.l('>>>>>>>>> LS_close');
  next(null, null);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// READ OPERATIONS -----------------------------------------------------------

function LS_get(coll, key, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  var value = exports.Storage.get(coll.name, key);
  if (typeof(value) == 'undefined' || value == null) next(null, []);
  else                                               next(null, [value]);
}

function LS_bget(coll, key, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  var value = exports.Storage.bget(coll.name, key);
  if (typeof(value) == 'undefined' || value == null) next(null, []);
  else                                               next(null, [value]);
}

function LS_get_field(coll, key, field, next) {
  if (ZH.DatabaseDisabled) return exports.DatabaseDisabled(next);
  //console.log('LS_get_field: key: ' + key);
  LS_get(coll, key, function(gerr, gval) {
    if (gval.length === 0) next(null, null);
    else {
      var data = gval[0];
      var f    = data[field];
      //console.log('GET result'); console.dir(f);
      if (typeof(f) === 'undefined') next(null, null);
      else                           next(null, f);
    }
  });
}

function LS_get_array_values(coll, key, field, next) {
  return LS_get_field(coll, key, field, next);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRITE OPERATIONS ----------------------------------------------------------

function get_set_value(val, key) {
  //ZH.l('get_set_value: VAL:' + JSON.stringify(val));
  if (ZH.IsUndefined(val) || val == null) {
    return {_id : key};
  }
  var sval = val;
  if (ZH.IsUndefined(sval._id)) {
    sval     = ZH.clone(val); // NOTE: do not modify VAL directly
    sval._id = key;
  }
  return sval;
}

function LS_set(coll, key, val, next) {
  if (ZH.DatabaseDisabled) return exports.DatabaseDisabled(next);
  //ZH.l('LS_set: key: ' + key + ' val: ' + JSON.stringify(val));
  var sval = get_set_value(val, key);
  exports.Storage.set(coll.name, key, sval);
  next(null, val);
}

function LS_bset(coll, key, val, next) {
  if (ZH.DatabaseDisabled) return exports.DatabaseDisabled(next);
  exports.Storage.bset(coll.name, key, val);
  next(null, val);
}

function LS_set_field(coll, key, field, toval, next) {
  if (ZH.DatabaseDisabled) return exports.DatabaseDisabled(next);
  if (ZH.IsUndefined(toval)) {
    throw(new Error('LS_set_field: toval is UNDEFINED'));
  }
  //ZH.l('LS_set_field: K: ' + key + ' F: ' + field + ' to: ' + toval);
  LS_get(coll, key, function(gerr, gval) {
    var data = (gval.length === 0) ? {} : gval[0];
    ZH.SetByDotNotation(data, field, toval);
    LS_set(coll, key, data, next);
  });
}

function LS_unset_field(coll, key, field, next) {
  if (ZH.DatabaseDisabled) return exports.DatabaseDisabled(next);
  //console.log('LS_unset_field : key: ' + key + ' field: ' + field);
  LS_get(coll, key, function(gerr, gval) {
    var data =  gval[0];
    if (ZH.IsUndefined(data)) data = {};
    ZH.DeleteByDotNotation(data, field);
    LS_set(coll, key, data, next);
  });
}

function LS_increment(coll, key, field, by, next) {
  if (ZH.DatabaseDisabled) return exports.DatabaseDisabled(next);
  field = String(field);
  //console.log('LS_increment: K: ' + key + ' field: ' + field + ' by: ' + by);
  LS_get(coll, key, function(gerr, gval) {
    var data =  gval[0];
    var dval;
    if (ZH.IsUndefined(data)) {
      data = {};
      dval = 0;
    } else {
      dval = ZH.LookupByDotNotation(data, field);
      if (ZH.IsUndefined(dval)) dval = 0;
    }
    dval += by;
    ZH.SetByDotNotation(data, field, dval);
    LS_set(coll, key, data, next);
  });
}

function LS_push(coll, key, aname, pval, next) {
  if (ZH.DatabaseDisabled) return exports.DatabaseDisabled(next);
  //console.log('LS_push: key: ' + key + ' aname: ' + aname + ' pval: ' + pval);
  LS_get(coll, key, function(gerr, gval) {
    var data = gval[0];
    if (ZH.IsUndefined(data))        data        = {};
    if (ZH.IsUndefined(data[aname])) data[aname] = [];
    data[aname].push(pval);
    LS_set(coll, key, data, function(serr, sres) {
      if (serr) next(serr, null);
      else      next(null, pval);
    });
  });
}

function LS_pull(coll, key, aname, pval, next) {
  if (ZH.DatabaseDisabled) return exports.DatabaseDisabled(next);
  //console.log('LS_pull: key: ' + key + ' aname: ' + aname + ' pval: ' + pval);
  LS_get(coll, key, function(gerr, gval) {
    var data = gval[0];
    if (ZH.IsUndefined(data)) next(new Error("key not found"), null);
    else {
      var arr = data[aname];
      if (ZH.IsUndefined(arr)) next(new Error("field not found"), null);
      else {
        var hit = -1;
        for (var i = 0; i < arr.length; i++) {
          if (arr[i] === pval) {
            hit = i;
            break;
          }
        }
        if (hit === -1) next(new Error("element not found"), null);
        else {
          data[aname].splice(hit, 1);
          LS_set(coll, key, data, function(serr, sres) {
            if (serr) next(serr, null);
            else      next(null, pval);
          });
        }
      }
    }
  });
}

function LS_remove(coll, key, next) {
  if (ZH.DatabaseDisabled) return exports.DatabaseDisabled(next);
  //console.log('LS_remove: key: ' + key);
  exports.Storage.remove(coll.name, key);
  next(null, null);
}

function LS_scan(coll, next) {
  if (ZH.DatabaseDisabled) return exports.DatabaseDisabled(next);
  var cprfx   = get_coll_prefix(coll)
  var clen    = cprfx.length;
  var items   = exports.Storage.scan(); // Returns ENTIRE LOCALSTORAGE
  var values = [];
  for (var ikey in items) {
    var cname = ikey.substr(0, clen); // Parse ColumnName from DB-KEY
    if (cname === cprfx) {
      var val = JSON.parse(items[ikey]);
      values.push(val);
    }
  }
  next(null, values);
}

function LS_find(coll, query, next) {
  if (ZH.DatabaseDisabled) return exports.DatabaseDisabled(next);
  var jquery = JSON.parse(query);
  if (jquery._id) LS_get(coll, jquery._id, next);
  else {
    // NOTE: best comment EVER on next line
    //TODO: implement a query engine :)
    LS_scan(coll, next);
  }
}

function LS_store_json(coll, key, val, sep, locex, delta, next) {
  LS_set(coll, key, val, next);
}
function LS_remove_json(coll, key, sep, next) {
  LS_remove(coll, key, next);
}

function LS_store_crdt(coll, key, val, sep, locex, delta, next) {
  LS_set(coll, key, val, next);
}
function LS_remove_crdt(coll, key, sep, next) {
  LS_remove(coll, key, next);
}

function LS_not_supported() {
  throw(new Error("NOT SUPPORTED BY LOCAL STORAGE PLUGIN"));
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EXPORTED PLUGIN -----------------------------------------------------------

exports.Storage = null; // NOTE: export -> MUST BE OVERRIDEN

exports.Plugin = {
  do_open                 : LS_open,
  do_create_collection    : LS_create_collection,
  set_collection_name     : set_collection_name,
  set_datacenter          : set_datacenter,
  set_namespace           : set_namespace,
  do_close                : LS_close,

  do_populate_data        : generic_populate, // NOTE: needs overriding
  do_populate_info        : generic_populate, // NOTE: needs overriding
  do_shutdown             : generic_shutdown, // NOTE: needs overriding

  do_get                  : LS_get,
  do_bget                 : LS_bget,
  do_get_field            : LS_get_field,
  do_set                  : LS_set,
  do_bset                 : LS_bset,
  do_set_field            : LS_set_field,
  do_unset_field          : LS_unset_field,
  do_increment            : LS_increment,
  do_push                 : LS_push,
  do_pull                 : LS_pull,
  do_get_array_values     : LS_get_array_values,
  do_remove               : LS_remove,
  do_scan                 : LS_scan,

  do_store_json           : LS_store_json,
  do_remove_json          : LS_remove_json,
  do_find_json            : LS_find,
  do_store_crdt           : LS_store_crdt,
  do_remove_crdt          : LS_remove_crdt,
  do_get_crdt             : LS_get,

  do_get_directory_members : LS_not_supported,

  ip                      : null, // NOTE: not used
  port                    : null, // NOTE: not used
  namespace               : null,
  collection              : null
};

})(typeof(exports) === 'undefined' ? this['ZGenericPlugin']={} : exports);
