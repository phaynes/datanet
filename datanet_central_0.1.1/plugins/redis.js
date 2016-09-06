
var T = require('../ztrace'); // TRACE (before strict)
"use strict";

var redis   = require("redis");
var msgpack = require('msgpack-js');
var lz4     = require('lz4');

var ZCMP = require('../zdcompress');
var ZS   = require('../zshared');
var ZH   = require('../zhelper');

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

function get_coll_prefix(coll) {
  return coll.name + '_';
}

function get_r_key(coll, key) {
  return get_coll_prefix(coll) + key;
}

function get_list_key(coll, key, aname) {
  return get_r_key(coll, key) + '|' + aname;
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SKELETON FOR STORAGE OBJECTS ----------------------------------------------

function RedisStorage() {
  this.set = function(key, value) {
    ZH.l('Redis: set: K: ' + key);
  }
  this.get = function(key) {
    ZH.l('Redis: get: K: ' + key);
  }
  this.scan = function() {
    ZH.l('Redis: scan');
  }
  this.remove = function(key) {
    ZH.l('Redis: remove: K: ' + key);
  }
}

function generic_populate(coll, duuid) { }
function generic_shutdown(collection)  { }

function database_disabled(next) {
  next(new Error(ZS.Errors.DatabaseDisabled, null));
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ADMIN OPERATIONS ----------------------------------------------------------

  // TODO different DBNUMs for different collections
  //exports.databasenum = dc.substring(1);
  //RClient.select(exports.databasenum, function(serr, sres) {

function r_set_namespace(rcli, ns) {
  exports.Plugin.namespace = ns;
}

function r_set_datacenter(rcli, dc) {
  exports.Plugin.datacenter = dc;
}

var RedisClients = {};

function get_redis_client_key(ip, port, xname) {
  return xname + "://" + ip + ":" + port;
}

function get_redis_client(ip, port, xname) {
  var ckey = get_redis_client_key(ip, port, xname);
  return RedisClients[ckey];
}

function r_open(rcli, db_ip, db_port, ns, xname, next) {
  var rcli           = redis.createClient(db_port, db_ip,
                                          {detect_buffers: true});
  var ckey           = get_redis_client_key(db_ip, db_port, xname);
  RedisClients[ckey] = rcli;
  rcli.on("error", ZH.OnErrThrow);
  ZH.l('r_open: IP: ' + db_ip + ' Port: ' + db_port + ' NS: ' + ns);
  exports.Plugin.namespace = ns;
  next(null, rcli);
}

function r_create_collection(rcli, db, cname, is_info) {
  var icname     = is_info ? ZS.InfoNamespace         + '_' + cname : 
                             exports.Plugin.namespace + '_' + cname;
  //ZH.l('r_collection: icname: ' + icname);
  var collection = {name : icname};
  return collection;
}

function r_set_collection_name(rcli, cname) {
  ZH.l('Redis.set_collection_name: ' + cname);
  exports.Plugin.cname = cname;
}

function r_close(rcli, next) {
  ZH.l('>>>>>>>>> r_close');
  rcli.quit(next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DATA CONVERSIONS ----------------------------------------------------------

function convert_j2r(d) {
  return JSON.stringify(d);
}

function convert_object_j2r(data) {
  var rdata = ZH.clone(data);
  for (var k in rdata) {
    rdata[k] = convert_j2r(rdata[k]);
  }
  return rdata;
}

function convert_r2j(d) {
  return JSON.parse(d);
}

function convert_object_r2j(rdata) {
  var data = ZH.clone(rdata);
  for (var k in data) {
    data[k] = convert_r2j(data[k]);
  }
  return data;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// READ OPERATIONS -----------------------------------------------------------

function r_get(rcli, coll, key, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  var rkey = get_r_key(coll, key);
  rcli.hgetall(rkey, function(gerr, rdata) {
    if (gerr) next(gerr, null);
    else {
      if (rdata === null) next(null, []);
      else {
        var data = convert_object_r2j(rdata);
        next(null, [data]);
      }
    }
  });
}

function r_bget(rcli, coll, key, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  var rkey = get_r_key(coll, key);
  rcli.get(new Buffer(rkey), function(gerr, rdata) {
    if (gerr) next(gerr, null);
    else {
      if (rdata === null) next(null, []);
      else                next(gerr, [rdata]);
    }
  });
}

function r_get_field(rcli, coll, key, field, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  var rkey = get_r_key(coll, key);
  rcli.hget(rkey, field, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      //ZH.l('r_get_field: K: ' + rkey + ' F: ' + field);
      var d = convert_r2j(gres);
      next(null, d);
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRITE OPERATIONS ----------------------------------------------------------

function r_set(rcli, coll, key, val, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  var rkey  = get_r_key(coll, key);
  var sval  = val;
  if (ZH.IsUndefined(sval._id)) {
    sval     = ZH.clone(val); // NOTE: do not modify VAL directly
    sval._id = key;
  }
  var rdata = convert_object_j2r(sval);
  //ZH.l('r_set: key: ' + rkey + ' rdata: ' + rdata);
  var multi = rcli.multi();              // MULTI-EXEC STATEMENT
  multi.del  (rkey, ZH.OnErrLog);        // DELETE the immediately
  multi.hmset(rkey, rdata, ZH.OnErrLog); // HMSET (->overwrite HASH)
  multi.exec(function(serr, sres) {
    next(serr, val);
  });
}

function r_bset(rcli, coll, key, val, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  var rkey  = get_r_key(coll, key);
  rcli.set(rkey, val, function(serr, sres) {
    next(serr, val);
  });
}

function r_set_field(rcli, coll, key, field, toval, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  if (ZH.IsUndefined(toval)) {
    throw(new Error('r_set_field: toval is UNDEFINED'));
  }
  var rkey = get_r_key(coll, key);
  var rval = convert_j2r(toval);
  //ZH.l('r_set_field: key: ' + rkey + ' field: ' + field + ' rval: ' + rval);
  rcli.hset(rkey, field, rval, next);
}

function r_unset_field(rcli, coll, key, field, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  var rkey = get_r_key(coll, key);
  rcli.hdel(rkey, field, next);
}

function r_increment(rcli, coll, key, field, by, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  field = String(field);
  var rkey = get_r_key(coll, key);
  rcli.hincrby(rkey, field, by, function(ierr, ires) {
    if (ierr) next(ierr, null);
    else {
      var ret = {}
      ret[field] = ires;
      next(null, ret);
    }
  });
}

function r_get_array_values(rcli, coll, key, aname, next) {
  var lkey = get_list_key(coll, key, aname);
  rcli.lrange(lkey, 0, -1, function(gerr, jarr) {
    if (gerr) next(gerr, null);
    else {
      var data = [];
      for (var i = 0 ; i < jarr.length; i++) {
        data.push(JSON.parse(jarr[i]));
      }
      next(gerr, data);
    }
  });
}

function r_push(rcli, coll, key, aname, pval, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  var lkey = get_list_key(coll, key, aname);
  var jtxt = JSON.stringify(pval);
  //ZH.l('r_push: lkey: ' + lkey + ' jtxt: ' + jtxt);
  rcli.rpush(lkey, jtxt, function(gerr, gval) {
    if (gerr) next(gerr, null);
    else      next(null, pval);
  });
}

function r_pull(rcli, coll, key, aname, pval, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  var lkey = get_list_key(coll, key, aname);
  var jtxt = JSON.stringify(pval);
  rcli.lrem(lkey, 1, jtxt, function(gerr, gval) {
    if (gerr) next(gerr, null);
    else {
      if (!gval) next(null, null);
      else       next(null, pval);
    }
  });
}

function r_remove(rcli, coll, key, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  var rkey = get_r_key(coll, key);
  //console.log('r_remove: key: ' + rkey);
  rcli.del(rkey, next);
}

// NOTE: ONLY CRDTS are stored as STRINGS
function do_scan_get(rcli, coll, key, next) {
  var rkey = get_r_key(coll, key);
  rcli.type(rkey, function(gerr, type) {
    if (gerr) next(gerr, null);
    else {
      if      (type == "hash")   r_get     (rcli, coll, key, next);
      else if (type == "string") r_get_crdt(rcli, coll, key, next);
      else throw(new Error("REDIS DO SCAN GET UNSUPPORTED TYPE"));
    }
  });
}

function get_scan_values(rcli, items, values, coll, cprfx, clen, next) {
  if (items.length === 0) next(null, values);
  else {
    var item  = items.shift();
    var cname = item.substr(0, clen); // Parse ColumnName from DB-KEY
    //ZH.l('r_scan: cname: ' + cname + ' cprfx: ' + cprfx);
    if (cname != cprfx) {
      setImmediate(get_scan_values, rcli, items, values,
                   coll, cprfx, clen, next);
    } else {
      var key = item.substr(clen);    // Parse DATA-KEY from DB-KEY
      do_scan_get(rcli, coll, key, function(gerr, gval) {
        if (gerr) next(gerr, null);
        else {
          var row = gval[0];
          row._id = key; // Mongo-ism
          values.push(row);
          setImmediate(get_scan_values, rcli, items, values,
                       coll, cprfx, clen, next);
        }
      });
    }
  }
}

function __do_r_scan(rcli, cursor, items, next) {
  rcli.scan(cursor, "COUNT", "100", function(serr, sres) {
    if (serr) next(serr, null);
    else {
      cursor  = sres[0];
      var got = sres[1];
      for (var i = 0; i < got.length; i++) {
        items.push(got[i]);
      }
      if (cursor === '0') next(null, items);
      else                __do_r_scan(rcli, cursor, items, next);
    }
  });
}

function do_r_scan(rcli, next) {
  var tn = T.G();
  var cursor = '0';
  var items  = [];
  __do_r_scan(rcli, cursor, items, function(serr, sres) {
    sres.sort();
    T.X(tn);
    next(serr, sres);
  });
}

function r_scan(rcli, coll, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  var cprfx = get_coll_prefix(coll)
  var clen  = cprfx.length;
  do_r_scan(rcli, function(serr, items) { // Returns ENTIRE DB
    if (serr) next(serr, null);
    else {
      var values = [];
      get_scan_values(rcli, items, values, coll, cprfx, clen, next);
    }
  });
}

function r_find(rcli, coll, query, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  var jquery;
  try {
    jquery = JSON.parse(query);
  } catch(e) {
    return next(e, null);
  }
  if (jquery._id) r_get(rcli, coll, jquery._id, next);
  else {
    // NOTE: best comment EVER on next line
    //TODO: implement a query engine :)
    r_scan(rcli, coll, next);
  }
}

function r_store_json(rcli, coll, key, val, sep, locex, delta, next) {
  r_set(rcli, coll, key, val, next);
}

function r_remove_json(rcli, coll, key, sep, next) {
  r_remove(rcli, coll, key, next);
}

function r_store_crdt(rcli, coll, key, crdt, sep, locex, delta, next) {
  ZH.l('r_store_crdt');
  var zcrdt      = ZCMP.CompressCrdt(crdt);
  var encoded    = msgpack.encode(zcrdt);
  var compressed = lz4.encode(encoded);
  r_bset(rcli, coll, key, compressed, next);
}

function r_get_crdt(rcli, coll, key, next) {
  ZH.l('r_get_crdt');
  r_bget(rcli, coll, key, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      if (gres.length === 0) next(null, gres);
      else {
        var compressed   = gres[0];
        var uncompressed = lz4.decode(compressed);
        var zcrdt        = msgpack.decode(uncompressed);
        var crdt         = ZCMP.DecompressCrdt(zcrdt);
        gres[0]          = crdt;
        next(null, gres);
      }
    }
  });
}

function r_remove_crdt(rcli, coll, key, sep, next) {
  r_remove(rcli, coll, key, next);
}

// NOTE: no 'coll' argument
function r_queue_pop(rcli, tnames, to, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  var aarg =[];
  for (var i = 0; i < tnames.length; i++) {
    aarg.push(tnames[i]);
  }
  aarg.push(to);
  //ZH.l('r_queue_pop'); ZH.p(aarg);
  rcli.blpop(aarg, function(gerr, gres) {
    if (gerr) next(gerr, null);
    else {
      var d = {key : gres[0], value : convert_r2j(gres[1])};
      next(null, d);
    }
  });
}

// NOTE: no 'coll' argument
function r_queue_push(rcli, tname, pval, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  var jtxt = JSON.stringify(pval);
  //ZH.l('r_queue_push: tname: ' + tname + ' jtxt: ' + jtxt);
  rcli.rpush(tname, jtxt, function(gerr, gval) {
    if (gerr) next(gerr, null);
    else      next(null, pval);
  });
}

// NOTE: no 'coll' argument
function r_queue_unshift(rcli, tname, pval, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  var jtxt = JSON.stringify(pval);
  //ZH.l('r_queue_unshift: tname: ' + tname + ' jtxt: ' + jtxt);
  rcli.lpush(tname, jtxt, function(gerr, gval) {
    if (gerr) next(gerr, null);
    else      next(null, pval);
  });
}

function r_sorted_add(rcli, coll, key, val, vname, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  var rkey = get_r_key(coll, key);
  //ZH.l('r_sorted_add: rkey: ' + rkey + ' val: ' + val + ' vname: ' + vname);
  var args = [rkey, val, vname];
  rcli.zadd(args, next);
}

function r_sorted_remove(rcli, coll, key, vname, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  var rkey = get_r_key(coll, key);
  //ZH.l('r_sorted_remove: rkey: ' + rkey + ' vname: ' + vname);
  rcli.zrem(rkey, vname, next);
}

function r_sorted_range(rcli, coll, key, vmin, vmax, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  var rkey = get_r_key(coll, key);
  //ZH.l('r_sorted_fange: rkey: ' + rkey + ' vmin: ' + vmin + ' vmax: ' + vmax);
  var args = [rkey, vmin, vmax];
  rcli.zrangebyscore(args, next);
}

function r_not_supported() {
  throw(new Error("NOT SUPPORTED BY REDIS PLUGIN"));
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EXPORTED PLUGIN -----------------------------------------------------------

exports.Storage = new RedisStorage(); // NOTE: export -> MUST BE OVERRIDEN

exports.Plugin = {
  do_open                 : r_open,
  do_create_collection    : r_create_collection,
  set_collection_name     : r_set_collection_name,
  set_datacenter          : r_set_datacenter,
  set_namespace           : r_set_namespace,
  do_close                : r_close,

  do_populate_data        : generic_populate, // NOTE: needs overriding
  do_populate_info        : generic_populate, // NOTE: needs overriding
  do_shutdown             : generic_shutdown, // NOTE: needs overriding

  do_get                  : r_get,
  do_bget                 : r_bget,
  do_get_field            : r_get_field,
  do_set                  : r_set,
  do_bset                 : r_bset,
  do_set_field            : r_set_field,
  do_unset_field          : r_unset_field,
  do_increment            : r_increment,
  do_push                 : r_push,
  do_pull                 : r_pull,
  do_get_array_values     : r_get_array_values,
  do_remove               : r_remove,
  do_scan                 : r_scan,

  do_store_json           : r_store_json,
  do_find_json            : r_find,
  do_remove_json          : r_remove_json,

  do_store_crdt           : r_store_crdt,
  do_get_crdt             : r_get_crdt,
  do_remove_crdt          : r_remove_crdt,

  do_queue_push           : r_queue_push,
  do_queue_unshift        : r_queue_unshift,
  do_queue_pop            : r_queue_pop,

  do_sorted_add           : r_sorted_add,
  do_sorted_remove        : r_sorted_remove,
  do_sorted_range         : r_sorted_range,

  ip                      : null,
  port                    : null,
  namespace               : null,
  collection              : null,
  name                    : 'Redis'
};

/* NOTE: the following two functions wrap() and exports.Contructor()
         are a very UGLY HACK to turn exports.Plugin[] into a class
         in order to support connections to multiple redis DBs
*/
function wrap(p, args, fname) {
  var rcli = get_redis_client(p.ip, p.port, p.xname);
  var func = exports.Plugin[fname];
  return func(rcli, args[0], args[1], args[2], args[3],
                    args[4], args[5], args[6]);
}

exports.Contructor = function() {
  this.do_open              = function() {
                 return wrap(this, arguments, "do_open"); }
  this.do_create_collection = function() {
                 return wrap(this, arguments, "do_create_collection"); }
  this.set_collection_name  = function() {
                 return wrap(this, arguments, "set_collection_name"); }
  this.set_datacenter       = function() {
                 return wrap(this, arguments, "set_datacenter"); }
  this.set_namespace        = function() {
                 return wrap(this, arguments, "set_namespace"); }
  this.do_close             = function() {
                 return wrap(this, arguments, "do_close"); }
  this.do_populate_data     = function() {
                 return wrap(this, arguments, "do_populate_data"); }
  this.do_populate_info     = function() {
                 return wrap(this, arguments, "do_populate_info"); }
  this.do_shutdown          = function() {
                 return wrap(this, arguments, "do_shutdown"); }
  this.do_get               = function() {
                 return wrap(this, arguments, "do_get"); }
  this.do_get_field         = function() {
                 return wrap(this, arguments, "do_get_field"); }
  this.do_set               = function() {
                 return wrap(this, arguments, "do_set"); }
  this.do_set_field         = function() {
                 return wrap(this, arguments, "do_set_field"); }
  this.do_unset_field       = function() {
                 return wrap(this, arguments, "do_unset_field"); }
  this.do_increment         = function() {
                 return wrap(this, arguments, "do_increment"); }
  this.do_push              = function() {
                 return wrap(this, arguments, "do_push"); }
  this.do_pull              = function() {
                 return wrap(this, arguments, "do_pull"); }
  this.do_get_array_values  = function() {
                 return wrap(this, arguments, "do_get_array_values"); }
  this.do_remove            = function() {
                 return wrap(this, arguments, "do_remove"); }
  this.do_scan              = function() {
                 return wrap(this, arguments, "do_scan"); }
  this.do_store_json        = function() {
                 return wrap(this, arguments, "do_store_json"); }
  this.do_remove_json       = function() {
                 return wrap(this, arguments, "do_remove_json"); }
  this.do_find_json         = function() {
                 return wrap(this, arguments, "do_find_json"); }
  this.do_store_crdt        = function() {
                 return wrap(this, arguments, "do_store_crdt"); }
  this.do_remove_crdt       = function() {
                 return wrap(this, arguments, "do_remove_crdt"); }
  this.do_get_crdt          = function() {
                 return wrap(this, arguments, "do_get_crdt"); }

  this.do_queue_push        = function() {
                 return wrap(this, arguments, "do_queue_push"); }
  this.do_queue_unshift     = function() {
                 return wrap(this, arguments, "do_queue_unshift"); }
  this.do_queue_pop         = function() {
                 return wrap(this, arguments, "do_queue_pop"); }


  this.do_sorted_add        = function() {
                 return wrap(this, arguments, "do_sorted_add"); }
  this.do_sorted_remove     = function() {
                 return wrap(this, arguments, "do_sorted_remove"); }
  this.do_sorted_range      = function() {
                 return wrap(this, arguments, "do_sorted_range"); }

  this.do_get_directory_members = function() { return r_not_supported(); }
}
  
  
