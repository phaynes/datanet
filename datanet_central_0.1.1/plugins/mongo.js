"use strict";

var ZS = require('../zshared');
var ZH = require('../zhelper');

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// MongoDB Plugin ------------------------------------------------------------

var MongoClient = require('mongodb').MongoClient;
var MongoServer = require('mongodb').Server;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SETTINGS ------------------------------------------------------------------

var InfoNamespace = 'ZYNC';


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

function generic_populate(coll, duuid) { }
function generic_shutdown(collection)  { }

function database_disabled(next) {
  next(new Error(ZS.Errors.DatabaseDisabled, null));
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ADMIN OPERATIONS ----------------------------------------------------------

var MDB               = null;                    // Mongo Database
var MConn             = false;                   // Active Connection
var ErrorNoConnection = 'Mongo Connection DOWN'; // Error message
var CollNonExistent   = 'Collection does not exist'; // Error message

function init_opened_mongo_db(mcli, mdb) {
  MConn = true;
  mdb.on('close', function() {
    MConn = false;
    ZH.l('MONGO: DB CLOSE');
    if (mcli._callBackStore) {
      for(var key in mcli._callBackStore._notReplied) {
        mcli._callHandler(key, null, new Error(ErrorNoConnection));
      }
    }
  });
  mdb.on('error', function(err) {
    ZH.l('MONGO: DB ERROR: ' + err);
  });
  mdb.on('reconnect', function() {
    MConn = true;
    ZH.l('MONGO: DB RECONNECT');
  });
}

function get_zns() {
  return exports.Plugin.datacenter + '_' + exports.Plugin.namespace;
}

function get_open_db_name() {
  return exports.Plugin.datacenter + '_' + InfoNamespace;
}

function mdb_open(db_ip, db_port, ns, xname, next) {
  ZH.l('mdb_open: DC: ' + exports.Plugin.datacenter + ' NS: ' + ns);
  if (!exports.Plugin.datacenter) {
    next(new Error('SetDataCenter() must be called before open()'), null);
  } else {
    exports.Plugin.namespace  = ns;
    if (MDB === null) {
      var url = "mongodb://" + db_ip + ":" + db_port + "/" + get_open_db_name();
      ZH.e('Mongo OPEN: url: ' + url);
      MongoClient.connect(url, function(oerr, db) {
        if (oerr) next(oerr, null);
        else {
          MDB   = db;
          MConn = true;
          //init_opened_mongo_db(MCli, MDB);
          next(null, MDB);
        }
      });
    } else {
      next(null, MDB);
    }
  }
}

function create_data_collection(db, cname) {
  var collection        = db.collection(cname);
  // NOTE ZYNC reserves [is_info, cname, ns]
  collection.is_info    = false;
  collection.cname      = cname;
  collection.ns         = exports.Plugin.namespace;
  collection.datacenter = exports.Plugin.datacenter;
  return collection;
}

// NOTE: ALL INFO collections are in single InfoNamespace
function create_info_collection(db, cname) {
  exports.Plugin.namespace = InfoNamespace;
  var db                   = MDB;
  var collection           = db.collection(cname);
  // NOTE ZYNC reserves [is_info, cname, ns]
  collection.is_info       = true;
  collection.cname         = cname;
  collection.ns            = exports.Plugin.namespace;
  collection.datacenter    = exports.Plugin.datacenter;
  return collection;
}

function mdb_create_collection(db, cname, is_info) {
  //ZH.l('mdb_create_collection: CN: ' + cname + ' is_info: ' + is_info);
  return is_info ? create_info_collection(db, cname) :
                   create_data_collection(db, cname);
}

function set_collection_name(cname) {
  ZH.l('Mongo.set_collection_name: ' + cname);
  exports.Plugin.cname = cname;
}

function mdb_close(next) {
  ZH.l('>>>>>>>>> mdb_close');
  MDB.close(next);
}

function set_namespace(ns) {
  ZH.l('mongo.set_namespace: ' + ns);
  exports.Plugin.namespace = ns;
}

function set_datacenter(dc) {
  exports.Plugin.datacenter = dc;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// INFO COLLECTION NAMESPACE SWITCHING ---------------------------------------

function preOpSwitch(coll) {
  var pns                  = exports.Plugin.namespace;
  exports.Plugin.namespace = coll.is_info ? InfoNamespace : coll.ns;
  //ZH.l('MONGO:NS: ' + exports.Plugin.namespace + ' CN: ' + coll.cname);
  var db   = MDB;
  var coll = create_data_collection(db, coll.cname);
  exports.Plugin.namespace = pns;
  //ZH.l('BACK: NS: ' + exports.Plugin.namespace);
  return coll;
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// READ OPERATIONS -----------------------------------------------------------

function __mdb_get(coll, key, next) {
  //ZH.l('mdb_get: key: ' + key);
  coll.find({ _id : key}).toArray(function(err, items) {
    if (err) next(err,  null);
    else     next(null, items);
  });
}
function mdb_get(coll, key, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!MConn)              return next(new Error(ErrorNoConnection), null);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  var tcoll = preOpSwitch(coll);
  __mdb_get(tcoll, key, function(ierr, ires) {
    next(ierr, ires);
  });
}

function __mdb_get_field(coll, key, field, next) {
  //ZH.l('mdb_get_field: key: ' + key + ' field: ' + field);
  var projection     = {_id : 0}; //Mongo-ism: "_id" must be explicitly excluded
  projection[field]  = true;
  coll.find({_id : key}, projection).toArray(function(err, items) {
    if (err) next(err,  null);
    else {
      if (items.length === 0) next(null, null);
      else { // return first value -> this call gets a single field
        for (var keys in items[0]) { 
          next(null, items[0][keys]);
          return;                                               // NOTE: return
        }
        next(null, null);
      }
    }
  });
}
function mdb_get_field(coll, key, field, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!MConn)              return next(new Error(ErrorNoConnection), null);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  var tcoll = preOpSwitch(coll);
  __mdb_get_field(tcoll, key, field, function(ierr, ires) {
    next(ierr, ires);
  });
}

function mdb_get_array_values(coll, key, aname, next) {
  return mdb_get_field(coll, key, aname, next);
}

function __mdb_scan(coll, next) {
  //ZH.l('mdb_scan');
  coll.find({}).toArray(function(err, items) {
    if (err) next(err,  null);
    else     next(null, items);
  });
}
function mdb_scan(coll, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!MConn)              return next(new Error(ErrorNoConnection), null);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  var tcoll = preOpSwitch(coll);
  __mdb_scan(tcoll, function(ierr, ires) {
    next(ierr, ires);
  });
}

function __mdb_query(coll, regex, next) {
  //ZH.l('mdb_query: regex'); console.dir(regex);
  coll.find(regex).toArray(function(err, items) {
    if (err) next(err,  null);
    else     next(null, items);
  });
}

function do_mdb_query(coll, regex, next) {
  var tcoll = preOpSwitch(coll);
  __mdb_query(tcoll, regex, function(ierr, ires) {
    next(ierr, ires);
  });
}

function mdb_find(coll, query, next) {
  //ZH.l('mdb_find: query: ' + query);
  var regex;
  try {
    regex = JSON.parse(query);
  } catch (e) {
    next(new Error('FIND ERROR: ' + e.message), null);
    return;
  }
  do_mdb_query(coll, regex, next);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// WRITE OPERATIONS ----------------------------------------------------------

function __mdb_set(coll, key, val, next) {
  var sval = val;
  if (ZH.IsUndefined(sval._id)) {
    sval     = ZH.clone(val); // NOTE: do not modify VAL directly
    sval._id = key;
  }
  //ZH.l('mdb_set: key: ' + key + ' val: ' + JSON.stringify(sval));
  coll.update({ _id: key }, sval, {upsert : true}, next);
}

function mdb_set(coll, key, val, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!MConn)              return next(new Error(ErrorNoConnection), null);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  var tcoll = preOpSwitch(coll);
  __mdb_set(tcoll, key, val, function(ierr, ires) {
    next(ierr, ires);
  });
}

function __mdb_set_field(coll, key, field, toval, next) {
  //ZH.l('mdb_set_field');
  var set_clause            = {$set : {}};
  set_clause["$set"][field] = toval;
  coll.update({ _id: key }, set_clause, { upsert : true}, function(err, res) {
    if (err) next(err,  null);
    else     next(null, res);
  });
}
function mdb_set_field(coll, key, field, toval, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!MConn)              return next(new Error(ErrorNoConnection), null);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  if (ZH.IsUndefined(toval)) {
    throw(new Error('mdb_set_field: toval is UNDEFINED'));
  }
  var tcoll = preOpSwitch(coll);
  __mdb_set_field(tcoll, key, field, toval, function(ierr, ires) {
    next(ierr, ires);
  });
}

function __mdb_unset_field(coll, key, field, next) {
  //ZH.l('mdb_set_field');
  var set_clause              = {$unset : {}};
  set_clause["$unset"][field] = "";
  coll.update({ _id: key }, set_clause, { upsert : true}, function(err, res) {
    if (err) next(err,  null);
    else     next(null, res);
  });
}
function mdb_unset_field(coll, key, field, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!MConn)              return next(new Error(ErrorNoConnection), null);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  var tcoll = preOpSwitch(coll);
  __mdb_unset_field(tcoll, key, field, function(ierr, ires) {
    next(ierr, ires);
  });
}

function __mdb_increment(coll, key, field, byval, next) {
  //ZH.l('mdb_increment: K: ' + key + ' F: ' + field);
  var set_clause = {$inc : {}};
  set_clause["$inc"][field] = byval;
  coll.findAndModify({ _id: key }, [], set_clause, {new : true, upsert : true},
  function(err, res, details) {
    if (err) next(err,  null);
    else { //NOTE: return res.value
      next(null, res.value);
    }
  });
}
function mdb_increment(coll, key, field, byval, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!MConn)              return next(new Error(ErrorNoConnection), null);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  var tcoll = preOpSwitch(coll);
  __mdb_increment(tcoll, key, field, byval, function(ierr, ires) {
    next(ierr, ires);
  });
}

function __mdb_push(coll, key, aname, pval, next) {
  //ZH.l('mdb_push');
  var set_clause = { "$addToSet" : {}};
  set_clause["$addToSet"][aname] = pval;
  coll.update({ _id: key }, set_clause, {new : true, upsert : true},
  function(err, res, details) {
    if (err) next(err,  null);
    else     next(null, res);
  });
}
function mdb_push(coll, key, aname, pval, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!MConn)              return next(new Error(ErrorNoConnection), null);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  var tcoll = preOpSwitch(coll);
  __mdb_push(tcoll, key, aname, pval, function(ierr, ires) {
    next(ierr, ires);
  });
}

function __mdb_pull(coll, key, aname, pval, next) {
  //ZH.l('mdb_pull');
  var set_clause = { "$pull" : {}};
  set_clause["$pull"][aname] = pval;
  coll.update({ _id: key }, set_clause, { multi: true },
  function(err, res, details) {
    if (err) next(err,  null);
    else     next(null, res);
  });
}
function mdb_pull(coll, key, aname, pval, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!MConn)              return next(new Error(ErrorNoConnection), null);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  var tcoll = preOpSwitch(coll);
  __mdb_pull(tcoll, key, aname, pval, function(ierr, ires) {
    next(ierr, ires);
  });
}

function __mdb_remove(coll, key, next) {
  //ZH.l('mdb_remove');
  coll.remove({ _id: key }, function(err, res) {
    if (err) next(err,  null);
    else     next(null, res);
  });
}
function mdb_remove(coll, key, next) {
  if (ZH.DatabaseDisabled) return database_disabled(next);
  if (!MConn)              return next(new Error(ErrorNoConnection), null);
  if (!coll)               return next(new Error(CollNonExistent),   null);
  var tcoll = preOpSwitch(coll);
  __mdb_remove(tcoll, key, function(ierr, ires) {
    next(ierr, ires);
  });
}

function mdb_store_json(coll, key, val, sep, locex, delta, next) {
  mdb_set(coll, key, val, next);
}

function mdb_remove_json(coll, key, sep, next) {
  mdb_remove(coll, key, next);
}

function mdb_store_crdt(coll, key, val, sep, locex, delta, next) {
  mdb_set(coll, key, val, next);
}

function mdb_remove_crdt(coll, key, sep, next) {
  mdb_remove(coll, key, next);
}

function mdb_not_supported() {
  throw(new Error("NOT SUPPORTED BY MONGO PLUGIN"));
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EXPORTED PLUGIN -----------------------------------------------------------

exports.Plugin = {
  do_open                 : mdb_open,
  do_create_collection    : mdb_create_collection,
  set_collection_name     : set_collection_name,
  set_datacenter          : set_datacenter,
  set_namespace           : set_namespace,
  do_close                : mdb_close,

  do_populate_data        : generic_populate,
  do_populate_info        : generic_populate,
  do_shutdown             : generic_shutdown,

  do_get                  : mdb_get,
  do_get_field            : mdb_get_field,
  do_set                  : mdb_set,
  do_set_field            : mdb_set_field,
  do_unset_field          : mdb_unset_field,
  do_increment            : mdb_increment,
  do_push                 : mdb_push,
  do_pull                 : mdb_pull,
  do_get_array_values     : mdb_get_array_values,
  do_remove               : mdb_remove,
  do_scan                 : mdb_scan,

  do_store_json           : mdb_store_json,
  do_remove_json          : mdb_remove_json,
  do_find_json            : mdb_find,
  do_store_crdt           : mdb_store_json,
  do_remove_crdt          : mdb_remove_crdt,
  do_get_crdt             : mdb_get,

  do_get_directory_members : mdb_not_supported,

  datacenter              : null,
  ip                      : '127.0.0.1',
  port                    : 27017,
  namespace               : null,
  collection              : null,
  name                    : 'MongoDB'
};


