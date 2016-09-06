"use strict";

var fs      = require('fs');
var msgpack = require('msgpack-js');
var lz4     = require('lz4');

var ZGenericPlugin = require('zync/plugins/generic');
var ZCMP           = require('../zdcompress');
var ZS             = require('../zshared');
var ZH             = require('../zhelper');

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// HELPERS -------------------------------------------------------------------

function parse_text_file_to_json(file) {
  var text;
  try {
    text = fs.readFileSync(file, 'utf8');
  } catch (e) { // File does not exist
    return null;
  }
  var json;
  try {
    json = JSON.parse(text);
  } catch (e) {
    throw(new Error('JSON PARSE ERROR: ' + e));
  }
  return json;
}

// KEY FORMATS:
//   1.) INFO: [Namespace]_D{duuid}_{SubCollName}_{KEY}
//             e.g. datastore_D0_ZyncAdmin_replication_channel_1 
//   2.) DATA: [Namespace]_D{duuid}_{SubCollName}_{OrigCollName}_{KEY}
//             e.g. datastore_D0_ZyncCrdt_testdata_User777
//
function parse_is_info(key) {
  var res = key.split("_");
  return (res[0] === "INFO");
}

function parse_metadata_collection_name(key) {
  var res = key.split("_");
  return res[3];
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// MEMORY STORAGE ------------------------------------------------------------

var Raw = {};

function MemoryStorage() {
  this.bset = function(cname, key, value) {
    var mkey = cname + "_" + key;
    Raw[mkey] = value; // NOTE: no serialization (HOG-WILD)
  }
  this.bget = function(cname, key) {
    var mkey = cname + "_" + key;
    return Raw[mkey]; // NOTE: no deserialization (HOG-WILD)
  }
  //TODO set/get() MAKE MEMORY COPIES -> NOT DE/SERIALIZE
  this.set = function(cname, key, value) {
    var mkey = cname + "_" + key;
    //ZH.e('MemoryStorage: set: K: ' + mkey); ZH.l('value'); ZH.p(value);
    Raw[mkey] = JSON.stringify(value);
  }
  this.get = function(cname, key) {
    var mkey   = cname + "_" + key;
    var jvalue = Raw[mkey];
    //ZH.e('MemoryStorage: get: K: ' + mkey); ZH.l('jvalue: ' + jvalue);
    if (!jvalue) return null;
    var value;
    try {
      value = JSON.parse(jvalue);
    } catch(e) {
      return null;
    }
    //ZH.l('value'); ZH.p(value);
    return value;
  }
  this.scan = function() {
    return Raw;
  }
  this.remove = function(cname, key) {
    var mkey = cname + "_" + key;
    //ZH.l('MemoryStorage: remove: K: ' + key);
    delete(Raw[mkey]);
  }
}

exports.ConstructMemoryStorage = function() {
  return new MemoryStorage();
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// DATABASE PERSISTENCE (ON SHUTDOWN/STARTUP) --------------------------------

function get_file_name(ftype, duuid) {
  return './data/memory_dbs/' + ftype + '_' + duuid + '.json';
}

exports.PopulateInfo = function(collections, duuid) {
  var db_md_file = get_file_name('metadata', duuid);
  ZH.e('PopulateInfo: U: ' + duuid + ' F: ' + db_md_file);
  var json       = parse_text_file_to_json(db_md_file);
  for (var mkey in json) {
    Raw[mkey] = JSON.stringify(json[mkey]);
  }
}

exports.PopulateData = function(n, duuid) {
  var db_data_file = get_file_name('data', duuid);
  ZH.e('PopulateData: U: ' + duuid + ' F: ' + db_data_file);
  var json         = parse_text_file_to_json(db_data_file);
  for (var mkey in json) {
    Raw[mkey] = JSON.stringify(json[mkey]);
  }
}

exports.Shutdown = function(collections) {
  if (!exports.Plugin.SaveDataOnShutdown) return;
  var duuid        = ZH.MyUUID;
  var db_md_file   = get_file_name('metadata', duuid);
  var db_data_file = get_file_name('data',     duuid);
  ZH.e('SHUTDOWN DUMP: Metadata: ' + db_md_file);
  ZH.e('SHUTDOWN DUMP: Data: ' + db_data_file);
  var md   = {};
  var data = {};
  for (var key in Raw) {
    //TODO CRDTs are BINARY -> TRANSFORM TO PARSE-JSON
    var is_info = parse_is_info(key);
    if (is_info) {
      //ZH.l('METADATA: key: ' + key);
      md[key]   = JSON.parse(Raw[key]);
    } else {
      //ZH.l('DATA: key: ' + key);
      data[key] = JSON.parse(Raw[key]);
    }
  }
  fs.writeFileSync(db_md_file,   JSON.stringify(md),   'utf8');
  fs.writeFileSync(db_data_file, JSON.stringify(data), 'utf8');
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CRDT STORE/GET OVERRIDE ---------------------------------------------------

function memory_store_crdt(coll, key, crdt, sep, locex, delta, next) {
  ZH.l('memory_store_crdt: K: ' + key);
  var zcrdt      = ZCMP.CompressCrdt(crdt);
  var encoded    = msgpack.encode(zcrdt);
  var compressed = lz4.encode(encoded);
  exports.Plugin.do_bset(coll, key, compressed, next);
}

function memory_get_crdt(coll, key, next) {
  ZH.l('memory_get_crdt: K: ' + key);
  exports.Plugin.do_bget(coll, key, function(gerr, gres) {
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


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EXPORTED PLUGIN -----------------------------------------------------------

ZGenericPlugin.Storage            = exports.ConstructMemoryStorage();
exports.Plugin                    = ZH.copy_members(ZGenericPlugin.Plugin);
exports.Plugin.url                = 'Memory'; // NOTE: not used
exports.Plugin.name               = 'Memory';

exports.Plugin.do_populate_data   = exports.PopulateData;
exports.Plugin.do_populate_info   = exports.PopulateInfo;
exports.Plugin.do_shutdown        = exports.Shutdown;
exports.Plugin.SaveDataOnShutdown = true;

exports.Plugin.do_store_crdt      = memory_store_crdt;
exports.Plugin.do_get_crdt        = memory_get_crdt;

})(typeof(exports) === 'undefined' ? this['ZMemoryPlugin']={} : exports);
