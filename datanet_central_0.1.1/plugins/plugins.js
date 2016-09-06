"use strict";

var ZMongoPlugin, ZRedisPlugin;
var ZLocalStoragePlugin, ZMemoryPlugin, ZMemcachePlugin;
var ZH;
if (typeof(exports) !== 'undefined') {
  ZMongoPlugin        = require('zync/plugins/mongo');
  ZRedisPlugin        = require('zync/plugins/redis');
  ZLocalStoragePlugin = require('zync/plugins/localstorage');
  ZMemoryPlugin       = require('zync/plugins/memory');
  ZMemcachePlugin     = require('zync/plugins/memcache_agent');
  ZH                  = require('zync/zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PLUGINS -------------------------------------------------------------------

exports.GetPlugin = function(name) {
  var plugin;
  if      (name === "REDIS")         plugin = new ZRedisPlugin.Contructor();
  else if (name === "MONGODB")       plugin = ZMongoPlugin.Plugin;
  else if (name === "LOCAL_STORAGE") plugin = ZLocalStoragePlugin.Plugin;
  else if (name === "MEMORY")        plugin = ZMemoryPlugin.Plugin;
  else if (name === "MEMCACHE")      plugin = ZMemcachePlugin.Plugin;
  else throw(new Error('-ERROR: PLUGIN: ' + name + ' NOT SUPPORTED'));
  plugin.name = name;
  return plugin;
}

// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZPlugins']={} : exports);
