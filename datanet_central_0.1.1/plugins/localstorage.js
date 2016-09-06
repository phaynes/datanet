"use strict";

var ZGenericPlugin, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  ZGenericPlugin = require('zync/plugins/generic');
  ZH             = require('../zhelper');
}

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EXPORTED PLUGIN -----------------------------------------------------------

ZGenericPlugin.Storage = new ZH.LStorage();
exports.Plugin         = ZH.copy_members(ZGenericPlugin.Plugin);
exports.Plugin.url     = 'LocalStorage'; // NOTE: not used
exports.Plugin.name    = 'LocalStorage';


})(typeof(exports) === 'undefined' ? this['ZLocalStoragePlugin']={} : exports);
