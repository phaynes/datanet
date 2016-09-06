"use strict";

var ZPlugins, ZS, ZH;
if (typeof(exports) !== 'undefined') {
  ZPlugins = require('./plugins/plugins');
  ZS       = require('./zshared');
  ZH       = require('./zhelper');
} 

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SETTINGS ------------------------------------------------------------------

var MaxClusterUUID = 10000;


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SETUP ---------------------------------------------------------------------

exports.SetPlugin = function(name) {
  var plugin = ZPlugins.GetPlugin(name);
  if (typeof(plugin) === 'undefined') {
    throw new Error("Plugin not defined");
  }
  this.plugin = plugin;
  ZH.l('ZDBP.SetPlugin: ' + this.plugin.name);
}

// EXPORT:  SetDeviceUUID()
// PURPOSE: External functions can set member: device_uuid
//
exports.SetDeviceUUID = function(uuid) {
  var duuid        = Number(uuid);
  ZH.l('ZDBP.SetDeviceUUID: ' + duuid);
  this.device_uuid = duuid;
  ZH.MyUUID        = duuid;
  if (ZH.Agent !== null) ZH.Agent.device_uuid = duuid;
}

exports.SetDataCenter = function(dc) {
  var me          = this;
  var plugin      = me.plugin;
  me.datacenter   = dc;
  ZH.MyDataCenter = dc;
  plugin.set_datacenter(dc);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CONNECT TO DATABASE -------------------------------------------------------

function create_info_coll(me, duuid, cname, vname) {
  var iccname;
  if (ZH.AmCentral) {
    var gname = ZH.AmRouter ? "RGLOBAL" : "SGLOBAL"; // SHARED DB
    iccname   = ZH.CreateDeviceCollname(gname, cname);
  } else {
    iccname   = ZH.CreateDeviceCollname(duuid, cname);
  }
  me.collections[vname] = me.plugin.do_create_collection(me.db_handle,
                                                         iccname, true);
}

function init_info_collections(me, duuid, next) {
  var a_cname = ZH.CreateDeviceCollname(duuid, ZS.Device.AdminCollName);
  me.collections.admin_coll = me.plugin.do_create_collection(me.db_handle,
                                                             a_cname, true);
  create_info_coll(me, duuid, ZS.Device.UserCollName,           'users_coll');
  create_info_coll(me, duuid, ZS.Device.UserInfoCollName,       'uinfo_coll');
  create_info_coll(me, duuid, ZS.Device.UserAuthCollName,       'uauth_coll');
  create_info_coll(me, duuid, ZS.Device.UserPermsCollName,      'uperms_coll');
  create_info_coll(me, duuid, ZS.Device.UserSubsCollName,       'usubs_coll');
  create_info_coll(me, duuid, ZS.Device.StationedUsersCollName, 'su_coll');
  create_info_coll(me, duuid, ZS.Device.GlobalCollName,         'global_coll');
  create_info_coll(me, duuid, ZS.Device.DeviceToKeyCollName,    'dtok_coll');

  create_info_coll(me, duuid, ZS.Metadata.KeyInfoCollName,      'kinfo_coll');
  create_info_coll(me, duuid, ZS.Metadata.KeyCollName,          'key_coll');
  create_info_coll(me, duuid, ZS.Metadata.GC_CollName,          'gc_coll');
  create_info_coll(me, duuid, ZS.Metadata.GCData_CollName,      'gcdata_coll');
  create_info_coll(me, duuid, ZS.Metadata.LDRCollName,          'ldr_coll');
  create_info_coll(me, duuid, ZS.Metadata.LRUCollName,          'lru_coll');
  create_info_coll(me, duuid, ZS.Metadata.DeltaCollName,        'delta_coll');
  create_info_coll(me, duuid, ZS.Metadata.CrdtCollName,         'crdt_coll');
  create_info_coll(me, duuid, ZS.Metadata.OOODeltaName,         'oood_coll');
  create_info_coll(me, duuid, ZS.Metadata.DeviceKeyName,        'dk_coll');
  next(null, null);
}

// EXPORT:  AdminConnect()
// PURPOSE: Once Device.UUID is finalized, get MyInfo from AdminDB
//
exports.AdminConnect = function(populate, next) {
  var me          = this;
  var plugin      = me.plugin;
  var collections = me.collections;
  var duuid       = me.device_uuid;
  init_info_collections(me, duuid, function(ierr, ires) {
    if (ierr) next(ierr, null);
    else {
      if (populate) plugin.do_populate_info(me, duuid);
      if      (duuid === -1) {           // UNINITIALIZED AGENT or BROWSER
        me.SetDeviceUUID(-1);
        next(null, me);
      } else if (duuid < MaxClusterUUID) { // CENTRAL DEVICE-UUID
        ZH.l('ZDBP.AdminConnect: U: ' + duuid);
        me.SetDeviceUUID(duuid);
        next(null, me);
      } else {                           // AGENT or BROWSER
        var sub  = ZH.CreateSub(duuid);
        var dkey = ZS.GetAdminDevices(sub)
        ZH.l('ZDBP.AdminConnect: U: ' + duuid + ' dkey: ' + dkey);
        plugin.do_get_field(collections.admin_coll, dkey, "NextOpID",
        function(gerr, nopid) {
          if      (gerr) next(gerr, null);
          else if (nopid === null) {
            ZH.l('DeviceUUID: (' + duuid + ') not found in AdminDB');
            me.SetDeviceUUID(-1);
            next(null, me);
          } else {
            next(null, me);
          }
        });
      }
    }
  });
}

exports.PluginConnect = function(ns, next) {
  var me          = this;
  me.plugin.xname = "data";
  me.plugin.set_namespace(ns);
  me.plugin.do_open(me.plugin.ip, me.plugin.port, ns, me.plugin.xname,
  function(cerr, db_hndl) {
    if (cerr) next(cerr, null);
    else {
      me.db_handle   = db_hndl;
      me.collections = {};
      next(null, me);
    }
  });
}

exports.PersistDeviceInfo = function(next) {
  var plugin      = ZH.Agent.net.plugin;
  var admin_cname = ZH.CreateDeviceCollname(ZH.MyUUID, ZS.Device.AdminCollName);
  var admin_coll  = plugin.do_create_collection(ZH.Agent.net.db_handle,
                                                admin_cname, true);
  var akey        = 'Devices_' + ZH.MyUUID;
  var first_op_id = ZH.MyUUID * 100000;
  var a_entry     = {_id      : "Devices_" + ZH.MyUUID,
                     NextOpID : first_op_id
                    };
  plugin.do_set(admin_coll, akey, a_entry, next);
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CLOSE ---------------------------------------------------------------------

exports.Close = function(next) {
  if (ZH.IsDefined(this.plugin)) {
    this.plugin.do_close(next);
  } else {
    next(null, null);
  }
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZDBP']={} : exports);

