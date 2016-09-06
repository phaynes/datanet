
// NOTE: REQUIRES not needed, only used in browser

(function(exports) { // START: Closure -> allows Node.js & Browser to use

"use strict"

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SETTINGS ------------------------------------------------------------------

var CatchLineProcessErrors = false;
CatchLineProcessErrors = true;

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SANITY CHECKS -------------------------------------------------------------

function supports_html5_storage() {
  try {
    return 'localStorage' in window && window['localStorage'] !== null;
  } catch (e) {
    return false;
  }
}

function supports_web_sockets() {
  //return ("WebSocket" in window);
  return ('WebSocket' in window && typeof WebSocket == 'function')
}

if (!supports_html5_storage) {
  alert('Your browser does not support LocalStorage,' + 
        ' Zync can not work on this browser');
}

if (!supports_web_sockets) {
  alert('Your browser does not support WebSockets,' + 
        ' Zync can not work on this browser');
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GLOBALS -------------------------------------------------------------------

// NOTE: OVERRIDE ZCC.CollectionChanged()
ZCC.CollectionChanged = function(cn) {}

var BrowserStorage = new ZH.LStorage();
var DeviceCollName = "LOCAL";
ZH.MyUUID          = BrowserStorage.get(DeviceCollName, "MyDeviceUUID");
if (!ZH.MyUUID) ZH.MyUUID = -1; // AgentOnline call will create a NEW UUID

ZDBP.SetDeviceUUID(ZH.MyUUID);
ZDBP.SetPlugin('LOCAL_STORAGE');
ZH.AmBrowser = true;

ZH.NetworkDebug = true;

ZCC.Initialize();


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// BROWSER DEVICE INITIALIZATION ---------------------------------------------

function display_connection_status_func() {
  ZH.l('display_connection_status_func');
  if (ZS.EngineCallback.DisplayConnectionStatus) {
    ZH.TryBlock(ZS.EngineCallback.DisplayConnectionStatus);
  }
}

function persist_device_info(next) {
  var net = ZH.Agent.net;
  BrowserStorage.set(DeviceCollName, "MyDeviceUUID", ZH.MyUUID);
  ZDBP.PersistDeviceInfo(next);
}

// Device initialization next callback
var NewDeviceNext = null;

// AgentOnline call (device.uuid==-1) response has new device.uuid
ZH.InitNewDevice = function(duuid, next) {
  ZH.l('init_new_browser_device: U: ' + duuid);
  ZH.MyUUID = duuid;
  persist_device_info(function(perr, pres) {
    if (perr) next(perr, null);
    else      ZDBP.AdminConnect(false, next);
  });
}

function start_reapers() {
  ZADaem.StartupAgentToSyncKeys();
  ZADaem.StartAgentDirtyDeltasDaemon();
  ZGCReap.StartAgentGCPurgeDaemon();
}

ZH.FireOnAgentSynced = function() {
  ZH.l('ZH.FireOnAgentSynced');
  ZH.Agent.dconn = true;
  if (NewDeviceNext) { // Z.connect()'s next() - runs ASYNC
    ZH.l('CALLING connect(next)');
    var znext     = NewDeviceNext;
    NewDeviceNext = null;
    znext(null, ZH.Agent.net.zhndl);
    // NOTE: BELOW is ASYNC
    display_connection_status_func();
    start_reapers();
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// BROWSER INIT & CONNECT ----------------------------------------------------

exports.init = function(dc_uuid, dc_hostname, dc_port) {
  ZH.Agent         = this; // Global Handle, Browser has only ONE client
  ZH.Agent.Storage = BrowserStorage;
  ZH.Agent.DisableAgentToSync            = false;
  ZH.Agent.DisableAgentDirtyDeltasDaemon = false;
  ZH.Agent.DisableAgentGCPurgeDaemon     = false;
  ZH.Agent.DisableReconnectAgent         = false;

  ZH.Agent.DisableSubscriberLatencies    = true;

  // NOTE: delta coalescing not supported in C++ -> DISABLE (for now)
  ZH.Agent.DisableDeltaCoalescing        = true;

  var me           = this;
  ZH.Agent.synced  = false;
  ZWss.InitAgent(ZH.Agent);
  if (typeof(dc_hostname) === 'undefined') {
    throw(new Error('DC_Hostname argument missing'));
  }
  if (typeof(dc_port)     === 'undefined') {
    throw(new Error('DC_Port argument missing'));
  }
  ZH.Agent.datacenter           = dc_uuid;
  ZH.Agent.dc_hostname          = dc_hostname;
  ZH.Agent.dc_port              = dc_port;

  ZH.CentralMaster.wss                 = {server : {}};
  ZH.CentralMaster.wss.server.hostname = me.dc_hostname;
  ZH.CentralMaster.wss.server.port     = me.dc_port;
  ZH.CentralMaster.device_uuid         = me.datacenter
  ZH.MyDataCenter                      = ZH.CentralMaster.device_uuid;

  ZH.FireOnAgentConnectionOpen  = display_connection_status_func;
  ZH.FireOnAgentConnectionError = display_connection_status_func;
  ZH.FireOnAgentConnectionClose = display_connection_status_func;
  return this;
}

function init_browser_data(next) {
  ZH.l('init_browser_data');
  var net  = ZH.Agent.net;
  var dkey = ZS.AgentDataCenters;
  net.plugin.do_get_field(net.collections.global_coll, dkey, "geo_nodes",
  function(gerr, geo_nodes) {
    if (gerr) next(gerr, null);
    else {
      if (geo_nodes) {
        ZISL.AgentGeoNodes = ZH.clone(geo_nodes);
        ZH.l('init_agent_data: ZISL.AgentGeoNodes'); ZH.p(ZISL.AgentGeoNodes);
      }
      ZDack.GetAgentAllCreated(net.plugin, net.collections,
      function(gerr, gres) {
        if (gerr) next(gerr, null);
        else {
          if (gres.length !== 0) {
            var createds = gres[0];
            delete(createds._id);
            ZH.l('init_agent_data: CREATEDS'); ZH.p(createds);
            ZH.AgentLastCentralCreated = createds;
          }
          next(null, null);
        }
      });
    }
  });
}

exports.connect = function(ns, next) {
  display_connection_status_func();
  var me = this;
  ZH.l('ZBrowser.connect: U: ' + ZH.MyUUID);
  ZBrowserClient.SetNamespace(ns);
  ZDBP.PluginConnect(ns, function(cerr, zhndl) {
    if (cerr) next(cerr, null);
    else  {
      ZH.Agent.net.zhndl       = zhndl;
      ZH.Agent.net.plugin      = zhndl.plugin;
      ZH.Agent.net.db_handle   = zhndl.db_handle;
      ZH.Agent.net.collections = zhndl.collections;
      if (ZH.MyUUID === -1) {
        ZWss.OpenAgentWss(ZH.OnErrLog);
        NewDeviceNext = next; // Called when AgentOnline gets a new DUUID
      } else {
        ZDBP.AdminConnect(true, function(aerr) {
          if (aerr) next(aerr, null);
          else {
            ZH.Agent.dconn = true;
            ZFix.Init(function(uerr, ures) {
              if (uerr) next(uerr, null);
              else {
                if (!ZH.FixLogActive) ZWss.OpenAgentWss(ZH.OnErrLog);
                init_browser_data(function(ierr, ires) {
                  if (ierr) next(ierr, null);
                  else {
                    next(null, zhndl);
                    // NOTE: BELOW is ASYNC
                    display_connection_status_func();
                    start_reapers();
                  }
                });
              }
            });
          }
        });
      }
    }
  });
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ON-CONNECT-TO-CENTRAL FUNCTION CALLER -------------------------------------

var OnCentralConnectionFunc    = null;
var OnCentralConnectionTimeOut = 100;
function on_central_connection_retry() {
  if (ZH.Agent.synced && ZH.MyUUID !== -1) {
    OnCentralConnectionFunc();
  } else {
    setTimeout(on_central_connection_retry, OnCentralConnectionTimeOut);
  }
}

function on_central_connection(cfunc) {
  OnCentralConnectionFunc = cfunc;
  on_central_connection_retry();
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// BROWSER COMMAND LINE HOOKS ------------------------------------------------

exports.processCommand = function(line) {
  ZCC.ProcessLine(line);
  ZCC.ResetHistoryAndCursor(line);
  document.forms["ZyncCommandForm"]["ZyncCommandText"].value = '';
}

exports.onkeydown = function(event) {
  if (event === undefined) event = window.event; // fix IE
  var ccode = (event.which) ? event.which : event.keyCode
  if        (ccode == 38) {
    var line = ZCC.GetPreviousDisplayHistory();
    document.forms["ZyncCommandForm"]["ZyncCommandText"].value = line;
    return false;
  } else if (ccode == 40) {
    var line = ZCC.GetNextDisplayHistory();
    document.forms["ZyncCommandForm"]["ZyncCommandText"].value = line;
    return false;
  } 
  return true;
}

exports.onkeypress = function(event) {
  var me = this;
  if (event === undefined) event = window.event; // fix IE
  var ccode = (event.which) ? event.which : event.keyCode
  if (ccode === 13) {
    var line = document.forms["ZyncCommandForm"]["ZyncCommandText"].value;
    try {
      me.processCommand(line);
    } catch(e) {
      if (CatchLineProcessErrors) {
        ZH.e('COMMAND ERROR: name: ' + e.name + ' message: ' + e.message);
      } else {
        throw(e);
      }
    }
    return false;
  }
  return true;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// BROWSER EVENT HANDLER REGISTRATION ----------------------------------------

exports.on = function(ename, efunc) {
  switch(ename) {
    case 'connect'                         :
      on_central_connection(efunc);
      break;
    case 'connection_change'               :
      ZS.EngineCallback.DisplayConnectionStatus    = efunc;
      break;
    case 'reconnect'                       :
      ZS.EngineCallback.AgentReconnectedFunc       = efunc;
      break;
    case 'scan_results'                    :
      ZS.EngineCallback.ScanFunc                   = efunc;
      break;
    case 'get_user_subscriptions_results'  :
      ZS.EngineCallback.GetUserSubscriptionsFunc   = efunc;
      break;
    case 'get_stationed_users_results'     :
      ZS.EngineCallback.GetStationedUsersFunc      = efunc;
      break;
    case 'get_agent_subscriptions_results' :
      ZS.EngineCallback.GetAgentSubscriptionsFunc  = efunc;
      break;
    case 'data_change'                     :
      ZS.EngineCallback.DataChange                 = efunc;
      break;
    case 'pre_authorization_data_change'   :
      ZS.EngineCallback.PreAuthorizationDataChange = efunc;
      break;
    case 'preprocess_command'              :
      ZS.EngineCallback.PreprocessCommandFunc      = efunc;
      break;
    case 'key_update'                      :
      ZS.EngineCallback.UpdateKeyFunc              = efunc;
      break;
    case 'collection_change'               :
      ZS.EngineCallback.UpdateCollectionFunc       = efunc;
      break;
    case 'namespace_change'                :
      ZS.EngineCallback.UpdateNamespaceFunc        = efunc;
      break;
    case 'user_change'                     :
      ZS.EngineCallback.ChangeUserFunc             = efunc;
      break;
    case 'message'                         :
      ZS.EngineCallback.SubscriberMessage          = efunc;
      break;
    case 'delta_failure'                   :
      ZS.EngineCallback.DeltaFailure               = efunc;
      break;
  }
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// EXTERNAL HELPERS ----------------------------------------------------------

exports.getIsolation = function() {
  return ZH.Isolation;
}

exports.setWriteStdout = function(wfunc) {
  ZCC.WriteStdout = wfunc;
}
exports.setWriteStderr = function(wfunc) {
  ZCC.WriteStderr = wfunc;
}
exports.setWriteResponse = function(wfunc) {
  ZCC.WriteResponse = wfunc;
}
exports.writeResponse = function(line) {
  ZCC.WriteResponse(line);
}
exports.setWritePrompt = function(wfunc) {
  ZCC.WritePrompt = wfunc;
}

exports.getPrompt = function() {
  return ZCC.Prompt;
}

exports.doPrintCmd = function(zdoc, argv) {
  ZCC.DoPrintCmd(zdoc, argv);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ZCLIENT ABSTRACTION -------------------------------------------------------

exports.isConnected = function() {
  return ZH.Agent.cconn;
}


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['Z']={} : exports);

