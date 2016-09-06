
-- INCLUDE DATANET C++ SHARED-OBJECT
local cpath   = ngx.config.prefix() .. '../c_client/?.so';
package.cpath = package.cpath .. ';' .. cpath;

-- GET DATANET CONFIG DIRECTORY -> set in nginx.conf -> enables multi-tenancy
local sconfig, conf_dname, conf_fname, err;
local dinfo  = ngx.shared.DATANET_INFO;
sconfig, err = dinfo:get("Config");
if (sconfig ~= nil) then -- NOT Config, RATHER: [ConfigDirectory + ConfigFile]
  ngx.log(ngx.ERR, "USING DATANET_INFO CONFIG");
  ngx.log(ngx.ERR, "SCONFIG: " .. sconfig);
else
  conf_dname, err = dinfo:get("ConfigDirectory");
  if (err) then
    ngx.log(ngx.ERR, "FAILED TO GET: ngx.shared.DATANET_INFO: ConfigDirectory");
    return;
  end
  conf_fname, err = dinfo:get("ConfigFile");
  if (err) then
    ngx.log(ngx.ERR, "FAILED TO GET: ngx.shared.DATANET_INFO: ConfigFile");
    return;
  end
end

local Config;
if (sconfig ~= nil) then
  Config = {config = cjson.decode(sconfig)} ;
else
  package.path = package.path .. ";" .. conf_dname .. "?.lua;";
  Config = require(conf_fname);
end

-- INCLUDE STANDARD DATANET LIBRARIES
require "datanet.wrapper";
local Tables         = require "datanet.tables";
local Helper         = require "datanet.helpers";
local Network        = require "datanet.network";
local Collection     = require "datanet.collection";
local Document       = require "datanet.document";
local Initialization = require "datanet.initialization";
local Actions        = require "datanet.actions";
local Frontend       = require "datanet.frontend";
local Subscriber     = require "datanet.subscriber";
local Replication    = require "datanet.replication";
local StartWorker    = require "datanet.start_worker";
local WorkerDaemon   = require "datanet.daemon";
local Debug          = require "datanet.debug";
CppHook              = require "datanet.cpp_hook"; -- GLOBAL (called from C++)
require "datanet_memory_lmdb_engine";

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- WORKER DEAMON OVERRIDES ---------------------------------------------------

local UUID_file = "UUID";

WorkerDaemon.OnPrimaryElected = function(self)
  LD("WorkerDaemon:OnPrimaryElected()");
  StartWorker:StartPrimary();
end

WorkerDaemon.OnDeadWorker = function(self, wid)
  c_notify_dead_worker(wid);
  StartWorker:PrimarySendAgentOnline();
end

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- CLIENT FACING API ---------------------------------------------------------

local _D   = {};
_D.__index = _D;

local err = Initialization:SetConfig(Config.config, Tables.names);
if (err ~= nil) then
  ngx.log(ngx.CRIT, "FAILURE: DATANET: Initialization: " .. err);
  return;
end

local LogLevel = WorkerDaemon.config.log.level;
if     (LogLevel == "TRACE")  then LogNumber = 0;
elseif (LogLevel == "NOTICE") then LogNumber = 1;
else            --[[ ERROR --]]    LogNumber = 2;
end

local ClusterActivated = (WorkerDaemon.config.cluster ~= nil);

local function reset_auth()
  Helper:SetAuthentication(WorkerDaemon.config.default.username,
                           WorkerDaemon.config.default.password);
end

function _D.reset_authentication(self)
  reset_auth();
end

function _D.get_config(self)
  return WorkerDaemon.config;
end

function _D.initialize_master(self) -- NOTE: Used in init_by_lua
  Initialization:InitializeMaster();
end

function _D.initialize_worker(self) --NOTE: Used in init_worker_by_lua
  Initialization:InitializeWorker();
end

function _D:do_replication(self)
  Replication:ReplicateWorkerCommits();
end

function _D.start_worker(self) --NOTE: Used in init_worker_by_lua
  StartWorker:Go();
end

function _D.collection(self, cn)
  return Collection:new(cn);
end

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- CLIENT NON-KQK CALLS ------------------------------------------------------

if (Debug.AllowAdminMethods) then
  function _D.grant_user(self, username, schanid, priv)
    if (username == nil or schanid == nil or priv == nil) then
      return nil, "ERROR: MISSING ARGS: (username, channel, privilege[R,W])";
    end
    return Actions:AdminGrantUser(username, schanid, priv);
  end
end

function _D.switch_user(self, username, password)
  if (username == nil or password == nil) then
    return nil, "ERROR: MISSING ARGS: (username, password)";
  end
  Helper:SetAuthentication(username, password);
  return Actions:ClientSwitchUser(username, password);
end

function _D.is_user_stationed(self, username)
  if (username == nil) then
    return nil, "ERROR: MISSING ARGS: (username)";
  end
  return Actions:ClientIsUserStationed(username);
end

function _D.station_user(self)
  local err = Helper:CheckAuthentication();
  if (err) then return nil, err; end
  return Actions:ClientStationUser();
end

function _D.destation_user(self)
  local err = Helper:CheckAuthentication();
  if (err) then return nil, err; end
  return Actions:ClientDestationUser();
end

function _D.is_subscribed(self, schanid)
  if (schanid == nil) then
    return nil, "ERROR: MISSING ARGS: (channel)";
  end
  return Actions:ClientIsSubscribed(schanid);
end

function _D.subscribe(self, schanid)
  if (schanid == nil) then
    return nil, "ERROR: MISSING ARGS: (channel)";
  end
  return Actions:ClientSubscribe(schanid);
end

function _D.unsubscribe(self, schanid)
  if (schanid == nil) then
    return nil, "ERROR: MISSING ARGS: (channel)";
  end
  return Actions:ClientUnsubscribe(schanid);
end

function _D.heartbeat(self, cmd, field, uuid, mlen, trim, isi, isa)
  local cname      = "statistics";
  local collection = self:collection(cname);
  return collection:heartbeat(cmd, field, uuid, mlen, trim, isi, isa);
end

function _D.isolation(self, isolate)
  if (isolate == nil) then
    return nil, "ERROR: MISSING ARGS: (isolate)";
  end
  return Actions:ClientIsolation(isolate);
end

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- EXTERNAL CALLS ------------------------------------------------------------

function _D.frontend_call(self, pbody)
  reset_auth();
  Frontend:AgentCall(pbody);
end

function _D.subscriber_call(self, pbody)
  reset_auth();
  Subscriber:SubscriberCall(pbody);
end

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- HELPERS -------------------------------------------------------------------

function _D.generic_handle_response(self, ret, err)
  if (err ~= nil) then
    if (type(err) == "string") then
      ngx.say('ERROR: ' .. err);
    else 
      local serr = cjson.encode(err);
      ngx.say('ERROR: ' .. serr);
    end
  else
    ngx.say("OK");
  end
end

function _D.read_full_post_body(self)
  ngx.req.read_body()
  local rbody = ngx.req.get_body_data()
  if (not rbody) then
    local rfname = ngx.req.get_body_file()
    if (rfname) then
      local file = io.open(rfname)
      rbody = file:read("*a")
      file:close()
    end
  end
  return rbody;
end

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- SINGLE REQUIRE (DATANET) --------------------------------------------------

_D.Helper   = Helper;
_D.Network  = Network;
_D.Document = Document;

return _D;
