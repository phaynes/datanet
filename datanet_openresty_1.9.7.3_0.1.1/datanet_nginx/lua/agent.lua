
local Helper        = require "datanet.helpers";
local Network       = require "datanet.network";
local Authorization = require "datanet.authorization";
local KeyPartition  = require "datanet.key_partition";
local InternalCache = require "datanet.internal_cache";
local WorkerDaemon  = require "datanet.daemon";
local ClientMethod  = require "datanet.client_method";
local Debug         = require "datanet.debug";

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLASS DECLARATION --------------------------------------------------------

local _A   = {};
_A.__index = _A;

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- HEARTBEAT START ----------------------------------------------------------

local function do_run_worker(self, kqk, jreq)
  local config        = WorkerDaemon.config;
  local sus, ism, err = KeyPartition:GetStickyUnixSocketKqk(config, kqk);
  if (err ~= nil) then return nil, err; end
  if (ism) then return self:InternalAgentCall(jreq, nil);
  else          return self:AgentHttpRequest(sus, jreq, nil);
  end
end

local function do_heartbeat_start(self, jreq)
  LT('do_heartbeat_start');
  local pdata    = jreq.params.data;
  local isi      = pdata.is_increment;
  local isa      = pdata.is_array;
  local ns       = "production";
  local cn       = "statistics";
  local key      = isi and "INCREMENT_HEARTBEAT" or
                   isa and "ARRAY_HEARTBEAT"     or
                           "TIMESTAMP_HEARTBEAT"
  local kqk      = Helper:CreateKqk(ns, cn, key);
  local pin      = false;
  local watch    = false;
  local sticky   = false;
  -- NOTE: FIRST CACHE (KQK-WORKER)
  local cjreq    = Helper:CreateClientCacheRequest(ns, cn, key,
                                                   pin, watch, sticky);
  local res, err = do_run_worker(self, kqk, cjreq);
  -- NOTE: IGNORE ERROR ('NO DATA FOUND' is OK)
  if (err ~= nil) then LE(cjson.encode(err)); end -- IGNORE ERROR
  local cmd      = "START";
  local field    = pdata.field;
  local uuid     = pdata.uuid;
  local mlen     = pdata.max_size;
  local trim     = pdata.trim;
  -- NOTE: SECOND START HEARTBEAT (KQK-WORKER)
  local ijreq    = Helper:CreateInternalHeartbeatRequest(ns, cn, key, cmd,
                                                         field, uuid, mlen,
                                                         trim, isi, isa);
  return do_run_worker(self, kqk, ijreq);
end

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLIENT MAP METHODS -------------------------------------------------------

local function run_agent_internal_client_method(self, jreq, jbody)
  LT('run_agent_internal_client_method');
  local id     = jreq.id;
  local method = jreq.method;
  local username, password, schanid;
  if     (method == "ClientSwitchUser"  or
          method == "ClientStationUser" or
          method == "ClientDestationUser") then
    username = jreq.params.authentication.username;
    password = jreq.params.authentication.password;
  elseif (method == "ClientSubscribe" or
          method == "ClientUnsubscribe") then
    schanid = jreq.params.data.channel.id;
  end

  if     (method == "ClientPing") then
    local res = {status = "OK"};
    return res, nil;
  elseif (method == "ClientHeartbeat") then
    local pdata = jreq.params.data;
    local cmd   = string.upper(pdata.command);
    if (cmd == "START") then
      return do_heartbeat_start(self, jreq);
    else
      return Helper:RunLocalClientMethod(jbody);
    end
  elseif (method == "GetInternalAgentSettings") then
    c_broadcast_workers_settings(); -- PRIMARY BROADCAST SETTINGS TO ALL WORKERS
    local res = {status = "OK"};
    return res, nil;
  elseif (method == "InternalAgentSettings") then
    local pdata = jreq.params.data;
    if (pdata.device and pdata.device.key) then
      Helper:SetDeviceKey(method, pdata.device.key);
    end
    if (pdata.central and pdata.central.wss) then
      local config = WorkerDaemon.config;
      local mhost  = pdata.central.wss.server.hostname;
      local mport  = pdata.central.wss.server.port;
      Helper:ResetCentral(config, mhost, mport);
    end
    return Helper:RunLocalClientMethod(jbody);
  elseif (method == "ClientSwitchUser") then
    Helper:SetAuthentication(username, password);
    local res, miss, err =  Authorization:DoAuthorize("BASIC", nil, nil, nil);
    return res, err;
  elseif (method == "DocumentCacheDelta") then
    InternalCache:HandleDocumentCacheDelta(jreq);
    local res = {status = "OK"};
    return res, nil;
  elseif (method == "ClientStationUser") then
    Helper:SetAuthentication(username, password);
    local jreq     = Helper:CreateAgentStationUserRequest();
    return self:InternalAgentCall(jreq, nil);
  elseif (method == "ClientDestationUser") then
    Helper:SetAuthentication(username, password);
    local jreq     = Helper:CreateAgentDestationUserRequest();
    return self:InternalAgentCall(jreq, nil);
  elseif (method == "ClientSubscribe") then
    local jreq    = Helper:CreateAgentSubscribeRequest(schanid);
    return self:InternalAgentCall(jreq, nil);
  elseif (method == "ClientUnsubscribe") then
    local jreq    = Helper:CreateAgentUnsubscribeRequest(schanid);
    return self:InternalAgentCall(jreq, nil);
  else
    return Helper:RunLocalClientMethod(jbody);
  end
end

local function run_agent_local_client_method(self, jreq, jbody)
  LT('run_agent_local_client_method');
  local id  = jreq.id;
  local err = Authorization:BasicAuthorize();
  if (err ~= nil) then return nil, err; end
  return Helper:RunLocalClientMethod(jbody);
end

local function run_agent_primary_client_method(self, jreq, jbody)
  LT('run_agent_primary_client_method');
  local config = WorkerDaemon.config;
  local id     = jreq.id;
  local method = jreq.method;
  local err    = nil;
  if (method == "AgentSubscribe") then
    local schanid = jreq.params.data.channel.id;
    err           = Authorization:SubscribeAuthorize(schanid);
  else
    err           = Authorization:BasicAuthorize();
  end
  if (err ~= nil) then return nil, err; end
  return Network:SendCentralHttpsRequest(config, jbody);
end

local function run_agent_worker_client_method(self, jreq, jbody)
  LT('run_agent_worker_client_method');
  local config           = WorkerDaemon.config;
  local id               = jreq.id;
  local method           = jreq.method;
  local kqk, rchans, err = Helper:CreateKqkFromWorkerCall(config, jreq);
  if (err ~= nil) then return nil, err; end;
  if     (method == "ClientRemove") then
    err = Authorization:BasicAuthorize();
  elseif (method == "ClientFetch" or method == "ClientFind") then
    err = Authorization:KeyReadAuthorize(kqk);
  elseif (method == "ClientStore") then
    err = Authorization:KeyStoreAuthorize(kqk, rchans);
  elseif (method == "InternalHeartbeat") then
    if (rchans == nil) then rchans = {"0"}; end -- VIRGIN START HEARTBEAT
    err = Authorization:KeyStoreAuthorize(kqk, rchans);
  elseif (method == "ClientCommit" or method == "ClientExpire") then
    err = Authorization:KeyCommitAuthorize(kqk);
  end
  if (err ~= nil) then return nil, err; end;
  return Helper:RunLocalClientMethod(jbody);
end

local function run_agent_cache_client_method(self, jreq, jbody)
  LT('run_agent_cache_client_method');
  local id        = jreq.id;
  local ns        = jreq.params.data.namespace;
  local cn        = jreq.params.data.collection
  local key       = jreq.params.data.key;
  local pin       = jreq.params.data.pin;
  local watch     = jreq.params.data.watch;
  local sticky    = jreq.params.data.sticky;
  local err       = Authorization:BasicAuthorize();
  if (err ~= nil) then return nil, err; end;
  local res, err  = Helper:RunLocalClientMethod(jbody);
  if (err ~= nil) then return nil, err; end;
  local miss  = res.miss;
  if (not miss) then
    return res, nil;
  else
    local config    = WorkerDaemon.config;
    local abody     = Helper:CreateAgentCacheJRB(jreq);
    local cres, err = Network:SendCentralHttpsRequest(config, abody);
    if (err ~= nil) then return nil, err; end;
    local md        = cres.merge_data;
    md.json         = Helper:CreatePrettyJson(md.crdt);
    return md, nil;
  end
end

local function run_agent_evict_client_method(self, jreq, jbody)
  LT('run_agent_evict_client_method');
  local config = WorkerDaemon.config;
  local id     = jreq.id;
  local method = jreq.method;
  local ns     = jreq.params.data.namespace;
  local cn     = jreq.params.data.collection
  local key    = jreq.params.data.key;
  local err    = Authorization:BasicAuthorize();
  if (err ~= nil) then return nil, err; end;
  local abody;
  if (method == "ClientEvict") then
    abody = Helper:CreateAgentEvictJRB(ns, cn, key);
  else --        ClientLocalEvict
    abody = Helper:CreateAgentLocalEvictJRB(ns, cn, key);
  end
  return Network:SendCentralHttpsRequest(config, abody);
end

local ClientMethodRunFunctions = {run_agent_internal_client_method,
                                  run_agent_local_client_method,
                                  run_agent_primary_client_method,
                                  run_agent_worker_client_method,
                                  run_agent_cache_client_method,
                                  run_agent_evict_client_method};

function _A.AgentHttpRequest(self, sus, jreq, jbody)
  LT('Agent:AgentHttpRequest: SUS: ' .. sus);
  local id = jreq.id;
  if (jbody == nil) then
    jbody = cjson.encode(jreq);
  end
  return Network:LocalHttpRequest(sus, jbody);
end

function _A.InternalAgentCall(self, jreq, jbody)
  local method = jreq.method;
  if (jbody == nil) then
    jbody = cjson.encode(jreq);
  end
  local spot = ClientMethod.Map[method];
  if (Debug.FullAgent) then
    LD('Agent:InternalAgentCall: method: ' .. method ..
       ' spot: ' .. tostring(spot) .. ' BODY: ' .. jbody);
  else
    LT('Agent:InternalAgentCall: method: ' .. method);
  end
  local f = ClientMethodRunFunctions[spot];
  return f(self, jreq, jbody);
end

return _A;
