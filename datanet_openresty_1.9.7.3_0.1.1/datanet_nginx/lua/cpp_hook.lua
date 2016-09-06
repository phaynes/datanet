
local Helper       = require "datanet.helpers";
local KeyPartition = require "datanet.key_partition";
local SHM          = require "datanet.shm";
local Network      = require "datanet.network";
local WorkerDaemon = require "datanet.daemon";

--[[ NOTE: none of the module's functions has a SELF reference
           this is not a module more like a named global method container ]]--

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLASS DECLARATION --------------------------------------------------------

local _CPP   = {};
_CPP.__index = _CPP;


-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- SEND CENTRAL AGENT DENTRIES ----------------------------------------------

local NumberDeltasPerBatch   = 5;
local BetweenBatchDrainSleep = 0.5; -- 500 MS

local function do_send_central_agent_dentries(premature, kqk, dentries, jreq)
  local nd = table.getn(dentries);
  if (nd <= NumberDeltasPerBatch) then
    LD("do_send_central_agent_dentries: K: " .. kqk .. " FINAL BATCH");
    local config              = WorkerDaemon.config;
    jreq.params.data.dentries = dentries;
    jreq.id                   = Helper:GetNextRpcID();
    local jbody               = cjson.encode(jreq);
    local res, err            = Network:SendCentralHttpsRequest(config, jbody);
    if (err ~= nil) then
      LE('ERROR: CPP:do_send_central_agent_dentries: ' .. err);
    end
  else
    LD("do_send_central_agent_dentries: K: " .. kqk .. " SEND BATCH");
    local pdentries = {};
    for i = 1, NumberDeltasPerBatch do
      table.insert(pdentries, table.remove(dentries, 1));
      local nd = table.getn(dentries);
      if (nd == 0) then break; end
    end
    local config              = WorkerDaemon.config;
    jreq.params.data.dentries = pdentries;
    jreq.id                   = Helper:GetNextRpcID();
    local jbody               = cjson.encode(jreq);
    local res, err            = Network:SendCentralHttpsRequest(config, jbody);
    if (err ~= nil) then
      LE('ERROR: CPP:do_send_central_agent_dentries: ' .. err);
    end
    local nd                  = table.getn(dentries);
    if (nd == 0) then
      return;
    else
      local ok, err = ngx.timer.at(BetweenBatchDrainSleep,
                                   do_send_central_agent_dentries,
                                   kqk, dentries, jreq);
      if (err ~= nil) then
        LE('ERROR: do_send_central_agent_dentries: ' .. err);
      end
    end
  end
end

function send_central_agent_dentries(jbody)
  local jreq     = cjson.decode(jbody);
  local ks       = jreq.params.data.ks;
  local dentries = jreq.params.data.dentries;
  local kqk      = Helper:CreateKqk(ks.ns, ks.cn, ks.key);
  local nd       = table.getn(dentries);
  LD("START: send_central_https_request: K: " .. kqk .. ' #D: ' .. nd);
  do_send_central_agent_dentries(false, kqk, dentries, jreq);
end

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- PRIVATE FUNCTIONS --------------------------------------------------------

local function broadcast_method(premature, jbody, xpid)
  if (premature) then return end
  LT('CPP.broadcast_method');
  local config = WorkerDaemon.config;
  local suses  = Helper:GetBroadcastUnixSockets(config, xpid);
  for sus, pid in pairs(suses) do
    local res, err = Network:LocalHttpRequest(sus, jbody);
    if (err ~= nil) then
      LE('ERROR: CPP:broadcast_method: US: ' .. sus .. ' E: ' .. err);
      return;
    end
  end
end

local function central_redirect(premature, mhost, mport, b)
  if (premature) then return end
  LT('CPP.central_redirect');
  local config  = WorkerDaemon.config;
  local changed = false;
  if ((config.central.https.hostname ~= mhost) or
      (config.central.https.port     ~= mport)) then
    changed = true;
  end
  if (changed) then
    Helper:ResetCentral(config, mhost, mport);
    LD('NEW CENTRAL-HOST:PORT -> BROADCAST WORKER SETTINGS');
    c_broadcast_workers_settings();
  end
  local res, err = Helper:SendCentralAgentOnline(config, b);
  if (err ~= nil) then LE('ERROR: CPP.central_redirect: ' .. err); end
end

local function send_url_https_request(premature, url, jbody)
  if (premature) then return end
  LT('CPP.send_url_https_request');
  local res, err = Network:SendUrlHttpsRequest(url, jbody);
  if (err ~= nil) then
    LE('ERROR: CPP:send_url_https_request: ' .. err);
  end
end

local function send_central_https_request(premature, jbody, is_dentries)
  if (premature) then return end
  LT('CPP.send_central_https_request');
  if (is_dentries) then
    send_central_agent_dentries(jbody);
  else
    local config   = WorkerDaemon.config;
    local res, err = Network:SendCentralHttpsRequest(config, jbody);
    if (err ~= nil) then
      LE('ERROR: CPP:send_central_https_request: ' .. err);
    end
  end
end

local function internal_delta_broadcast(premature, jbody, kqk, xpid)
  if (premature) then return end
  local config     = WorkerDaemon.config;
  local sxpid      = tostring(xpid);
  local smc        = SHM.GetDictionaryName("DOCUMENT_CACHE");
  local spids, err = smc:get(kqk);
  if (err) then return; end
  local pids;
  if (spids == nil) then pids = {};
  else                   pids = cjson.decode(spids);
  end
  pids[sxpid] = nil; -- NOT TO XPID (AD->AuthorPID, SD->KQKWorkerPid)
  LT('CPP.internal_delta_broadcast: K: ' .. kqk ..
        ' PIDS: ' .. cjson.encode(pids));
  for pid, v in pairs(pids) do
    local sus      = KeyPartition:CreateStickySocketName(config, pid);
    local res, err = Network:LocalHttpRequest(sus, jbody);
    if (err ~= nil) then
      LE('ERROR: CPP:internal_delta_broadcast: ' .. err);
    end
  end
end

function fire_unexpected_subscriber_delta(premature, kqk, sectok, auuid)
  if (premature) then return end
  LT('CPP.fire_unexpected_subscriber_delta (UNEXSD)');
  c_engine_handle_unexpected_subscriber_delta(kqk, sectok, auuid);
end

function fire_timer_callback(premature, cbname)
  if (premature) then return end
  LT('CPP.fire_timer_callback: CB: ' .. cbname);
  c_engine_handle_generic_callback(cbname);
end

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- PUBLIC FUNCTIONS --------------------------------------------------------

function _CPP.CentralRedirect(mhost, mport, b, to)
  LT('CppHook.CentralRedirect: TO: ' .. to);
  local ok, err = ngx.timer.at(to, central_redirect, mhost, mport, b);
  if (err ~= nil) then
    LE('ERROR: CppHook.CentralRedirect: ' .. err);
  end
  return 1;
end

function _CPP.SendURLHttpsRequest(url, jbody)
  LT('CppHook.SendURLHttpsRequest');
  local ok, err = ngx.timer.at(0, send_url_https_request, url, jbody);
  if (err ~= nil) then
    LE('ERROR: CppHook.SendURLHttpsRequest: ' .. err);
  end
  return 1;
end

function _CPP.SendCentralHttpsRequest(to, jbody, is_dentries)
  LT('CppHook.SendCentralHttpsRequest: TO: ' .. to);
  local ok, err = ngx.timer.at(to, send_central_https_request,
                               jbody, is_dentries);
  if (err ~= nil) then
    LE('ERROR: CppHook.SendCentralHttpsRequest: ' .. err);
  end
  return 1;
end

function _CPP.InternalBroadcast(body, xpid)
  LT('CppHook.InternalBroadcast');
  local ok, err = ngx.timer.at(0, broadcast_method, body, xpid);
  if (err ~= nil) then
    LE('ERROR: CppHook.InternalBroadcast: ' .. err);
  end
  return 1;
end

function _CPP.InternalDeltaBroadcast(jbody, kqk, xpid)
  local ok, err = ngx.timer.at(0, internal_delta_broadcast, jbody, kqk, xpid);
  if (err ~= nil) then
    LE('ERROR: CppHook.InternalDeltaBroadcast: ' .. err);
  end
  return 1;
end

--TODO AUUID not needed
function _CPP.SetUnexpectedSDTimer(kqk, sectok, auuid, to)
  local sto = (to / 1000); -- MS to S
  LT('CppHook.SetUnexpectedSDTimer (UNEXSD): TO: ' .. to);
  local ok, err = ngx.timer.at(sto, fire_unexpected_subscriber_delta,
                               kqk, sectok, auuid);
  if (err ~= nil) then
    LE('ERROR: CppHook.SetUnexpectedSDTimer: ' .. err);
  end
  return 1;
end

function _CPP.SetTimerCallback(cbname, to)
  local sto = (to / 1000); -- MS to S
  LT('CppHook.SetTimerCallback: TO: ' .. to);
  local ok, err = ngx.timer.at(sto, fire_timer_callback, cbname);
  if (err ~= nil) then
    LE('ERROR: CppHook.SetTimerCallback: ' .. err);
  end
  return 1;
end

return _CPP;
