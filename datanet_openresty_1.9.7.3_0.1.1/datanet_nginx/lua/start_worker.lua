
local Helper         = require "datanet.helpers";
local Initialization = require "datanet.initialization";
local WorkerDaemon   = require "datanet.daemon";
local SHM            = require "datanet.shm";

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- SETTINGS ------------------------------------------------------------------

local HearbeatDelay                 = 1;
local GC_PurgeDelay                 = 21;
local AgentDirtyDeltasDelay         = 50;
local AgentToSyncKeysDelay          = 20;
local StartDaemonsDelay             = 2;
local WorkerWaitOnPrimaryStartSleep = 0.1;
local PrimaryResendAgentOnlineDelay = 1;

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- HELPERS -------------------------------------------------------------------

local function get_pid_file()
  local config = WorkerDaemon.config;
  local data   = WorkerDaemon.data;
  local pfp    = config.debug.pid_file_prefix;
  local wid    = data.worker.id;
  local ppfile = pfp .. "_" .. wid;
  if (data.primary) then
     ppfile = ppfile .. "_MASTER";
  end
  return ppfile;
end

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- CLASS DECLARATION ---------------------------------------------------------

local _SW   = {};   
_SW.__index = _SW;

local function run_daemon_agent_heartbeat(premature)
  if (premature) then return end
  c_run_primary_daemon_heartbeat();
  ngx.timer.at(HearbeatDelay, run_daemon_agent_heartbeat);
end

local function start_daemon_agent_heartbeat()
  LT("start_daemon_agent_heartbeat");
  run_daemon_agent_heartbeat();
end

local function run_daemon_agent_gc_purge(premature)
  if (premature) then return end
  c_run_primary_daemon_gc_purge();
  ngx.timer.at(GC_PurgeDelay, run_daemon_agent_gc_purge);
end

local function start_daemon_agent_gc_purge()
  LT("start_daemon_agent_gc_purge");
  run_daemon_agent_gc_purge();
end

local function run_daemon_agent_dirty_deltas(premature)
  if (premature) then return end
  c_run_primary_daemon_dirty_deltas();
  ngx.timer.at(AgentDirtyDeltasDelay, run_daemon_agent_dirty_deltas);
end

local function start_daemon_agent_dirty_deltas()
  LT("start_daemon_agent_dirty_deltas");
  run_daemon_agent_dirty_deltas();
end

local function run_daemon_agent_to_sync_keys(premature)
  if (premature) then return end
  c_run_primary_daemon_to_sync_keys();
  ngx.timer.at(AgentToSyncKeysDelay, run_daemon_agent_to_sync_keys);
end

local function start_daemon_agent_to_sync_keys()
  LT("start_daemon_agent_to_sync_keys");
  run_daemon_agent_to_sync_keys();
end

local function start_kqk_daemons(premature)
  if (premature) then return end
  LT("start_kqk_daemons");
  start_daemon_agent_to_sync_keys();
  start_daemon_agent_dirty_deltas();
  start_daemon_agent_heartbeat();
  start_daemon_agent_gc_purge();
end

local function primary_send_agent_online(premature)
  if (premature) then return end
  LT('primary_send_agent_online');
  local config   = WorkerDaemon.config;
  local res, err = Helper:SendCentralAgentOnline(config, true);
  if (err ~= nil) then
    LE('FAILED: primary_send_agent_online -> RETRY');
    ngx.timer.at(PrimaryResendAgentOnlineDelay, primary_send_agent_online);
  end
end

function _SW.PrimarySendAgentOnline(self)
  primary_send_agent_online(false);
end

local function on_primary_start()
  local sms = SHM.GetDictionaryName("STARTUP");
  sms:set("primary_started", true);
  LD("PRIMARY STARTED");
end

local function start_primary(premature)
  if (premature) then return end
  local config = WorkerDaemon.config;
  local ppfile = get_pid_file();
  local ret    = c_initialize_primary(ppfile);
  if (ret ~= 0) then
    ngx.log(ngx.CRIT, "FAILURE: DATANET: c_initialize_primary");
    return;
  end
  ngx.timer.at(0, primary_send_agent_online);
  on_primary_start(); -- WORKER stops waiting & starts
  ngx.timer.at(StartDaemonsDelay, start_kqk_daemons);
  if (ClusterActivated) then
    Initialization:InitializeCluster();
  end
end

function _SW.StartPrimary(self)
  start_primary(false);
end

local function get_internal_agent_settings(premature)
  if (premature) then return end
  local config = WorkerDaemon.config;
  Helper:SendPrimaryGetInternalAgentSettingsJRB(config);
end

local function worker_wait_until_primary_has_started(premature)
  if (premature) then return end
  local config   = WorkerDaemon.config;
  LD("worker_wait_until_primary_has_started: DO CHECK");
  local sms      = SHM.GetDictionaryName("STARTUP");
  local pts, err = sms:get("primary_started");
  if (err) then return; end
  if (pts ~= nil) then
    LD("worker_wait_until_primary_has_started: CHECK -> OK");
    ngx.timer.at(0,                 get_internal_agent_settings);
    ngx.timer.at(StartDaemonsDelay, start_kqk_daemons);
  else
    local to = WorkerWaitOnPrimaryStartSleep;
    LD("worker_wait_until_primary_has_started: SLEEP: " .. to);
    ngx.timer.at(to, worker_wait_until_primary_has_started);
  end
end

local TestSlowStartingPrimary = false;
local function start_worker()
  if (not TestSlowStartingPrimary) then
    worker_wait_until_primary_has_started(false);
  else
    ngx.timer.at(5, worker_wait_until_primary_has_started);
  end
end

function _SW.Go(self) --NOTE: Used in init_worker_by_lua
  local config = WorkerDaemon.config;
  local data   = WorkerDaemon.data;
  local wdata  = data.worker;
  local cname  = config.central.name;
  local wid    = wdata.id;
  local wport  = wdata.port;
  local wprt   = wdata.partition;
  local cmaxb  = config.cache  and config.cache.max_bytes  or 0;
  local dmaxb  = config.deltas and config.deltas.max_bytes or 0;
  local cbhost = config.central.callback.hostname;
  local cbport = config.central.callback.port;
  local cbkey  = config.central.callback.key;
  local isp    = data.primary and 1 or 0;
  local ppfile = get_pid_file();
  local lcfile = config.default.log_configuration_file;
  local ldbd   = config.default.ldb_data_directory;
  local ret    = c_initialize_worker(cname, wid, wport, wprt, cmaxb, dmaxb,
                                     cbhost, cbport, cbkey, isp,
                                     ppfile, lcfile, ldbd);
  if (ret ~= 0) then
    ngx.log(ngx.CRIT, "FAILURE: DATANET: c_initialize_worker");
    return;
  end
  if (data.primary) then
    start_primary();
  else -- AVOID RACE CONDITION
    start_worker();
  end
end

return _SW;
