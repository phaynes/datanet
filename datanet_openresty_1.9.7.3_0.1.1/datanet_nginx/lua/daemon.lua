
local Helper  = require "datanet.helpers";
local Network = require "datanet.network";

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLASS DECLARATION --------------------------------------------------------

local _WD   = {};
_WD.__index = _WD;

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- SETTINGS ------------------------------------------------------------------

_WD.worker_reaper_delay = 5;
_WD.worker_stale_secs   = 11;

_WD.storage_LMDB        = true;

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- DEFAULTS -----------------------------------------------------------------

local DefaultNamespace                = "production";
local DefaultUsername                 = "default";
local DefaultPassword                 = "password";
local DefaultChannel                  = "0";

local DefaultLogLevel                 = "ERROR";

local DefaultStickyUnixSocketWildcard = "unix:/tmp/sticky_nginx_socket_";
local DefaultPidFilePrefix            = "/tmp/NGINX_PID";
local DefaultLogConfigurationFile     = "./easylogging.conf";
local DefaultLDBDataDirectory         = "./LDB";

function _WD.PopulateDefaultConfigSettings(self, config)
  if (config.sticky == nil) then
    config.sticky = {unix_socket_wildcard = DefaultStickyUnixSocketWildcard};
  end
  if (config.log == nil) then
    config.log = {};
  end
  if (config.log.level == nil) then
    config.log.level = DefaultLogLevel;
    LD('USING: DefaultLogLevel: ' .. DefaultLogLevel);
  end
  if (config.debug == nil) then
    config.debug  = {pid_file_prefix = DefaultPidFilePrefix};
  end
  if (config.default == nil) then
    config.default = {};
  end
  if (config.default.namespace == nil) then
    config.default.namespace = DefaultNamespace;
    LD('USING: DefaultNamespace: ' .. DefaultNamespace);
  end
  if (config.default.username == nil) then
    config.default.username = DefaultUsername;
    LD('USING: DefaultUsername: ' .. DefaultUsername);
  end
  if (config.default.password == nil) then
    config.default.password = DefaultPassword;
    LD('USING: DefaultPassword: ' .. DefaultPassword);
  end
  if (config.default.channel == nil) then
    config.default.channel = DefaultChannel;
    LD('USING: DefaultChannel: ' .. DefaultChannel);
  end
  if (config.default.log_configuration_file == nil) then
    config.default.log_configuration_file = DefaultLogConfigurationFile;
  end
  if (config.default.ldb_data_directory == nil) then
    config.default.ldb_data_directory     = DefaultLDBDataDirectory;
  end
end

function _WD.ValidateConfig(self, config)
  if (config.name == nil) then
    return "CONFIG.name not SET";
  end
  if (config.central                   == nil or
      config.central.name              == nil or
      config.central.https             == nil or
      config.central.https.hostname    == nil or
      config.central.https.port        == nil or
      config.central.callback          == nil or
      config.central.callback.hostname == nil or
      config.central.callback.port     == nil or
      config.central.callback.key      == nil) then
    return "SYNTAX: FAIL: CONFIG.central{name, hostname, port, https{hostname, port}, callback{hostname, port, key}}";
  end
  if (config.sticky                      == nil or
      config.sticky.unix_socket_wildcard == nil) then
    return "SYNTAX: FAIL: CONFIG.sticky{unix_socket_wildcard}";
  end


  local level = config.log.level;
  if (level ~= "TRACE" and level ~= "NOTICE" and level ~= "ERROR") then
    return "SUPPORTED LOG-LEVELS: [TRACE, NOTICE, ERROR]";
  end
  return nil;
end

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- OVERRIDE FUNCTIONS --------------------------------------------------------

function _WD.OnInitialization(self)  -- OVERRIDE ME
end

function _WD.OnFinalShutdown(self)   -- OVERRIDE ME
end

function _WD.OnPrimaryElected(self)  -- OVERRIDE ME
end

function _WD.OnDeadWorker(self, wid) -- OVERRIDE ME
end

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- PRIVATE FUNCTIONS ---------------------------------------------------------

local function redo_worker_partition_table(sms, smp, wid)
  local wpid        = tonumber(wid); -- WORKER.ID -> PID
  LT('redo_worker_partition_table: WPID: ' .. wpid);
  local alive       = {};
  local nwrkrs, err = sms:get("num_workers_running");
  if (err ~= nil) then return err; end;
  for i = 0, (nwrkrs - 1) do
    local pid, err = smp:get(i);
    LT('SMP:GET(' .. i .. '): PID: ' .. pid);
    if (err ~= nil) then return err; end;
    if (pid ~= wpid) then -- only for NOT DEAD workers
      table.insert(alive, pid);
    end
  end
  smp:flush_all(); -- empty WORKER_PARTITION_TABLE
  for k, pid in pairs(alive) do
    local i = (k - 1); -- LuaTables start @1, WORKER_PARTITION_TABLE @0
    LT('REDO-SET: worker_partition_table: I: ' .. i .. ' PID: ' .. pid);
    local s, err, f = smp:set(i, pid); -- insert ALIVE workers
    if (err ~= nil) then return err; end;
    c_reset_worker_partition(pid, i);
  end
  return nil;
end

local function remove_worker(self, sms, smw, smp, wid)
  local s, err, f   = smw:delete(wid);
  if (err ~= nil) then return err; end;
  local err         = redo_worker_partition_table(sms, smp, wid)
  if (err ~= nil) then return err; end;
  local nwrkrs, err = sms:incr("num_workers_running", -1);
  if (err ~= nil) then return err; end;
  LT('remove_worker: WID: ' .. wid .. ' #W: ' .. nwrkrs);
  self:OnDeadWorker(wid);
  return nil;
end

local function run_worker_shutdown(self, sms, smw, smp, w)
  local wid    = w.worker_uuid;
  local err    = remove_worker(self, sms, smw, smp, wid);
  if (err ~= nil) then return err; end;
  local nwrkrs = ngx.worker.count();
  LT('run_worker_shutdown: NUM_WORKERS: ' .. nwrkrs);
  if (nwrkrs == 0) then self:OnFinalShutdown(); end
  return nil;
end

local function update_worker_ts(self, smw, w)
  local now       = ngx.time();
  local wid       = w.worker_uuid;
  local swid      = tostring(wid);
  local s, err, f = smw:set(swid, now);
  if (err ~= nil) then return err; end;
  return nil;
end

local function update_primary_ts(self, sms, smw, w)
  local now       = ngx.time();
  local s, err, f = sms:set("primary_updated", now);
  if (s) then return update_worker_ts(self, smw, w);
  else        return err;
  end
end

local function grab_primary_lock(self, sms, smw, w, startup)
  local wid             = w.worker_uuid;
  local primary, err, f = sms:add("primary_worker_uuid", wid);
  if (primary) then
    LT('grab_primary_lock: (PRIMARY) WID: ' .. wid);
    self.data.primary = true;
    w.primary         = true;
    local err = update_primary_ts(self, sms, smw, w);
    if (err ~= nil) then return nil, err; end;
    if (not startup) then
      self:OnPrimaryElected();
    end
    return true, nil;
  else
    self.data.primary = false;
    local err = update_worker_ts(self, smw, w);
    if (err ~= nil) then return nil, err; end;
    return false, nil;
  end
end

local function primary_worker_reaper(self, sms, smw, smp, w)
  local err = update_primary_ts(self, sms, smw, w);
  if (err ~= nil) then return err; end
  local keys  = smw:get_keys(0);
  for i, wid in ipairs(keys) do
    local wts, err = smw:get(wid);
    if (err ~= nil) then return err; end
    if (wts ~= nil) then
      local now  = ngx.time();
      local diff = now - wts;
      if (diff > self.worker_stale_secs) then
        local err = remove_worker(self, sms, smw, smp, wid);
        if (err ~= nil) then return err; end
      end
    end
  end
end

local function become_primary(self, sms, smw, w)
  local s, err, f = sms:add("lock_become_primary", true); -- LOCK
  if (s) then
    LT('OLD PRIMARY DIED -> BECOME PRIMARY');
    local s, err, f = sms:delete("primary_worker_uuid");
    if (s) then
      grab_primary_lock(self, sms, smw, w, false);
    end
    sms:delete("lock_become_primary");                    -- UNLOCK
  end
end

local function normal_worker_reaper(self, sms, smw, w)
  local err      = update_worker_ts(self, smw, w);
  if (err ~= nil) then return err; end
  local pts, err = sms:get("primary_updated");
  if (err ~= nil) then return err; end
  if (pts == nil) then
    return nil;
  end
  local now = ngx.time();
  if ((now - pts) > self.worker_stale_secs) then
    become_primary(self, sms, smw, w);
  end
  return nil;
end

local function worker_reaper(premature, self, sms, smw, smp, w)
  if (premature) then
    run_worker_shutdown(self, sms, smw, smp, w);
  else
    if (w.primary) then
      primary_worker_reaper(self, sms, smw, smp, w);
    else
      normal_worker_reaper(self, sms, smw, w);
    end
    ngx.timer.at(self.worker_reaper_delay,
                 worker_reaper, self, sms, smw, smp, w); -- RECURRING TIMER
  end
end

local function init_worker_reaper(self, sms, smw, smp, w)
  return ngx.timer.at(self.worker_reaper_delay,
                      worker_reaper, self, sms, smw, smp, w); -- START TIMER
end

local function initialize_locks(sms)
  sms:delete("lock_become_primary");
end

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- FORCE VOTE ON INITIALIZATION ----------------------------------------------

function initialize_worker_force_vote(premature, self, sms, smw, smp)
  if (premature) then return end
  LT('initialize_worker_force_vote');
  local config = self.config;
  local xpid   = ngx.worker.pid();
  local jreq   = Helper:CreateClientPingJRB();
  local jbody  = cjson.encode(jreq);
  local suses  = Helper:GetBroadcastUnixSockets(config, xpid);
  for sus, pid in pairs(suses) do
    local res, err = Network:LocalHttpRequest(sus, jbody);
    if (err) then
      LE('ERROR on PID: ' .. pid);
      local err = remove_worker(self, sms, smw, smp, pid);
      if (err ~= nil) then return err; end
      break;
    end
  end
  return nil;
end

function run_initialize_worker_force_vote(self, sms, smw, smp)
  local ok, err = ngx.timer.at(0, initialize_worker_force_vote,
                                  self, sms, smw, smp);
  if (err ~= nil) then
    LE('ERROR: ngx.timer.at: initialize_worker_force_vote: ' .. err);
  end
end

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- PUBLIC API ----------------------------------------------------------------

_WD.data = {}; -- stores datanet.get_startup_info

function _WD.InitializeDaemonWorker(self, sms, smw, smp)
  LT("WorkerDaemon:InitializeDaemonWorker");
  local pid          = ngx.worker.pid();
  local nwr, err     = sms:incr("num_workers_running", 1);
  if (err ~= nil) then return nil, nil, nil, err; end
  local part         = (nwr - 1); -- WORKER_PARTITION_TABLE starts @0
  LT('SET: worker_partition_table: PART: ' .. part .. ' PID: ' .. pid);
  local s, err, f    = smp:set(part, pid);
  if (err ~= nil) then return nil, nil, nil, err; end
  local wid          = pid;
  local w            = {worker_uuid = wid, primary = false};
  local primary, err = grab_primary_lock(self, sms, smw, w, true);
  if (err ~= nil) then return nil, nil, nil, err; end
  local ok, err      = init_worker_reaper(self, sms, smw, smp, w);
  if (err ~= nil) then return nil, nil, nil, err; end
  self.data.worker           = {};
  self.data.worker.id        = wid;
  self.data.worker.partition = part;
  local nwrkrs               = ngx.worker.count();
  local vote                 = (nwr > nwrkrs);
  if (vote) then
    run_initialize_worker_force_vote(self, sms, smw, smp);
  end
  return wid, primary, nil;
end

function _WD.InitializeDaemonMaster(self, sms)
  LT("WorkerDaemon:InitializeDaemonMaster");
  local s, err, f   = sms:add("initialized", true);
  if (s) then
    local s, err, f = sms:set("num_workers_running", 0);
  end
  initialize_locks(sms);
end

return _WD;
