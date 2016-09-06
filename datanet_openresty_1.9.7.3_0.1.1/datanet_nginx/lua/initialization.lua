local Helper       = require "datanet.helpers";
local Network      = require "datanet.network";
local SHM          = require "datanet.shm";
local WorkerDaemon = require "datanet.daemon";
local Cluster      = require "datanet.cluster";

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLASS DECLARATION --------------------------------------------------------

local _I   = {};
_I.__index = _I;

function _I.SetConfig(self, config, tables, ns)
  if (ns ~= nil) then
    SHM.TableNamespace = ns;
  end
  WorkerDaemon.config        = config;
  WorkerDaemon.config.tables = tables;
  WorkerDaemon:PopulateDefaultConfigSettings(config);
  local err = WorkerDaemon:ValidateConfig(config);
  if (err ~= nil) then return err; end
  local err = c_set_log_level(config.log.level);
  if (err ~= nil) then return err; end
  return nil;
end

function _I.InitializeMaster(self)
  local config = WorkerDaemon.config;
  local sms    = SHM.GetDictionaryName("STARTUP");
  WorkerDaemon:InitializeDaemonMaster(sms);
  WorkerDaemon:OnInitialization();
  if (c_initialize_master ~= nil) then
    local ldbd = config.default.ldb_data_directory;
    local ret  = c_initialize_master(ldbd);
    if (ret ~= 0) then
      ngx.log(ngx.CRIT, "FAILURE: DATANET: c_initialize_master");
    end
  end
end

function _I.InitializeWorker(self)
  local config = WorkerDaemon.config;
  if (c_initialize_logging ~= nil) then
    local lcfile = config.default.log_configuration_file
    c_initialize_logging(lcfile); --NOTE: @beginning of worker code
  end
  local sms = SHM.GetDictionaryName("STARTUP");
  local smw = SHM.GetDictionaryName("STARTUP_WORKERS");
  local smp = SHM.GetDictionaryName("WORKER_PARTITION_TABLE");
  local wid, primary, err = WorkerDaemon:InitializeDaemonWorker(sms, smw, smp);
  if (err) then return nil, err; end
  local nwrkrs            = ngx.worker.count();
  LD('INITIALIZE-WORKER: wid: ' .. wid .. 
     ' primary: ' .. tostring(primary) .. ' #W: ' .. nwrkrs);
  local uwild = config.sticky.unix_socket_wildcard;
  ngx.unique_socket_per_worker(uwild);
  return primary;
end

local InitClusterDelay = 1;
local function initialize_cluster(premature)
  if (premature) then return end
  LT('initialize_cluster');
  local config = WorkerDaemon.config;
  local uuid   = c_get_agent_uuid();
  if (uuid == nil) then -- TRY AGAIN LATER
    ngx.timer.at(InitClusterDelay, initialize_cluster);
  else
    Cluster:Initialize(config.cluster.ip, config.cluster.port,
                       config.sticky.port, uuid, config.cluster.discovery);
  end
end

function _I.InitializeCluster(self)
  ngx.timer.at(0, initialize_cluster);
end

return _I;
