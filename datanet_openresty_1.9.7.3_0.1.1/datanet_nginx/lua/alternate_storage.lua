
local Helper         = require "datanet.helpers";
local WorkerDaemon   = require "datanet.daemon";

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- WORKER DEAMON OVERRIDES ---------------------------------------------------

WorkerDaemon.OnInitialization = function(self)
  if (self.storage_LMDB) then
    LD("WorkerDaemon.OnInitialization: LMDB: LOAD-DATA -> NO-OP");
    return;
  end
  local ddir  = self.config.ddir;
  local dfile = ddir .. UUID_file;
  local dtxt  = Helper:ReadFile(dfile);
  if (dtxt == nil) then return; end
  local uuid  = tonumber(dtxt);
  LD("WorkerDaemon.OnInitialization: UUID: " .. uuid);
  c_set_agent_uuid(uuid);
  for i, tname in ipairs(self.config.tables) do
    local dtname = SHM.GetTableName(tname);
    local dfile  = ddir .. dtname;
    local dtxt   = Helper:ReadFile(dfile);
    if (dtxt == nil) then return; end
    local data    = cjson.decode(dtxt)
    if (data == nil) then return; end
    SHM.SetTable(tname, data);
  end
end

WorkerDaemon.OnFinalShutdown = function(self)
  LD("WorkerDaemon:OnFinalShutdown()");
  if (self.storage_LMDB) then
    LD('LMDB STORAGE FINAL SHUTDOWN -> NO-OP');
    return;
  end
  local ddir  = self.config.ddir;
  local dfile = ddir .. UUID_file;
  local uuid  = c_get_agent_uuid();
  if (uuid == nil) then return; end
  local dtxt  = tostring(uuid);
  if (not Helper:WriteFile(dfile, dtxt)) then return; end

  for i, tname in ipairs(self.config.tables) do
    local dtname = SHM.GetTableName(tname);
    local dfile  = ddir .. dtname;
    local data   = SHM.GetTable(tname);
    local dtxt   = cjson.encode(data);
    if (not Helper:WriteFile(dfile, dtxt)) then return; end
  end
end

