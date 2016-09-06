
local Helper       = require "datanet.helpers";
local KeyPartition = require "datanet.key_partition";
local SHM          = require "datanet.shm";
local Debug        = require "datanet.debug";

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLASS DECLARATION --------------------------------------------------------

local _DC = {};
_DC.__index = _DC;

local Documents = {};

local cache_hits   = 0;
local cache_misses = 0;

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- HELPERS ------------------------------------------------------------------

local function map_pid_to_document_cache(kqk)
  local pid        = ngx.worker.pid();
  local spid       = tostring(pid);
  local smc        = SHM.GetDictionaryName("DOCUMENT_CACHE");
  local spids, err = smc:get(kqk);
  if (err) then return false; end
  local pids;
  if (spids == nil) then pids = {};
  else                   pids = cjson.decode(spids);
  end
  pids[spid]      = true;
  spids           = cjson.encode(pids);
  LD('DOCUMENT_CACHE: K: ' .. kqk .. ' PIDS: ' .. spids);
  local s, err, f = smc:set(kqk, spids);
  if (err) then return false; end
  local kpid, err = KeyPartition:SetStickyKeyPartition(kqk);
  if (err) then return false; end
  return true;
end

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- PUBLIC METHODS -----------------------------------------------------------

function _DC.RawGet(self, kqk)
  return Documents[kqk];
end

function _DC.Get(self, kqk)
  local doc = self:RawGet(kqk);
  if (doc == nil) then cache_misses = cache_misses + 1;
  else                 cache_hits   = cache_hits   + 1;
  end
  if (Debug.DocumentCache) then
    LD('DocumentCache: #HIT: ' .. cache_hits .. ' #M: ' .. cache_misses);
  end
  return doc;
end

local function handle_set_version(self, kqk, doc, cow)
  if (not cow) then return true;
  else
    local cdoc = self:RawGet(kqk);
    if (cdoc == nil) then return true; -- NOT YET CACHED
    else
      local dvrsn  = doc.__root.delta_version;
      local cdvrsn = cdoc.__root.delta_version;
      LT('COW: handle_set_version: DV: ' .. dvrsn .. ' (C)DV: ' .. cdvrsn);
      return (dvrsn ~= cdvrsn);
    end
  end
end

function _DC.Set(self, kqk, doc, initial, cow)
  LT('DocumentCache:Set: K: ' .. kqk);
  if (not handle_set_version(self, kqk, doc, cow)) then return false; end
  doc.__root.dentry    = nil;-- DENTRY RESET
  doc.__root.oplog     = {}; -- OPLOG RESET
  doc.__root.info.auth = {}; -- AUTH NOT CACHED
  Documents[kqk]       = doc;
  if (initial) then
    map_pid_to_document_cache(kqk);
  end
  return true;
end

function _DC.UpdateDependencies(self, kqk, deps)
  local doc = self:RawGet(kqk);
  if (doc == nil) then return false; end
  doc.__root.dependencies = deps;
  Documents[kqk]          = doc;
  return true;
end

function _DC.Remove(self, kqk)
  LT('DocumentCache:Remove: K: ' .. kqk);
  Documents[kqk] = nil;
  --TODO: unmap_pid_to_document_cache
  return true;
end

return _DC;

