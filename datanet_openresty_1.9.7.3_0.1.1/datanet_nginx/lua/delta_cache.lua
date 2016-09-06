
local Helper    = require "datanet.helpers";

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLASS DECLARATION --------------------------------------------------------

local _DTC = {};
_DTC.__index = _DTC;

local DeltasByKqk = {};

function _DTC.GetDeltas(self, kqk)
  LT('DeltaCache:GetDeltas: K: ' .. kqk);
  return DeltasByKqk[kqk];
end

function _DTC.Set(self, kqk, ccid, doc_root)
  if (DeltasByKqk[kqk] == nil) then
    DeltasByKqk[kqk] = {};
  end
  LT('DeltaCache:Set: K: ' .. kqk .. ' CCID: ' .. ccid);
  DeltasByKqk[kqk][ccid] = doc_root;
  return true;
end

local function do_delta_cache_remove(kqk, ccid)
  LT('DeltaCache:Remove: K: ' .. kqk .. ' CCID: ' ..ccid);
  DeltasByKqk[kqk][ccid] = nil;
  local cnt = Helper:CountDictionary(DeltasByKqk[kqk]);
  if (cnt == 0) then
    LD('DeltaCache:Remove: ARRAY: K: ' .. kqk);
    DeltasByKqk[kqk] = nil;
  end
  return true;
end

function _DTC.Remove(self, kqk, ccid)
  if (DeltasByKqk[kqk] ~= nil) then
    local doc_root = DeltasByKqk[kqk][ccid];
    if (doc_root ~= nil) then return do_delta_cache_remove(kqk, ccid); end
  end
  return false; -- RECENTLY EVICTED (OK)
end

return _DTC;

