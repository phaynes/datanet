
local SHM     = require "datanet.shm";

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLASS DECLARATION --------------------------------------------------------

local _KP   = {};
_KP.__index = _KP;

function _KP.GetShardKeyValue(self)
  local args   = ngx.req.get_uri_args()
  local skname = 'shard-key';
  local sk     = args[skname];
  if (sk == nil) then return nil; end
  return args[sk];
end

function _KP.CreateStickySocketName(self, config, pid)
  local uwild = config.sticky.unix_socket_wildcard;
  return uwild .. pid;
end

local function get_sticky_unix_socket(self, config, spid)
  local pid = ngx.worker.pid();
  local sus = self:CreateStickySocketName(config, spid);
  local ism = (tonumber(spid) == pid);
  return sus, ism, nil;
end

function _KP.GetStickyUnixSocketShardKey(self, config, kqk)
  local uwild     = config.sticky.unix_socket_wildcard;
  local nwrkrs    = ngx.worker.count();
  local hash      = ngx.crc32_short(kqk)
  local slot      = hash % nwrkrs;
  local smp       = SHM.GetDictionaryName("WORKER_PARTITION_TABLE");
  local spid, err = smp:get(slot);
  if (err) then return nil, nil, err;
  else          return get_sticky_unix_socket(self, config, spid);
  end
end

function _KP.SetStickyKeyPartition(self, kqk)
  local smk       = SHM.GetDictionaryName("STICKY_KEY_PARTITION");
  local kpid, err = smk:get(kqk);
  if (err) then return nil, err; end
  if (kpid ~= nil) then return kpid, nil;
  else
    local pid       = ngx.worker.pid();
    LT('KeyPartition:SetStickyKeyPartition: K: ' .. kqk .. ' PID: ' .. pid);
    local s, err, f = smk:set(kqk, pid);
    return pid, err;
  end
end

function _KP.GetStickyUnixSocketKqk(self, config, kqk)
  local smk       = SHM.GetDictionaryName("STICKY_KEY_PARTITION");
  local kpid, err = smk:get(kqk);
  if (err ~= nil) then return nil, nil, err; end
  if (kpid) then return get_sticky_unix_socket(self, config, kpid);
  else           return self:GetStickyUnixSocketShardKey(config, kqk);
  end
end

return _KP;
