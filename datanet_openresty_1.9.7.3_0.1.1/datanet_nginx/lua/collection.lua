
local Helper         = require "datanet.helpers";
local DocumentCache  = require "datanet.document_cache";
local Document       = require "datanet.document";
local Actions        = require "datanet.actions";
local WorkerDaemon   = require "datanet.daemon";

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- CLASS DECLARATION ---------------------------------------------------------

local _C   = {};
_C.__index = _C;

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- PUBLIC API ----------------------------------------------------------------

function _C:new(cn)
  local config    = WorkerDaemon.config;
  local self      = setmetatable({}, _C);
  self.__index    = self
  self.namespace  = config.default.namespace;
  self.collection = cn;
  return self;
end

function _C.fetch(self, key, cache)
  return Actions:ClientFetch(self.namespace, self.collection, key, cache);
end

function _C.store(self, value)
  return Actions:ClientStore(self.namespace, self.collection, value);
end

function _C.remove(self, key)
  return Actions:ClientRemove(self.namespace, self.collection, key);
end

function _C.cache(self, key, pin, watch, sticky)
  return Actions:ClientCache(self.namespace, self.collection, key,
                             pin, watch, sticky);
end

function _C.evict(self, key)
  return Actions:ClientEvict(self.namespace, self.collection, key);
end

--TODO local_evict()

function _C.expire(self, key, expire)
  return Actions:ClientExpire(self.namespace, self.collection, key, expire);
end

function _C.heartbeat(self, cmd, field, uuid, mlen, trim, isi, isa)
  return Actions:ClientHeartbeat(self.namespace, self.collection,
                                 cmd, field, uuid, mlen, trim, isi, isa);
end

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- LUA TABLE CACHE -----------------------------------------------------------

local function collection_get(ns, cn, key)
  local kqk  = Helper:CreateKqk(ns, cn, key); 
  local cdoc = DocumentCache:Get(kqk);
  if (cdoc ~= nil) then
    return cdoc, nil;
  else 
    local bdoc, err = Actions:ClientFetch(ns, cn, key);
    if (err ~= nil) then return nil, err;
    else
      local doc = Document:New(bdoc);
      DocumentCache:Set(kqk, doc, true, false);
      return doc, nil;
    end
  end
end

function _C.get(self, key)
  local ns  = self.namespace;
  local cn  = self.collection;
  local doc = collection_get(ns, cn, key);
  if (doc ~= nil) then
    doc.__root.info.auth.username = Helper.username;
    doc.__root.info.auth.password = Helper.password;
  end
  return doc;
end

return _C;

