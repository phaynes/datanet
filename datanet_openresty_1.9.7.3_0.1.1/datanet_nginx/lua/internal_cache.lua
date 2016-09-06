
local Helper        = require "datanet.helpers";
local Document      = require "datanet.document";
local DocumentCache = require "datanet.document_cache";
local DeltaCache    = require "datanet.delta_cache";

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLASS DECLARATION --------------------------------------------------------

local _IC = {};
_IC.__index = _IC;

local function set_document_cache(kqk, scrdt, deps)
  local crdt = cjson.decode(scrdt);
  -- NOTE: First arg: BaseDocument (module) not needed -> only Document needed
  local bdoc = Helper:CreateFromCrdt(nil, crdt, deps);
  local doc  = Document:New(bdoc);
  DocumentCache:Set(kqk, doc, false, false);
  return true;
end

local function do_apply_doc_roots(kqk, xcrdt, cavrsns, doc_roots)
  if (doc_roots == nil) then return true; end
  LT('InternalCache:HandleDocumentCacheDelta: MERGE: K: ' .. kqk);
  local sxcrdt = cjson.encode(xcrdt);
  for k, doc_root in pairs(doc_roots) do
    local dentry  = doc_root.dentry;
    local sdentry = cjson.encode(dentry);
    sxcrdt        = c_local_apply_dentry(kqk, sxcrdt, sdentry);
    if (string.len(sxcrdt) == 0) then return false; end
  end
  return set_document_cache(kqk, sxcrdt, cavrsns);
end

local function handle_document_cache_merge(kqk, doc, xcrdt, cavrsns)
  local cdeps = doc.__root.dependencies;
  if (not Helper:CmpDependencies(cdeps, cavrsns)) then
    return true;
  else
    local doc_roots = DeltaCache:GetDeltas(kqk);
    return do_apply_doc_roots(kqk, xcrdt, cavrsns, doc_roots);
  end
end

local function handle_document_cache_delta(kqk, doc, crdt, dentry)
  local deps = dentry.delta._meta.dependencies;
  LT('InternalCache:HandleDocumentCacheDelta: DELTA: K: ' .. kqk);
  local sdentry = cjson.encode(dentry);
  local sncrdt  = c_local_apply_dentry(kqk, doc.__root.scrdt, sdentry);
  if (string.len(sncrdt) == 0) then
    LE('ERROR: c_local_apply_dentry');
    return false;
  end
  return set_document_cache(kqk, sncrdt, deps);
end

function _IC.HandleDocumentCacheDelta(self, jreq)
  local kqk = jreq.params.data.kqk;
  local ism = jreq.params.data.is_merge;
  local doc = DocumentCache:RawGet(kqk);
  if (doc == nil) then return; end -- RECENTLY EVICTED (OK)
  if (ism) then
    local xcrdt   = jreq.params.data.crdt;
    local cavrsns = jreq.params.data.central_agent_versions;
    return handle_document_cache_merge(kqk, doc, xcrdt, cavrsns);
  else
    local crdt   = jreq.params.data.crdt;
    local dentry = jreq.params.data.dentry;
    return handle_document_cache_delta(kqk, doc, crdt, dentry);
  end
end

return _IC;

