
local Helper        = require "datanet.helpers";
local BaseDocument  = require "datanet.base_document";
local Document      = require "datanet.document";
local DocumentCache = require "datanet.document_cache";
local DirtyKeys     = require "datanet.dirty_keys";
local DeltaCache    = require "datanet.delta_cache";

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLASS DECLARATION --------------------------------------------------------

local _R = {};
_R.__index = _R;

local function local_apply_document_root(kqk, doc_root)
  local scrdt       = doc_root.scrdt;
  local oplog       = doc_root.oplog;
  local opcontents  = {contents = oplog};       -- OPLOG is an ARRAY
  local sopcontents = cjson.encode(opcontents); -- ARRAYs dont encode
  --LT('local_apply_document_root: sopcontents: ' .. sopcontents);
  local username    = doc_root.info.auth.username;
  local password    = doc_root.info.auth.password;
  return c_local_apply_oplog(kqk, scrdt, sopcontents, username, password);
end

local function do_replicate_key(kqk, doc_root)
  local deps     = doc_root.dependencies; -- NOTE: no dependency change
  local sapplied = local_apply_document_root(kqk, doc_root);
  if (string.len(sapplied) == 0) then
    LE('ERROR: c_local_apply_delta');
    return false;
  else
    local applied   = cjson.decode(sapplied);
    local ncrdt     = applied.crdt;
    local bdoc      = Helper:CreateFromCrdt(BaseDocument, ncrdt, deps);
    local doc       = Document:New(bdoc);
    DocumentCache:Set(kqk, doc, false, false);
    local ccid      = Helper:GetUniqueRequestID();
    doc_root.dentry = applied.dentry;
    DeltaCache:Set(kqk, ccid, doc_root);
    return BaseDocument:SendInternalClientCommit(kqk, ccid, doc_root);
  end
end

function _R.ReplicateWorkerCommits(self)
  local ruuid = ngx.var.RequestUUID;
  local dkeys = DirtyKeys:GetDirtyKeys(ruuid);
  if (dkeys == nil) then return true; end
  for kqk, doc_root in pairs(dkeys) do
    do_replicate_key(kqk, doc_root)
  end
  DirtyKeys:DropDirtyKeys(ruuid); -- CLEAR THIS REQUESTS DIRTY-KEYS
  return true;
end

return _R;

