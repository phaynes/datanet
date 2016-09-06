
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLASS DECLARATION --------------------------------------------------------

local _DK = {};
_DK.__index = _DK;

local DirtyKeys = {}; --[RequestUUID,KQK] -> DocRoot

function _DK.SetDirty(self, kqk, data)
  local ruuid = ngx.var.RequestUUID;
  if (DirtyKeys[ruuid] == nil) then
    DirtyKeys[ruuid] = {};
  end
  if (DirtyKeys[ruuid][kqk] == nil) then
    LT('Replication:SetDirty: R: ' .. ruuid .. ' K: ' .. kqk);
    DirtyKeys[ruuid][kqk] = data.__root;
    return true;
  end
  return false;
end

function _DK.UnsetDirty(self, kqk)
  local ruuid = ngx.var.RequestUUID;
  LT('DirtyKeys:UnsetDirty: R: ' .. ruuid .. ' K: ' .. kqk);
  DirtyKeys[ruuid][kqk] = nil;
end

function _DK.GetDirtyKeys(self, ruuid)
  return DirtyKeys[ruuid];
end

function _DK.DropDirtyKeys(self, ruuid)
  DirtyKeys[ruuid] = nil;
end

return _DK;

