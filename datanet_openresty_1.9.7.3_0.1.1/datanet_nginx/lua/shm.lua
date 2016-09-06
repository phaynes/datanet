
local Debug = require "datanet.debug";

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLASS DECLARATION --------------------------------------------------------

local _S   = {};
_S.__index = _S;

--[[ NOTE: none of the module's functions has a SELF reference
           this is not a module more like a named global method container ]]--

_S.TableNamespace = "DATANET"; -- NOTE: OVERRIDE-ABLE

function _S.GetTableName(tname)
  return _S.TableNamespace .. "_" .. tname;
end

function _S.GetDictionaryName(tname)
  return ngx.shared[_S.GetTableName(tname)];
end

function _S.SetKey(tname, pk, sval)
  if (Debug.SHM) then
    LD('SetKey: T: ' .. tname .. ' PK: ' .. pk ..  ' sval: ' .. tostring(sval));
  end
  local dmem      = _S.GetDictionaryName(tname);
  local s, err, f = dmem:set(pk, sval);
  return 1;
end

function _S.GetKey(tname, pk)
  if (Debug.SHM) then LD('GetKey: T: ' .. tname .. ' PK: ' .. pk); end
  local dmem      = _S.GetDictionaryName(tname);
  local sval, err = dmem:get(pk);
  if (Debug.SHM and sval ~= nil) then
    LD('GetKey: sval: ' .. tostring(sval));
  end
  return sval;
end

function _S.AddKey(tname, pk)
  if (Debug.SHM) then LD('AddKey: T: ' .. tname .. ' PK: ' .. pk); end
  local dmem      = _S.GetDictionaryName(tname);
  local s, err, f = dmem:add(pk, 1);
  if (s) then return 1;
  else        return 0;
  end
end

function _S.DeleteKey(tname, pk)
  if (Debug.SHM) then LD('DeleteKey: T: ' .. tname .. ' PK: ' .. pk); end
  local dmem = _S.GetDictionaryName(tname);
  dmem:delete(pk); -- NO RETURN VALUE
  return 1;
end

function _S.DeleteTable(tname)
  if (Debug.SHM) then LD('DeleteTable: T: ' .. tname); end
  local dmem = _S.GetDictionaryName(tname);
  dmem:flush_all();
  return 1;
end

local function __get_table(tname, serialized)
  local dmem = _S.GetDictionaryName(tname);
  local keys = dmem:get_keys(0);
  local res  = {};
  if (keys == nil) then return res; end
  for i, k in ipairs(keys) do
    local sval, err = dmem:get(k);
    if (err) then return nil; end
    if (serialized) then
      res[k] = sval;
    else
      if (type(sval) == "string") then
        res[k] = cjson.decode(sval);
      else
        res[k] = sval;
      end
    end
  end
  return res;
end

-- NOTE: returns K-V table where V is serialized JSON
--       -> simplifies passing data from Lua to C (AKA: lazy coding)
function _S.GetTableSemiSerialized(tname)
  if (Debug.SHM) then LD('GetTableSemiSerialized: T: ' .. tname); end
  return __get_table(tname, true);
end

function _S.GetTable(tname)
  if (Debug.SHM) then LD('GetTable: T: ' .. tname); end
  return __get_table(tname, false);
end

function _S.SetTable(tname, data)
  if (Debug.SHM) then LD('SetTable: T: ' .. tname); end
  local dmem = _S.GetDictionaryName(tname);
  for k, v in pairs(data) do
    local sval      = cjson.encode(v);
    if (sval == nil) then return -1; end
    local s, err, f = dmem:set(k, sval);
    if (not s) then return -1; end
    if (Debug.SHM) then
      LD('SetTable: SET: T: ' .. tname .. ' K: ' .. k ..
         ' V: ' .. tostring(sval));
    end
  end
  return 1;
end

return _S;

