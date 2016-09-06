
local start        = false;
local is_increment = false;
local is_array     = false;
local is_timestamp = false;
local field        = null;

local args = ngx.req.get_uri_args()
for key, val in pairs(args) do
  if type(val) ~= "table" then
    if (key == "S") then
      start = true;
    elseif (key == "I") then
      is_increment = true;
    elseif (key == "A") then
      is_array = true;
    elseif (key == "T") then
      is_timestamp = true;
    elseif (key == "F") then
      field = val;
    else
      ngx.say("ERROR: Bad URL PARAMETER -> only [S,I,A,T,F] supported");
      return;
    end
  end
end

if (not is_increment and not is_array and not is_timestamp) then
  ngx.say("ERROR: Missing TYPE URL PARAMETER: -> [I,A,T] supported");
  return;
end

if (start and is_increment and field == null) then
  ngx.say("ERROR: INCREMENT-TIMESTAMP requires FIELD [F] argument");
end

local cmd    = start and "START" or "STOP";
local uuid   = "OR";
local mlen   = -16;
local trim   = 4;
local isi    = is_increment;
local isa    = is_array;

c_log('heartbeat.lua: cmd: ' .. tostring(cmd) .. ' isi: ' .. tostring(isi) ..
      ' isa: ' .. tostring(isa) .. ' field: ' .. tostring(field));

local ok, err = Datanet:heartbeat(cmd, field, uuid, mlen, trim, isi, isa);
Datanet:generic_handle_response(ok, err);

