
local isolate = false;

local args = ngx.req.get_uri_args()
for key, val in pairs(args) do
  if type(val) ~= "table" then
    if (key == "I") then
      isolate = true;
    else
      ngx.say("ERROR: Bad URL PARAMETER -> only [I] supported");
      return;
    end
  end
end

c_log('isolation.lua: isolate: ' .. tostring(isolate));

local ok, err = Datanet:isolation(isolate);
Datanet:generic_handle_response(ok, err);

