
Datanet:switch_user("hbuser", "password");

local DebugTestBadChannelsDecalartion = false;

local cname      = "users";
local collection = Datanet:collection(cname);

local btext = '{"_id" : "BDOC", "_channels" : [1], "b" : 1, "y" : [7,8,9]}';
if (DebugTestBadChannelsDecalartion) then
  btext = '{"_id" : "BDOC", "_channels" : {"x" : 1}, "b" : 1}';
end
ngx.say('STORE: ' .. btext);
local ret, err = collection:store(btext);
Datanet:generic_handle_response(ret, err);

