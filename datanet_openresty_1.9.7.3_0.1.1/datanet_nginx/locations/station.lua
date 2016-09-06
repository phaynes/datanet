
local username = "hbuser";
local password = "password";
Datanet:switch_user(username, password);
ngx.say('STATION: U: ' .. username);
local ok, err = Datanet:station_user();
Datanet:generic_handle_response(ok, err);

