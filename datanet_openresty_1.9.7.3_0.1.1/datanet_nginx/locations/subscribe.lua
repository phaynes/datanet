
local username = "hbuser";
local password = "password";
local schanid  = 1;
Datanet:switch_user(username, password);
ngx.say('SUBSCRIBE: U: ' .. username .. ' C: ' .. schanid);
local ok, err = Datanet:subscribe(schanid);
Datanet:generic_handle_response(ok, err);

