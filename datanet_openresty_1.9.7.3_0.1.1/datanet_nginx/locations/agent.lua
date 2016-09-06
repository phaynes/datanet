
local rbody = Datanet:read_full_post_body();
if (not rbody) then ngx.log(ngx.EMERG, "FAILED TO READ BODY");
else                Datanet:frontend_call(rbody);
end

