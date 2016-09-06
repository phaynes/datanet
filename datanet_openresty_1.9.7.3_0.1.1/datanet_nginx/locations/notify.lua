
local rbody = Datanet:read_full_post_body();
if (not rbody) then
  return ngx.log(ngx.EMERG, "FAILED TO READ BODY");
end

local jreq = cjson.decode(rbody);
local pmd  = jreq.post_merge_deltas;
if (pmd == nil or type(pmd) ~= "table") then return; end
local fpmd = pmd[1];
if (fpmd == nil) then return; end

local cname = Datanet.Helper:BuildConditionName(fpmd.op, fpmd.path, fpmd.value);
Datanet.Helper:ConditionSignal(cname);

