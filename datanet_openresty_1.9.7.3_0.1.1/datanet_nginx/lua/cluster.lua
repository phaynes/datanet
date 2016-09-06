
local http      = require "datanet.http";

local Partition = require "datanet.partition";
local Helper    = require "datanet.helpers";
local SHM       = require "datanet.shm";

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- CLASS DECLARATION ---------------------------------------------------------

local _C   = {};
_C.__index = _C;

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- SETTINGS ------------------------------------------------------------------

_C.cluster_ping_delay_min = 10;
_C.cluster_ping_delay_max = 15;
_C.cluster_ping_staleness = 30;

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- HELPERS -------------------------------------------------------------------

local function get_cluster_table_name()
  return "CLUSTER";
end

local function get_cluster_info_table_name()
  return "CLUSTER_INFO";
end

local function get_partition_table_name()
  return "PARTITION_TABLE";
end

local ElectionCount = 0;
local function generate_vote_id(uuid)
  ElectionCount = ElectionCount + 1;
  return uuid .. '_' .. ElectionCount;
end

local function get_partition_table_version()
  local tname = get_partition_table_name();
  local kname = "version";
  local ptv   =  SHM.GetKey(tname, kname);
  return (ptv == nil) and 0 or ptv;
end

local function persist_partition_table_version(val)
  local tname = get_partition_table_name();
  local kname = "version";
  SHM.SetKey(tname, kname, val);
end

local function get_partition_entry(ptv, i)
  local tname = get_partition_table_name();
  local kname = "partition_" .. ptv .. "_" .. i;
  return SHM.GetKey(tname, kname);
end

local function persist_partition_entry(ptv, i, n)
  local tname = get_partition_table_name();
  local kname = "partition_" .. ptv .. "_" .. i;
  SHM.SetKey(tname, kname, n);
end

local function get_current_partition_table()
  local ptv    = get_partition_table_version();
  local kmatch = "partition_" .. ptv .. "_";
  local klen   = string.len(kmatch);
  local tname  = get_partition_table_name();
  local rptbl  = SHM.GetTable(tname);
  local ptbl   = {};
  for k, v in pairs(rptbl) do
    local kstart = string.sub(k, 1, klen);
    if (kmatch == kstart) then
      table.insert(ptbl, {master = v});
    end
  end
  return ptbl;
end

local function persist_cluster_node(uuid, ip, port, sport)
  local tname = get_cluster_table_name();
  local ts    = ngx.time();
  local cnode = {ip = ip, port = port, server_port = sport, ts = ts};
  local sval  = cjson.encode(cnode);
  SHM.SetKey(tname, uuid, sval);
end

local function persist_cluster_nodes(cnodes)
  for uuid, cinfo in pairs(cnodes) do
    persist_cluster_node(uuid, cinfo.ip, cinfo.port, cinfo.server_port);
  end
end

local function get_cluster_nodes()
  local tname = get_cluster_table_name();
  return SHM.GetTable(tname);
end

local function set_cluster_info_key(kname, val)
  local tname = get_cluster_info_table_name();
  SHM.SetKey(tname, kname, val);
end

local function get_cluster_info_key(kname)
  local tname = get_cluster_info_table_name();
  return SHM.GetKey(tname, kname);
end

local function persist_cluster_state(cnodes)
  local tname = get_cluster_info_table_name();
  local sval  = cjson.encode(cnodes);
  SHM.SetKey(tname, "cluster_state", sval);
end

local function get_cluster_state()
  local tname = get_cluster_info_table_name();
  local sval  = SHM.GetKey(tname, "cluster_state");
  if (sval == nil) then return nil;
  else                  return cjson.decode(sval);
  end
end

local function get_partition_table_map_key(i)
  return "partition_map_" .. i;
end

local function get_partition_table_map_entry(slot)
  local pkey = get_partition_table_map_key(slot);
  local sval = get_cluster_info_key(pkey);
  if (sval == nil) then return nil;
  else                  return cjson.decode(sval);
  end
end

local function persist_partition_table_map(cnodes)
  local slot = 0;
  for uuid, cinfo in pairs(cnodes) do
    local pkey   = get_partition_table_map_key(slot);
    local scinfo = cjson.encode(cinfo);
    LD('persist_partition_table_map: PKEY: ' .. pkey .. ' INFO: ' .. scinfo);
    set_cluster_info_key(pkey, scinfo);
    slot         = slot + 1;
  end
end

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- NETWORKING ----------------------------------------------------------------

local function http_respond_data(id, rdata, err, edata)
  Helper:HttpRespondData(id, rdata, err, edata);
end

local function http_cluster_request(host, port, body)
  local uri     = "https://" .. host .. ":" .. port .. "/__cluster";
  local options = {
    method     = "POST",
    body       = body,
    ssl_verify = false
  };
  --LT('http_cluster_request: URI: ' .. uri .. ' BODY: ' .. body);

  local httpc    = http.new()
  local res, err = httpc:request_uri(uri, options);
  if (not res) then
    LE("ERROR: http_cluster_request FAILED: " .. err);
    return nil;
  else
    return res;
  end
end

local function parse_http_cluster_request(host, port, body)
  local res   = http_cluster_request(host, port, body);
  if (res == nil) then return; end
  local rbody = res.body;
  --LT("parse_http_cluster_request: RBODY: " .. rbody);
  return cjson.decode(rbody)
end


------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- CLUSTER STATE CHANGE VOTE/COMMIT ------------------------------------------

local function assign_cluster_nodes_as_new_nodes(self, cnodes)
  local ts = ngx.time();
  for uuid, cinfo in pairs(cnodes) do
    local nkey = "NewNodes_" .. uuid;
    set_cluster_info_key(nkey, ts);
  end
end

local function cmp_device_uuid(acnode, bcnode)
  local aduuid = acnode.device_uuid;
  local bduuid = bcnode.device_uuid;
  return (aduuid == bduuid) and 0 or ((aduuid > bduuid) and 1 or -1);
end

local function persist_cluster_commit_info(self, ptbl, cnodes)
  Helper:EncodeCLog('persist_cluster_commit_info: PT: ', ptbl);
  Helper:EncodeCLog('persist_cluster_commit_info: cnodes: ', cnodes);
  local ptv = get_partition_table_version();
  ptv       = ptv + 1;
  for i,v in ipairs(ptbl) do
    local n = ptbl[i].master;
    local j = (i - 1); -- LUA ARRAYS START AT 1, PARTITION TABLE AT 0
    persist_partition_entry(ptv, j, n);
  end
  persist_partition_table_version(ptv); -- ATOMIC UPDATE
  persist_cluster_state(cnodes);
  persist_partition_table_map(cnodes);
end

local function finalize_cluster_state_change_commit(self, term, vid, cnodes)
  LT('finalize_cluster_state_change_commit');
  local tname = get_cluster_table_name();
  SHM.DeleteTable(tname);
  persist_cluster_nodes(cnodes);
  assign_cluster_nodes_as_new_nodes(self, cnodes);
end

local function count_cluster_commit(self, data)
  if (data.error) then
    LE('count_cluster_commit: ERROR: ' .. data.error);
  else
    local res       = data.result;
    local vid       = res.vote_id;
    local cnodes    = res.cluster_nodes;
    local nnodes    = Helper:CountDictionary(cnodes);
    local ckey      = "NumCommits_" .. vid;
    local numc      = (get_cluster_info_key(ckey) + 1);
    set_cluster_info_key(ckey, numc);
    LD('count_cluster_commit: V: ' .. vid .. ' #C: ' .. numc);
    if (numc == nnodes) then
      LD('COMMIT SUCCEEDED ON ALL CLUSTER-NODES: #CN: ' .. nnodes);
      set_cluster_info_key("LastClusterCommitId", nil);
    end
  end
end

local function handle_cluster_state_change_commit(self, id, params)
  local term   = params.data.term_number;
  local vid    = params.data.vote_id;
  local cnodes = params.data.cluster_nodes;
  local ptbl   = params.data.partition_table;
  if (ptbl == nil) then
    ptbl = Partition:SimpleComputePartitionTable(cnodes);
  end
  local numc   = Helper:CountDictionary(cnodes);
  LD('handle_cluster_state_change_commit: V: ' .. vid .. ' TERM: ' .. term ..
     ' #CN: ' .. numc);
  set_cluster_info_key("TermNumber",            term); --NOTE: NEW TermNumber
  set_cluster_info_key("ForceVote",             false);
  set_cluster_info_key("ClusterVoteInProgress", false);
  persist_cluster_commit_info(self, ptbl, cnodes);
  finalize_cluster_state_change_commit(self, term, vid, cnodes);
  local rdata = {term_number   = term,
                 vote_id       = vid,
                 cluster_nodes = cnodes};
  http_respond_data(id, rdata);
end

local function broadcast_cluster_state_change_commit(self, term, vid,
                                                     cnodes, ptbl)
  Helper:EncodeCLog('broadcast_cluster_state_change_commit: CN: ', cnodes);
  local body = Helper:CreateClusterStateChangeCommitJRB(term, vid,
                                                        cnodes, ptbl);
  for uuid, cinfo in pairs(cnodes) do
    local data = parse_http_cluster_request(cinfo.ip, cinfo.port, body);
    if (data == nil) then return; end
    count_cluster_commit(self, data);
  end
end

local function count_cluster_vote(self, data)
  if (data.error) then
    local lterm  = get_cluster_info_key("TermNumber");
    LE('count_cluster_vote: ERROR: ' .. data.error);
    --LE('lterm: '..lterm); Helper:EncodeCLog('details', data.error_details);
    if (data.error_details.term_number > lterm) then
      set_cluster_info_key("TermNumber", data.error_details.term_number);
      LD('UPDATING TermNumber: ' .. data.error_details.term_number);
    end
  else
    local res     = data.result;
    local term    = res.term_number;
    local vid     = res.vote_id;
    local cnodes  = res.cluster_nodes;
    --Helper:EncodeCLog('count_cluster_vote: CN: ', cnodes);
    local nnodes  = Helper:CountDictionary(cnodes);
    local vkey    = "NumVotes_" .. vid;
    local numv    = (get_cluster_info_key(vkey) + 1);
    set_cluster_info_key(vkey, numv);
    LD('count_cluster_vote: V: ' .. vid .. ' #V: ' .. numv);
    if (numv == nnodes) then
      LD('ALL CLUSTER VOTES RECEIVED');
      local ckey    = "NumCommits_" .. vid;
      set_cluster_info_key(ckey, 0);
      set_cluster_info_key("LastClusterCommitId", vid);
      local ocnodes = get_cluster_state();
      local optbl   = get_current_partition_table();
      local ptbl    = Partition:ComputeNewFromOldPartitionTable(cnodes, ocnodes,
                                                                optbl);
      broadcast_cluster_state_change_commit(self, term, vid, cnodes, ptbl);
    end
  end
end

local function handle_cluster_state_change_vote(self, id, params)
  local term   = params.data.term_number;
  local vid    = params.data.vote_id;
  local cnodes = params.data.cluster_nodes;
  local lterm  = get_cluster_info_key("TermNumber");
  LD('handle_cluster_state_change_vote: V: ' .. vid .. ' TERM: ' .. term);
  local akey   = "AlreadyVoted_" .. term;
  if (get_cluster_info_key(akey) == true) then
    local edata = {term_number = lterm};
    http_respond_data(id, nil, "AlreadyVoted", edata);
  elseif (term < lterm) then
    local edata = {term_number = lterm};
    http_respond_data(id, nil, "OldTermNumber", edata);
  else
    set_cluster_info_key("ClusterVoteInProgress", true);
    set_cluster_info_key(akey,                    true);
    local rdata = {term_number   = term,
                   vote_id       = vid,
                   cluster_nodes = cnodes};
    http_respond_data(id, rdata);
  end
end

local function broadcast_cluster_state_change_vote(self, term, vid, cnodes)
  Helper:EncodeCLog('broadcast_cluster_state_change_vote: CN: ', cnodes);
  local body  = Helper:CreateClusterStateChangeVoteJRB(term, vid, cnodes);
  for uuid, cinfo in pairs(cnodes) do
    local data = parse_http_cluster_request(cinfo.ip, cinfo.port, body);
    if (data == nil) then return; end
    count_cluster_vote(self, data);
  end
end

local function self_assign_cluster_leader(self, cnodes)
  for uuid, cinfo in pairs(cnodes) do
    if (cinfo.ip == self.ip and cinfo.port == self.port) then
      cinfo.leader = true;
    else
      cinfo.leader = false;
    end
  end
end

local function start_new_cluster_vote(self, cnodes)
  Helper:EncodeCLog('start_new_cluster_vote: CN: ', cnodes);
  -- Commit comes after ALL Votes received
  set_cluster_info_key("LastClusterCommitId", nil);
  local lterm = (get_cluster_info_key("TermNumber") + 1);-- NOTE: NEW TermNumber
  set_cluster_info_key("TermNumber", lterm);
  local vid   = generate_vote_id(self.uuid);
  local term  = lterm;
  local vkey  = "NumVotes_" .. vid;
  set_cluster_info_key(vkey, 0);
  self_assign_cluster_leader(self, cnodes);
  broadcast_cluster_state_change_vote(self, term, vid, cnodes);
end

local function analyze_cluster_status(self, cnodes)
  local ts      = ngx.time();
  local ncnodes = {};
  local changed = false;
  for uuid, cinfo in pairs(cnodes) do
    local cts   = tonumber(cinfo.ts);
    local stale = (ts - cts);
    local nkey  = "NewNodes_" .. uuid;
    if (get_cluster_info_key(nkey) ~= nil) then
      if (stale < _C.cluster_ping_staleness) then -- Normal Healthy node
        ncnodes[uuid] = cinfo;
      else
        changed = true;
        LD('CN: UUID: ' .. uuid .. ' is DOWN: stale: ' .. stale);
      end
    else -- NEW NODE
      if (stale < _C.cluster_ping_staleness) then
        set_cluster_info_key(nkey, ts);
        changed        = true;
        ncnodes[uuid]  = cinfo;
        LD('CN: UUID: ' .. uuid .. ' is NEW');
      else
        set_cluster_info_key(nkey, nil);
      end
    end
  end
  if (changed) then
    start_new_cluster_vote(self, ncnodes);
  else
    LD('analyze_cluster_status: NO CLUSTER CHANGE');
  end
end

local function force_vote(self, cnodes)
  LD('FORCE CLUSTER_VOTE');
  assign_cluster_nodes_as_new_nodes(self, cnodes);
  start_new_cluster_vote(self, cnodes);
end

local function evaluate_last_cluster_commit(self, cnodes)
  local nnodes = Helper:CountDictionary(cnodes);
  local lccid  = get_cluster_info_key("LastClusterCommitId");
  if (lccid ~= nil) then
    local ckey  = "NumCommits_" .. lccid;
    local ncmts = get_cluster_info_key(ckey);
    if (ncmts ~= nil) then
      if  (ncmts ~= nnodes) then
        LD('FORCE CLUSTER_VOTE: #CMT: ' .. ncmts .. ' #CN: ' .. nnodes);
        set_cluster_info_key("ForceVote", true);
      end
    end
  end
  if (get_cluster_info_key("ClusterVoteInProgress") == true) then
    LD('FORCE CLUSTER_VOTE: LAST CLUSTER_VOTE NEVER ENDED');
    set_cluster_info_key("ForceVote", true);
  end
  set_cluster_info_key("ClusterVoteInProgress", false);
end

local function cluster_check(self)
  local cnodes = get_cluster_nodes();
  evaluate_last_cluster_commit(self, cnodes);
  if (get_cluster_info_key("ForceVote") == true) then
    force_vote(self, cnodes);
  else
    analyze_cluster_status(self, cnodes);
  end
end

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- PING DAEMON ---------------------------------------------------------------

local function ping_entire_cluster(self)
  local body = Helper:CreateClusterPingJRB(self.ip, self.port, self.server_port,
                                           self.uuid);
  local cnodes = get_cluster_nodes();
  for uuid, cinfo in pairs(cnodes) do
    http_cluster_request(cinfo.ip, cinfo.port, body);
  end
end

local function run_ping_daemon(premature, self)
   if (premature) then return end
  ping_entire_cluster(self);
  cluster_check(self);
  local to = math.random(_C.cluster_ping_delay_min, _C.cluster_ping_delay_max);
  ngx.timer.at(to, run_ping_daemon, self);
end

local function initialize_ping_daemon(self)
  set_cluster_info_key("ForceVote",             true);
  local lterm  = get_cluster_info_key("TermNumber");
  if (lterm == nil) then -- CASE: NewPrimaryElected
    set_cluster_info_key("TermNumber", 1);
  end
  set_cluster_info_key("ClusterVoteInProgress", false);
  set_cluster_info_key("LastClusterCommitId",   nil);
end

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- CLUSTER PING HANDLER ------------------------------------------------------

local function handle_cluster_ping(self, params)
  local cuuid  = params.data.device.uuid;
  local cip    = params.data.ip;
  local cport  = params.data.port;
  local csport = params.data.server_port;
  persist_cluster_node(cuuid, cip, cport, csport);
end

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- DISCOVERY -----------------------------------------------------------------

local function ask_discovery(self, ip, port, dip, dport)
  local body  = Helper:CreateGetClusterInfoJRB(ip, port);
  local res   = http_cluster_request(dip, dport, body);
  if (res == nil) then return; end

  local rbody = res.body;
  --LT("RBODY: " .. rbody);
  local data  = cjson.decode(rbody)
  if (data == nil) then return; end
  if (data.result == nil or data.result.cluster_nodes == nil) then return; end
  local cnodes = data.result.cluster_nodes;
  persist_cluster_nodes(cnodes);
end

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- PUBLIC API ----------------------------------------------------------------

function _C.Initialize(self, ip, port, sport, uuid, discovery)
  self.ip          = ip;
  self.port        = port;
  self.server_port = sport;
  self.uuid        = uuid;
  LD('Cluster:Initialize: IP: ' .. self.ip ..  ' PORT: ' .. self.port);
  persist_cluster_node(self.uuid, self.ip, self.port, self.server_port);
  if (discovery ~= nil) then
    ask_discovery(self, ip, port, discovery.ip, discovery.port);
  end
  initialize_ping_daemon(self);
  run_ping_daemon(false, self);
end

function _C.HandleRequest(self, qbody)
  --LT('DatanetCluster:handle_request: QBODY: ' .. qbody);
  local data   = cjson.decode(qbody)
  if (data == nil) then return; end
  local method = data.method;
  if (method == nil) then
    LE("ERROR: NO METHOD");
    ngx.exit(501);
  else
    local id = data.id;
    if (method == "GetClusterInfo") then
      local cnodes = get_cluster_nodes();
      local rdata  = {cluster_nodes = cnodes};
      http_respond_data(id, rdata);
      handle_cluster_ping(self, data.params);
    elseif (method == "ClusterPing") then
      local rdata = {status = "OK"};
      http_respond_data(id, rdata);
      handle_cluster_ping(self, data.params);
    elseif (method == "ClusterStateChangeVote") then
      handle_cluster_state_change_vote(self, id, data.params);
    elseif (method == "ClusterStateChangeCommit") then
      handle_cluster_state_change_commit(self, id, data.params);
    else
      LE("ERROR: UNSUPPORTED METHOD: " .. method);
      ngx.exit(501);
    end
  end
end

function _C.GetStickyServer(self, sk)
  local np    = Partition.NumberClusterPartitions;
  local hash  = ngx.crc32_short(sk)
  local hslot = hash % np;
  local ptv   = get_partition_table_version();
  local pslot = get_partition_entry(ptv, hslot);
  return get_partition_table_map_entry(pslot);
end

return _C;
