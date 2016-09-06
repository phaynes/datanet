
local Helper    = require "datanet.helpers";
local Debug     = require "datanet.debug";

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- CLASS DECLARATION ---------------------------------------------------------

local _P   = {};
_P.__index = _P;


------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- SETTINGS ------------------------------------------------------------------

_P.NumberClusterPartitions = 11;

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- HELPERS -------------------------------------------------------------------

local function shuffle(arr)
  local cslot = table.getn(arr);
  while (cslot ~= 0) do
    local rslot = math.floor(math.random() * cslot); -- pick random element
    cslot       = cslot - 1;
    local tval  = arr[cslot]; -- swap with the current element
    arr[cslot]  = arr[rslot];
    arr[rslot]  = tval;
  end
  return arr;
end

local function cmp_device_uuid(acnode, bcnode)
  local aduuid = acnode.device_uuid;
  local bduuid = bcnode.device_uuid;
  return (aduuid == bduuid) and 0 or ((aduuid > bduuid) and 1 or -1);
end

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- SIMPLE PARTITION TABLE ----------------------------------------------------

function _P.SimpleComputePartitionTable(self, cnodes)
  Helper:EncodeCLog('simple_compute_partition_table: CN: ', cnodes);
  local nptbl  = {};
  table.sort(cnodes, cmp_device_uuid);
  local nnodes = Helper:CountDictionary(cnodes);
  for i = 1, self.NumberClusterPartitions do
    local n         = nnodes and (i % nnodes) or 0;
    nptbl[i]        = {};
    nptbl[i].master = n;
  end
  return nptbl;
end

------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- CLUSTER BRAIN -------------------------------------------------------------

local create_partiton_histogram; -- LOCAL FUNCTION DEFINED BELOW

local function debug_create_diff_ptbl(odiff, ocnodes, ncnodes, optbl,
                                      nptbl, phist, nlen, per)
  if (odiff < 1) then LD("GAINED " .. math.abs(odiff) .. " NODES");
  else                LD("LOST "   .. odiff           .. " NODES"); end
  Helper:EncodeCLog("ocnodes: ", ocnodes);
  Helper:EncodeCLog("ncnodes: ", ncnodes);
  Helper:EncodeCLog("optbl: ",   optbl);
  if (odiff > 0) then Helper:EncodeCLog("nptbl: ", nptbl); end
  Helper:EncodeCLog("phist: ",   phist);
end

local function deep_debug_final_diff_ptbl(nptbl, nop)
  local phist = create_partiton_histogram(nptbl, nop);
  Helper:EncodeCLog("FINAL: nptbl: ", nptbl);
  Helper:EncodeCLog("FINAL: phist: ", phist);
end

local function debug_final_diff_ptbl(cnptbl, nptbl, nop)
  if (Debug.FinalDiffPartitionTable) then
    deep_debug_final_diff_ptbl(nptbl, nop);
  end
  local hits = 0;
  for i = 1, nop do
    local nn = nptbl[i]["master"];
    local cn = cnptbl[i]["master"];
    if (cn == nn) then hits = hits + 1; end
  end
  local hitrate = math.floor((hits * 100) / nop);
  LD("NEW PARTITION-TABLE: HIT-RATE: " .. hitrate);
end

local function cmp_partition_histogram(h1, h2)
  local cnt1 = h1["cnt"];
  local cnt2 = h2["cnt"];
  return (cnt1 == cnt2) and 0 or ((cnt1 > cnt2) and -1 or 1); -- DESC
end

local function create_uuid_dict(cnodes, nc)
  local d = {};
  local i = 1;
  for uuid, cnode in pairs(cnodes) do
    d[uuid] = i;
    i       = i + 1;
  end
  return d;
end

local function create_slot_dict(cnodes, nc)
  local d = {};
  local i = 1;
  for uuid, cnode in pairs(cnodes) do
    d[i] = uuid;
    i    = i + 1;
  end
  return d;
end

local function reorder_old_partition_table(optbl, ncnodes, ocnodes,
                                           nop, noc, nnc)
  local nud   = create_uuid_dict(ncnodes, nnc);
  local osd   = create_slot_dict(ocnodes, noc);
  local nptbl = Helper:Copy(optbl);
  for i = 1, nop do
    local on    = optbl[i]["master"] + 1; -- Lua arrays start at 1
    local ouuid = osd[on];
    local nn    = nud[ouuid];
    --LD("i: " .. i .. " on: " .. on .. " ouuid: " .. ouuid .. " nn: " ..nn);
    if (on ~= nn) then
      if (nn == nil) then
        nptbl[i]["master"] = nil;
      else
        nptbl[i]["master"] = (nn - 1); -- PartitionTable starts at 0
      end
    end
  end
  return nptbl;
end

local function find_diff_cluster_node_slot(ncnodes, ocnodes, nnc, noc)
  local nud    = create_uuid_dict(ncnodes, nnc);
  local oud    = create_uuid_dict(ocnodes, noc);
  local odiff  = noc - nnc;
  local bigd   = (odiff < 0) and nud or oud;
  local smalld = (odiff < 0) and oud or nud;
  local xn     = {};
  for uuid, val in pairs(bigd) do
    if (smalld[uuid] == nil) then
      table.insert(xn, (val - 1)); -- PartitionTable starts at 0
    end
  end
  return xn;
end

create_partiton_histogram = function(ptbl, nop) -- LOCAL FUNCTION
  local hist = {};
  local maxn = 0;
  for i = 1, nop do
    local m = ptbl[i]["master"];
    if (m ~= nil) then
      local n   = m + 1; -- Lua arrays start at 1
      local cnt = hist[n] and hist[n]["cnt"] or 0;
      cnt       = cnt + 1;
      hist[n]   = {n = n, cnt = cnt};
      if (n > maxn) then maxn = n; end
    end
  end
  for i = 1, maxn do
    if (hist[i] == nil) then
      hist[i] = {n = i, cnt = 0};
    end
  end
  table.sort(hist, cmp_partition_histogram);
  return hist;
end

function _P.ComputeNewFromOldPartitionTable(self, ncnodes, ocnodes, optbl)
  LT("Partition:ComputeNewFromOldPartitionTable");
  local nnc    = Helper:CountDictionary(ncnodes);
  local noc    = Helper:CountDictionary(ocnodes);
  local nop    = Helper:CountDictionary(optbl);
  if (nop == 0) then return nil; end
  local xn     = find_diff_cluster_node_slot(ncnodes, ocnodes, nnc, noc);
  local nxn    = Helper:CountDictionary(xn);
  local nptbl  = reorder_old_partition_table(optbl, ncnodes, ocnodes,
                                             nop, noc, nnc);
  local cnptbl = Helper:Copy(nptbl);
  local odiff  = noc - nnc;
  LD("create_diff_partition_table: odiff: " .. odiff);
  Helper:EncodeCLog("XN: ", xn);
  local phist  = create_partiton_histogram(nptbl, nop);
  local nph    = Helper:CountDictionary(phist);
  if (nph == 0) then return optbl;
  else
    if (Debug.CreateDiffPartitionTable) then
      debug_create_diff_ptbl(odiff, ocnodes, ncnodes, optbl, nptbl, phist);
    end
    if (odiff < 0) then -- GAINED NODE(S)
      local iarr = {};
      for i = 1, nop do
        iarr[i] = i;
      end
      local nlen = nnc;
      local per  = (math.floor(nop / nlen) * nxn) + 1;
      local xnc  = 1;
      LD("nlen: " .. nlen .. " per: " .. per);
      while (per > 0) do
        shuffle(iarr); -- Traverse nptbl[] in a "random" order
        local head  = table.remove(phist, nph); -- ARRAY-SHIFT ->
        local tn    = (head["n"] - 1); -- PartitionTable starts at 0
        head["cnt"] = head["cnt"] - 1;
        table.insert(phist, head);              -- ARRAY-PUSH <-
        table.sort(phist, cmp_partition_histogram);
        for k, i in pairs(iarr) do
          local n = nptbl[i]["master"];
          if (tn == n) then
            local xnm = xn[xnc];
            xnc       = xnc + 1;
            if (xnc > nxn) then xnc = 1; end
            if (xnm ~= n) then
              nptbl[i]["master"] = xnm;
              per                = per - 1;
              break;
            end
          end
          if (per == 0) then break; end
        end
      end
      debug_final_diff_ptbl(cnptbl, nptbl, nop);
      return nptbl;
    else                -- LOST NODE(S)
      for i = 1, nop do
        if (nptbl[i]["master"] == nil) then
          local tail         = table.remove(phist, 1); -- ARRAY-POP ->
          local tn           = (tail["n"] - 1); -- PartitionTable starts at 0
          nptbl[i]["master"] = tn;
          tail["cnt"]        = tail["cnt"] + 1;
          table.insert(phist, tail);                   -- ARRAY-PUSH <-
          table.sort(phist, cmp_partition_histogram);
        end
      end
      debug_final_diff_ptbl(cnptbl, nptbl, nop);
      return nptbl;
    end
  end
end

function _P.TestCreatePartitionTable(self, cnodes, ocnodes, optbl)
  Debug.CreateDiffPartitionTable = true;
  Debug.FinalDiffPartitionTable  = true;
  local nop                     = Helper:CountDictionary(optbl);
  self.NumberClusterPartitions  = nop;
  LT("ZPart.TestCreatePartitionTable: NumberClusterPartitions: " .. nop);
  return self:ComputeNewFromOldPartitionTable(cnodes, ocnodes, optbl);
end

return _P;
