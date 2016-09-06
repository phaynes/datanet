"use strict";

(function(exports) { // START: Closure -> allows Node.js & Browser to use

// TODO TOO many delimiters [|-_] for keyname, 2 should suffice (e.g. [|-])
//      |-> dont forget to change all the string.split(delims)

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// COLLECTION CONSTANTS ------------------------------------------------------

exports.InfoNamespace = 'INFO';

exports.Device                        = {};
exports.Device.AdminCollName          = 'DatanetAdmin';
exports.Device.UserCollName           = 'DatanetUser';
exports.Device.UserInfoCollName       = 'DatanetUserInfo';
exports.Device.UserAuthCollName       = 'DatanetUserAuth';
exports.Device.UserPermsCollName      = 'DatanetUserPermissions';
exports.Device.UserSubsCollName       = 'DatanetUserSubscriptions';
exports.Device.StationedUsersCollName = 'DatanetStationedUsers';
exports.Device.GlobalCollName         = 'DatanetGlobal';
exports.Device.DeviceToKeyCollName    = 'DatanetDeviceToKey';

exports.Metadata                  = {};
exports.Metadata.KeyInfoCollName  = 'DatanetKeyInfo';
exports.Metadata.KeyCollName      = 'DatanetKey';
exports.Metadata.GC_CollName      = 'DatanetGarbage';
exports.Metadata.GCData_CollName  = 'DatanetGarbageData';
exports.Metadata.LDRCollName      = 'DatanetLastDeltaRemove';
exports.Metadata.LRUCollName      = 'DatanetLRU';
exports.Metadata.DeltaCollName    = 'DatanetDelta';
exports.Metadata.CrdtCollName     = 'DatanetCrdt';
exports.Metadata.OOODeltaName     = 'DatanetOOODelta';
exports.Metadata.DeviceKeyName    = 'DatanetDeviceKey';


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// FIELD CONSTANTS -----------------------------------------------------------

// STANDALONE
exports.ServiceStart                 = "ServiceStart";
exports.NextDeviceUUID               = "NextDeviceUUID";
exports.AgentIsolation               = "DeviceIsolation";
exports.AllIsolatedDevices           = "IsolatedDevices";

exports.AgentLastCreated             = "AgentLastCreated";

exports.AgentKeysToSync              = "AgentKeysToSync";
exports.AgentKeysOutOfSync           = "AgentKeysOutOfSync";
exports.AgentDirtyKeys               = "AgentDirtyKeys";
exports.AgentDirtyDeltas             = "AgentDirtyDeltas";
exports.AgentSentDeltas              = "AgentSentDeltas";

exports.CentralKeysToSync            = "CentralKeysToSync";//TODO new collection
exports.CentralDirtyDeltas           = "CentralDirtyDeltas";//TODO new collectio

exports.ClusterNodeAliveStatus       = "ClusterNodeAliveStatus";
exports.LastVoteTermNumber           = "LastVoteTermNumber"
exports.ClusterState                 = "ClusterState";
exports.PartitionTable               = "PartitionTable";

exports.GeoNodeAliveStatus           = "GeoNodeAliveStatus";
exports.LastVoteGeoTermNumber        = "LastVoteGeoTermNumber"
exports.LastGeoState                 = "LastGeoState";

exports.KnownDataCenters             = "KnownDataCenters";
exports.DataCenterLastCreated        = "DataCenterLastCreated";

exports.AgentDataCenters             = "AgentDataCenters";

exports.AllNamespaceCollections      = "AllNamespacesCollections";

exports.FixLog                       = "FixLog";           //TODO new collection

exports.SubscriberLatencyHistogram   = "SubscriberLatencyHistogram";

exports.NotifyURLs                   = "NotifyURLs";

exports.AgentCacheNumBytes           = "AgentCacheNumBytes";

exports.StorageSprinkle              = "StorageSprinkle";
exports.StorageQueueMinSync          = "StorageQueueMinSync";

exports.LRUIndex                     = "LRUIndex";

// ADMIN
exports.AdminDevices_name            = "Devices";
exports.DeviceSubscriptions_name     = "DeviceSubscriptions";
exports.IsolatedState_name           = "IsolatedState";
exports.ChannelToDevice_name         = "ChannelToDevice";
exports.DeviceKey_name               = "DeviceKey";
exports.AgentDeviceKey_name          = "AgentDeviceKey";

// SUBSCRIPTION MAPPINGS
exports.ChannelToKeys_name           = "ChannelToKeys";
exports.KeyModification_name         = "KeyModification";
exports.KeyRepChans_name             = "KeyReplicationChannels";

// CACHE MAPPINGS
exports.KeyToDevices_name            = "KeyToDevices";
exports.DeviceToKeys_name            = "DeviceToKeys";
exports.AgentUserToCachedKeys_name   = "AgentUserToCachedKeys";
exports.AgentCachedKeyToUsers_name   = "AgentCachedKeyToUsers";
exports.AgentWatchKeys_name          = "AgentWatchKeys";
exports.Evicted_name                 = "EvictedKey";

// DELTA-VERSION
exports.DeviceToCentralAgentVersion_name = "DeviceToCentralAgentVersion";
exports.KeyToCentralAgentVersion_name    = "KeyToCentralAgentVersion";
exports.CentralKeyAgentVersions_name     = "CentralKeyAgentVersions";

exports.DeviceSubscriberVersion_name    = "DeviceSubscriberVersion";
exports.PerKeySubscriberVersion_name    = "PerKeySubscriberVersion";
exports.AgentKeyDeltaVersions_name      = "AgentKeyDeltaVersions";
exports.KeyAgentVersion_name            = "KeyAgentVersion";

exports.CentralGeoAckDeltaMap_name      = "CentralGeoAckDeltaMap";
exports.CentralGeoAckCommitMap_name     = "CentralGeoAckCommitMap";

exports.RemoveReferenceDelta_name       = "RemoveReferenceDelta";

exports.DeltaCommitted_name             = "DeltaCommitted";

// PERSIST-DELTA
exports.AgentPersistDelta_name        = "AgentDelta";
exports.CentralPersistDelta_name      = "ClusterDelta";
exports.PersistedStorageDelta_name    = "PersistedStorageDelta";
exports.PersistedSubscriberDelta_name = "PersistedSubscriberDelta";
exports.GCVSummary_name               = "GCVSummary";
exports.GCVersionMap_name             = "GCVersionMap";
exports.GCVReapKeyHighwater_name      = "GCVReapKeyHighwater";

// OOO-SUBSCIBER AGENT VERSION
exports.OOOKey_name              = "OOOKey";
exports.OOODelta_name            = "OOODelta";
exports.OOOKeyDeltaVersions_name = "OOOKeyDeltaVersions";
exports.AgentKeyOOOGCV_name      = "AgentKeyOOOGCV"
exports.DeltaNeedReference_name  = "DeltaNeedReference";
exports.DeltaNeedReorder_name    = "DeltaNeedReorder";

// USER-PERMISSIONS
exports.UserAuthentication           = "Authentication";
exports.UserChannelPermissions       = "Permissions";
exports.UserChannelSubscriptions     = "UserSubscriptions";

// STATION-USER
exports.AgentStationedUsers          = "StationedUsers";
exports.UserStationedOnAgents_name   = "UserStationedOnAgents";

// PER-KEY-INFO
exports.KeyInfo_name                 = "KeyInfo";
exports.KeyGCVersion_name            = "KeyGCVersion";
exports.AgentKeyMaxGCVersion_name    = "AgentKeyMaxGCVersion";
exports.LastRemoveDelta_name         = "LastRemoveDelta";
exports.NeedMergeRequestID_name      = "NeedMergeRequestID";

// AGENT-CLUSTER
exports.LastClusterVoteSummary_name  = "LastClusterVoteSummary";
exports.AppServerClusterState_name   = "AppServerClusterState";
exports.MemcacheClusterState_name    = "MemcacheClusterState";

// GARBAGE-COLLECTION
exports.AgentKeyGCVNeedsReorder_name    = "AgentKeyGCVNeedsReorder";
exports.AgentDeltaGCVNeedsReorder_name  = "AgentDeltaGCVNeedsReorder";
exports.AgentKeyGCWaitIncomplete_name   = "AgentKeyGCWaitIncomplete";



// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GET ADMIN KEYS ------------------------------------------------------------

exports.GetAdminDevices = function(sub) {
  return this.AdminDevices_name + '_' +  sub.UUID;
}
exports.GetDeviceSubscriptions = function(sub) {
  return this.DeviceSubscriptions_name + '_' +  sub.UUID;
}
exports.GetIsolatedState = function(sub) {
  return this.IsolatedState_name + '_' + sub.UUID;
}
exports.GetChannelToDevice = function(rchanid) {
  return this.ChannelToDevice_name + '_' + rchanid;
}

exports.GetDeviceKey = function(auuid) {
  return this.DeviceKey_name + "|" + auuid;
}
exports.GetAgentDeviceKey = function() {
  return this.AgentDeviceKey_name;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// SUBSCRIPTION MAPPINGS -----------------------------------------------------

exports.GetChannelToKeys = function(rchanid) {
  return this.ChannelToKeys_name + '_' + rchanid;
}
exports.GetKeyModification = function(kqk) {
  return this.KeyModification_name + '_' + kqk;
}

exports.GetKeyRepChans = function(ks) {
  return this.KeyRepChans_name + '|' + ks.kqk;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CACHE MAPPINGS ------------------------------------------------------------

exports.GetKeyToDevices = function(kqk) {
  return this.KeyToDevices_name + '_' + kqk;
}
exports.GetDeviceToKeys = function(duuid) {
  return this.DeviceToKeys_name + '_' + duuid;
}

exports.GetAgentUserToCachedKeys = function(username) {
  return this.AgentUserToCachedKeys_name + '_' + username;
}

exports.GetAgentCachedKeyToUsers = function(ks) {
  return this.AgentCachedKeyToUsers_name + '_' + ks.kqk;
}

exports.GetAgentWatchKeys = function(ks, suuid) {
  return this.AgentWatchKeys_name + '_' + ks.kqk + "_" + suuid;
}

exports.GetEvicted = function(ks) {
  return this.Evicted_name + '_' + ks.kqk;
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PERSIST DELTA -------------------------------------------------------------

exports.GetCentralPersistDelta = function(kqk, auuid, avrsn) {
  return this.CentralPersistDelta_name + '_' + kqk + '_' + auuid +
                                         '_' + avrsn;
}

exports.GetPersistedStorageDelta = function(ks, author) {
  return this.PersistedStorageDelta_name + '-' + get_ks_author(ks, author);
}

exports.GetPersistedSubscriberDelta = function(ks, author) {
  return this.PersistedSubscriberDelta_name + '-' + get_ks_author(ks, author);
}

exports.GetAgentPersistDelta = function(kqk, auuid, avrsn) {
  return this.AgentPersistDelta_name + '_' + kqk + '_' + auuid + '_' + avrsn;
}

exports.GetGCVSummary = function(kqk, gcv) {
  return this.GCVSummary_name + '-' + kqk + '-' + gcv;
}

exports.GetGCVersionMap = function(kqk) {
  return this.GCVersionMap_name + '-' + kqk;
}

exports.GetGCVReapKeyHighwater = function(ks) {
  return this.GCVReapKeyHighwater_name + '-' + ks.kqk;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// OOO SUBSCRIBER DELTA ------------------------------------------------------

exports.GetOOOKey = function(kqk) {
  return this.OOOKey_name + '-' + kqk;
}

exports.GetOOODelta = function(ks, author) {
  return this.OOODelta_name + '-' + get_ks_author(ks, author);
}

exports.GetOOOKeyDeltaVersions = function(kqk) {
  return this.OOOKeyDeltaVersions_name + '-' + kqk;
}

exports.GetAgentKeyOOOGCV = function(ks, auuid) {
  return this.AgentKeyOOOGCV_name + "_" + ks.kqk + "_" + auuid;
}

exports.GetDeltaNeedReference = function(ks, author) {
  return this.DeltaNeedReference_name + "_" + get_ks_author(ks, author);
}

exports.GetDeltaNeedReorder = function(ks, author) {
  return this.DeltaNeedReorder_name + "_" + get_ks_author(ks, author);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT-VERSION -------------------------------------------------------------

// CENTRAL: PER [SUB,KQK] AGENT-VERSION
exports.GetDeviceToCentralAgentVersion = function(suuid, kqk) {
  return this.DeviceToCentralAgentVersion_name + '_' + suuid + '_' + kqk;
}

// CENTRAL: PER [KQK,SUB] AGENT-VERSION
exports.GetKeyToCentralAgentVersion = function(kqk) {
  return this.KeyToCentralAgentVersion_name + '_' + kqk;
}

// SUBSCRIBER: PER [AUUID] AGENT-VERSION
exports.GetDeviceSubscriberVersion  = function(suuid) {
  return this.DeviceSubscriberVersion_name + '_' + suuid;
}

// SUBSCRIBER: PER [KQK] AGENT-VERSION
exports.GetPerKeySubscriberVersion  = function(kqk) {
  return this.PerKeySubscriberVersion_name + '_' + kqk;
}

exports.GetKeyAgentVersion = function(kqk) {    // PER KEY: agent-version
  return this.KeyAgentVersion_name + '_' + kqk;
}

exports.GetAgentKeyDeltaVersions = function(kqk) {
  return this.AgentKeyDeltaVersions_name + '_' + kqk;
}

exports.GetCentralKeyAgentVersions = function(kqk) {
  return this.CentralKeyAgentVersions_name + '_' + kqk;
}

function get_ks_author(ks, author) {
  var auuid = author.agent_uuid;
  var avrsn = author.agent_version;
  return ks.kqk + "_" + auuid + "_" + avrsn;
}

exports.GetCentralGeoAckDeltaMap = function(ks, author) {
  return this.CentralGeoAckDeltaMap_name + '_' + get_ks_author(ks, author);
}

exports.GetCentralGeoAckCommitMap = function(ks, author) {
  return this.CentralGeoAckCommitMap_name + '_' + get_ks_author(ks, author);
}

exports.GetRemoveReferenceDelta = function(ks, author) {
  return this.RemoveReferenceDelta_name + "_" + get_ks_author(ks, author);
}

exports.GetDeltaCommitted = function(ks, author) {
  return this.DeltaCommitted_name + '_' + get_ks_author(ks, author);
}

exports.GetEtherKey = function(author) {
  var auuid = author.agent_uuid;
  var avrsn = author.agent_version;
  return auuid + '-' + avrsn;
}

exports.CentralDirtyDeltaKeyFromValues = function(kqk, auuid, avrsn) {
  return kqk + '-' + auuid + '-' + avrsn;
}
exports.CentralDirtyDeltaKey = function(kqk, author) {
  var auuid = author.agent_uuid;
  var avrsn = author.agent_version;
  return exports.CentralDirtyDeltaKeyFromValues(kqk, auuid, avrsn);
}

exports.GetDirtySubscriberDeltaKey = function(kqk, author) {
  var auuid = author.agent_uuid;
  var avrsn = author.agent_version;
  return kqk + '-' + auuid + '-' + avrsn;
}

exports.GetDirtyAgentDeltaKey = function(kqk, auuid, avrsn) {
  var author = {agent_uuid : auuid, agent_version : avrsn};
  return exports.GetDirtySubscriberDeltaKey(kqk, author);
}

exports.GetSentAgentDeltaKey = function(kqk, avrsn) {
  return kqk + '-' + avrsn;
}

exports.CreateSubscriberDeltaKey = function(auuid, avrsn) {
  return auuid + '-' + avrsn;
}

exports.CreateAssassinKey = function(tval) {
  return tval["_"] + '|' + tval["#"];
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// PER-KEY-INFO --------------------------------------------------------------

exports.GetKeyInfo = function(ks) {
  return this.KeyInfo_name + '-' + ks.kqk;
}

exports.GetKeyGCVersion = function(kqk) {
  return this.KeyGCVersion_name + '-' + kqk;
}

exports.GetAgentKeyMaxGCVersion = function(kqk) {
  return this.AgentKeyMaxGCVersion_name + '-' + kqk;
}

exports.GetLastRemoveDelta = function(ks) {
  return this.LastRemoveDelta_name + '-' + ks.kqk;
}

exports.GetNeedMergeRequestID = function(ks) {
  return this.NeedMergeRequestID_name + '-' + ks.kqk;
}

exports.GetAllNamespaceCollectionsValue = function(ks) {
  return ks.ns + '|' + ks.cn;
} 


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// AGENT-CLUSTER -------------------------------------------------------------

exports.GetLastClusterVoteSummary = function(duuid) {
  return this.LastClusterVoteSummary_name + '_' + duuid;
}

exports.GetAppServerClusterState = function(clname) {
  return this.AppServerClusterState_name + '_' + clname;
}

exports.GetMemcacheClusterState = function(clname) {
  return this.MemcacheClusterState_name + '_' + clname;
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// USER PERMISSIONS ----------------------------------------------------------

exports.GetUserAuthentication = function(uname) {
  return this.UserAuthentication + '_' + uname;
}

exports.GetUserChannelPermissions = function(uname) {
  return this.UserChannelPermissions + '_' + uname;
}

exports.GetUserChannelSubscriptions = function(uname) {
  return this.UserChannelSubscriptions + '_' + uname;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// STATION-USER --------------------------------------------------------------

exports.GetAgentStationedUsers = function(duuid) {
  return this.AgentStationedUsers + '_' + duuid;
}
exports.GetUserStationedOnAgents = function(username) {
  return this.UserStationedOnAgents_name + '_' + username;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GARBAGE-COLLECTION --------------------------------------------------------

exports.GetAgentKeyGCVNeedsReorder = function(ks) {
  return exports.AgentKeyGCVNeedsReorder_name + "_" + ks.kqk;
}

exports.GetAgentDeltaGCVNeedsReorder = function(ks) {
  return exports.AgentDeltaGCVNeedsReorder_name + "-" + ks.kqk;
}

exports.GetAgentKeyGCWaitIncomplete = function(ks) {
  return exports.AgentKeyGCWaitIncomplete_name + "_" + ks.kqk;
}


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ERRORS --------------------------------------------------------------------

exports.Errors = {
  'GenericError'       :
                   '-ERROR: Generic Error: system is temporarily kinda wonky',

  'NotCurrentlySupported' : '-ERROR: Function not CURRENTLY supported',

  'WssReconnecting'    :
             '-ERROR: Agent currently reconnecting to AgentMaster-ClusterNode',
  'TooManyConnFailures' : '-ERROR: Agent has failed to connect too many consecutive times - sleep a little and try again',

  'SocketNotWritable'  : '-ERROR: Internal Socket NOT writable',
  'SocketNotReady'     : '-ERROR: Internal Socket NOT Ready',
  'WssUnavailable'     : '-ERROR: Secure-Websocket currently not available',
  'WssConnectInProgress' : '-ERROR: WSS currently connecting',

  'HttpsSocketClose' : '-ERROR: HTTPS Server socket closed',

  'NoCentralConn'      :
                       '-ERROR: Agent: Command requires connection to Central',
  'CentralConnDown'    : '-ERROR: Agent: No connection to Central',
  'AgentNotSynced'     : '-ERROR: Agent: Not yet synced -> NO-OP',

  'SecurityViolation'  : '-ERROR: Security Violation, tokens do not match',

  'NoDataFound'        : '-ERROR: No Data Found',
  'FieldNotFound'      : '-ERROR: Field not Found',
  'ModifyWatchKey'     : '-ERROR: COMMIT/REMOVE on WATCHED KEY -> must CACHE KEY to COMMIT/REMOVE',

  'IncrOnNaN'          : '-ERROR: INCR only supported on Numbers',
  'IncrByNaN'          : '-ERROR: INCR field by a NaN',
  'KeyNotString'       : '-ERROR: KEY must be a STRING',
  'ReservedFieldMod'   :
           '-ERROR: Changing reserved fields (_id, _meta,_channels) prohibited',
  'SetArrayInvalid'    :
                  '-ERROR: SET() on an array invalid format, use SET(num, val)',
  'SetValueInvalid'    : '-ERROR: SET(key, val) invalid typeof(val)',
  'SetPrimitiveInvalid' :
         '-ERROR: Treating primitive (number,string) as nestable(object,array)',
  'InsertArgsInvalid'  :
    '-ERROR: USAGE: insert(key, index, value) - "index" must be a positive integer',
  'InsertTypeInvalid'  :
                  '-ERROR: INSERT is an array command, does not work on type: ',
  'NestedFieldMissing' : '-ERROR: Nested field does not exist, field_name: ',
  'LargeListOnlyRPUSH' : '-ERROR: LARGE_LIST supports ONLY RPUSH',
  'RpushUnsupportedType' : '-ERROR: RPUSH on unsupported type (must be array)',

  'BasicAuthMissing'   : '-ERROR: Basic Authentication MISSING', // ZC-PROXY
  'BasicAuthFail'      : '-ERROR: Basic Authentication FAIL',
  'SubscribePermsFail' : '-ERROR: Subscribe permissions FAIL',
  'WritePermsFail'     : '-ERROR: Write permissions FAIL',
  'ReadPermsFail'      : '-ERROR: Read permissions FAIL',
  'MessageAuthFail'    : '-ERROR: Message Authentication FAIL',

  'BadPrivilegeDesc'   : '-ERROR: Bad Privilege -> USE [READ,WRITE,REVOKE]',
  'AdminAuthFail'      : '-ERROR: Admin Authentication FAIL',
  'AuthError'          : '-ERROR: Authentication Error',
  'AuthIsolation'      :
              '-ERROR: Authentication not possible in Device ISOLATION',
  'AuthNoCentralConn'  :
              '-ERROR: Authentication not possible NO Central Connections',

  'OpNotOnSelf'      : '-ERROR: Operation only possible on current user',

  'ChannelPerms'       : '-ERROR: Channel Permissions Failed',
  'SubscriberOffline'  : '-ERROR: Send to Offline Subscriber',
  'AlreadySubscribed'  : '-ERROR: User Already Subscribed to Channel',
  'NotSubscribed'      : '-ERROR: User NOT Subscribed to Channel',
  'AlreadyStationed'   : '-ERROR: User ALREADY Stationed',
  'NotStationed'       : '-ERROR: User NOT Stationed',
  'EvictUncached'      : '-ERROR: Evicting KEY that has NOT been CACHED',
  'CacheOnSubscribe'   :
                      '-ERROR: CACHE Key -> ALREADY SUBSCRIBED to KEYs CHANNEL',

  'OutOfOrderDelta'        : '-ERROR: OUT-OF-ORDER Delta',
  'RepeatDelta'            : '-ERROR: REPEAT DELTA -> IGNORED',
  'MalformedAckAgentDelta' : '-ERROR: AgentDelta-ACK is MALFORMED',
  'MalformedDentries'      : '-ERROR: Dentries[] is MALFORMED',
  'RepeatSubscriberDelta'  : '-ERROR: REPEAT SUBSCRIBER DELTA -> IGNORED',

  'NonExistentUser'    : '-ERROR: USER does not exist',
  'BadUserRole'        : '-ERROR: ROLE: [ADMIN,USER]',

  'AlreadyVoted'       : '-ERROR: Already voted (CLUSTER) this TERM',
  'OldTermNumber'      : '-ERROR: (CLUSTER) Term Number in the past',
  'ClusterVoteInProgress' :
                  '-ERROR: Operation FAILED -> Cluster ReORG Vote In Progress',

  'AlreadyGeoVoted'    : '-ERROR: Already voted (GEO) this TERM',
  'OldGeoTermNumber'   : '-ERROR: (GEO) Term Number in the past',
  'BackwardsProgressGeoVote' :
              '-ERROR: Vote does not cover current GeoCluster',

  'InGeoSync'          : '-ERROR: DataCenter currently GEO-SYNCing',
  'NoRandomGeoNode'    : '-ERROR: Could not find Random GEO-Node',
  'MeNotInCluster'     :
             '-ERROR: Post Geo-Vote-Commit MY DataCenter is NOT in GeoCluster',
  'TlsClientNotReady'   : '-ERROR: TLS Client not READY',
  'ClusterNodeNotReady' : '-ERROR: Cluster Node not READY',
  'SelfGeoNotReady'     : '-ERROR: Geo-Cluster not READY',
  'GeoNodeNotFound'     : '-ERROR: GeoNode not found',
  'GeoNodeUnknown'      : '-ERROR: Geo-Node is UNKNOWN',
  'ErrorClientLocked'   : '-ERROR: Client locked (in process of being created)',
  'ErrorGeoClientLocked' : '-ERROR: GEO-Client locked (in process of being created)',

  'CentralNotSynced'    : '-ERROR: Cluster not yet SYNCED',

  'DatabaseDisabled'    : '-ERROR: Database disabled (via SIGUSR2)',
  'FixLogRunning'       : '-ERROR: FixLog Replay Running',

  'FrozenKeyOperation'   : '-ERROR: Operation on Frozen Key -> NO-OP',
  'DeltaNotSync'         : '-ERROR: Delta not SYNCED',
  'KeyInToSyncState'     : '-ERROR: Key currently in TO-SYNC state',

  'CommitOnOutOfSyncKey' :
                  '-ERROR: COMMIT on KEY currently in NeedsSync state -> FAIL',
  'NoKeyToRemove'        : '-ERROR: REMOVE on NON-existent key',
  'NoKeyToExpire'        : '-ERROR: EXPIRE on NON-existent key',

  'PullDeltaClock'       :
                        '-ERROR: on PULL, DELTA NEWER than CRDT -> IMPOSSIBLE',

  'HttpsNotSupported'     : '-ERROR: HTTPS not supported - use WSS or TLS',
  'WrongJsonRpcId'        : '-ERROR: JSONRPC version != 2.0',
  'RequestMissingUUID'    : 
      '-ERROR: Request requires the following data-structure: params : {data: {device: {uuid: 123 }}}',
  'MethodMustBePost'      : '-ERROR: HTTP Method must be POST',
  'HttpsServerNotReady'   : '-ERROR: HTTPS/WSS server not ready yet',
  'AuthenticationMissing' : '-ERROR: required field: AUTHENTICATION missing',
  'AgentConnectingToDB'   : '-ERROR: Agent connecting to DB',
  'JsonRpcParamsMissing'  : '-ERROR: Json RPC PARAMS{} are empty',

  'IncrementHeartbeatUsage'    :
     '-ERROR: USAGE: INCREMENT-HEARTBEAT START|STOP|QUERY [fieldname]',
  'HeartbeatBadStart' :
     '-ERROR: USAGE: (INCREMENT/TIMESTAMP/ARRAY)-HEARTBEAT START fieldname',
  'TimestampHeartbeatUsage'      :
     '-ERROR: USAGE: TIMESTAMP-HEARTBEAT START|STOP|QUERY [UUID max-size trim]',
  'ArrayHeartbeatUsage'      :
     '-ERROR: USAGE: ARRAY-HEARTBEAT START|STOP|QUERY [UUID max-size trim]',

  'ClusterIDRequired'     :
                        '-ERROR: field {cluster : { id : CLUSTER# }} required',

  'JsonParseFail'        : '-ERROR: Failed to parse JSON',
  'JsonIDMissing'        : '-ERROR: DocumentKey must be defined in "_id" field',
  'JsonMetaDefined'      : '-ERROR: "_meta" field is RESERVED',
  'JsonChannelsNotArray' : '-ERROR: "_channels" field must be an array',
  'JsonChannelsFormat'   : '-ERROR: "_channels" field must be an array of strings (e.g. ["1"]',
  'JsonChannelsTooMany'  : '-ERROR: "_channels" currently only supports an array w/ a single string channel (e.g. ["1"])',

  'RchanChange'          : '-ERROR: "_channels" can NOT be changed once set',

  'ExpireNaN'            : '-ERROR: "_expire" field must be a NUMBER',

  'OverlappingSubscriberMergeRequests' :
           '-ERROR: more recent SubscriberMerge on its way -> IGNORE this one',
  'BrowserNotify'        :
                     '-ERROR: NOTIFY command only valid for stand-alone Agent',
  'NotifyFormat'        : '-ERROR: NOTIFY [ADD|REMOVE] URL',
  'NotifyURLNotWSS'     : '-ERROR: NOTIFY URL MUST BEGIN w/ "WSS://"',

  'BrowserShutdown'      :
                   '-ERROR: SHUTDOWN command only valid for stand-alone Agent',
  'ServerFilterNotFound'  : '-ERROR: SERVER-FILTER NOT FOUND in zfilter.js',
  'ServerFilterFailed'    : '-ERROR: SERVER-FILTER FAILED',
  'FilterFunctionTimeout' : '-ERROR: SERVER-FILTER TIMED-OUT',

  'VirginNoRepChans'      : '-ERROR: STATELESS/MEMCACHE-COMMIT on non-existent key requires "_channels[]", use zdoc.set_channels() or "CHANNELS" command',
  'TestKeyNotLocal'       : '-ERROR: Key not found -> FETCH or CACHE key first',
  'TestKeyNotLocalValOpEq' :
    '-ERROR: Key not found, needed for "+" operation -> FETCH or CACHE key first',
  'NoClientMemcacheClusterState' : '-ERROR: MemcacheClusterState not yet initialized, call zcmd_client.GetMemcacheClusterState() FIRST ... or external scripts can call ZMC.SetClientMemcacheClusterState()',
  'MemcacheClusterNoClientFound' : '-ERROR: MemcacheClient not found',
  'MemcacheClusterStateInvalid'  : '-ERROR: MemcacheClusterState is INVALID -> FORMAT: {cluster_nodes:[{ip,aport,mport,device_uuid}]}',
  'MemcacheKeyNotLocal'          : '-ERROR: Memcache Key NOT Local, MemcacheClusterState NOT up-to-date',

  'InvalidTestCommand'   : '-ERROR: test command syntax {file|command|comment}',

  'MessageFormat'        : '-ERROR: MESSAGE FORMAT {route.[key,user,device]} are mutually exclusive',

  'DeviceKeyMismatch'    : '-ERROR: DeviceKey in Agent Method does NOT match',

  // NOTE: next two are error-texts FROM the DB
  'ElementNotFound'       : 'element not found',    // NOTE: no -ERROR
  'KeyNotFound'           : 'key not found',        // NOTE: no -ERROR
  'NetConnectRefused'     : 'connect ECONNREFUSED', // NOTE: no -ERROR
};


// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ENGINE CALLBACKS ----------------------------------------------------------

exports.EngineCallback = {}; // NOTE: ALL MUST BE OVERRIDDEN


// END: Closure -> allows Node.js & Browser to use
})(typeof(exports) === 'undefined' ? this['ZS']={} : exports);

