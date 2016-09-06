
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- CLASS DECLARATION --------------------------------------------------------

local _T    = {};
_T.__index  = _T;

--TODO cross reference w/ c_client/storage.cpp -> OUT-OF-DATE

_T.names = {"DOCUMENTS",
            "LAST_REMOVE_DENTRY",
            "USER_AUTHENTICATION",
            "STATIONED_USERS",
            "TO_SYNC_KEYS",
            "OUT_OF_SYNC_KEYS",
            "AGENT_KEY_OOO_GCV",
            "OOO_KEY",
            "OOO_DELTA",
            "OOO_DELTA_VERSION",
            "DELTA_COMMITTED",
            "DIRTY_KEYS",
            "FIXLOG",
            "AGENT_PERMISSONS",
            "AGENT_SUBSCRIPTIONS",
            "DELTAS",
            "DIRTY_DELTAS",
            "SUBSCRIBER_DELTA_VERSIONS",
            "AGENT_SENT_DELTAS",
            "GCV_SUMMARIES",
            "REPLICATION_CHANNELS",
            "DEVICE_CHANNELS",
            "USER_CHANNEL_PERMISSIONS",
            "USER_CHANNEL_SUBSCRIPTIONS",
            "USER_CACHED_KEYS",
            "SUBSCRIBER_LATENCY_HISTOGRAM",
            "AGENT_PROCESSED",
            "AGENT_INFO",
            "CONNECTION_STATUS",
            "GEO_NODES",
            "AGENT_CREATEDS",
            "AGENT_WATCH_KEYS",
            "KEY_AGENT_VERSION",
            "DEVICE_SUBSCRIBER_VERSION",
            "PER_KEY_SUBSCRIBER_VERSION",
            "KEY_REPLICATION_CHANNELS",
            "DELTA_NEED_REORDER",
            "DELTA_NEED_GC_REORDER",
            "REMOVE_REFERENCE_DELTA",
            "KEY_INFO",
            "CACHED_KEYS",
            "LRU_KEYS",
            "LRU_KEYS",
            "KEY_GC_SUMMARY_VERSION_LIST"};

return _T;

