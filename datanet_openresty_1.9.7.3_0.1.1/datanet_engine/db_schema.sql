
-- SINGLE-ROW-TABLES
DROP TABLE NUM_WORKERS(REFERENCE TEXT    NOT NULL,
                       COUNT     INTEGER NOT NULL,
                       PRIMARY KEY(REFERENCE));
INSERT INTO NUM_WORKERS(REFERENCE, VALUE) VALUES ("ME", 1);

DROP TABLE AGENT_HEARTBEAT;
CREATE TABLE AGENT_HEARTBEAT(REFERENCE TEXT NOT NULL,
                             INFO      TEXT NOT NULL,
                             PRIMARY KEY(REFERENCE));

DROP TABLE AGENT_INFO;                           -- TYPE: ROW
CREATE TABLE AGENT_INFO(REFERENCE        TEXT NOT NULL,
                        AGENT_UUID       INTEGER,
                        DC_UUID          TEXT,
                        NEXT_OP_ID       INTEGER
                        CACHE_NUM_BYTES  INTEGER,
                        CALLBACK_SERVER  TEXT,
                        CALLBACK_PORT    INTEGER,
                        BACKOFF_END_TIME INTEGER,
                        PRIMARY KEY(REFERENCE));
INSERT INTO AGENT_INFO(REFERENCE) VALUES ("ME");


DROP TABLE CONNECTION_STATUS;                    -- TYPE: ROW
CREATE TABLE CONNECTION_STATUS(AGENT_UUID INTEGER NOT NULL,
                               CONNECTED  INTEGER,
                               ISOLATION  INTEGER,
                               PRIMARY KEY(AGENT_UUID));

DROP TABLE AGENT_CREATEDS;                       -- TYPE: INT
CREATE TABLE AGENT_CREATEDS(DATACENTER_UUID TEXT    NOT NULL,
                            CREATED         INTEGER NOT NULL,
                            PRIMARY KEY(DATACENTER_UUID));

DROP TABLE DEVICE_KEY;
CREATE TABLE DEVICE_KEY(REFERENCE TEXT NOT NULL,
                        KEY       TEXT NOT NULL,
                        PRIMARY KEY(REFERENCE));

DROP TABLE NOTIFY;
CREATE TABLE NOTIFY(REFERENCE TEXT    NOT NULL,
                    VALUE     INTEGER NOT NULL,
                    PRIMARY KEY(REFERENCE));

DROP TABLE NOTIFY_URL;
CREATE TABLE NOTIFY_URL(URL TEXT    NOT NULL,
                        VALUE     INTEGER NOT NULL,
                        PRIMARY KEY(URL));



-- SINGLE-INT: PK: PID
DROP TABLE WORKER_PARTITIONS;
CREATE TABLE WORKER_PARTITIONS(PID       INTEGER NOT NULL,
                               PARTITION INTEGER NOT NULL,
                               PRIMARY KEY(PID));

-- SINGLE-INT: PK: CHANNEL
DROP TABLE REPLICATION_CHANNELS;                 -- TYPE: INT
CREATE TABLE REPLICATION_CHANNELS(CHANNEL           TEXT    NOT NULL,
                                  NUM_SUBSCRIPTIONS INTEGER NOT NULL,
                                  PRIMARY KEY(CHANNEL));

DROP TABLE DEVICE_CHANNELS;                      -- TYPE: INT
CREATE TABLE DEVICE_CHANNELS(CHANNEL           TEXT    NOT NULL,
                             NUM_SUBSCRIPTIONS INTEGER NOT NULL,
                             PRIMARY KEY(CHANNEL));



-- BOOLEAN PK:USERNAME
DROP TABLE STATIONED_USERS;                      -- TYPE: BOOLEAN
CREATE TABLE STATIONED_USERS(USERNAME TEXT    NOT NULL,
                             VALUE    INTEGER NOT NULL,
                             PRIMARY KEY(USERNAME));

-- BOOLEAN PK:KQK
DROP TABLE CACHED_KEYS;                          -- TYPE: BOOLEAN
CREATE TABLE CACHED_KEYS(KQK    TEXT    NOT NULL,
                         VALUE  INTEGER NOT NULL,
                         PRIMARY KEY(KQK));

DROP TABLE TO_EVICT_KEYS;                        -- TYPE: BOOLEAN
CREATE TABLE TO_EVICT_KEYS(KQK    TEXT    NOT NULL,
                           VALUE  INTEGER NOT NULL,
                           PRIMARY KEY(KQK));

DROP TABLE EVICTED_KEYS;                         -- TYPE: BOOLEAN
CREATE TABLE EVICTED_KEYS(KQK    TEXT    NOT NULL,
                          VALUE  INTEGER NOT NULL,
                          PRIMARY KEY(KQK));

DROP TABLE OUT_OF_SYNC_KEYS;                     -- TYPE: BOOLEAN
CREATE TABLE OUT_OF_SYNC_KEYS(KQK    TEXT    NOT NULL,
                              VALUE  INTEGER NOT NULL,
                              PRIMARY KEY(KQK));

DROP TABLE AGENT_WATCH_KEYS;                     -- TYPE: BOOLEAN
CREATE TABLE AGENT_WATCH_KEYS(KQK    TEXT    NOT NULL,
                              VALUE  INTEGER NOT NULL,
                              PRIMARY KEY(KQK));

DROP TABLE AGENT_KEY_GC_WAIT_INCOMPLETE          -- TYPE: BOOLEAN
CREATE TABLE AGENT_KEY_GC_WAIT_INCOMPLETE(KQK    TEXT    NOT NULL,
                                          VALUE  INTEGER NOT NULL,
                                          PRIMARY KEY(KQK));


-- BOOLEAN PK: DELTA_KEY
DROP TABLE OOO_DELTA;                            -- TYPE: BOOLEAN
CREATE TABLE OOO_DELTA(DELTA_KEY TEXT    NOT NULL,
                       VALUE     INTEGER NOT NULL,
                       PRIMARY KEY(DELTA_KEY));

DROP TABLE DELTA_COMMITTED;                      -- TYPE: BOOLEAN
CREATE TABLE DELTA_COMMITTED(DELTA_KEY TEXT    NOT NULL,
                             VALUE     INTEGER NOT NULL,
                             PRIMARY KEY(DELTA_KEY));

DROP TABLE REMOVE_REFERENCE_DELTA;               -- TYPE: BOOLEAN
CREATE TABLE REMOVE_REFERENCE_DELTA(DELTA_KEY TEXT    NOT NULL,
                                    VALUE     INTEGER NOT NULL,
                                    PRIMARY KEY(DELTA_KEY));

-- SINGLE-INT: PK: KQK
DROP TABLE KEY_GC_VERSION;                       -- TYPE: INT
CREATE TABLE KEY_GC_VERSION(KQK        TEXT    NOT NULL,
                            GC_VERSION INTEGER NOT NULL,
                            PRIMARY KEY(KQK));

DROP TABLE AGENT_KEY_MAX_GC_VERSION;             -- TYPE: INT
CREATE TABLE AGENT_KEY_MAX_GC_VERSION(KQK        TEXT    NOT NULL,
                                      GC_VERSION INTEGER NOT NULL,
                                      PRIMARY KEY(KQK));

DROP TABLE DIRTY_KEYS;                           -- TYPE: INT
CREATE TABLE DIRTY_KEYS(KQK       TEXT    NOT NULL,
                        NUM_DIRTY INTEGER NOT NULL,
                        PRIMARY KEY(KQK));

DROP TABLE OOO_KEY;                              -- TYPE: INT
CREATE TABLE OOO_KEY(KQK   TEXT    NOT NULL,
                     COUNT INTEGER NOT NULL,
                     PRIMARY KEY(KQK));


-- SINGLE-INT: PK: DELTA_KEY
DROP TABLE AGENT_SENT_DELTAS;                    -- TYPE: INT
CREATE TABLE AGENT_SENT_DELTAS(DELTA_KEY TEXT NOT NULL,
                               TS        INTEGER NOT NULL,
                               PRIMARY KEY(DELTA_KEY));

DROP TABLE DIRTY_DELTAS;                         -- TYPE: INT
CREATE TABLE DIRTY_DELTAS(DELTA_KEY TEXT NOT NULL,
                          TS        INTEGER NOT NULL,
                          PRIMARY KEY(DELTA_KEY));


-- SINGLE-INT: PK: KA_KEY
DROP TABLE AGENT_KEY_OOO_GCV;                    -- TYPE: INT
CREATE TABLE AGENT_KEY_OOO_GCV(KA_KEY TEXT    NOT NULL,
                               VALUE  INTEGER NOT NULL,
                               PRIMARY KEY(KA_KEY));

-- SINGLE-INT: PK: KAV_KEY
DROP TABLE AGENT_DELTA_OOO_GCV;                  -- TYPE BOOLEAN
CREATE TABLE AGENT_DELTA_OOO_GCV(KAV_KEY TEXT    NOT NULL,
                                 VALUE   INTEGER NOT NULL,
                                 PRIMARY KEY(KAV_KEY));

DROP TABLE PERSISTED_SUBSCRIBER_DELTA;           -- TYPE BOOLEAN
CREATE TABLE PERSISTED_SUBSCRIBER_DELTA(KAV_KEY TEXT    NOT NULL,
                                        VALUE   INTEGER NOT NULL,
                                        PRIMARY KEY(KAV_KEY));


-- FIXED-SIZE: PK: KQK
DROP TABLE KEY_AGENT_VERSION;                    -- TYPE: ROW
CREATE TABLE KEY_AGENT_VERSION(KQK           TEXT NOT NULL,
                               AGENT_VERSION TEXT NOT NULL,
                               PRIMARY KEY(KQK));
--TODO treat AGENT_VERSION as UINT128 -> TYPE: UINT128

DROP TABLE TO_SYNC_KEYS;                         -- TYPE: ROW
CREATE TABLE TO_SYNC_KEYS(KQK            TEXT NOT NULL,
                          SECURITY_TOKEN TEXT NOT NULL,
                          PRIMARY KEY(KQK));

DROP TABLE KEY_INFO;                             -- TYPE: ROW
CREATE TABLE KEY_INFO(KQK      TEXT NOT NULL,
                      STICKY   INTEGER,
                      SEPARATE INTEGER,
                      PRESENT  INTEGER,
                      PRIMARY KEY(KQK));

DROP TABLE LRU_KEYS;                             -- TYPE: ROW
CREATE TABLE LRU_KEYS(KQK                   TEXT NOT NULL,
                      TS                    INTEGER,
                      LOCAL_READ            INTEGER,
                      LOCAL_MODIFICATION    INTEGER,
                      EXTERNAL_MODIFICATION INTEGER,
                      NUM_BYTES             INTEGER,
                      PIN                   INTEGER,
                      WATCH                 INTEGER,
                      PRIMARY KEY(KQK));

DROP TABLE NEED_MERGE_REQUEST_ID;                -- TYPE: ROW
CREATE TABLE NEED_MERGE_REQUEST_ID(KQK TEXT NOT NULL,
                                   ID  TEXT NOT NULL,
                                   PRIMARY KEY(KQK));


-- FIXED-SIZE: PK: USERNAME
DROP TABLE USER_AUTHENTICATION;                  -- TYPE: ROW
CREATE TABLE USER_AUTHENTICATION(USERNAME TEXT NOT NULL,
                                 HASH     TEXT NOT NULL,
                                 ROLE     TEXT NOT NULL,
                                 PRIMARY KEY(USERNAME));

-- FIXED-SIZE: PK: KA_KEY
DROP TABLE DEVICE_SUBSCRIBER_VERSION;            -- TYPE: ROW
CREATE TABLE DEVICE_SUBSCRIBER_VERSION(KA_KEY        TEXT    NOT NULL,
                                       AGENT_VERSION TEXT    NOT NULL,
                                       TS            INTEGER NOT NULL,
                                       PRIMARY KEY(KA_KEY));
--TODO: DEVICE_SUBSCRIBER_VERSION can be broken up into two UINT128 tables




-- JSON-VALUE: PK: KQK
DROP TABLE DOCUMENTS;                            -- TYPE: JSON
CREATE TABLE DOCUMENTS(KQK  TEXT NOT NULL,
                       CRDT TEXT NOT NULL,
                       PRIMARY KEY(KQK));

DROP TABLE LAST_REMOVE_DENTRY;                   -- TYPE: JSON
CREATE TABLE LAST_REMOVE_DENTRY(KQK  TEXT NOT NULL,
                                META TEXT NOT NULL,
                                PRIMARY KEY(KQK));

DROP TABLE USER_CACHED_KEYS;                     -- TYPE: ARRAY[STRING]
CREATE TABLE USER_CACHED_KEYS(KQK   TEXT NOT NULL,
                              USERS TEXT NOT NULL,
                              PRIMARY KEY(KQK));

DROP TABLE PER_KEY_SUBSCRIBER_VERSION;           -- TYPE: MAP
CREATE TABLE PER_KEY_SUBSCRIBER_VERSION(KQK               TEXT NOT NULL,
                                        AGENT_VERSION_MAP TEXT NOT NULL,
                                        PRIMARY KEY(KQK));

DROP TABLE OOO_DELTA_VERSIONS;                   -- TYPE: ARRAY[AGENT_VERSIONS]
CREATE TABLE OOO_DELTA_VERSIONS(KQK            TEXT NOT NULL,
                                AGENT_VERSIONS TEXT NOT NULL,
                                PRIMARY KEY(KQK));

DROP TABLE DELTA_VERSIONS;                       -- TYPE: ARRAY[AGENT_VERSIONS]
CREATE TABLE DELTA_VERSIONS(KQK            TEXT NOT NULL,
                            AGENT_VERSIONS TEXT NOT NULL,
                            PRIMARY KEY(KQK));

DROP TABLE KEY_GC_SUMMARY_VERSIONS;              -- TYPE: ARRAY[INT]
CREATE TABLE KEY_GC_SUMMARY_VERSIONS(KQK         TEXT NOT NULL,
                                     GC_VERSIONS TEXT NOT NULL,
                                     PRIMARY KEY(KQK));

DROP TABLE AGENT_KEY_GCV_NEEDS_REORDER;          -- TYPE: MAP
CREATE TABLE AGENT_KEY_GCV_NEEDS_REORDER(KQK     TEXT NOT NULL,
                                         GCV_MAP TEXT NOT NULL,
                                         PRIMARY KEY(KQK));

-- JSON-VALUE: PK: USERNAME
DROP TABLE USER_CHANNEL_PERMISSIONS;             -- TYPE: ARRAY[STRING]
CREATE TABLE USER_CHANNEL_PERMISSIONS(USERNAME TEXT NOT NULL,
                                      CHANNELS TEXT NOT NULL,
                                      PRIMARY KEY(USERNAME));

DROP TABLE USER_CHANNEL_SUBSCRIPTIONS;           -- TYPE: ARRAY[STRING]
CREATE TABLE USER_CHANNEL_SUBSCRIPTIONS(USERNAME TEXT NOT NULL,
                                        CHANNELS TEXT NOT NULL,
                                        PRIMARY KEY(USERNAME);

-- JSON-VALUE: PK: DELTA_KEY
DROP TABLE DELTAS;                               -- TYPE: JSON
CREATE TABLE DELTAS(DELTA_KEY TEXT NOT NULL,
                    DENTRY    TEXT NOT NULL,
                    AUTH      TEXT,
                    PRIMARY KEY(DELTA_KEY));

DROP TABLE DELTA_NEED_REFERENCE;                -- TYPE: JSON
CREATE TABLE DELTA_NEED_REFERENCE(DELTA_KEY        TEXT NOT NULL,
                                  REFERENCE_AUTHOR TEXT NOT NULL,
                                  PRIMARY KEY(DELTA_KEY));

DROP TABLE DELTA_NEED_REORDER;                  -- TYPE: JSON
CREATE TABLE DELTA_NEED_REORDER(DELTA_KEY      TEXT NOT NULL,
                                REORDER_AUTHOR TEXT NOT NULL,
                                PRIMARY KEY(DELTA_KEY));

-- JSON-VALUE: PK: G-KEY
DROP TABLE GCV_SUMMARIES;                        -- TYPE: JSON
CREATE TABLE GCV_SUMMARIES(G_KEY   TEXT NOT NULL,
                           SUMMARY TEXT NOT NULL,
                           PRIMARY KEY(G_KEY));

-- JSON-VALUE: PK: OTHER
DROP TABLE FIXLOG;                               -- TYPE: JSON
CREATE TABLE FIXLOG(FID TEXT NOT NULL,
                    PC  TEXT NOT NULL,
                    PRIMARY KEY(FID));

DROP TABLE SUBSCRIBER_LATENCY_HISTOGRAM;         -- TYPE: JSON
CREATE TABLE SUBSCRIBER_LATENCY_HISTOGRAM(DATACENTER_NAME TEXT NOT NULL,
                                          HISTOGRAM       TEXT NOT NULL,
                                          PRIMARY KEY(DATACENTER_NAME));




-- INTERNAL
DROP TABLE WORKERS;                              -- TYPE: ROW
CREATE TABLE WORKERS(UUID   INTEGER NOT NULL,
                     PORT   INTEGER NOT NULL,
                     MASTER INTEGER,
                     PRIMARY KEY(UUID));

DROP TABLE LOCKS;                                -- TYPE: BOOLEAN
CREATE TABLE LOCKS(NAME INTEGER NOT NULL,
                   PRIMARY KEY(NAME));


