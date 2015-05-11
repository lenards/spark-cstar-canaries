CREATE KEYSPACE IF NOT EXISTS canary_ks
WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 1};

USE canary_ks;

CREATE TABLE kv (key int PRIMARY KEY, value text);

CREATE TABLE kv2 (
    key int,
    ts timestamp,
    value text,
    PRIMARY KEY (key, ts)
) WITH CLUSTERING ORDER BY (ts DESC);

CREATE KEYSPACE IF NOT EXISTS canary
WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE canary;

CREATE TABLE IF NOT EXISTS eventrecord (
   key text,
   cmpkey text,
   ts timestamp,
   event text,
   num int,
   PRIMARY KEY ((key, cmpkey), ts)
) WITH CLUSTERING ORDER BY (ts DESC);