CREATE KEYSPACE complex_ks WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1' } AND DURABLE_WRITES = true;

CREATE TYPE complex_ks.btype (
    a text
);

CREATE TYPE complex_ks.xtype (
    d text
);

CREATE TYPE complex_ks.ztype (
    c text,
    a int
);

CREATE TYPE complex_ks.ctype (
    "Z" frozen<complex_ks.ztype>,
    x frozen<complex_ks.xtype>
);

CREATE TYPE complex_ks.atype (
    c frozen<complex_ks.ctype>
);

CREATE TABLE complex_ks.rank_by_year_and_name (
    race_year int,
    race_name text,
    rank int,
    cyclist_name text,
    PRIMARY KEY ((race_year, race_name), rank)
) WITH CLUSTERING ORDER BY (rank ASC)
    AND read_repair_chance = 0.0
    AND dclocal_read_repair_chance = 0.1
    AND gc_grace_seconds = 864000
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = ''
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy' }
    AND compression = { 'sstable_compression' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99.0PERCENTILE'
    AND min_index_interval = 128
    AND max_index_interval = 2048;

CREATE INDEX rrank ON complex_ks.rank_by_year_and_name (rank);

CREATE INDEX ryear ON complex_ks.rank_by_year_and_name (race_year);

CREATE TABLE complex_ks.ztable (
    zkey text,
    a frozen<complex_ks.atype>,
    PRIMARY KEY (zkey)
) WITH read_repair_chance = 0.0
    AND dclocal_read_repair_chance = 0.1
    AND gc_grace_seconds = 864000
    AND bloom_filter_fp_chance = 0.1
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = ''
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'sstable_size_in_mb' : 95 }
    AND compression = { 'sstable_compression' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99.0PERCENTILE'
    AND min_index_interval = 128
    AND max_index_interval = 2048;
