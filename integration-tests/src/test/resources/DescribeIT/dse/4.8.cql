
CREATE KEYSPACE ks_0 WITH replication = { 'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1' } AND durable_writes = true;

CREATE TYPE ks_0.btype (
    a text
);

CREATE TYPE ks_0.xtype (
    d text
);

CREATE TYPE ks_0.ztype (
    c text,
    a int
);

CREATE TYPE ks_0.ctype (
    z frozen<ks_0.ztype>,
    x frozen<ks_0.xtype>
);

CREATE TYPE ks_0.atype (
    c frozen<ks_0.ctype>
);

CREATE TABLE ks_0.rank_by_year_and_name (
    race_year int,
    race_name text,
    rank int,
    cyclist_name text,
    PRIMARY KEY ((race_year, race_name), rank)
) WITH CLUSTERING ORDER BY (rank DESC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'class':'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression':'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND dclocal_read_repair_chance = 0.1
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';

CREATE INDEX ryear ON ks_0.rank_by_year_and_name (race_year);

CREATE INDEX rrank ON ks_0.rank_by_year_and_name (rank);

CREATE TABLE ks_0.ztable (
    zkey text,
    a frozen<ks_0.atype>,
    PRIMARY KEY (zkey)
) WITH bloom_filter_fp_chance = 0.1
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'class':'org.apache.cassandra.db.compaction.LeveledCompactionStrategy','sstable_size_in_mb':'95'}
    AND compression = {'sstable_compression':'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND dclocal_read_repair_chance = 0.1
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';
