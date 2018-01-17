CREATE KEYSPACE complex_ks WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1' } AND DURABLE_WRITES = true;

CREATE TABLE complex_ks.rank_by_year_and_name (
    race_year int,
    race_name text,
    rank int,
    cyclist_name text,
    PRIMARY KEY ((race_year, race_name), rank)
) WITH CLUSTERING ORDER BY (rank ASC)
    AND read_repair_chance = 0.0
    AND dclocal_read_repair_chance = 0.1
    AND replicate_on_write = true
    AND gc_grace_seconds = 864000
    AND bloom_filter_fp_chance = 0.01
    AND caching = 'KEYS_ONLY'
    AND comment = ''
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy' }
    AND compression = { 'sstable_compression' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99.0PERCENTILE'
    AND index_interval = 128;

CREATE INDEX rrank ON complex_ks.rank_by_year_and_name (rank);

CREATE INDEX ryear ON complex_ks.rank_by_year_and_name (race_year);

CREATE TABLE complex_ks.ztable (
    zkey text,
    a int,
    PRIMARY KEY (zkey)
) WITH read_repair_chance = 0.0
    AND dclocal_read_repair_chance = 0.1
    AND replicate_on_write = true
    AND gc_grace_seconds = 864000
    AND bloom_filter_fp_chance = 0.1
    AND caching = 'KEYS_ONLY'
    AND comment = ''
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'sstable_size_in_mb' : 95 }
    AND compression = { 'sstable_compression' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99.0PERCENTILE'
    AND index_interval = 128;
