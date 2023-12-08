
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

CREATE TABLE ks_0.cyclist_mv (
    cid uuid,
    age int,
    birthday date,
    country text,
    name text,
    PRIMARY KEY (cid)
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys':'ALL','rows_per_partition':'NONE'}
    AND comment = ''
    AND compaction = {'class':'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy','max_threshold':'32','min_threshold':'4'}
    AND compression = {'chunk_length_in_kb':'64','class':'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND extensions = {}
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE INDEX cyclist_by_country ON ks_0.cyclist_mv (country);

CREATE TABLE ks_0.rank_by_year_and_name (
    race_year int,
    race_name text,
    rank int,
    cyclist_name text,
    PRIMARY KEY ((race_year, race_name), rank)
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys':'ALL','rows_per_partition':'NONE'}
    AND comment = ''
    AND compaction = {'class':'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy','max_threshold':'32','min_threshold':'4'}
    AND compression = {'chunk_length_in_kb':'64','class':'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND extensions = {}
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE INDEX rrank ON ks_0.rank_by_year_and_name (rank);

CREATE INDEX ryear ON ks_0.rank_by_year_and_name (race_year);

CREATE TABLE ks_0.ztable (
    zkey text,
    a frozen<ks_0.atype>,
    PRIMARY KEY (zkey)
) WITH bloom_filter_fp_chance = 0.1
    AND caching = {'keys':'ALL','rows_per_partition':'NONE'}
    AND comment = ''
    AND compaction = {'class':'org.apache.cassandra.db.compaction.LeveledCompactionStrategy','sstable_size_in_mb':'95'}
    AND compression = {'chunk_length_in_kb':'64','class':'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND extensions = {}
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE MATERIALIZED VIEW ks_0.cyclist_by_a_age AS
SELECT * FROM ks_0.cyclist_mv
WHERE age IS NOT NULL AND cid IS NOT NULL
PRIMARY KEY (age, cid) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys':'ALL','rows_per_partition':'NONE'}
    AND comment = ''
    AND compaction = {'class':'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy','max_threshold':'32','min_threshold':'4'}
    AND compression = {'chunk_length_in_kb':'64','class':'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND extensions = {}
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE MATERIALIZED VIEW ks_0.cyclist_by_age AS
SELECT
    age,
    cid,
    birthday,
    country,
    name
FROM ks_0.cyclist_mv
WHERE age IS NOT NULL AND cid IS NOT NULL
PRIMARY KEY (age, cid) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys':'ALL','rows_per_partition':'NONE'}
    AND comment = 'simple view'
    AND compaction = {'class':'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy','max_threshold':'32','min_threshold':'4'}
    AND compression = {'chunk_length_in_kb':'64','class':'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND extensions = {}
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE MATERIALIZED VIEW ks_0.cyclist_by_r_age AS
SELECT
    age,
    cid,
    birthday,
    country,
    name
FROM ks_0.cyclist_mv
WHERE age IS NOT NULL AND cid IS NOT NULL
PRIMARY KEY (age, cid) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys':'ALL','rows_per_partition':'NONE'}
    AND comment = ''
    AND compaction = {'class':'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy','max_threshold':'32','min_threshold':'4'}
    AND compression = {'chunk_length_in_kb':'64','class':'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND extensions = {}
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE FUNCTION ks_0.avgfinal(state tuple<int, bigint>)
    CALLED ON NULL INPUT
    RETURNS double
    LANGUAGE java
    AS 'double r = 0; if (state.getInt(0) == 0) return null; r = state.getLong(1); r /= state.getInt(0); return Double.valueOf(r);';

CREATE FUNCTION ks_0.avgstate(state tuple<int, bigint>,val int)
    CALLED ON NULL INPUT
    RETURNS tuple<int, bigint>
    LANGUAGE java
    AS 'if (val !=null) { state.setInt(0, state.getInt(0)+1); state.setLong(1, state.getLong(1)+val.intValue()); } return state;';

CREATE AGGREGATE ks_0.average(int)
    SFUNC avgstate
    STYPE tuple<int, bigint>
    FINALFUNC avgfinal
    INITCOND (0,0);

CREATE AGGREGATE ks_0.mean(int)
    SFUNC avgstate
    STYPE tuple<int, bigint>
    FINALFUNC avgfinal
    INITCOND (0,0);
