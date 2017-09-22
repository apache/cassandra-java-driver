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
    z frozen<complex_ks.ztype>,
    x frozen<complex_ks.xtype>
);

CREATE TYPE complex_ks.atype (
    c frozen<complex_ks.ctype>
);

CREATE TABLE complex_ks.cyclist_mv (
    cid uuid,
    age int,
    birthday date,
    country text,
    name text,
    PRIMARY KEY (cid)
) WITH read_repair_chance = 0.0
    AND dclocal_read_repair_chance = 0.1
    AND gc_grace_seconds = 864000
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = ''
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 64, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99PERCENTILE'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0;

CREATE INDEX cyclist_by_country ON complex_ks.cyclist_mv (country);

CREATE MATERIALIZED VIEW complex_ks.cyclist_by_a_age AS
    SELECT *
    FROM complex_ks.cyclist_mv
    WHERE age IS NOT NULL AND cid IS NOT NULL
    PRIMARY KEY (age, cid)
    WITH CLUSTERING ORDER BY (cid ASC)
    AND read_repair_chance = 0.0
    AND dclocal_read_repair_chance = 0.1
    AND gc_grace_seconds = 864000
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = ''
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 64, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99PERCENTILE'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0;

CREATE MATERIALIZED VIEW complex_ks.cyclist_by_age AS
    SELECT age, cid, birthday, country, name
    FROM complex_ks.cyclist_mv
    WHERE age IS NOT NULL AND cid IS NOT NULL
    PRIMARY KEY (age, cid)
    WITH CLUSTERING ORDER BY (cid ASC)
    AND read_repair_chance = 0.0
    AND dclocal_read_repair_chance = 0.1
    AND gc_grace_seconds = 864000
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = 'simple view'
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 64, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99PERCENTILE'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0;

CREATE MATERIALIZED VIEW complex_ks.cyclist_by_r_age AS
    SELECT age, cid, birthday, country, name
    FROM complex_ks.cyclist_mv
    WHERE age IS NOT NULL AND cid IS NOT NULL
    PRIMARY KEY (age, cid)
    WITH CLUSTERING ORDER BY (cid DESC)
    AND read_repair_chance = 0.0
    AND dclocal_read_repair_chance = 0.1
    AND gc_grace_seconds = 864000
    AND bloom_filter_fp_chance = 0.01
    AND caching = { 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }
    AND comment = ''
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 64, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99PERCENTILE'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0;

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
    AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, 'min_threshold' : 4 }
    AND compression = { 'chunk_length_in_kb' : 64, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99PERCENTILE'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0;

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
    AND compression = { 'chunk_length_in_kb' : 64, 'class' : 'org.apache.cassandra.io.compress.LZ4Compressor' }
    AND default_time_to_live = 0
    AND speculative_retry = '99PERCENTILE'
    AND min_index_interval = 128
    AND max_index_interval = 2048
    AND crc_check_chance = 1.0;

CREATE FUNCTION complex_ks.avgfinal(state tuple<int, bigint>)
    CALLED ON NULL INPUT
    RETURNS double
    LANGUAGE java
    AS 'double r = 0; if (state.getInt(0) == 0) return null; r = state.getLong(1); r /= state.getInt(0); return Double.valueOf(r);';

CREATE FUNCTION complex_ks.avgstate(state tuple<int, bigint>,val int)
    CALLED ON NULL INPUT
    RETURNS tuple<int, bigint>
    LANGUAGE java
    AS 'if (val !=null) { state.setInt(0, state.getInt(0)+1); state.setLong(1, state.getLong(1)+val.intValue()); } return state;';

CREATE AGGREGATE complex_ks.average(int)
    SFUNC avgstate
    STYPE tuple<int, bigint>
    FINALFUNC avgfinal
    INITCOND (0,0);

CREATE AGGREGATE complex_ks.mean(int)
    SFUNC avgstate
    STYPE tuple<int, bigint>
    FINALFUNC avgfinal
    INITCOND (0,0);
