package com.datastax.driver.core.schemabuilder;

import static com.datastax.driver.core.schemabuilder.TableOptions.Caching.ROWS_ONLY;
import static org.assertj.core.api.Assertions.assertThat;
import org.testng.annotations.Test;
import com.datastax.driver.core.DataType;

public class AlterTest {


    @Test
    public void should_alter_column_type() throws Exception {
        //When
        final String built = SchemaBuilder.alterTable("test").alterColumn("name").type(DataType.ascii());

        //Then
        assertThat(built).isEqualTo("ALTER TABLE test ALTER name TYPE ascii");
    }

    @Test
    public void should_alter_column_type_with_keyspace() throws Exception {
        //When
        final String built = SchemaBuilder.alterTable("ks", "test").alterColumn("name").type(DataType.ascii());

        //Then
        assertThat(built).isEqualTo("ALTER TABLE ks.test ALTER name TYPE ascii");
    }

    @Test
    public void should_add_column() throws Exception {
        //When
        final String built = SchemaBuilder.alterTable("test").addColumn("location").type(DataType.ascii());

        //Then
        assertThat(built).isEqualTo("ALTER TABLE test ADD location ascii");
    }

    @Test
    public void should_rename_column() throws Exception {
        //When
        final String built = SchemaBuilder.alterTable("test").renameColumn("name").to("description");

        //Then
        assertThat(built).isEqualTo("ALTER TABLE test RENAME name TO description");
    }

    @Test
    public void should_drop_column() throws Exception {
        //When
        final String built = SchemaBuilder.alterTable("test").dropColumn("name");

        //Then
        assertThat(built).isEqualTo("ALTER TABLE test DROP name");
    }

    @Test
    public void should_alter_table_options() throws Exception {
        //When
        final String built = SchemaBuilder.alterTable("test").withOptions()
                .bloomFilterFPChance(0.01)
                .caching(ROWS_ONLY)
                .comment("This is a comment")
                .compactionOptions(TableOptions.CompactionOptions.leveledStrategy().ssTableSizeInMB(160))
                .compressionOptions(TableOptions.CompressionOptions.lz4())
                .dcLocalReadRepairChance(0.21)
                .defaultTimeToLive(100)
                .gcGraceSeconds(9999L)
                .indexInterval(512)
                .memtableFlushPeriodInMillis(12L)
                .populateIOOnCacheFlush(true)
                .replicateOnWrite(true)
                .speculativeRetry(TableOptions.SpeculativeRetryValue.always())
                .build();

        //Then
        assertThat(built).isEqualTo("ALTER TABLE test " +
                "WITH caching = 'rows_only' " +
                "AND bloom_filter_fp_chance = 0.01 " +
                "AND comment = 'This is a comment' " +
                "AND compression = {'sstable_compression' : 'LZ4Compressor'} " +
                "AND compaction = {'class' : 'LeveledCompactionStrategy', 'sstable_size_in_mb' : 160} " +
                "AND dclocal_read_repair_chance = 0.21 " +
                "AND default_time_to_live = 100 " +
                "AND gc_grace_seconds = 9999 " +
                "AND index_interval = 512 " +
                "AND memtable_flush_period_in_ms = 12 " +
                "AND populate_io_cache_on_flush = true " +
                "AND replicate_on_write = true " +
                "AND speculative_retry = 'ALWAYS'");
    }
}
