/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core.schemabuilder;

import com.datastax.driver.core.DataType;
import org.testng.annotations.Test;

import static com.datastax.driver.core.schemabuilder.SchemaBuilder.*;
import static org.assertj.core.api.Assertions.assertThat;

public class AlterTest {

    @Test(groups = "unit")
    public void should_alter_column_type() throws Exception {
        //When
        SchemaStatement statement = alterTable("test").alterColumn("name").type(DataType.ascii());

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tALTER TABLE test ALTER name TYPE ascii");
    }

    @Test(groups = "unit")
    public void should_alter_column_type_with_keyspace() throws Exception {
        //When
        SchemaStatement statement = alterTable("ks", "test").alterColumn("name").type(DataType.ascii());

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tALTER TABLE ks.test ALTER name TYPE ascii");
    }

    @Test(groups = "unit")
    public void should_alter_column_type_to_UDT() throws Exception {
        //When
        SchemaStatement statement = alterTable("ks", "test").alterColumn("address").udtType(frozen("address"));

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tALTER TABLE ks.test ALTER address TYPE frozen<address>");
    }

    @Test(groups = "unit")
    public void should_add_column() throws Exception {
        //When
        SchemaStatement statement = alterTable("test").addColumn("location").type(DataType.ascii());

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tALTER TABLE test ADD location ascii");
    }

    @Test(groups = "unit")
    public void should_add_column_with_UDT_type() throws Exception {
        //When
        SchemaStatement statement = alterTable("test").addColumn("location").udtType(frozen("address"));

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tALTER TABLE test ADD location frozen<address>");
    }

    @Test(groups = "unit")
    public void should_rename_column() throws Exception {
        //When
        SchemaStatement statement = alterTable("test").renameColumn("name").to("description");

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tALTER TABLE test RENAME name TO description");
    }

    @Test(groups = "unit")
    public void should_drop_column() throws Exception {
        //When
        SchemaStatement statement = alterTable("test").dropColumn("name");

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tALTER TABLE test DROP name");
    }

    @Test(groups = "unit")
    public void should_alter_table_options() throws Exception {
        //When
        // Note that this does not necessarily represent a valid configuration, the goal is just to test all options
        // (some of which might be specific to C* 2.0 or 2.1)
        SchemaStatement statement = alterTable("test").withOptions()
                .bloomFilterFPChance(0.01)
                .caching(Caching.ROWS_ONLY)
                .comment("This is a comment")
                .compactionOptions(leveledStrategy().ssTableSizeInMB(160))
                .compressionOptions(lz4())
                .dcLocalReadRepairChance(0.21)
                .defaultTimeToLive(100)
                .gcGraceSeconds(9999)
                .indexInterval(256)
                .minIndexInterval(64)
                .maxIndexInterval(512)
                .memtableFlushPeriodInMillis(12)
                .populateIOCacheOnFlush(true)
                .replicateOnWrite(true)
                .readRepairChance(0.42)
                .speculativeRetry(always())
                .cdc(true);

        SchemaStatement statementWith21Caching = alterTable("test").withOptions()
                .caching(KeyCaching.NONE, rows(100));

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tALTER TABLE test\n\t" +
                "WITH caching = 'rows_only' " +
                "AND bloom_filter_fp_chance = 0.01 " +
                "AND comment = 'This is a comment' " +
                "AND compression = {'sstable_compression' : 'LZ4Compressor'} " +
                "AND compaction = {'class' : 'LeveledCompactionStrategy', 'sstable_size_in_mb' : 160} " +
                "AND dclocal_read_repair_chance = 0.21 " +
                "AND default_time_to_live = 100 " +
                "AND gc_grace_seconds = 9999 " +
                "AND index_interval = 256 " +
                "AND min_index_interval = 64 " +
                "AND max_index_interval = 512 " +
                "AND memtable_flush_period_in_ms = 12 " +
                "AND populate_io_cache_on_flush = true " +
                "AND read_repair_chance = 0.42 " +
                "AND replicate_on_write = true " +
                "AND speculative_retry = 'ALWAYS' " +
                "AND cdc = true");

        assertThat(statementWith21Caching.getQueryString()).isEqualTo("\n\tALTER TABLE test\n\t" +
                "WITH caching = {'keys' : 'none', 'rows_per_partition' : 100}");
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "The keyspace name 'add' is not allowed because it is a reserved keyword")
    public void should_fail_if_keyspace_name_is_a_reserved_keyword() throws Exception {
        alterTable("add", "test")
                .addColumn("test").type(DataType.ascii());
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "The table name 'add' is not allowed because it is a reserved keyword")
    public void should_fail_if_table_name_is_a_reserved_keyword() throws Exception {
        alterTable("add")
                .addColumn("test").type(DataType.ascii());
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "The new column name 'add' is not allowed because it is a reserved keyword")
    public void should_fail_if_added_column_is_a_reserved_keyword() throws Exception {
        alterTable("test")
                .addColumn("add").type(DataType.ascii()).getQueryString();
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "The altered column name 'add' is not allowed because it is a reserved keyword")
    public void should_fail_if_altered_column_is_a_reserved_keyword() throws Exception {
        alterTable("test")
                .alterColumn("add").type(DataType.ascii()).getQueryString();
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "The renamed column name 'add' is not allowed because it is a reserved keyword")
    public void should_fail_if_renamed_column_is_a_reserved_keyword() throws Exception {
        alterTable("test")
                .renameColumn("add");
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "The new column name 'add' is not allowed because it is a reserved keyword")
    public void should_fail_if_new_renamed_column_is_a_reserved_keyword() throws Exception {
        alterTable("test")
                .renameColumn("col").to("add");
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "The dropped column name 'add' is not allowed because it is a reserved keyword")
    public void should_fail_if_drop_column_is_a_reserved_keyword() throws Exception {
        alterTable("test")
                .dropColumn("add").getQueryString();
    }

    @Test(groups = "unit")
    public void should_add_static_column() throws Exception {
        //When
        SchemaStatement statement = alterTable("test").addStaticColumn("stat").type(DataType.text());

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tALTER TABLE test ADD stat text static");
    }
}
