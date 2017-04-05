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

import org.testng.annotations.Test;

import static com.datastax.driver.core.schemabuilder.SchemaBuilder.*;
import static com.datastax.driver.core.schemabuilder.TableOptions.CompactionOptions.DateTieredCompactionStrategyOptions.TimeStampResolution;
import static org.assertj.core.api.Assertions.assertThat;

public class CompactionOptionsTest {

    @Test(groups = "unit")
    public void should_create_sized_tiered_compaction_options() throws Exception {
        //When
        final String built = sizedTieredStategy()
                .bucketLow(0.5)
                .bucketHigh(1.2)
                .coldReadsRatioToOmit(0.89)
                .enabled(true)
                .minThreshold(2)
                .maxThreshold(4)
                .minSSTableSizeInBytes(5000000L)
                .tombstoneCompactionIntervalInDay(3)
                .tombstoneThreshold(0.7)
                .uncheckedTombstoneCompaction(true)
                .build();

        //Then
        assertThat(built).isEqualTo("{'class' : 'SizeTieredCompactionStrategy', " +
                "'enabled' : true, " +
                "'tombstone_compaction_interval' : 3, " +
                "'tombstone_threshold' : 0.7, " +
                "'unchecked_tombstone_compaction' : true, " +
                "'bucket_high' : 1.2, " +
                "'bucket_low' : 0.5, " +
                "'cold_reads_to_omit' : 0.89, " +
                "'min_threshold' : 2, " +
                "'max_threshold' : 4, " +
                "'min_sstable_size' : 5000000}");
    }

    @Test(groups = "unit")
    public void should_create_leveled_compaction_option() throws Exception {
        //When
        final String built = leveledStrategy()
                .enabled(true)
                .ssTableSizeInMB(160)
                .tombstoneCompactionIntervalInDay(3)
                .tombstoneThreshold(0.7)
                .uncheckedTombstoneCompaction(true)
                .build();

        //Then
        assertThat(built).isEqualTo("{'class' : 'LeveledCompactionStrategy', " +
                "'enabled' : true, " +
                "'tombstone_compaction_interval' : 3, " +
                "'tombstone_threshold' : 0.7, " +
                "'unchecked_tombstone_compaction' : true, " +
                "'sstable_size_in_mb' : 160}");
    }

    @Test(groups = "unit")
    public void should_create_date_tiered_compaction_option() throws Exception {
        //When
        String built = dateTieredStrategy()
                .baseTimeSeconds(7200)
                .enabled(true)
                .maxSSTableAgeDays(400)
                .minThreshold(2)
                .maxThreshold(4)
                .timestampResolution(TimeStampResolution.MICROSECONDS)
                .tombstoneCompactionIntervalInDay(3)
                .tombstoneThreshold(0.7)
                .uncheckedTombstoneCompaction(true)
                .build();

        //Then
        assertThat(built).isEqualTo("{'class' : 'DateTieredCompactionStrategy', " +
                "'enabled' : true, " +
                "'tombstone_compaction_interval' : 3, " +
                "'tombstone_threshold' : 0.7, " +
                "'unchecked_tombstone_compaction' : true, " +
                "'base_time_seconds' : 7200, " +
                "'max_sstable_age_days' : 400, " +
                "'min_threshold' : 2, " +
                "'max_threshold' : 4, " +
                "'timestamp_resolution' : 'MICROSECONDS'}");
    }

    @Test(groups = "unit")
    public void should_handle_freeform_options() {
        //When
        String built = dateTieredStrategy()
                .freeformOption("foo", "bar")
                .freeformOption("baz", 1)
                .build();

        //Then
        assertThat(built).isEqualTo("{'class' : 'DateTieredCompactionStrategy', " +
                "'foo' : 'bar', " +
                "'baz' : 1}");

    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void should_throw_exception_if_cold_read_ratio_out_of_range() throws Exception {
        sizedTieredStategy()
                .bucketLow(0.5)
                .bucketHigh(1.2)
                .coldReadsRatioToOmit(1.89)
                .build();
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void should_throw_exception_if_cold_read_ratio_negative() throws Exception {
        sizedTieredStategy()
                .bucketLow(0.5)
                .bucketHigh(1.2)
                .coldReadsRatioToOmit(-1.0)
                .build();
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void should_throw_exception_if_tombstone_threshold_out_of_range() throws Exception {
        sizedTieredStategy()
                .bucketLow(0.5)
                .bucketHigh(1.2)
                .tombstoneThreshold(1.89)
                .build();
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void should_throw_exception_if_tombstone_threshold_negative() throws Exception {
        sizedTieredStategy()
                .bucketLow(0.5)
                .bucketHigh(1.2)
                .coldReadsRatioToOmit(-1.0)
                .build();
    }
}
