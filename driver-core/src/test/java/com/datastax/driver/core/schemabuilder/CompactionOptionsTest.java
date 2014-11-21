/*
 *      Copyright (C) 2012-2014 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core.schemabuilder;

import static org.assertj.core.api.Assertions.assertThat;
import org.testng.annotations.Test;

public class CompactionOptionsTest {

    @Test
    public void should_create_sized_tiered_compaction_options() throws Exception {
        //When
        final String build = TableOptions.CompactionOptions
                .sizedTieredStategy()
                .bucketLow(0.5)
                .bucketHigh(1.2)
                .coldReadsRatioToOmit(0.89)
                .minThreshold(2)
                .maxThreshold(4)
                .minSSTableSizeInBytes(5000000L)
                .enableAutoCompaction(true)
                .tombstoneCompactionIntervalInDay(3)
                .tombstoneThreshold(0.7)
                .build();

        //Then
        assertThat(build).isEqualTo("{'class' : 'SizeTieredCompactionStrategy', 'enabled' : true, 'max_threshold' : 4, 'tombstone_compaction_interval' : 3, 'tombstone_threshold' : 0.7, 'bucket_high' : 1.2, 'bucket_low' : 0.5, 'cold_reads_to_omit' : 0.89, 'min_threshold' : 2, 'min_sstable_size' : 5000000}");
    }

    @Test
    public void should_create_leveled_compaction_option() throws Exception {
        //When
        final String build = TableOptions.CompactionOptions
                .leveledStrategy()
                .ssTableSizeInMB(160)
                .enableAutoCompaction(true)
                .maxThreshold(5)
                .tombstoneCompactionIntervalInDay(3)
                .tombstoneThreshold(0.7)
                .build();

        //Then
        assertThat(build).isEqualTo("{'class' : 'LeveledCompactionStrategy', 'enabled' : true, 'max_threshold' : 5, 'tombstone_compaction_interval' : 3, 'tombstone_threshold' : 0.7, 'sstable_size_in_mb' : 160}");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void should_throw_exception_if_cold_read_ration_out_of_range() throws Exception {
        TableOptions.CompactionOptions
                .sizedTieredStategy()
                .bucketLow(0.5)
                .bucketHigh(1.2)
                .coldReadsRatioToOmit(1.89)
                .build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void should_throw_exception_if_cold_read_ration_negative() throws Exception {
        TableOptions.CompactionOptions
                .sizedTieredStategy()
                .bucketLow(0.5)
                .bucketHigh(1.2)
                .coldReadsRatioToOmit(-1.0)
                .build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void should_throw_exception_if_tombstone_threshold_out_of_range() throws Exception {
        TableOptions.CompactionOptions
                .sizedTieredStategy()
                .bucketLow(0.5)
                .bucketHigh(1.2)
                .tombstoneThreshold(1.89)
                .build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void should_throw_exception_if_tombstone_threshold_negative() throws Exception {
        TableOptions.CompactionOptions
                .sizedTieredStategy()
                .bucketLow(0.5)
                .bucketHigh(1.2)
                .coldReadsRatioToOmit(-1.0)
                .build();
    }
}
