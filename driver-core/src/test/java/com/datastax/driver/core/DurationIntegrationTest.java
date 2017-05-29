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
package com.datastax.driver.core;

import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.UUID;

import static com.datastax.driver.core.Assertions.assertThat;

@CassandraVersion("3.10")
public class DurationIntegrationTest extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE test_duration (pk uuid PRIMARY KEY, c1 duration)");
    }

    @DataProvider
    Object[][] durations() {
        return new Object[][]{
                {"1y2mo"},
                {"-1y2mo"},
                {"1Y2MO"},
                {"2w"},
                {"2d10h"},
                {"2d"},
                {"30h"},
                {"30h20m"},
                {"20m"},
                {"56s"},
                {"567ms"},
                {"1950us"},
                {"1950µs"},
                {"1950000ns"},
                {"1950000NS"},
                {"-1950000ns"},
                {"1y3mo2h10m"},
                {"P1Y2D"},
                {"P1Y2M"},
                {"P2W"},
                {"P1YT2H"},
                {"-P1Y2M"},
                {"P2D"},
                {"PT30H"},
                {"PT30H20M"},
                {"PT20M"},
                {"PT56S"},
                {"P1Y3MT2H10M"},
                {"P0001-00-02T00:00:00"},
                {"P0001-02-00T00:00:00"},
                {"P0001-00-00T02:00:00"},
                {"-P0001-02-00T00:00:00"},
                {"P0000-00-02T00:00:00"},
                {"P0000-00-00T30:00:00"},
                {"P0000-00-00T30:20:00"},
                {"P0000-00-00T00:20:00"},
                {"P0000-00-00T00:00:56"},
                {"P0001-03-00T02:10:00"}
        };
    }

    /**
     * Validates that columns using the duration type are properly handled by the driver when used as a parameter
     * and retrieved in a row result for a variety of sample inputs.
     *
     * @jira_ticket JAVA-1347
     * @test_category metadata
     */
    @Test(groups = "short", dataProvider = "durations")
    public void should_serialize_and_deserialize_durations(String durationStr) {
        // read and write
        UUID id = UUID.randomUUID();
        Duration expected = Duration.from(durationStr);
        session().execute("INSERT INTO test_duration (pk, c1) VALUES (?, ?)", id, expected);
        Row row = session().execute("SELECT c1 from test_duration WHERE pk = ?", id).one();
        Duration actual = row.get("c1", Duration.class);
        assertThat(actual).isEqualTo(expected);
    }


    /**
     * Validates that columns using the duration type are properly represented in {@link TableMetadata}.
     *
     * @jira_ticket JAVA-1347
     * @test_category metadata
     */
    @Test(groups = "short")
    public void should_parse_column_metadata() {
        // column metadata
        TableMetadata table = cluster().getMetadata().getKeyspace(keyspace).getTable("test_duration");
        assertThat(table.getColumn("c1")).hasType(DataType.duration());
        assertThat(table.asCQLQuery()).contains("c1 duration");
    }

}
