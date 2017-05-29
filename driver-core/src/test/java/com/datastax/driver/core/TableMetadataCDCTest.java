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
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

@CCMConfig(config = "cdc_enabled:true")
@CassandraVersion(value = "3.8", description = "Requires CASSANDRA-12041 added in 3.8")
public class TableMetadataCDCTest extends CCMTestsSupport {

    /**
     * Ensures that if a table is configured with change data capture enabled that
     * {@link TableOptionsMetadata#isCDC()} returns true for that table.
     *
     * @test_category metadata
     * @jira_ticket JAVA-1287
     * @jira_ticket CASSANDRA-12041
     */
    @Test(groups = "short")
    public void should_parse_cdc_from_table_options() {
        // given
        // create a simple table with cdc as true.
        String cql = String.format("CREATE TABLE %s.cdc_table (\n"
                + "    k text,\n"
                + "    c int,\n"
                + "    v timeuuid,\n"
                + "    PRIMARY KEY (k, c)\n"
                + ") WITH cdc=true;", keyspace);
        session().execute(cql);

        // when retrieving the table's metadata.
        TableMetadata table = cluster().getMetadata().getKeyspace(keyspace).getTable("cdc_table");
        // then the table's options should have cdc as true.
        assertThat(table.getOptions().isCDC()).isEqualTo(true);
        assertThat(table.asCQLQuery(true)).contains("cdc = true");
    }
}
