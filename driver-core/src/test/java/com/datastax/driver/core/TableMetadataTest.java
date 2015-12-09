/*
 *      Copyright (C) 2012-2015 DataStax Inc.
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
package com.datastax.driver.core;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

public class TableMetadataTest extends CCMBridge.PerClassSingleNodeCluster {
    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList(
                "CREATE TABLE single_quote (\n"
                        + "    c1 int PRIMARY KEY\n"
                        + ") WITH  comment = 'comment with single quote '' should work'"
        );
    }

    @Override
    protected Cluster.Builder configure(Cluster.Builder builder) {
        return builder.withQueryOptions(new QueryOptions()
                        .setRefreshNodeIntervalMillis(0)
                        .setRefreshNodeListIntervalMillis(0)
                        .setRefreshSchemaIntervalMillis(0)
        );
    }

    @Test(groups = "short")
    public void should_escape_single_quote_table_comment() {
        TableMetadata table = cluster.getMetadata().getKeyspace(keyspace).getTable("single_quote");
        assertThat(table.asCQLQuery()).contains("comment with single quote '' should work");
    }
}
