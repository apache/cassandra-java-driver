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

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.ClusteringOrder.DESC;
import static com.datastax.driver.core.DataType.cint;

@CassandraVersion("3.0")
public class MaterializedViewMetadataTest extends CCMTestsSupport {

    /**
     * Validates that a materialized view is properly retrieved and parsed.
     *
     * @test_category metadata, materialized_view
     * @jira_ticket JAVA-825
     */
    @Test(groups = "short")
    public void should_create_view_metadata() {

        // given
        String createTable = String.format(
                "CREATE TABLE %s.scores("
                        + "user TEXT,"
                        + "game TEXT,"
                        + "year INT,"
                        + "month INT,"
                        + "day INT,"
                        + "score INT,"
                        + "PRIMARY KEY (user, game, year, month, day)"
                        + ")",
                keyspace);
        String createMV = String.format(
                "CREATE MATERIALIZED VIEW %s.monthlyhigh AS "
                        + "SELECT game, year, month, score, user, day FROM %s.scores "
                        + "WHERE game IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL AND day IS NOT NULL "
                        + "PRIMARY KEY ((game, year, month), score, user, day) "
                        + "WITH CLUSTERING ORDER BY (score DESC, user ASC, day ASC)",
                keyspace, keyspace);

        // when
        session().execute(createTable);
        session().execute(createMV);

        // then
        TableMetadata table = cluster().getMetadata().getKeyspace(keyspace).getTable("scores");
        MaterializedViewMetadata mv = cluster().getMetadata().getKeyspace(keyspace).getMaterializedView("monthlyhigh");

        assertThat(table).isNotNull().hasName("scores").hasMaterializedView(mv).hasNumberOfColumns(6);
        assertThat(table.getColumns().get(0)).isNotNull().hasName("user").isPartitionKey();
        assertThat(table.getColumns().get(1)).isNotNull().hasName("game").isClusteringColumn();
        assertThat(table.getColumns().get(2)).isNotNull().hasName("year").isClusteringColumn();
        assertThat(table.getColumns().get(3)).isNotNull().hasName("month").isClusteringColumn();
        assertThat(table.getColumns().get(4)).isNotNull().hasName("day").isClusteringColumn();
        assertThat(table.getColumns().get(5)).isNotNull().hasName("score").isRegularColumn();

        assertThat(mv).isNotNull().hasName("monthlyhigh").hasBaseTable(table).hasNumberOfColumns(6).isEqualTo(table.getView("monthlyhigh"));
        assertThat(mv.getColumns().get(0)).isNotNull().hasName("game").isPartitionKey();
        assertThat(mv.getColumns().get(1)).isNotNull().hasName("year").isPartitionKey();
        assertThat(mv.getColumns().get(2)).isNotNull().hasName("month").isPartitionKey();
        assertThat(mv.getColumns().get(3)).isNotNull().hasName("score").isClusteringColumn().hasClusteringOrder(DESC);
        assertThat(mv.getColumns().get(4)).isNotNull().hasName("user").isClusteringColumn();
        assertThat(mv.getColumns().get(5)).isNotNull().hasName("day").isClusteringColumn();
        assertThat(mv.asCQLQuery(false)).contains(createMV);
    }

    /**
     * Validates that a materialized view is properly retrieved and parsed when using quoted identifiers.
     *
     * @test_category metadata, materialized_view
     * @jira_ticket JAVA-825
     */
    @Test(groups = "short")
    public void should_create_view_metadata_with_quoted_identifiers() {
        // given
        String createTable = String.format(
                "CREATE TABLE %s.\"T1\" ("
                        + "\"theKey\" int, "
                        + "\"the;Clustering\" int, "
                        + "\"the Value\" int, "
                        + "PRIMARY KEY (\"theKey\", \"the;Clustering\"))",
                keyspace);
        String createMV = String.format(
                "CREATE MATERIALIZED VIEW %s.\"Mv1\" AS "
                        + "SELECT \"theKey\", \"the;Clustering\", \"the Value\" "
                        + "FROM %s.\"T1\" "
                        + "WHERE \"theKey\" IS NOT NULL AND \"the;Clustering\" IS NOT NULL AND \"the Value\" IS NOT NULL "
                        + "PRIMARY KEY (\"theKey\", \"the;Clustering\")",
                keyspace, keyspace);
        // when
        session().execute(createTable);
        session().execute(createMV);
        // then
        TableMetadata table = cluster().getMetadata().getKeyspace(keyspace).getTable("\"T1\"");
        MaterializedViewMetadata mv = cluster().getMetadata().getKeyspace(keyspace).getMaterializedView("\"Mv1\"");
        assertThat(table).isNotNull().hasName("T1").hasMaterializedView(mv).hasNumberOfColumns(3);
        assertThat(table.getViews()).hasSize(1).containsOnly(mv);
        assertThat(table.getColumns().get(0)).isNotNull().hasName("theKey").isPartitionKey().hasType(cint());
        assertThat(table.getColumns().get(1)).isNotNull().hasName("the;Clustering").isClusteringColumn().hasType(cint());
        assertThat(table.getColumns().get(2)).isNotNull().hasName("the Value").isRegularColumn().hasType(cint());
        assertThat(mv).isNotNull().hasName("Mv1").hasBaseTable(table).hasNumberOfColumns(3);
        assertThat(mv.getColumns().get(0)).isNotNull().hasName("theKey").isPartitionKey().hasType(cint());
        assertThat(mv.getColumns().get(1)).isNotNull().hasName("the;Clustering").isClusteringColumn().hasType(cint());
        assertThat(mv.getColumns().get(2)).isNotNull().hasName("the Value").isRegularColumn().hasType(cint());
        assertThat(mv.asCQLQuery(false)).contains(createMV);
    }

}
