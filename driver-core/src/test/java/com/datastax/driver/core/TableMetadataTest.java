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

import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;

@CCMConfig(clusterProvider = "createClusterBuilderNoDebouncing")
public class TableMetadataTest extends CCMTestsSupport {
    @Override
    public void onTestContextInitialized() {
        execute(
                "CREATE TABLE single_quote (\n"
                        + "    c1 int PRIMARY KEY\n"
                        + ") WITH  comment = 'comment with single quote '' should work'"
        );
    }

    @Test(groups = "short")
    public void should_escape_single_quote_table_comment() {
        TableMetadata table = cluster().getMetadata().getKeyspace(keyspace).getTable("single_quote");
        assertThat(table.asCQLQuery()).contains("comment with single quote '' should work");
    }

    /**
     * Validates that a table with case-sensitive column names and column names
     * consisting of (quoted) reserved keywords is correctly parsed
     * and that the generated CQL is valid.
     *
     * @jira_ticket JAVA-1064
     * @test_category metadata
     */
    @Test(groups = "short")
    public void should_parse_table_with_case_sensitive_column_names_and_reserved_keywords() throws Exception {
        // given
        String c1 = Metadata.quote("quotes go \"\" here \"\" ");
        String c2 = Metadata.quote("\\x00\\x25");
        String c3 = Metadata.quote("columnfamily");
        String c4 = Metadata.quote("select");
        String c5 = Metadata.quote("who''s there'? ");
        String c6 = Metadata.quote("faux )");
        String c7 = Metadata.quote("COMPACT STORAGE");
        // single partition key
        String cql1 = String.format("CREATE TABLE %s.\"MyTable1\" ("
                + "%s text, "
                + "%s text, "
                + "%s text, "
                + "%s text, "
                + "%s text, "
                + "%s text, "
                + "%s text, "
                + "PRIMARY KEY (%s, %s, %s, %s, %s, %s)"
                + ")", keyspace, c1, c2, c3, c4, c5, c6, c7, c1, c2, c3, c4, c5, c6);
        // composite partition key
        String cql2 = String.format("CREATE TABLE %s.\"MyTable2\" ("
                + "%s text, "
                + "%s text, "
                + "%s text, "
                + "%s text, "
                + "%s text, "
                + "%s text, "
                + "%s text, "
                + "PRIMARY KEY ((%s, %s), %s, %s, %s, %s)"
                + ")", keyspace, c1, c2, c3, c4, c5, c6, c7, c1, c2, c3, c4, c5, c6);
        // when
        execute(cql1, cql2);
        TableMetadata table1 = cluster().getMetadata().getKeyspace(keyspace).getTable("\"MyTable1\"");
        TableMetadata table2 = cluster().getMetadata().getKeyspace(keyspace).getTable("\"MyTable2\"");
        // then
        assertThat(table1)
                .hasColumn(c1)
                .hasColumn(c2)
                .hasColumn(c3)
                .hasColumn(c4)
                .hasColumn(c5)
                .hasColumn(c6)
                .hasColumn(c7);
        assertThat(table1.asCQLQuery()).startsWith(cql1);
        assertThat(table2)
                .hasColumn(c1)
                .hasColumn(c2)
                .hasColumn(c3)
                .hasColumn(c4)
                .hasColumn(c5)
                .hasColumn(c6)
                .hasColumn(c7);
        assertThat(table2.asCQLQuery()).startsWith(cql2);
        execute(
                "DROP TABLE \"MyTable1\"",
                "DROP TABLE \"MyTable2\"",
                table1.asCQLQuery(),
                table2.asCQLQuery()
        );
    }

}
