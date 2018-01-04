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

import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@CassandraVersion("2.0")
@CCMConfig(config = "enable_user_defined_functions:true")
public class ExportAsStringTest extends CCMTestsSupport {

    private static final Logger logger = LoggerFactory.getLogger(ExportAsStringTest.class);

    /**
     * Creates a keyspace using a variety of features and ensures {@link KeyspaceMetadata#exportAsString()}
     * contains the expected data in the expected order.  This is not exhaustive, but covers quite a bit of different
     * scenarios (materialized views, aggregates, functions, nested UDTs, etc.).
     * <p>
     * The test also verifies that the generated schema is the same whether the keyspace and its schema was created
     * during the lifecycle of the cluster or before connecting.
     * <p>
     * Note that this test might be fragile in the future if default option values change in cassandra.  In order to
     * deal with new features, we create a schema for each tested C* version, and if one is not present the test
     * is failed.
     */
    @Test(groups = "short")
    public void should_create_schema_and_ensure_exported_cql_is_as_expected() {
        String keyspace = "complex_ks";
        Map<String, Object> replicationOptions = ImmutableMap.<String, Object>of("class", "SimpleStrategy", "replication_factor", 1);

        // create keyspace
        session().execute(SchemaBuilder.createKeyspace(keyspace).with().replication(replicationOptions));

        // create session from this keyspace.
        Session session = cluster().connect(keyspace);

        KeyspaceMetadata ks;

        // udts require 2.1+
        if (ccm().getCassandraVersion().compareTo(VersionNumber.parse("2.1")) >= 0) {
            // Usertype 'ztype' with two columns.  Given name to ensure that even though it has an alphabetically
            // later name, it shows up before other user types ('ctype') that depend on it.
            session.execute(SchemaBuilder.createType("ztype")
                    .addColumn("c", DataType.text()).addColumn("a", DataType.cint()));

            // Usertype 'xtype' with two columns.  At same level as 'ztype' since both are depended on by ctype, should
            // show up before 'ztype' because it's alphabetically before, even though it was created after.
            session.execute(SchemaBuilder.createType("xtype")
                    .addColumn("d", DataType.text()));

            ks = cluster().getMetadata().getKeyspace(keyspace);

            // Usertype 'ctype' which depends on both ztype and xtype, therefore ztype and xtype should show up earlier.
            session.execute(SchemaBuilder.createType("ctype")
                    .addColumn("\"Z\"", ks.getUserType("ztype").copy(true))
                    .addColumn("x", ks.getUserType("xtype").copy(true)));

            // Usertype 'btype' which has no dependencies, should show up before 'xtype' and 'ztype' since it's
            // alphabetically before.
            session.execute(SchemaBuilder.createType("btype").addColumn("a", DataType.text()));

            // Refetch keyspace for < 3.0 schema this is required as a new keyspace metadata reference may be created.
            ks = cluster().getMetadata().getKeyspace(keyspace);

            // Usertype 'atype' which depends on 'ctype', so should show up after 'ctype', 'xtype' and 'ztype'.
            session.execute(SchemaBuilder.createType("atype")
                    .addColumn("c", ks.getUserType("ctype").copy(true)));

            // A simple table with a udt column and LCS compaction strategy.
            session.execute(SchemaBuilder.createTable("ztable")
                    .addPartitionKey("zkey", DataType.text())
                    .addColumn("a", ks.getUserType("atype").copy(true))
                    .withOptions().compactionOptions(SchemaBuilder.leveledStrategy().ssTableSizeInMB(95)));
        } else {
            // A simple table with LCS compaction strategy.
            session.execute(SchemaBuilder.createTable("ztable")
                    .addPartitionKey("zkey", DataType.text())
                    .addColumn("a", DataType.cint())
                    .withOptions().compactionOptions(SchemaBuilder.leveledStrategy().ssTableSizeInMB(95)));
        }

        // date type requries 2.2+
        if (ccm().getCassandraVersion().compareTo(VersionNumber.parse("2.2")) >= 0) {
            // A table that will have materialized views (copied from mv docs)
            session.execute(SchemaBuilder.createTable("cyclist_mv").addPartitionKey("cid", DataType.uuid())
                    .addColumn("name", DataType.text())
                    .addColumn("age", DataType.cint())
                    .addColumn("birthday", DataType.date())
                    .addColumn("country", DataType.text()));

            // index on table with view, index should be printed first.
            session.execute(SchemaBuilder.createIndex("cyclist_by_country")
                    .onTable("cyclist_mv")
                    .andColumn("country"));

            // materialized views require 3.0+
            if (ccm().getCassandraVersion().compareTo(VersionNumber.parse("3.0")) >= 0) {
                // A materialized view for cyclist_mv, reverse clustering.  created first to ensure creation order does not
                // matter, alphabetical does.
                session.execute("CREATE MATERIALIZED VIEW cyclist_by_r_age " +
                        "AS SELECT age, birthday, name, country " +
                        "FROM cyclist_mv " +
                        "WHERE age IS NOT NULL AND cid IS NOT NULL " +
                        "PRIMARY KEY (age, cid) " +
                        "WITH CLUSTERING ORDER BY (cid DESC)"
                );

                // A materialized view for cyclist_mv, select *
                session.execute("CREATE MATERIALIZED VIEW cyclist_by_a_age " +
                        "AS SELECT * " +
                        "FROM cyclist_mv " +
                        "WHERE age IS NOT NULL AND cid IS NOT NULL " +
                        "PRIMARY KEY (age, cid)");

                // A materialized view for cyclist_mv, select columns
                session.execute("CREATE MATERIALIZED VIEW cyclist_by_age " +
                        "AS SELECT age, birthday, name, country " +
                        "FROM cyclist_mv " +
                        "WHERE age IS NOT NULL AND cid IS NOT NULL " +
                        "PRIMARY KEY (age, cid) WITH comment = 'simple view'");
            }
        }

        // A table with a secondary index, taken from documentation on secondary index.
        session.execute(SchemaBuilder.createTable("rank_by_year_and_name")
                .addPartitionKey("race_year", DataType.cint())
                .addPartitionKey("race_name", DataType.text())
                .addClusteringColumn("rank", DataType.cint())
                .addColumn("cyclist_name", DataType.text()));

        session.execute(SchemaBuilder.createIndex("ryear")
                .onTable("rank_by_year_and_name")
                .andColumn("race_year"));

        session.execute(SchemaBuilder.createIndex("rrank")
                .onTable("rank_by_year_and_name")
                .andColumn("rank"));

        // udfs and udas require 2.22+
        if (ccm().getCassandraVersion().compareTo(VersionNumber.parse("2.2")) >= 0) {
            // UDFs
            session.execute("CREATE OR REPLACE FUNCTION avgState ( state tuple<int,bigint>, val int ) CALLED ON NULL INPUT RETURNS tuple<int,bigint> LANGUAGE java AS \n" +
                    "  'if (val !=null) { state.setInt(0, state.getInt(0)+1); state.setLong(1, state.getLong(1)+val.intValue()); } return state;';");
            session.execute("CREATE OR REPLACE FUNCTION avgFinal ( state tuple<int,bigint> ) CALLED ON NULL INPUT RETURNS double LANGUAGE java AS \n" +
                    "  'double r = 0; if (state.getInt(0) == 0) return null; r = state.getLong(1); r /= state.getInt(0); return Double.valueOf(r);';");

            // UDAs
            session.execute("CREATE AGGREGATE IF NOT EXISTS mean ( int ) \n" +
                    "SFUNC avgState STYPE tuple<int,bigint> FINALFUNC avgFinal INITCOND (0,0);");
            session.execute("CREATE AGGREGATE IF NOT EXISTS average ( int ) \n" +
                    "SFUNC avgState STYPE tuple<int,bigint> FINALFUNC avgFinal INITCOND (0,0);");
        }

        ks = cluster().getMetadata().getKeyspace(keyspace);

        // validate that the exported schema matches what was expected exactly.
        assertThat(ks.exportAsString().trim()).isEqualTo(getExpectedCqlString());

        // Also validate that when you create a Cluster with schema already created that the exported string
        // is the same.
        Cluster newCluster = this.createClusterBuilderNoDebouncing()
                .addContactPointsWithPorts(this.getContactPointsWithPorts())
                .build();
        try {
            newCluster.init();
            ks = newCluster.getMetadata().getKeyspace(keyspace);
            assertThat(ks.exportAsString().trim()).isEqualTo(getExpectedCqlString());
        } finally {
            newCluster.close();
        }
    }

    private String getExpectedCqlString() {
        String majorMinor = ccm().getCassandraVersion().getMajor() + "." + ccm().getCassandraVersion().getMinor();
        String resourceName = "/export_as_string_test_" + majorMinor + ".cql";

        Closer closer = Closer.create();
        try {
            InputStream is = ExportAsStringTest.class.getResourceAsStream(resourceName);
            assertThat(is)
                    .as("No reference script for this version (was looking for src/test/resources" + resourceName + ")")
                    .isNotNull();
            closer.register(is);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos);
            ByteStreams.copy(is, ps);
            return baos.toString().trim();
        } catch (IOException e) {
            logger.warn("Failure to read {}", resourceName, e);
            fail("Unable to read " + resourceName + " is it defined?");
        } finally {
            try {
                closer.close();
            } catch (IOException e) { // no op
                logger.warn("Failure closing streams", e);
            }
        }
        return "";
    }
}
