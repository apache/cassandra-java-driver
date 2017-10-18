/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.api.core.metadata;

import com.datastax.oss.driver.api.core.CassandraVersion;
import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@Category(ParallelizableTests.class)
public class DescribeIT {

  private static final Logger logger = LoggerFactory.getLogger(DescribeIT.class);

  @ClassRule public static CcmRule ccmRule = CcmRule.getInstance();

  @ClassRule
  public static ClusterRule clusterRule =
      new ClusterRule(
          ccmRule,
          false,
          true,
          new NodeStateListener[0],
          "request.timeout = 30 seconds",
          "metadata.schema.debouncer.window = 0 seconds"); // disable debouncer to speed up test.

  /**
   * Creates a keyspace using a variety of features and ensures {@link
   * com.datastax.oss.driver.api.core.metadata.schema.Describable#describe(boolean)} contains the
   * expected data in the expected order. This is not exhaustive, but covers quite a bit of
   * different scenarios (materialized views, aggregates, functions, nested UDTs, etc.).
   *
   * <p>The test also verifies that the generated schema is the same whether the keyspace and its
   * schema was created during the lifecycle of the cluster or before connecting.
   *
   * <p>Note that this test might be fragile in the future if default option values change in
   * cassandra. In order to deal with new features, we create a schema for each tested C* version,
   * and if one is not present the test is failed.
   */
  @Test
  public void create_schema_and_ensure_exported_cql_is_as_expected() {
    CqlIdentifier keyspace = ClusterUtils.uniqueKeyspaceId();
    String keyspaceAsCql = keyspace.asCql(true);
    String expectedCql = getExpectedCqlString(keyspaceAsCql);

    // create keyspace
    clusterRule
        .session()
        .execute(
            String.format(
                "CREATE KEYSPACE %s "
                    + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
                keyspace));

    // create session from this keyspace.
    CqlSession session = clusterRule.cluster().connect(keyspace);

    KeyspaceMetadata originalKsMeta = clusterRule.cluster().getMetadata().getKeyspace(keyspace);

    // Usertype 'ztype' with two columns.  Given name to ensure that even though it has an alphabetically
    // later name, it shows up before other user types ('ctype') that depend on it.
    session.execute("CREATE TYPE ztype(c text, a int)");

    // Usertype 'xtype' with two columns.  At same level as 'ztype' since both are depended on by ctype, should
    // show up before 'ztype' because it's alphabetically before, even though it was created after.
    session.execute("CREATE TYPE xtype(d text)");

    // Usertype 'ctype' which depends on both ztype and xtype, therefore ztype and xtype should show up earlier.
    session.execute(
        String.format(
            "CREATE TYPE ctype(z frozen<%s.ztype>, x frozen<%s.xtype>)",
            keyspaceAsCql, keyspaceAsCql));

    // Usertype 'btype' which has no dependencies, should show up before 'xtype' and 'ztype' since it's
    // alphabetically before.
    session.execute("CREATE TYPE btype(a text)");

    // Usertype 'atype' which depends on 'ctype', so should show up after 'ctype', 'xtype' and 'ztype'.
    session.execute(String.format("CREATE TYPE atype(c frozen<%s.ctype>)", keyspaceAsCql));

    // A simple table with a udt column and LCS compaction strategy.
    session.execute(
        String.format(
            "CREATE TABLE ztable(zkey text, a frozen<%s.atype>, PRIMARY KEY(zkey)) "
                + "WITH compaction = {'class' : 'LeveledCompactionStrategy', 'sstable_size_in_mb' : 95}",
            keyspaceAsCql));

    // date type requries 2.2+
    if (ccmRule.getCassandraVersion().compareTo(CassandraVersion.V2_2_0) >= 0) {
      // A table that will have materialized views (copied from mv docs)
      session.execute(
          "CREATE TABLE cyclist_mv(cid uuid, name text, age int, birthday date, country text, "
              + "PRIMARY KEY(cid))");

      // index on table with view, index should be printed first.
      session.execute("CREATE INDEX cyclist_by_country ON cyclist_mv(country)");

      // materialized views require 3.0+
      if (ccmRule.getCassandraVersion().compareTo(CassandraVersion.V3_0_0) >= 0) {
        // A materialized view for cyclist_mv, reverse clustering.  created first to ensure creation order does not
        // matter, alphabetical does.
        session.execute(
            "CREATE MATERIALIZED VIEW cyclist_by_r_age "
                + "AS SELECT age, birthday, name, country "
                + "FROM cyclist_mv "
                + "WHERE age IS NOT NULL AND cid IS NOT NULL "
                + "PRIMARY KEY (age, cid) "
                + "WITH CLUSTERING ORDER BY (cid DESC)");

        // A materialized view for cyclist_mv, select *
        session.execute(
            "CREATE MATERIALIZED VIEW cyclist_by_a_age "
                + "AS SELECT * "
                + "FROM cyclist_mv "
                + "WHERE age IS NOT NULL AND cid IS NOT NULL "
                + "PRIMARY KEY (age, cid)");

        // A materialized view for cyclist_mv, select columns
        session.execute(
            "CREATE MATERIALIZED VIEW cyclist_by_age "
                + "AS SELECT age, birthday, name, country "
                + "FROM cyclist_mv "
                + "WHERE age IS NOT NULL AND cid IS NOT NULL "
                + "PRIMARY KEY (age, cid) WITH comment = 'simple view'");
      }
    }

    // A table with a secondary index, taken from documentation on secondary index.
    session.execute(
        "CREATE TABLE rank_by_year_and_name(race_year int, race_name text, rank int, cyclist_name text, "
            + "PRIMARY KEY((race_year, race_name), rank))");

    session.execute("CREATE INDEX ryear ON rank_by_year_and_name(race_year)");

    session.execute("CREATE INDEX rrank ON rank_by_year_and_name(rank)");

    // udfs and udas require 2.22+
    if (ccmRule.getCassandraVersion().compareTo(CassandraVersion.V2_2_0) >= 0) {
      // UDFs
      session.execute(
          "CREATE OR REPLACE FUNCTION avgState ( state tuple<int,bigint>, val int ) CALLED ON NULL INPUT RETURNS tuple<int,bigint> LANGUAGE java AS \n"
              + "  'if (val !=null) { state.setInt(0, state.getInt(0)+1); state.setLong(1, state.getLong(1)+val.intValue()); } return state;';");
      session.execute(
          "CREATE OR REPLACE FUNCTION avgFinal ( state tuple<int,bigint> ) CALLED ON NULL INPUT RETURNS double LANGUAGE java AS \n"
              + "  'double r = 0; if (state.getInt(0) == 0) return null; r = state.getLong(1); r /= state.getInt(0); return Double.valueOf(r);';");

      // UDAs
      session.execute(
          "CREATE AGGREGATE IF NOT EXISTS mean ( int ) \n"
              + "SFUNC avgState STYPE tuple<int,bigint> FINALFUNC avgFinal INITCOND (0,0);");
      session.execute(
          "CREATE AGGREGATE IF NOT EXISTS average ( int ) \n"
              + "SFUNC avgState STYPE tuple<int,bigint> FINALFUNC avgFinal INITCOND (0,0);");
    }

    // Since metadata is immutable, do not expect anything in the original keyspace meta.
    assertThat(originalKsMeta.getTables()).isEmpty();
    assertThat(originalKsMeta.getViews()).isEmpty();
    assertThat(originalKsMeta.getFunctions()).isEmpty();
    assertThat(originalKsMeta.getAggregates()).isEmpty();
    assertThat(originalKsMeta.getUserDefinedTypes()).isEmpty();

    // validate that the exported schema matches what was expected exactly.
    KeyspaceMetadata ks = clusterRule.cluster().getMetadata().getKeyspace(keyspace);
    assertThat(ks.describeWithChildren(true).trim()).isEqualTo(expectedCql);

    // Also validate that when you create a Cluster with schema already created that the exported string
    // is the same.
    try (Cluster<CqlSession> newCluster = ClusterUtils.newCluster(ccmRule)) {
      ks = newCluster.getMetadata().getKeyspace(keyspace);
      assertThat(ks.describeWithChildren(true).trim()).isEqualTo(expectedCql);
    }
  }

  private String getExpectedCqlString(String keyspace) {
    String majorMinor =
        ccmRule.getCassandraVersion().getMajor() + "." + ccmRule.getCassandraVersion().getMinor();
    String resourceName = "/describe_it_test_" + majorMinor + ".cql";

    Closer closer = Closer.create();
    try {
      InputStream is = DescribeIT.class.getResourceAsStream(resourceName);
      if (is == null) {
        // If no schema file is defined for tested cassandra version, just try 3.11.
        if (ccmRule.getCassandraVersion().compareTo(CassandraVersion.V3_0_0) >= 0) {
          logger.warn("Could not find schema file for {}, assuming C* 3.11.x", majorMinor);
          is = DescribeIT.class.getResourceAsStream("/describe_it_test_3.11.cql");
          if (is == null) {
            throw new IOException();
          }
        }
      }

      closer.register(is);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(baos);
      ByteStreams.copy(is, ps);
      return baos.toString().replaceAll("ks_0", keyspace).trim();
    } catch (IOException e) {
      logger.warn("Failure to read {}", resourceName, e);
      fail("Unable to read " + resourceName + " is it defined?", e);
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
