package com.datastax.oss.driver.core.metadata;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.KeyspaceTableNamePair;
import com.datastax.oss.driver.api.core.metadata.Tablet;
import com.datastax.oss.driver.api.testinfra.CassandraSkip;
import com.datastax.oss.driver.api.testinfra.ScyllaRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.internal.core.protocol.TabletInfo;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ScyllaRequirement(
    minOSS = "6.0.0",
    minEnterprise = "2024.2",
    description = "Needs to support tablets")
@CassandraSkip(description = "Tablets are ScyllaDB-only extension")
public class DefaultMetadataTabletMapIT {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetadataTabletMapIT.class);
  private static final CustomCcmRule CCM_RULE =
      CustomCcmRule.builder()
          .withNodes(2)
          .withCassandraConfiguration(
              "experimental_features", "['consistent-topology-changes','tablets']")
          .build();
  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(CCM_RULE)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(5))
                  .build())
          .build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static final int INITIAL_TABLETS = 32;
  private static final int QUERIES = 1600;
  private static final int REPLICATION_FACTOR = 2;
  private static String KEYSPACE_NAME = "tabletsTest";
  private static String TABLE_NAME = "tabletsTable";
  private static String CREATE_KEYSPACE_QUERY =
      "CREATE KEYSPACE IF NOT EXISTS "
          + KEYSPACE_NAME
          + " WITH replication = {'class': "
          + "'NetworkTopologyStrategy', "
          + "'replication_factor': '"
          + REPLICATION_FACTOR
          + "'}  AND durable_writes = true AND tablets = "
          + "{'initial': "
          + INITIAL_TABLETS
          + "};";
  private static String CREATE_TABLE_QUERY =
      "CREATE TABLE IF NOT EXISTS "
          + KEYSPACE_NAME
          + "."
          + TABLE_NAME
          + " (pk int, ck int, PRIMARY KEY(pk, ck));";

  @Test
  public void should_receive_each_tablet_exactly_once() {
    CqlSession session = SESSION_RULE.session();

    session.execute(CREATE_KEYSPACE_QUERY);
    session.execute(CREATE_TABLE_QUERY);

    for (int i = 1; i <= QUERIES; i++) {
      session.execute(
          "INSERT INTO "
              + KEYSPACE_NAME
              + "."
              + TABLE_NAME
              + " (pk,ck) VALUES ("
              + i
              + ","
              + i
              + ");");
    }

    PreparedStatement preparedStatement =
        session.prepare(
            SimpleStatement.builder(
                    "select pk,ck from "
                        + KEYSPACE_NAME
                        + "."
                        + TABLE_NAME
                        + " WHERE pk = ? AND ck = ?")
                .setTracing(true)
                .build());
    // preparedStatement.enableTracing();
    int counter = 0;
    for (int i = 1; i <= QUERIES; i++) {
      ResultSet rs = session.execute(preparedStatement.bind(i, i).setTracing(true));
      Map<String, ByteBuffer> payload = rs.getExecutionInfo().getIncomingPayload();
      if (payload.containsKey(TabletInfo.TABLETS_ROUTING_V1_CUSTOM_PAYLOAD_KEY)) {
        counter++;
      }
    }

    LOG.debug("Ran first set of queries");

    // With enough queries we should hit a wrong node for each tablet exactly once.
    Assert.assertEquals(INITIAL_TABLETS, counter);

    ConcurrentMap<KeyspaceTableNamePair, ConcurrentSkipListSet<Tablet>> tabletMapping =
        session.getMetadata().getTabletMap().getMapping();
    KeyspaceTableNamePair ktPair =
        new KeyspaceTableNamePair(
            CqlIdentifier.fromCql(KEYSPACE_NAME), CqlIdentifier.fromCql(TABLE_NAME));
    Assert.assertTrue(tabletMapping.containsKey(ktPair));

    Set<Tablet> tablets = tabletMapping.get(ktPair);
    Assert.assertEquals(INITIAL_TABLETS, tablets.size());

    for (Tablet tab : tablets) {
      Assert.assertEquals(REPLICATION_FACTOR, tab.getReplicaNodes().size());
    }

    // All tablet information should be available by now (unless for some reason cluster did sth on
    // its own)
    // We should not receive any tablet payloads now, since they are sent only on mismatch.
    for (int i = 1; i <= QUERIES; i++) {

      ResultSet rs = session.execute(preparedStatement.bind(i, i));
      Map<String, ByteBuffer> payload = rs.getExecutionInfo().getIncomingPayload();

      if (payload.containsKey(TabletInfo.TABLETS_ROUTING_V1_CUSTOM_PAYLOAD_KEY)) {
        throw new RuntimeException(
            "Received non empty payload with tablets routing information: " + payload);
      }
    }
  }
}
