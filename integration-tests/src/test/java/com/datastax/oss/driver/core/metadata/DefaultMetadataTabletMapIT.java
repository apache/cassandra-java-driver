package com.datastax.oss.driver.core.metadata;

import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.KeyspaceTableNamePair;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.Tablet;
import com.datastax.oss.driver.api.testinfra.ScyllaOnly;
import com.datastax.oss.driver.api.testinfra.ScyllaRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.internal.core.protocol.TabletInfo;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.Assert;
import org.junit.BeforeClass;
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
@ScyllaOnly(description = "Tablets are ScyllaDB-only extension")
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
  private static final String KEYSPACE_NAME = "tabletsTest";
  private static final String TABLE_NAME = "tabletsTable";
  private static final String CREATE_KEYSPACE_QUERY =
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
  private static final String CREATE_TABLE_QUERY =
      "CREATE TABLE IF NOT EXISTS "
          + KEYSPACE_NAME
          + "."
          + TABLE_NAME
          + " (pk int, ck int, val int, PRIMARY KEY(pk, ck));";

  private static final SimpleStatement STMT_INSERT =
      buildStatement("INSERT INTO %s.%s (pk, ck) VALUES (?, ?);");

  private static final SimpleStatement STMT_INSERT_NO_KS =
      buildStatement("INSERT INTO %s (pk, ck) VALUES (?, ?);");

  private static final SimpleStatement STMT_INSERT_CONCRETE =
      buildStatement("INSERT INTO %s.%s (pk, ck) VALUES (1, 1);");

  private static final SimpleStatement STMT_INSERT_PK_CONCRETE =
      buildStatement("INSERT INTO %s.%s (pk, ck) VALUES (1, ?);");

  private static final SimpleStatement STMT_INSERT_CK_CONCRETE =
      buildStatement("INSERT INTO %s.%s (pk, ck) VALUES (?, 1);");

  private static final SimpleStatement STMT_INSERT_LWT_IF_NOT_EXISTS =
      buildStatement("INSERT INTO %s.%s (pk, ck) VALUES (?, ?) IF NOT EXISTS;");

  private static final SimpleStatement STMT_SELECT =
      buildStatement("SELECT pk,ck FROM %s.%s WHERE pk = ? AND ck = ?");

  private static final SimpleStatement STMT_SELECT_NO_KS =
      buildStatement("SELECT pk, ck FROM %s WHERE pk = ? AND ck = ?");

  private static final SimpleStatement STMT_SELECT_CONCRETE =
      buildStatement("SELECT pk,ck FROM %s.%s WHERE pk = 1 AND ck = 1");

  private static final SimpleStatement STMT_SELECT_PK_CONCRETE =
      buildStatement("SELECT pk, ck FROM %s.%s WHERE pk = 1 AND ck = ?");

  private static final SimpleStatement STMT_SELECT_CK_CONCRETE =
      buildStatement("SELECT pk, ck FROM %s.%s WHERE pk = ? AND ck = 1");

  private static final SimpleStatement STMT_UPDATE =
      buildStatement("UPDATE %s.%s SET val = 1 WHERE pk = ? AND ck = ?");

  private static final SimpleStatement STMT_UPDATE_NO_KS =
      buildStatement("UPDATE %s SET val = 1 WHERE pk = ? AND ck = ?");

  private static final SimpleStatement STMT_UPDATE_CONCRETE =
      buildStatement("UPDATE %s.%s SET val = 1 WHERE pk = 1 AND ck = 1");

  private static final SimpleStatement STMT_UPDATE_PK_CONCRETE =
      buildStatement("UPDATE %s.%s SET val = 1 WHERE pk = 1 AND ck = ?");

  private static final SimpleStatement STMT_UPDATE_CK_CONCRETE =
      buildStatement("UPDATE %s.%s SET val = 1 WHERE pk = ? AND ck = 1");

  private static final SimpleStatement STMT_UPDATE_LWT_IF_EXISTS =
      buildStatement("UPDATE %s.%s SET val = 1 WHERE pk = ? AND ck = ? IF EXISTS");

  private static final SimpleStatement STMT_UPDATE_LWT_IF_VAL =
      buildStatement("UPDATE %s.%s SET val = 1 WHERE pk = ? AND ck = ? IF val = 2");

  private static final SimpleStatement STMT_DELETE =
      buildStatement("DELETE FROM %s.%s WHERE pk = ? AND ck = ?");

  private static final SimpleStatement STMT_DELETE_NO_KS =
      buildStatement("DELETE FROM %s WHERE pk = ? AND ck = ?");

  private static final SimpleStatement STMT_DELETE_CONCRETE =
      buildStatement("DELETE FROM %s.%s WHERE pk = 1 AND ck = 1");

  private static final SimpleStatement STMT_DELETE_PK_CONCRETE =
      buildStatement("DELETE FROM %s.%s WHERE pk = 1 AND ck = ?");

  private static final SimpleStatement STMT_DELETE_CK_CONCRETE =
      buildStatement("DELETE FROM %s.%s WHERE pk = ? AND ck = 1");

  private static final SimpleStatement STMT_DELETE_IF_EXISTS =
      buildStatement("DELETE FROM %s.%s WHERE pk = ? AND ck = ? IF EXISTS");

  @BeforeClass
  public static void setup() {
    CqlSession session = SESSION_RULE.session();
    session.execute(CREATE_KEYSPACE_QUERY);
    session.execute(CREATE_TABLE_QUERY);
  }

  @Test
  public void every_statement_should_deliver_tablet_info() {
    Map<String, Supplier<CqlSession>> sessions = new HashMap<>();
    sessions.put(
        "REGULAR",
        () -> CqlSession.builder().addContactEndPoints(CCM_RULE.getContactPoints()).build());
    sessions.put(
        "WITH_KEYSPACE",
        () ->
            CqlSession.builder()
                .addContactEndPoints(CCM_RULE.getContactPoints())
                .withKeyspace(KEYSPACE_NAME)
                .build());
    sessions.put(
        "USE_KEYSPACE",
        () -> {
          CqlSession s =
              CqlSession.builder().addContactEndPoints(CCM_RULE.getContactPoints()).build();
          s.execute("USE " + KEYSPACE_NAME);
          return s;
        });

    Map<String, Function<CqlSession, Statement>> statements = new HashMap<>();
    statements.put("SELECT_CONCRETE", s -> STMT_SELECT_CONCRETE);
    statements.put("SELECT_PREPARED", s -> s.prepare(STMT_SELECT).bind(2, 2));
    statements.put("SELECT_NO_KS_PREPARED", s -> s.prepare(STMT_SELECT_NO_KS).bind(2, 2));
    statements.put("SELECT_CONCRETE_PREPARED", s -> s.prepare(STMT_SELECT_CONCRETE).bind());
    statements.put("SELECT_PK_CONCRETE_PREPARED", s -> s.prepare(STMT_SELECT_PK_CONCRETE).bind(2));
    statements.put("SELECT_CK_CONCRETE_PREPARED", s -> s.prepare(STMT_SELECT_CK_CONCRETE).bind(2));
    statements.put("INSERT_CONCRETE", s -> STMT_INSERT_CONCRETE);
    statements.put("INSERT_PREPARED", s -> s.prepare(STMT_INSERT).bind(2, 2));
    statements.put("INSERT_NO_KS_PREPARED", s -> s.prepare(STMT_INSERT_NO_KS).bind(2, 2));
    statements.put("INSERT_CONCRETE_PREPARED", s -> s.prepare(STMT_INSERT_CONCRETE).bind());
    statements.put("INSERT_PK_CONCRETE_PREPARED", s -> s.prepare(STMT_INSERT_PK_CONCRETE).bind(2));
    statements.put("INSERT_CK_CONCRETE_PREPARED", s -> s.prepare(STMT_INSERT_CK_CONCRETE).bind(2));
    statements.put(
        "INSERT_LWT_IF_NOT_EXISTS", s -> s.prepare(STMT_INSERT_LWT_IF_NOT_EXISTS).bind(2, 2));
    statements.put("UPDATE_CONCRETE", s -> STMT_UPDATE_CONCRETE);
    statements.put("UPDATE_PREPARED", s -> s.prepare(STMT_UPDATE).bind(2, 2));
    statements.put("UPDATE_NO_KS_PREPARED", s -> s.prepare(STMT_UPDATE_NO_KS).bind(2, 2));
    statements.put("UPDATE_CONCRETE_PREPARED", s -> s.prepare(STMT_UPDATE_CONCRETE).bind());
    statements.put("UPDATE_PK_CONCRETE_PREPARED", s -> s.prepare(STMT_UPDATE_PK_CONCRETE).bind(2));
    statements.put("UPDATE_CK_CONCRETE_PREPARED", s -> s.prepare(STMT_UPDATE_CK_CONCRETE).bind(2));
    statements.put("UPDATE_LWT_IF_EXISTS", s -> s.prepare(STMT_UPDATE_LWT_IF_EXISTS).bind(2, 2));
    statements.put("STMT_UPDATE_LWT_IF_VAL", s -> s.prepare(STMT_UPDATE_LWT_IF_VAL).bind(2, 2));
    statements.put("DELETE_CONCRETE", s -> STMT_DELETE_CONCRETE);
    statements.put("DELETE_PREPARED", s -> s.prepare(STMT_DELETE).bind(2, 2));
    statements.put("DELETE_NO_KS_PREPARED", s -> s.prepare(STMT_DELETE_NO_KS).bind(2, 2));
    statements.put("DELETE_CONCRETE_PREPARED", s -> s.prepare(STMT_DELETE_CONCRETE).bind());
    statements.put("DELETE_PK_CONCRETE_PREPARED", s -> s.prepare(STMT_DELETE_PK_CONCRETE).bind(2));
    statements.put("DELETE_CK_CONCRETE_PREPARED", s -> s.prepare(STMT_DELETE_CK_CONCRETE).bind(2));
    statements.put("DELETE_LWT_IF_EXISTS", s -> s.prepare(STMT_DELETE_IF_EXISTS).bind(2, 2));

    List<String> testErrors = new ArrayList<>();
    for (Map.Entry<String, Supplier<CqlSession>> sessionEntry : sessions.entrySet()) {
      for (Map.Entry<String, Function<CqlSession, Statement>> stmtEntry : statements.entrySet()) {
        if (stmtEntry.getKey().contains("CONCRETE")
            && !stmtEntry.getKey().contains("CK_CONCRETE")) {
          // Scylla does not return tablet info for queries with PK built into query
          continue;
        }
        if (stmtEntry.getKey().contains("LWT")) {
          // LWT is not yet supported by scylla on tables with tablets
          continue;
        }
        if (sessionEntry.getKey().equals("REGULAR") && stmtEntry.getKey().contains("NO_KS")) {
          // Preparation of the statements without KS will fail on the session with no ks specified
          continue;
        }
        try (CqlSession session = sessionEntry.getValue().get()) {
          // Empty out tablets information
          if (session.getMetadata().getTabletMap().isPresent()) {
            session
                .getMetadata()
                .getTabletMap()
                .get()
                .removeByKeyspace(CqlIdentifier.fromCql(KEYSPACE_NAME));
          }
          Statement stmt;
          try {
            stmt = stmtEntry.getValue().apply(session);
          } catch (Exception e) {
            RuntimeException ex =
                new RuntimeException(
                    String.format(
                        "Failed to build statement %s on session %s",
                        stmtEntry.getKey(), sessionEntry.getKey()));
            ex.addSuppressed(e);
            throw ex;
          }
          try {
            if (!executeOnAllHostsAndReturnIfResultHasTabletsInfo(session, stmt)) {
              testErrors.add(
                  String.format(
                      "Statement %s on session %s got no tablet info",
                      stmtEntry.getKey(), sessionEntry.getKey()));
              continue;
            }
          } catch (Exception e) {
            testErrors.add(
                String.format(
                    "Failed to execute statement %s on session %s: %s",
                    stmtEntry.getKey(), sessionEntry.getKey(), e));
            continue;
          }
          if (!waitSessionLearnedTabletInfo(session)) {
            testErrors.add(
                String.format(
                    "Statement %s on session %s did not trigger session tablets update",
                    stmtEntry.getKey(), sessionEntry.getKey()));
            continue;
          }
          if (!checkIfRoutedProperly(session, stmt)) {
            testErrors.add(
                String.format(
                    "Statement %s on session %s was routed to different nodes",
                    stmtEntry.getKey(), sessionEntry.getKey()));
          }
        }
      }
    }

    if (!testErrors.isEmpty()) {
      throw new AssertionError(
          String.format(
              "Found queries that got no tablet info: \n%s", String.join("\n", testErrors)));
    }
  }

  @Test
  public void should_receive_each_tablet_exactly_once() {
    int counter = 0;
    try (CqlSession session =
        CqlSession.builder().addContactEndPoints(CCM_RULE.getContactPoints()).build()) {
      PreparedStatement preparedStatement = session.prepare(STMT_INSERT);
      for (int i = 1; i <= QUERIES; i++) {
        if (executeAndReturnIfResultHasTabletsInfo(session, preparedStatement.bind(i, i))) {
          counter++;
        }
      }
      Assert.assertEquals(INITIAL_TABLETS, counter);
      assertSessionTabletMapIsFilled(session);
    }

    try (CqlSession session =
        CqlSession.builder().addContactEndPoints(CCM_RULE.getContactPoints()).build()) {
      counter = 0;
      PreparedStatement preparedStatement = session.prepare(STMT_SELECT);
      for (int i = 1; i <= QUERIES; i++) {
        if (executeAndReturnIfResultHasTabletsInfo(session, preparedStatement.bind(i, i))) {
          counter++;
        }
      }

      LOG.debug("Ran first set of queries");

      // With enough queries we should hit a wrong node for each tablet exactly once.
      Assert.assertEquals(INITIAL_TABLETS, counter);
      assertSessionTabletMapIsFilled(session);

      // All tablet information should be available by now (unless for some reason cluster did sth
      // on its own). We should not receive any tablet payloads now, since they are sent only on
      // mismatch.
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

  private static boolean waitSessionLearnedTabletInfo(CqlSession session) {
    try {
      await()
          .atMost(Duration.ofSeconds(5))
          .pollInterval(Duration.ofMillis(50))
          .until(() -> isSessionLearnedTabletInfo(session));
      return true;
    } catch (org.awaitility.core.ConditionTimeoutException e) {
      return false;
    }
  }

  private static boolean checkIfRoutedProperly(CqlSession session, Statement stmt) {
    // DefaultLoadBalancingPolicy suppose to prioritize nodes from replica list randomly shuffling
    // them
    // If routing goes wrong you will see more than REPLICATION_FACTOR unique first nodes in
    // execution plan

    int expectedNodesCount = stmt.isLWT() ? 1 : REPLICATION_FACTOR;
    Set<Node> nodes = new HashSet<>();
    for (int i = 0; i < REPLICATION_FACTOR * 3; i++) {
      nodes.add(session.execute(stmt).getExecutionInfo().getCoordinator());
    }
    return nodes.size() <= expectedNodesCount;
  }

  private static boolean isSessionLearnedTabletInfo(CqlSession session) {
    if (!session.getMetadata().getTabletMap().isPresent()) {
      return false;
    }
    ConcurrentMap<KeyspaceTableNamePair, ConcurrentSkipListSet<Tablet>> tabletMapping =
        session.getMetadata().getTabletMap().get().getMapping();
    KeyspaceTableNamePair ktPair =
        new KeyspaceTableNamePair(
            CqlIdentifier.fromCql(KEYSPACE_NAME), CqlIdentifier.fromCql(TABLE_NAME));

    Set<Tablet> tablets = tabletMapping.get(ktPair);
    if (tablets == null || tablets.isEmpty()) {
      return false;
    }

    for (Tablet tab : tablets) {
      if (tab.getReplicaNodes().size() >= REPLICATION_FACTOR) {
        return true;
      }
    }
    return false;
  }

  private static void assertSessionTabletMapIsFilled(CqlSession session) {
    Assert.assertTrue(session.getMetadata().getTabletMap().isPresent());
    ConcurrentMap<KeyspaceTableNamePair, ConcurrentSkipListSet<Tablet>> tabletMapping =
        session.getMetadata().getTabletMap().get().getMapping();
    KeyspaceTableNamePair ktPair =
        new KeyspaceTableNamePair(
            CqlIdentifier.fromCql(KEYSPACE_NAME), CqlIdentifier.fromCql(TABLE_NAME));
    Assert.assertTrue(tabletMapping.containsKey(ktPair));

    Set<Tablet> tablets = tabletMapping.get(ktPair);
    Assert.assertEquals(INITIAL_TABLETS, tablets.size());

    for (Tablet tab : tablets) {
      Assert.assertEquals(REPLICATION_FACTOR, tab.getReplicaNodes().size());
    }
  }

  private static boolean executeOnAllHostsAndReturnIfResultHasTabletsInfo(
      CqlSession session, Statement stmt) {
    session.refreshSchema();
    for (Node node : session.getMetadata().getNodes().values()) {
      if (executeAndReturnIfResultHasTabletsInfo(session, stmt.setNode(node))) {
        return true;
      }
    }
    return false;
  }

  private static boolean executeAndReturnIfResultHasTabletsInfo(
      CqlSession session, Statement statement) {
    ResultSet rs = session.execute(statement);
    return rs.getExecutionInfo()
        .getIncomingPayload()
        .containsKey(TabletInfo.TABLETS_ROUTING_V1_CUSTOM_PAYLOAD_KEY);
  }

  private static SimpleStatement buildStatement(String statement) {
    if (statement.contains("%s.%s")) {
      return SimpleStatement.builder(String.format(statement, KEYSPACE_NAME, TABLE_NAME)).build();
    }
    return SimpleStatement.builder(String.format(statement, TABLE_NAME)).build();
  }
}
