package com.datastax.oss.driver.examples.basic;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.TraceEvent;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import java.nio.ByteBuffer;
import java.util.Set;

/**
 * Demonstrates usage of TokenMap and NodeShardingInfo Needs a Scylla cluster to be running locally
 * or adjustment of session builder.
 */
public class TokenMapAndShardIdLookup {

  private static String CREATE_KEYSPACE =
      "CREATE KEYSPACE IF NOT EXISTS tokenmap_example_ks "
          + "WITH replication = {"
          + "'class': 'SimpleStrategy', "
          + "'replication_factor': 1"
          + "}";

  private static String CREATE_TABLE =
      ""
          + "CREATE TABLE IF NOT EXISTS tokenmap_example_ks.example_tab ("
          + "my_column bigint,"
          + "PRIMARY KEY (my_column)"
          + ")";

  private static String INSERT_COLUMN =
      "INSERT INTO tokenmap_example_ks.example_tab (my_column) VALUES (2)";

  private static String SELECT_COLUMN =
      "SELECT * FROM tokenmap_example_ks.example_tab WHERE my_column = 2";

  private static ByteBuffer PARTITION_KEY = TypeCodecs.BIGINT.encode(2L, DefaultProtocolVersion.V3);

  public static void main(String[] args) {

    try (CqlSession session = CqlSession.builder().build()) {

      System.out.printf("Connected session: %s%n", session.getName());

      session.execute(CREATE_KEYSPACE);
      session.execute(CREATE_TABLE);
      session.execute(INSERT_COLUMN);

      Metadata metadata = session.refreshSchema();

      System.out.println("Prepared example data");

      TokenMap tokenMap = metadata.getTokenMap().get();

      Set<Node> nodes =
          tokenMap.getReplicas(CqlIdentifier.fromCql("tokenmap_example_ks"), PARTITION_KEY);
      System.out.println("Replica set size: " + nodes.size());

      Token token = tokenMap.newToken(PARTITION_KEY);
      assert nodes.size() > 0;
      Node node = nodes.iterator().next();

      assert node.getShardingInfo() != null;
      int shardId = node.getShardingInfo().shardId(token);

      System.out.println(
          "Hardcoded partition key should belong to shard number "
              + shardId
              + " (on Node: "
              + node
              + ")");

      System.out.println("You can compare it with SELECT query trace:");
      // If there is only 1 node, then the SELECT has to hit the one we did shardId calculation for.
      SimpleStatement statement = SimpleStatement.builder(SELECT_COLUMN).setTracing(true).build();
      ResultSet rs = session.execute(statement);

      for (TraceEvent event : rs.getExecutionInfo().getQueryTrace().getEvents()) {
        System.out.println(event);
      }
    }
  }
}
