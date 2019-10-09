/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.osgi.support;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.relation.Relation.column;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.DseSessionBuilder;
import com.datastax.dse.driver.api.querybuilder.DseSchemaBuilder;
import com.datastax.dse.driver.api.testinfra.DseSessionBuilderInstantiator;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import java.net.InetSocketAddress;

public interface DseOsgiSimpleTests {

  String CREATE_KEYSPACE =
      "CREATE KEYSPACE IF NOT EXISTS %s "
          + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}";

  /** @return config loader builder to be used to create session. */
  default ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder() {
    return DseSessionBuilderInstantiator.configLoaderBuilder();
  }

  /** @return The session builder to use for the OSGi tests. */
  default DseSessionBuilder sessionBuilder() {
    return DseSession.builder()
        .addContactEndPoint(new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9042)))
        // use the DSE driver's ClassLoader instead of the OSGI application thread's.
        .withClassLoader(DseSession.class.getClassLoader())
        .withConfigLoader(configLoaderBuilder().build());
  }

  /**
   * A very simple test that ensures a session can be established and a query made when running in
   * an OSGi container.
   */
  default void connectAndQuerySimple() {

    try (DseSession session = sessionBuilder().build()) {

      session.execute(String.format(CREATE_KEYSPACE, "test_osgi"));

      session.execute(
          // Exercise the DSE query builder
          DseSchemaBuilder.createTable("test_osgi", "t1")
              .ifNotExists()
              .withPartitionKey("pk", DataTypes.INT)
              .withColumn("v", DataTypes.INT)
              .build());

      session.execute(
          SimpleStatement.newInstance("INSERT INTO test_osgi.t1 (pk, v) VALUES (0, 1)"));

      Row row =
          session
              .execute(
                  // test that the Query Builder is available
                  selectFrom("test_osgi", "t1")
                      .column("v")
                      .where(column("pk").isEqualTo(literal(0)))
                      .build())
              .one();

      assertThat(row).isNotNull();
      assertThat(row.getInt(0)).isEqualTo(1);
    }
  }
}
