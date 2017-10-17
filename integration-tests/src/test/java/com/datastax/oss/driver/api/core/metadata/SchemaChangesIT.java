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

import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.cql.CqlSession;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterUtils;
import com.datastax.oss.driver.api.testinfra.utils.ConditionChecker;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

public class SchemaChangesIT {

  @Rule public CcmRule ccmRule = CcmRule.getInstance();

  // A client that we only use to set up the tests
  @Rule
  public ClusterRule adminClusterRule =
      new ClusterRule(
          ccmRule, "request.timeout = 30 seconds", "metadata.schema.debouncer.window = 0 seconds");

  @Before
  public void setup() {
    // Always drop and re-create the keyspace to start from a clean state
    adminClusterRule
        .session()
        .execute(String.format("DROP KEYSPACE %s", adminClusterRule.keyspace()));
    ClusterUtils.createKeyspace(adminClusterRule.cluster(), adminClusterRule.keyspace());
  }

  @Test
  public void should_handle_keyspace_creation() {
    CqlIdentifier newKeyspaceId = ClusterUtils.uniqueKeyspaceId();
    should_handle_creation(
        null,
        String.format(
            "CREATE KEYSPACE %s "
                + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            newKeyspaceId),
        metadata -> metadata.getKeyspace(newKeyspaceId),
        keyspace -> {
          assertThat(keyspace.getName()).isEqualTo(newKeyspaceId);
          assertThat(keyspace.isDurableWrites()).isTrue();
          assertThat(keyspace.getReplication())
              .hasSize(2)
              .containsEntry("class", "org.apache.cassandra.locator.SimpleStrategy")
              .containsEntry("replication_factor", "1");
        },
        (listener, keyspace) -> Mockito.verify(listener).onKeyspaceCreated(keyspace),
        newKeyspaceId);
  }

  @Test
  public void should_handle_keyspace_drop() {
    CqlIdentifier newKeyspaceId = ClusterUtils.uniqueKeyspaceId();
    should_handle_drop(
        ImmutableList.of(
            String.format(
                "CREATE KEYSPACE %s "
                    + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
                newKeyspaceId.asCql(true))),
        String.format("DROP KEYSPACE %s", newKeyspaceId.asCql(true)),
        metadata -> metadata.getKeyspace(newKeyspaceId),
        (listener, oldKeyspace) -> Mockito.verify(listener).onKeyspaceDropped(oldKeyspace),
        newKeyspaceId);
  }

  @Test
  public void should_handle_keyspace_update() {
    CqlIdentifier newKeyspaceId = ClusterUtils.uniqueKeyspaceId();
    should_handle_update(
        ImmutableList.of(
            String.format(
                "CREATE KEYSPACE %s "
                    + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
                newKeyspaceId.asCql(true))),
        String.format(
            "ALTER KEYSPACE %s "
                + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1} "
                + "AND durable_writes = 'false'",
            newKeyspaceId.asCql(true)),
        metadata -> metadata.getKeyspace(newKeyspaceId),
        newKeyspace -> assertThat(newKeyspace.isDurableWrites()).isFalse(),
        (listener, oldKeyspace, newKeyspace) ->
            Mockito.verify(listener).onKeyspaceUpdated(newKeyspace, oldKeyspace),
        newKeyspaceId);
  }

  @Test
  public void should_handle_table_creation() {
    should_handle_creation(
        null,
        "CREATE TABLE foo(k int primary key)",
        metadata ->
            metadata
                .getKeyspace(adminClusterRule.keyspace())
                .getTable(CqlIdentifier.fromInternal("foo")),
        table -> {
          assertThat(table.getKeyspace()).isEqualTo(adminClusterRule.keyspace());
          assertThat(table.getName().asInternal()).isEqualTo("foo");
          assertThat(table.getColumns()).containsOnlyKeys(CqlIdentifier.fromInternal("k"));
          ColumnMetadata k = table.getColumn(CqlIdentifier.fromInternal("k"));
          assertThat(k.getType()).isEqualTo(DataTypes.INT);
          assertThat(table.getPartitionKey()).containsExactly(k);
          assertThat(table.getClusteringColumns()).isEmpty();
        },
        (listener, table) -> Mockito.verify(listener).onTableCreated(table));
  }

  @Test
  public void should_handle_table_drop() {
    should_handle_drop(
        ImmutableList.of("CREATE TABLE foo(k int primary key)"),
        "DROP TABLE foo",
        metadata ->
            metadata
                .getKeyspace(adminClusterRule.keyspace())
                .getTable(CqlIdentifier.fromInternal("foo")),
        (listener, oldTable) -> Mockito.verify(listener).onTableDropped(oldTable));
  }

  @Test
  public void should_handle_table_update() {
    should_handle_update(
        ImmutableList.of("CREATE TABLE foo(k int primary key)"),
        "ALTER TABLE foo ADD v int",
        metadata ->
            metadata
                .getKeyspace(adminClusterRule.keyspace())
                .getTable(CqlIdentifier.fromInternal("foo")),
        newTable -> assertThat(newTable.getColumn(CqlIdentifier.fromInternal("v"))).isNotNull(),
        (listener, oldTable, newTable) ->
            Mockito.verify(listener).onTableUpdated(newTable, oldTable));
  }

  @Test
  public void should_handle_type_creation() {
    should_handle_creation(
        null,
        "CREATE TYPE t(i int)",
        metadata ->
            metadata
                .getKeyspace(adminClusterRule.keyspace())
                .getUserDefinedType(CqlIdentifier.fromInternal("t")),
        type -> {
          assertThat(type.getKeyspace()).isEqualTo(adminClusterRule.keyspace());
          assertThat(type.getName().asInternal()).isEqualTo("t");
          assertThat(type.getFieldNames()).containsExactly(CqlIdentifier.fromInternal("i"));
          assertThat(type.getFieldTypes()).containsExactly(DataTypes.INT);
        },
        (listener, type) -> Mockito.verify(listener).onUserDefinedTypeCreated(type));
  }

  @Test
  public void should_handle_type_drop() {
    should_handle_drop(
        ImmutableList.of("CREATE TYPE t(i int)"),
        "DROP TYPE t",
        metadata ->
            metadata
                .getKeyspace(adminClusterRule.keyspace())
                .getUserDefinedType(CqlIdentifier.fromInternal("t")),
        (listener, oldType) -> Mockito.verify(listener).onUserDefinedTypeDropped(oldType));
  }

  @Test
  public void should_handle_type_update() {
    should_handle_update(
        ImmutableList.of("CREATE TYPE t(i int)"),
        "ALTER TYPE t ADD j int",
        metadata ->
            metadata
                .getKeyspace(adminClusterRule.keyspace())
                .getUserDefinedType(CqlIdentifier.fromInternal("t")),
        newType ->
            assertThat(newType.getFieldNames())
                .containsExactly(CqlIdentifier.fromInternal("i"), CqlIdentifier.fromInternal("j")),
        (listener, oldType, newType) ->
            Mockito.verify(listener).onUserDefinedTypeUpdated(newType, oldType));
  }

  @Test
  @CassandraRequirement(min = "3.0")
  public void should_handle_view_creation() {
    should_handle_creation(
        "CREATE TABLE scores(user text, game text, score int, PRIMARY KEY (user, game))",
        "CREATE MATERIALIZED VIEW highscores "
            + "AS SELECT user, score FROM scores "
            + "WHERE game IS NOT NULL AND score IS NOT NULL PRIMARY KEY (game, score, user) "
            + "WITH CLUSTERING ORDER BY (score DESC)",
        metadata ->
            metadata
                .getKeyspace(adminClusterRule.keyspace())
                .getView(CqlIdentifier.fromInternal("highscores")),
        view -> {
          assertThat(view.getKeyspace()).isEqualTo(adminClusterRule.keyspace());
          assertThat(view.getName().asInternal()).isEqualTo("highscores");
          assertThat(view.getBaseTable().asInternal()).isEqualTo("scores");
          assertThat(view.includesAllColumns()).isFalse();
          assertThat(view.getWhereClause()).isEqualTo("game IS NOT NULL AND score IS NOT NULL");
          assertThat(view.getColumns())
              .containsOnlyKeys(
                  CqlIdentifier.fromInternal("game"),
                  CqlIdentifier.fromInternal("score"),
                  CqlIdentifier.fromInternal("user"));
        },
        (listener, view) -> Mockito.verify(listener).onViewCreated(view));
  }

  @Test
  @CassandraRequirement(min = "3.0")
  public void should_handle_view_drop() {
    should_handle_drop(
        ImmutableList.of(
            "CREATE TABLE scores(user text, game text, score int, PRIMARY KEY (user, game))",
            "CREATE MATERIALIZED VIEW highscores "
                + "AS SELECT user, score FROM scores "
                + "WHERE game IS NOT NULL AND score IS NOT NULL PRIMARY KEY (game, score, user) "
                + "WITH CLUSTERING ORDER BY (score DESC)"),
        "DROP MATERIALIZED VIEW highscores",
        metadata ->
            metadata
                .getKeyspace(adminClusterRule.keyspace())
                .getView(CqlIdentifier.fromInternal("highscores")),
        (listener, oldView) -> Mockito.verify(listener).onViewDropped(oldView));
  }

  @Test
  @CassandraRequirement(min = "3.0")
  public void should_handle_view_update() {
    should_handle_update(
        ImmutableList.of(
            "CREATE TABLE scores(user text, game text, score int, PRIMARY KEY (user, game))",
            "CREATE MATERIALIZED VIEW highscores "
                + "AS SELECT user, score FROM scores "
                + "WHERE game IS NOT NULL AND score IS NOT NULL PRIMARY KEY (game, score, user) "
                + "WITH CLUSTERING ORDER BY (score DESC)"),
        "ALTER MATERIALIZED VIEW highscores WITH comment = 'The best score for each game'",
        metadata ->
            metadata
                .getKeyspace(adminClusterRule.keyspace())
                .getView(CqlIdentifier.fromInternal("highscores")),
        newView ->
            assertThat(newView.getOptions().get(CqlIdentifier.fromInternal("comment")))
                .isEqualTo("The best score for each game"),
        (listener, oldView, newView) -> Mockito.verify(listener).onViewUpdated(newView, oldView));
  }

  @Test
  @CassandraRequirement(min = "2.2")
  public void should_handle_function_creation() {
    should_handle_creation(
        null,
        "CREATE FUNCTION id(i int) RETURNS NULL ON NULL INPUT RETURNS int "
            + "LANGUAGE java AS 'return i;'",
        metadata ->
            metadata
                .getKeyspace(adminClusterRule.keyspace())
                .getFunction(CqlIdentifier.fromInternal("id"), DataTypes.INT),
        function -> {
          assertThat(function.getKeyspace()).isEqualTo(adminClusterRule.keyspace());
          assertThat(function.getSignature().getName().asInternal()).isEqualTo("id");
          assertThat(function.getSignature().getParameterTypes()).containsExactly(DataTypes.INT);
          assertThat(function.getReturnType()).isEqualTo(DataTypes.INT);
          assertThat(function.getLanguage()).isEqualTo("java");
          assertThat(function.isCalledOnNullInput()).isFalse();
          assertThat(function.getBody()).isEqualTo("return i;");
        },
        (listener, function) -> Mockito.verify(listener).onFunctionCreated(function));
  }

  @Test
  @CassandraRequirement(min = "2.2")
  public void should_handle_function_drop() {
    should_handle_drop(
        ImmutableList.of(
            "CREATE FUNCTION id(i int) RETURNS NULL ON NULL INPUT RETURNS int "
                + "LANGUAGE java AS 'return i;'"),
        "DROP FUNCTION id",
        metadata ->
            metadata
                .getKeyspace(adminClusterRule.keyspace())
                .getFunction(CqlIdentifier.fromInternal("id"), DataTypes.INT),
        (listener, oldFunction) -> Mockito.verify(listener).onFunctionDropped(oldFunction));
  }

  @Test
  @CassandraRequirement(min = "2.2")
  public void should_handle_function_update() {
    should_handle_update_via_drop_and_recreate(
        ImmutableList.of(
            "CREATE FUNCTION id(i int) RETURNS NULL ON NULL INPUT RETURNS int "
                + "LANGUAGE java AS 'return i;'"),
        "DROP FUNCTION id",
        "CREATE FUNCTION id(j int) RETURNS NULL ON NULL INPUT RETURNS int "
            + "LANGUAGE java AS 'return j;'",
        metadata ->
            metadata
                .getKeyspace(adminClusterRule.keyspace())
                .getFunction(CqlIdentifier.fromInternal("id"), DataTypes.INT),
        newFunction -> assertThat(newFunction.getBody()).isEqualTo("return j;"),
        (listener, oldFunction, newFunction) ->
            Mockito.verify(listener).onFunctionUpdated(newFunction, oldFunction));
  }

  @Test
  @CassandraRequirement(min = "2.2")
  public void should_handle_aggregate_creation() {
    should_handle_creation(
        "CREATE FUNCTION plus(i int, j int) RETURNS NULL ON NULL INPUT RETURNS int "
            + "LANGUAGE java AS 'return i+j;'",
        "CREATE AGGREGATE sum(int) SFUNC plus STYPE int INITCOND 0",
        metadata ->
            metadata
                .getKeyspace(adminClusterRule.keyspace())
                .getAggregate(CqlIdentifier.fromInternal("sum"), DataTypes.INT),
        aggregate -> {
          assertThat(aggregate.getKeyspace()).isEqualTo(adminClusterRule.keyspace());
          assertThat(aggregate.getSignature().getName().asInternal()).isEqualTo("sum");
          assertThat(aggregate.getSignature().getParameterTypes()).containsExactly(DataTypes.INT);
          assertThat(aggregate.getStateType()).isEqualTo(DataTypes.INT);
          assertThat(aggregate.getStateFuncSignature().getName().asInternal()).isEqualTo("plus");
          assertThat(aggregate.getStateFuncSignature().getParameterTypes())
              .containsExactly(DataTypes.INT, DataTypes.INT);
          assertThat(aggregate.getFinalFuncSignature()).isNull();
          assertThat(aggregate.getInitCond()).isEqualTo(0);
        },
        (listener, aggregate) -> Mockito.verify(listener).onAggregateCreated(aggregate));
  }

  @Test
  @CassandraRequirement(min = "2.2")
  public void should_handle_aggregate_drop() {
    should_handle_drop(
        ImmutableList.of(
            "CREATE FUNCTION plus(i int, j int) RETURNS NULL ON NULL INPUT RETURNS int "
                + "LANGUAGE java AS 'return i+j;'",
            "CREATE AGGREGATE sum(int) SFUNC plus STYPE int INITCOND 0"),
        "DROP AGGREGATE sum",
        metadata ->
            metadata
                .getKeyspace(adminClusterRule.keyspace())
                .getAggregate(CqlIdentifier.fromInternal("sum"), DataTypes.INT),
        (listener, oldAggregate) -> Mockito.verify(listener).onAggregateDropped(oldAggregate));
  }

  @Test
  @CassandraRequirement(min = "2.2")
  public void should_handle_aggregate_update() {
    should_handle_update_via_drop_and_recreate(
        ImmutableList.of(
            "CREATE FUNCTION plus(i int, j int) RETURNS NULL ON NULL INPUT RETURNS int "
                + "LANGUAGE java AS 'return i+j;'",
            "CREATE AGGREGATE sum(int) SFUNC plus STYPE int INITCOND 0"),
        "DROP AGGREGATE sum",
        "CREATE AGGREGATE sum(int) SFUNC plus STYPE int INITCOND 1",
        metadata ->
            metadata
                .getKeyspace(adminClusterRule.keyspace())
                .getAggregate(CqlIdentifier.fromInternal("sum"), DataTypes.INT),
        newAggregate -> assertThat(newAggregate.getInitCond()).isEqualTo(1),
        (listener, oldAggregate, newAggregate) ->
            Mockito.verify(listener).onAggregateUpdated(newAggregate, oldAggregate));
  }

  private String keyspaceFilterOption(CqlIdentifier... keyspaces) {
    // create option to filter keyspace refreshes based on input keyspaces, if none are provided, assume the
    // one associated wiht the cluster rule.
    if (keyspaces.length == 0) {
      keyspaces = new CqlIdentifier[] {adminClusterRule.keyspace()};
    }

    String keyspaceStr =
        Arrays.stream(keyspaces).map(i -> i.asCql(false)).collect(Collectors.joining(","));

    return String.format("metadata.schema.refreshed-keyspaces = [%s]", keyspaceStr);
  }

  private <T> void should_handle_creation(
      String beforeStatement,
      String createStatement,
      Function<Metadata, T> extract,
      Consumer<T> verifyMetadata,
      BiConsumer<SchemaChangeListener, T> verifyListener,
      CqlIdentifier... keyspaces) {

    if (beforeStatement != null) {
      adminClusterRule.session().execute(beforeStatement);
    }

    // cluster1 executes the DDL query and gets a SCHEMA_CHANGE response.
    // cluster2 gets a SCHEMA_CHANGE push event on its control connection.
    try (Cluster<CqlSession> cluster1 =
            ClusterUtils.newCluster(
                ccmRule, "request.timeout = 30 seconds", keyspaceFilterOption(keyspaces));
        Cluster<CqlSession> cluster2 =
            ClusterUtils.newCluster(ccmRule, keyspaceFilterOption(keyspaces))) {

      SchemaChangeListener listener1 = Mockito.mock(SchemaChangeListener.class);
      cluster1.register(listener1);
      SchemaChangeListener listener2 = Mockito.mock(SchemaChangeListener.class);
      cluster2.register(listener2);

      cluster1.connect(adminClusterRule.keyspace()).execute(createStatement);

      // Refreshes on a response are synchronous:
      T newElement1 = extract.apply(cluster1.getMetadata());
      verifyMetadata.accept(newElement1);
      verifyListener.accept(listener1, newElement1);

      // Refreshes on a server event are asynchronous:
      ConditionChecker.checkThat(
              () -> {
                T newElement2 = extract.apply(cluster2.getMetadata());
                verifyMetadata.accept(newElement2);
                verifyListener.accept(listener2, newElement2);
              })
          .becomesTrue();
    }
  }

  private <T> void should_handle_drop(
      Iterable<String> beforeStatements,
      String dropStatement,
      Function<Metadata, T> extract,
      BiConsumer<SchemaChangeListener, T> verifyListener,
      CqlIdentifier... keyspaces) {

    for (String statement : beforeStatements) {
      adminClusterRule.session().execute(statement);
    }

    try (Cluster<CqlSession> cluster1 =
            ClusterUtils.newCluster(
                ccmRule, "request.timeout = 30 seconds", keyspaceFilterOption(keyspaces));
        Cluster<CqlSession> cluster2 =
            ClusterUtils.newCluster(ccmRule, keyspaceFilterOption(keyspaces))) {

      T oldElement = extract.apply(cluster1.getMetadata());
      assertThat(oldElement).isNotNull();

      SchemaChangeListener listener1 = Mockito.mock(SchemaChangeListener.class);
      cluster1.register(listener1);
      SchemaChangeListener listener2 = Mockito.mock(SchemaChangeListener.class);
      cluster2.register(listener2);

      cluster1.connect(adminClusterRule.keyspace()).execute(dropStatement);

      assertThat(extract.apply(cluster1.getMetadata())).isNull();
      verifyListener.accept(listener1, oldElement);

      ConditionChecker.checkThat(
              () -> {
                assertThat(extract.apply(cluster2.getMetadata())).isNull();
                verifyListener.accept(listener2, oldElement);
              })
          .becomesTrue();
    }
  }

  private <T> void should_handle_update(
      Iterable<String> beforeStatements,
      String updateStatement,
      Function<Metadata, T> extract,
      Consumer<T> verifyNewMetadata,
      TriConsumer<SchemaChangeListener, T, T> verifyListener,
      CqlIdentifier... keyspaces) {

    for (String statement : beforeStatements) {
      adminClusterRule.session().execute(statement);
    }

    try (Cluster<CqlSession> cluster1 =
            ClusterUtils.newCluster(
                ccmRule, "request.timeout = 30 seconds", keyspaceFilterOption(keyspaces));
        Cluster<CqlSession> cluster2 =
            ClusterUtils.newCluster(ccmRule, keyspaceFilterOption(keyspaces))) {

      T oldElement = extract.apply(cluster1.getMetadata());
      assertThat(oldElement).isNotNull();

      SchemaChangeListener listener1 = Mockito.mock(SchemaChangeListener.class);
      cluster1.register(listener1);
      SchemaChangeListener listener2 = Mockito.mock(SchemaChangeListener.class);
      cluster2.register(listener2);

      cluster1.connect(adminClusterRule.keyspace()).execute(updateStatement);

      T newElement = extract.apply(cluster1.getMetadata());
      verifyNewMetadata.accept(newElement);
      verifyListener.accept(listener1, oldElement, newElement);

      ConditionChecker.checkThat(
              () -> {
                verifyNewMetadata.accept(extract.apply(cluster2.getMetadata()));
                verifyListener.accept(listener2, oldElement, newElement);
              })
          .becomesTrue();
    }
  }

  // Some element types don't have an ALTER command, but we can still observe an update if they get
  // dropped and recreated while schema metadata is disabled
  private <T> void should_handle_update_via_drop_and_recreate(
      Iterable<String> beforeStatements,
      String dropStatement,
      String recreateStatement,
      Function<Metadata, T> extract,
      Consumer<T> verifyNewMetadata,
      TriConsumer<SchemaChangeListener, T, T> verifyListener,
      CqlIdentifier... keyspaces) {

    for (String statement : beforeStatements) {
      adminClusterRule.session().execute(statement);
    }

    try (Cluster<CqlSession> cluster1 =
            ClusterUtils.newCluster(
                ccmRule, "request.timeout = 30 seconds", keyspaceFilterOption(keyspaces));
        Cluster<CqlSession> cluster2 =
            ClusterUtils.newCluster(ccmRule, keyspaceFilterOption(keyspaces))) {

      T oldElement = extract.apply(cluster1.getMetadata());
      assertThat(oldElement).isNotNull();

      SchemaChangeListener listener1 = Mockito.mock(SchemaChangeListener.class);
      cluster1.register(listener1);
      SchemaChangeListener listener2 = Mockito.mock(SchemaChangeListener.class);
      cluster2.register(listener2);

      cluster1.setSchemaMetadataEnabled(false);
      cluster2.setSchemaMetadataEnabled(false);

      cluster1.connect(adminClusterRule.keyspace()).execute(dropStatement);
      cluster1.connect(adminClusterRule.keyspace()).execute(recreateStatement);

      cluster1.setSchemaMetadataEnabled(true);
      cluster2.setSchemaMetadataEnabled(true);

      ConditionChecker.checkThat(
              () -> {
                T newElement = extract.apply(cluster1.getMetadata());
                verifyNewMetadata.accept(newElement);
                verifyListener.accept(listener1, oldElement, newElement);
              })
          .becomesTrue();

      ConditionChecker.checkThat(
              () -> {
                T newElement = extract.apply(cluster2.getMetadata());
                verifyNewMetadata.accept(extract.apply(cluster2.getMetadata()));
                verifyListener.accept(listener2, oldElement, newElement);
              })
          .becomesTrue();
    }
  }

  interface TriConsumer<T, U, V> {
    void accept(T t, U u, V v);
  }
}
