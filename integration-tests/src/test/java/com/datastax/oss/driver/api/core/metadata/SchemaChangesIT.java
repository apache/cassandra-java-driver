/*
 * Copyright DataStax, Inc.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.utils.ConditionChecker;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class SchemaChangesIT {

  private CcmRule ccmRule = CcmRule.getInstance();

  // A client that we only use to set up the tests
  private SessionRule<CqlSession> adminSessionRule =
      SessionRule.builder(ccmRule)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
                  .withDuration(DefaultDriverOption.METADATA_SCHEMA_WINDOW, Duration.ofSeconds(0))
                  .build())
          .build();

  @Rule public TestRule chain = RuleChain.outerRule(ccmRule).around(adminSessionRule);

  @Before
  public void setup() {
    // Always drop and re-create the keyspace to start from a clean state
    adminSessionRule
        .session()
        .execute(String.format("DROP KEYSPACE %s", adminSessionRule.keyspace()));
    SessionUtils.createKeyspace(adminSessionRule.session(), adminSessionRule.keyspace());
  }

  @Test
  public void should_handle_keyspace_creation() {
    CqlIdentifier newKeyspaceId = SessionUtils.uniqueKeyspaceId();
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
        (listener, keyspace) -> verify(listener).onKeyspaceCreated(keyspace),
        newKeyspaceId);
  }

  @Test
  public void should_handle_keyspace_drop() {
    CqlIdentifier newKeyspaceId = SessionUtils.uniqueKeyspaceId();
    should_handle_drop(
        ImmutableList.of(
            String.format(
                "CREATE KEYSPACE %s "
                    + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
                newKeyspaceId.asCql(true))),
        String.format("DROP KEYSPACE %s", newKeyspaceId.asCql(true)),
        metadata -> metadata.getKeyspace(newKeyspaceId),
        (listener, oldKeyspace) -> verify(listener).onKeyspaceDropped(oldKeyspace),
        newKeyspaceId);
  }

  @Test
  public void should_handle_keyspace_update() {
    CqlIdentifier newKeyspaceId = SessionUtils.uniqueKeyspaceId();
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
            verify(listener).onKeyspaceUpdated(newKeyspace, oldKeyspace),
        newKeyspaceId);
  }

  @Test
  public void should_handle_table_creation() {
    should_handle_creation(
        null,
        "CREATE TABLE foo(k int primary key)",
        metadata ->
            metadata
                .getKeyspace(adminSessionRule.keyspace())
                .orElseThrow(IllegalStateException::new)
                .getTable(CqlIdentifier.fromInternal("foo")),
        table -> {
          assertThat(table.getKeyspace()).isEqualTo(adminSessionRule.keyspace());
          assertThat(table.getName().asInternal()).isEqualTo("foo");
          assertThat(table.getColumns()).containsOnlyKeys(CqlIdentifier.fromInternal("k"));
          assertThat(table.getColumn(CqlIdentifier.fromInternal("k")))
              .hasValueSatisfying(
                  k -> {
                    assertThat(k.getType()).isEqualTo(DataTypes.INT);
                    Assertions.<ColumnMetadata>assertThat(table.getPartitionKey())
                        .containsExactly(k);
                  });
          assertThat(table.getClusteringColumns()).isEmpty();
        },
        (listener, table) -> verify(listener).onTableCreated(table));
  }

  @Test
  public void should_handle_table_drop() {
    should_handle_drop(
        ImmutableList.of("CREATE TABLE foo(k int primary key)"),
        "DROP TABLE foo",
        metadata ->
            metadata
                .getKeyspace(adminSessionRule.keyspace())
                .flatMap(ks -> ks.getTable(CqlIdentifier.fromInternal("foo"))),
        (listener, oldTable) -> verify(listener).onTableDropped(oldTable));
  }

  @Test
  public void should_handle_table_update() {
    should_handle_update(
        ImmutableList.of("CREATE TABLE foo(k int primary key)"),
        "ALTER TABLE foo ADD v int",
        metadata ->
            metadata
                .getKeyspace(adminSessionRule.keyspace())
                .flatMap(ks -> ks.getTable(CqlIdentifier.fromInternal("foo"))),
        newTable -> assertThat(newTable.getColumn(CqlIdentifier.fromInternal("v"))).isPresent(),
        (listener, oldTable, newTable) -> verify(listener).onTableUpdated(newTable, oldTable));
  }

  @Test
  public void should_handle_type_creation() {
    should_handle_creation(
        null,
        "CREATE TYPE t(i int)",
        metadata ->
            metadata
                .getKeyspace(adminSessionRule.keyspace())
                .flatMap(ks -> ks.getUserDefinedType(CqlIdentifier.fromInternal("t"))),
        type -> {
          assertThat(type.getKeyspace()).isEqualTo(adminSessionRule.keyspace());
          assertThat(type.getName().asInternal()).isEqualTo("t");
          assertThat(type.getFieldNames()).containsExactly(CqlIdentifier.fromInternal("i"));
          assertThat(type.getFieldTypes()).containsExactly(DataTypes.INT);
        },
        (listener, type) -> verify(listener).onUserDefinedTypeCreated(type));
  }

  @Test
  public void should_handle_type_drop() {
    should_handle_drop(
        ImmutableList.of("CREATE TYPE t(i int)"),
        "DROP TYPE t",
        metadata ->
            metadata
                .getKeyspace(adminSessionRule.keyspace())
                .flatMap(ks -> ks.getUserDefinedType(CqlIdentifier.fromInternal("t"))),
        (listener, oldType) -> verify(listener).onUserDefinedTypeDropped(oldType));
  }

  @Test
  public void should_handle_type_update() {
    should_handle_update(
        ImmutableList.of("CREATE TYPE t(i int)"),
        "ALTER TYPE t ADD j int",
        metadata ->
            metadata
                .getKeyspace(adminSessionRule.keyspace())
                .flatMap(ks -> ks.getUserDefinedType(CqlIdentifier.fromInternal("t"))),
        newType ->
            assertThat(newType.getFieldNames())
                .containsExactly(CqlIdentifier.fromInternal("i"), CqlIdentifier.fromInternal("j")),
        (listener, oldType, newType) ->
            verify(listener).onUserDefinedTypeUpdated(newType, oldType));
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
                .getKeyspace(adminSessionRule.keyspace())
                .flatMap(ks -> ks.getView(CqlIdentifier.fromInternal("highscores"))),
        view -> {
          assertThat(view.getKeyspace()).isEqualTo(adminSessionRule.keyspace());
          assertThat(view.getName().asInternal()).isEqualTo("highscores");
          assertThat(view.getBaseTable().asInternal()).isEqualTo("scores");
          assertThat(view.includesAllColumns()).isFalse();
          assertThat(view.getWhereClause()).hasValue("game IS NOT NULL AND score IS NOT NULL");
          assertThat(view.getColumns())
              .containsOnlyKeys(
                  CqlIdentifier.fromInternal("game"),
                  CqlIdentifier.fromInternal("score"),
                  CqlIdentifier.fromInternal("user"));
        },
        (listener, view) -> verify(listener).onViewCreated(view));
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
                .getKeyspace(adminSessionRule.keyspace())
                .flatMap(ks -> ks.getView(CqlIdentifier.fromInternal("highscores"))),
        (listener, oldView) -> verify(listener).onViewDropped(oldView));
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
                .getKeyspace(adminSessionRule.keyspace())
                .flatMap(ks -> ks.getView(CqlIdentifier.fromInternal("highscores"))),
        newView ->
            assertThat(newView.getOptions().get(CqlIdentifier.fromInternal("comment")))
                .isEqualTo("The best score for each game"),
        (listener, oldView, newView) -> verify(listener).onViewUpdated(newView, oldView));
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
                .getKeyspace(adminSessionRule.keyspace())
                .flatMap(ks -> ks.getFunction(CqlIdentifier.fromInternal("id"), DataTypes.INT)),
        function -> {
          assertThat(function.getKeyspace()).isEqualTo(adminSessionRule.keyspace());
          assertThat(function.getSignature().getName().asInternal()).isEqualTo("id");
          assertThat(function.getSignature().getParameterTypes()).containsExactly(DataTypes.INT);
          assertThat(function.getReturnType()).isEqualTo(DataTypes.INT);
          assertThat(function.getLanguage()).isEqualTo("java");
          assertThat(function.isCalledOnNullInput()).isFalse();
          assertThat(function.getBody()).isEqualTo("return i;");
        },
        (listener, function) -> verify(listener).onFunctionCreated(function));
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
                .getKeyspace(adminSessionRule.keyspace())
                .flatMap(ks -> ks.getFunction(CqlIdentifier.fromInternal("id"), DataTypes.INT)),
        (listener, oldFunction) -> verify(listener).onFunctionDropped(oldFunction));
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
                .getKeyspace(adminSessionRule.keyspace())
                .flatMap(ks -> ks.getFunction(CqlIdentifier.fromInternal("id"), DataTypes.INT)),
        newFunction -> assertThat(newFunction.getBody()).isEqualTo("return j;"),
        (listener, oldFunction, newFunction) ->
            verify(listener).onFunctionUpdated(newFunction, oldFunction));
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
                .getKeyspace(adminSessionRule.keyspace())
                .flatMap(ks -> ks.getAggregate(CqlIdentifier.fromInternal("sum"), DataTypes.INT)),
        aggregate -> {
          assertThat(aggregate.getKeyspace()).isEqualTo(adminSessionRule.keyspace());
          assertThat(aggregate.getSignature().getName().asInternal()).isEqualTo("sum");
          assertThat(aggregate.getSignature().getParameterTypes()).containsExactly(DataTypes.INT);
          assertThat(aggregate.getStateType()).isEqualTo(DataTypes.INT);
          assertThat(aggregate.getStateFuncSignature().getName().asInternal()).isEqualTo("plus");
          assertThat(aggregate.getStateFuncSignature().getParameterTypes())
              .containsExactly(DataTypes.INT, DataTypes.INT);
          assertThat(aggregate.getFinalFuncSignature()).isEmpty();
          assertThat(aggregate.getInitCond()).hasValue(0);
        },
        (listener, aggregate) -> verify(listener).onAggregateCreated(aggregate));
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
                .getKeyspace(adminSessionRule.keyspace())
                .flatMap(ks -> ks.getAggregate(CqlIdentifier.fromInternal("sum"), DataTypes.INT)),
        (listener, oldAggregate) -> verify(listener).onAggregateDropped(oldAggregate));
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
                .getKeyspace(adminSessionRule.keyspace())
                .flatMap(ks -> ks.getAggregate(CqlIdentifier.fromInternal("sum"), DataTypes.INT)),
        newAggregate -> assertThat(newAggregate.getInitCond()).hasValue(1),
        (listener, oldAggregate, newAggregate) ->
            verify(listener).onAggregateUpdated(newAggregate, oldAggregate));
  }

  private <T> void should_handle_creation(
      String beforeStatement,
      String createStatement,
      Function<Metadata, Optional<T>> extract,
      Consumer<T> verifyMetadata,
      BiConsumer<SchemaChangeListener, T> verifyListener,
      CqlIdentifier... keyspaces) {

    if (beforeStatement != null) {
      adminSessionRule.session().execute(beforeStatement);
    }

    SchemaChangeListener listener1 = mock(SchemaChangeListener.class);
    SchemaChangeListener listener2 = mock(SchemaChangeListener.class);

    // cluster1 executes the DDL query and gets a SCHEMA_CHANGE response.
    // cluster2 gets a SCHEMA_CHANGE push event on its control connection.

    List<String> keyspaceList = Lists.newArrayList();
    for (CqlIdentifier keyspace : keyspaces) {
      keyspaceList.add(keyspace.asInternal());
    }

    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
            .withStringList(DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES, keyspaceList)
            .build();

    try (CqlSession session1 =
            SessionUtils.newSession(
                ccmRule, adminSessionRule.keyspace(), null, listener1, null, loader);
        CqlSession session2 =
            SessionUtils.newSession(ccmRule, null, null, listener2, null, loader)) {

      session1.execute(createStatement);

      // Refreshes on a response are synchronous:
      T newElement1 = extract.apply(session1.getMetadata()).orElseThrow(AssertionError::new);
      verifyMetadata.accept(newElement1);
      verifyListener.accept(listener1, newElement1);

      // Refreshes on a server event are asynchronous:
      ConditionChecker.checkThat(
              () -> {
                T newElement2 =
                    extract.apply(session2.getMetadata()).orElseThrow(AssertionError::new);
                verifyMetadata.accept(newElement2);
                verifyListener.accept(listener2, newElement2);
              })
          .becomesTrue();
    }
  }

  private <T> void should_handle_drop(
      Iterable<String> beforeStatements,
      String dropStatement,
      Function<Metadata, Optional<T>> extract,
      BiConsumer<SchemaChangeListener, T> verifyListener,
      CqlIdentifier... keyspaces) {

    for (String statement : beforeStatements) {
      adminSessionRule.session().execute(statement);
    }

    SchemaChangeListener listener1 = mock(SchemaChangeListener.class);
    SchemaChangeListener listener2 = mock(SchemaChangeListener.class);

    List<String> keyspaceList = Lists.newArrayList();
    for (CqlIdentifier keyspace : keyspaces) {
      keyspaceList.add(keyspace.asInternal());
    }
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
            .withStringList(DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES, keyspaceList)
            .build();

    try (CqlSession session1 =
            SessionUtils.newSession(
                ccmRule, adminSessionRule.keyspace(), null, listener1, null, loader);
        CqlSession session2 =
            SessionUtils.newSession(ccmRule, null, null, listener2, null, loader)) {

      T oldElement = extract.apply(session1.getMetadata()).orElseThrow(AssertionError::new);
      assertThat(oldElement).isNotNull();

      session1.execute(dropStatement);

      assertThat(extract.apply(session1.getMetadata())).isEmpty();
      verifyListener.accept(listener1, oldElement);

      ConditionChecker.checkThat(
              () -> {
                assertThat(extract.apply(session2.getMetadata())).isEmpty();
                verifyListener.accept(listener2, oldElement);
              })
          .becomesTrue();
    }
  }

  private <T> void should_handle_update(
      Iterable<String> beforeStatements,
      String updateStatement,
      Function<Metadata, Optional<T>> extract,
      Consumer<T> verifyNewMetadata,
      TriConsumer<SchemaChangeListener, T, T> verifyListener,
      CqlIdentifier... keyspaces) {

    for (String statement : beforeStatements) {
      adminSessionRule.session().execute(statement);
    }

    SchemaChangeListener listener1 = mock(SchemaChangeListener.class);
    SchemaChangeListener listener2 = mock(SchemaChangeListener.class);
    List<String> keyspaceList = Lists.newArrayList();
    for (CqlIdentifier keyspace : keyspaces) {
      keyspaceList.add(keyspace.asInternal());
    }
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
            .withStringList(DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES, keyspaceList)
            .build();

    try (CqlSession session1 =
            SessionUtils.newSession(
                ccmRule, adminSessionRule.keyspace(), null, listener1, null, loader);
        CqlSession session2 =
            SessionUtils.newSession(ccmRule, null, null, listener2, null, loader)) {

      T oldElement = extract.apply(session1.getMetadata()).orElseThrow(AssertionError::new);
      assertThat(oldElement).isNotNull();

      session1.execute(updateStatement);

      T newElement = extract.apply(session1.getMetadata()).orElseThrow(AssertionError::new);
      verifyNewMetadata.accept(newElement);
      verifyListener.accept(listener1, oldElement, newElement);

      ConditionChecker.checkThat(
              () -> {
                verifyNewMetadata.accept(
                    extract.apply(session2.getMetadata()).orElseThrow(AssertionError::new));
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
      Function<Metadata, Optional<T>> extract,
      Consumer<T> verifyNewMetadata,
      TriConsumer<SchemaChangeListener, T, T> verifyListener,
      CqlIdentifier... keyspaces) {

    for (String statement : beforeStatements) {
      adminSessionRule.session().execute(statement);
    }

    SchemaChangeListener listener1 = mock(SchemaChangeListener.class);
    SchemaChangeListener listener2 = mock(SchemaChangeListener.class);
    List<String> keyspaceList = Lists.newArrayList();
    for (CqlIdentifier keyspace : keyspaces) {
      keyspaceList.add(keyspace.asInternal());
    }
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
            .withStringList(DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES, keyspaceList)
            .build();
    try (CqlSession session1 =
            SessionUtils.newSession(
                ccmRule, adminSessionRule.keyspace(), null, listener1, null, loader);
        CqlSession session2 =
            SessionUtils.newSession(ccmRule, null, null, listener2, null, loader)) {

      T oldElement = extract.apply(session1.getMetadata()).orElseThrow(AssertionError::new);
      assertThat(oldElement).isNotNull();

      session1.setSchemaMetadataEnabled(false);
      session2.setSchemaMetadataEnabled(false);

      session1.execute(dropStatement);
      session1.execute(recreateStatement);

      session1.setSchemaMetadataEnabled(true);
      session2.setSchemaMetadataEnabled(true);

      ConditionChecker.checkThat(
              () -> {
                T newElement =
                    extract.apply(session1.getMetadata()).orElseThrow(AssertionError::new);
                verifyNewMetadata.accept(newElement);
                verifyListener.accept(listener1, oldElement, newElement);
              })
          .becomesTrue();

      ConditionChecker.checkThat(
              () -> {
                T newElement =
                    extract.apply(session2.getMetadata()).orElseThrow(AssertionError::new);
                verifyNewMetadata.accept(newElement);
                verifyListener.accept(listener2, oldElement, newElement);
              })
          .becomesTrue();
    }
  }

  interface TriConsumer<T, U, V> {
    void accept(T t, U u, V v);
  }
}
