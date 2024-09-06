/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.core.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.Gauge;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareAsyncProcessor;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareSyncProcessor;
import com.datastax.oss.driver.internal.core.metadata.schema.events.TypeChangeEvent;
import com.datastax.oss.driver.internal.core.session.BuiltInRequestProcessors;
import com.datastax.oss.driver.internal.core.session.RequestProcessor;
import com.datastax.oss.driver.internal.core.session.RequestProcessorRegistry;
import com.datastax.oss.driver.shaded.guava.common.cache.CacheBuilder;
import com.datastax.oss.driver.shaded.guava.common.cache.RemovalListener;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// These tests must be isolated because setup modifies SessionUtils.SESSION_BUILDER_CLASS_PROPERTY
@Category(IsolatedTests.class)
public class PreparedStatementCachingIT {

  private static final CustomCcmRule ccmRule = CustomCcmRule.builder().build();

  private static final SessionRule<CqlSession> sessionRule =
      SessionRule.builder(ccmRule)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 2)
                  .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
                  .build())
          .build();

  @Rule public TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  private static class PreparedStatementRemovalEvent {

    private final ByteBuffer queryId;

    public PreparedStatementRemovalEvent(ByteBuffer queryId) {
      this.queryId = queryId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || !(o instanceof PreparedStatementRemovalEvent)) return false;
      PreparedStatementRemovalEvent that = (PreparedStatementRemovalEvent) o;
      return Objects.equals(queryId, that.queryId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(queryId);
    }

    @Override
    public String toString() {
      return "PreparedStatementRemovalEvent{" + "queryId=" + queryId + '}';
    }
  }

  private static class TestCqlPrepareAsyncProcessor extends CqlPrepareAsyncProcessor {

    private static final Logger LOG =
        LoggerFactory.getLogger(PreparedStatementCachingIT.TestCqlPrepareAsyncProcessor.class);

    private static RemovalListener<PrepareRequest, CompletableFuture<PreparedStatement>>
        buildCacheRemoveCallback(@NonNull Optional<DefaultDriverContext> context) {
      return (evt) -> {
        try {
          CompletableFuture<PreparedStatement> future = evt.getValue();
          ByteBuffer queryId = Uninterruptibles.getUninterruptibly(future).getId();
          context.ifPresent(
              ctx -> ctx.getEventBus().fire(new PreparedStatementRemovalEvent(queryId)));
        } catch (Exception e) {
          LOG.error("Unable to register removal handler", e);
        }
      };
    }

    public TestCqlPrepareAsyncProcessor(@NonNull Optional<DefaultDriverContext> context) {
      // Default CqlPrepareAsyncProcessor uses weak values here as well.  We avoid doing so
      // to prevent cache entries from unexpectedly disappearing mid-test.
      super(
          CacheBuilder.newBuilder().removalListener(buildCacheRemoveCallback(context)).build(),
          context);
    }
  }

  private static class TestDefaultDriverContext extends DefaultDriverContext {
    public TestDefaultDriverContext(
        DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
      super(configLoader, programmaticArguments);
    }

    @Override
    protected RequestProcessorRegistry buildRequestProcessorRegistry() {
      // Re-create the processor cache to insert the TestCqlPrepareAsyncProcessor with it's strong
      // prepared statement cache, see JAVA-3062
      List<RequestProcessor<?, ?>> processors =
          BuiltInRequestProcessors.createDefaultProcessors(this);
      processors.removeIf((processor) -> processor instanceof CqlPrepareAsyncProcessor);
      processors.removeIf((processor) -> processor instanceof CqlPrepareSyncProcessor);
      CqlPrepareAsyncProcessor asyncProcessor = new TestCqlPrepareAsyncProcessor(Optional.of(this));
      processors.add(2, asyncProcessor);
      processors.add(3, new CqlPrepareSyncProcessor(asyncProcessor));
      return new RequestProcessorRegistry(
          getSessionName(), processors.toArray(new RequestProcessor[0]));
    }
  }

  private static class TestSessionBuilder extends SessionBuilder {

    @Override
    protected Object wrap(@NonNull CqlSession defaultSession) {
      return defaultSession;
    }

    @Override
    protected DriverContext buildContext(
        DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
      return new TestDefaultDriverContext(configLoader, programmaticArguments);
    }
  }

  @BeforeClass
  public static void setup() {
    System.setProperty(
        SessionUtils.SESSION_BUILDER_CLASS_PROPERTY, PreparedStatementCachingIT.class.getName());
  }

  @AfterClass
  public static void teardown() {
    System.clearProperty(SessionUtils.SESSION_BUILDER_CLASS_PROPERTY);
  }

  public static SessionBuilder builder() {
    return new TestSessionBuilder();
  }

  private void invalidationResultSetTest(
      Consumer<CqlSession> setupTestSchema, Set<String> expectedChangedTypes) {
    invalidationTestInner(
        setupTestSchema,
        "select f from test_table_1 where e = ?",
        "select h from test_table_2 where g = ?",
        expectedChangedTypes);
  }

  private void invalidationVariableDefsTest(
      Consumer<CqlSession> setupTestSchema,
      boolean isCollection,
      Set<String> expectedChangedTypes) {
    String condition = isCollection ? "contains ?" : "= ?";
    invalidationTestInner(
        setupTestSchema,
        String.format("select e from test_table_1 where f %s allow filtering", condition),
        String.format("select g from test_table_2 where h %s allow filtering", condition),
        expectedChangedTypes);
  }

  private void invalidationTestInner(
      Consumer<CqlSession> setupTestSchema,
      String preparedStmtQueryType1,
      String preparedStmtQueryType2,
      Set<String> expectedChangedTypes) {

    try (CqlSession session = sessionWithCacheSizeMetric()) {

      assertThat(getPreparedCacheSize(session)).isEqualTo(0);
      setupTestSchema.accept(session);

      session.prepare(preparedStmtQueryType1);
      ByteBuffer queryId2 = session.prepare(preparedStmtQueryType2).getId();
      assertThat(getPreparedCacheSize(session)).isEqualTo(2);

      CountDownLatch preparedStmtCacheRemoveLatch = new CountDownLatch(1);
      CountDownLatch typeChangeEventLatch = new CountDownLatch(expectedChangedTypes.size());

      DefaultDriverContext ctx = (DefaultDriverContext) session.getContext();
      Map<String, Boolean> changedTypes = new ConcurrentHashMap<>();
      AtomicReference<Optional<ByteBuffer>> removedQueryIds =
          new AtomicReference<>(Optional.empty());
      AtomicReference<Optional<String>> typeChangeEventError =
          new AtomicReference<>(Optional.empty());
      AtomicReference<Optional<String>> removedQueryEventError =
          new AtomicReference<>(Optional.empty());
      ctx.getEventBus()
          .register(
              TypeChangeEvent.class,
              (e) -> {
                // expect one event per type changed and for every parent type that nests it
                if (Boolean.TRUE.equals(
                    changedTypes.putIfAbsent(e.oldType.getName().toString(), true))) {
                  // store an error if we see duplicate change event
                  // any non-empty error will fail the test so it's OK to do this multiple times
                  typeChangeEventError.set(Optional.of("Duplicate type change event " + e));
                }
                typeChangeEventLatch.countDown();
              });
      ctx.getEventBus()
          .register(
              PreparedStatementRemovalEvent.class,
              (e) -> {
                if (!removedQueryIds.compareAndSet(Optional.empty(), Optional.of(e.queryId))) {
                  // store an error if we see multiple cache invalidation events
                  // any non-empty error will fail the test so it's OK to do this multiple times
                  removedQueryEventError.set(
                      Optional.of("Unable to set reference for PS removal event"));
                }
                preparedStmtCacheRemoveLatch.countDown();
              });

      // alter test_type_2 to trigger cache invalidation and above events
      session.execute("ALTER TYPE test_type_2 add i blob");

      // wait for latches and fail if they don't reach zero before timeout
      assertThat(
              Uninterruptibles.awaitUninterruptibly(
                  preparedStmtCacheRemoveLatch, 10, TimeUnit.SECONDS))
          .withFailMessage("preparedStmtCacheRemoveLatch did not trigger before timeout")
          .isTrue();
      assertThat(Uninterruptibles.awaitUninterruptibly(typeChangeEventLatch, 10, TimeUnit.SECONDS))
          .withFailMessage("typeChangeEventLatch did not trigger before timeout")
          .isTrue();

      /* Okay, the latch triggered so cache processing should now be done.  Let's validate :allthethings: */
      assertThat(changedTypes.keySet()).isEqualTo(expectedChangedTypes);
      assertThat(removedQueryIds.get()).isNotEmpty().get().isEqualTo(queryId2);
      assertThat(getPreparedCacheSize(session)).isEqualTo(1);

      // check no errors were seen in callback (and report those as fail msgs)
      // if something is broken these may still succeed due to timing
      // but shouldn't intermittently fail if the code is working properly
      assertThat(typeChangeEventError.get())
          .withFailMessage(() -> typeChangeEventError.get().get())
          .isEmpty();
      assertThat(removedQueryEventError.get())
          .withFailMessage(() -> removedQueryEventError.get().get())
          .isEmpty();
    }
  }

  Consumer<CqlSession> setupCacheEntryTestBasic =
      (session) -> {
        session.execute("CREATE TYPE test_type_1 (a text, b int)");
        session.execute("CREATE TYPE test_type_2 (c int, d text)");
        session.execute("CREATE TABLE test_table_1 (e int primary key, f frozen<test_type_1>)");
        session.execute("CREATE TABLE test_table_2 (g int primary key, h frozen<test_type_2>)");
      };

  @Test
  public void should_invalidate_cache_entry_on_basic_udt_change_result_set() {
    invalidationResultSetTest(setupCacheEntryTestBasic, ImmutableSet.of("test_type_2"));
  }

  @Test
  public void should_invalidate_cache_entry_on_basic_udt_change_variable_defs() {
    invalidationVariableDefsTest(setupCacheEntryTestBasic, false, ImmutableSet.of("test_type_2"));
  }

  Consumer<CqlSession> setupCacheEntryTestCollection =
      (session) -> {
        session.execute("CREATE TYPE test_type_1 (a text, b int)");
        session.execute("CREATE TYPE test_type_2 (c int, d text)");
        session.execute(
            "CREATE TABLE test_table_1 (e int primary key, f list<frozen<test_type_1>>)");
        session.execute(
            "CREATE TABLE test_table_2 (g int primary key, h list<frozen<test_type_2>>)");
      };

  @Test
  public void should_invalidate_cache_entry_on_collection_udt_change_result_set() {
    invalidationResultSetTest(setupCacheEntryTestCollection, ImmutableSet.of("test_type_2"));
  }

  @Test
  public void should_invalidate_cache_entry_on_collection_udt_change_variable_defs() {
    invalidationVariableDefsTest(
        setupCacheEntryTestCollection, true, ImmutableSet.of("test_type_2"));
  }

  Consumer<CqlSession> setupCacheEntryTestTuple =
      (session) -> {
        session.execute("CREATE TYPE test_type_1 (a text, b int)");
        session.execute("CREATE TYPE test_type_2 (c int, d text)");
        session.execute(
            "CREATE TABLE test_table_1 (e int primary key, f tuple<int, test_type_1, text>)");
        session.execute(
            "CREATE TABLE test_table_2 (g int primary key, h tuple<text, test_type_2, int>)");
      };

  @Test
  public void should_invalidate_cache_entry_on_tuple_udt_change_result_set() {
    invalidationResultSetTest(setupCacheEntryTestTuple, ImmutableSet.of("test_type_2"));
  }

  @Test
  public void should_invalidate_cache_entry_on_tuple_udt_change_variable_defs() {
    invalidationVariableDefsTest(setupCacheEntryTestTuple, false, ImmutableSet.of("test_type_2"));
  }

  Consumer<CqlSession> setupCacheEntryTestNested =
      (session) -> {
        session.execute("CREATE TYPE test_type_1 (a text, b int)");
        session.execute("CREATE TYPE test_type_2 (c int, d text)");
        session.execute("CREATE TYPE test_type_3 (e frozen<test_type_1>, f int)");
        session.execute("CREATE TYPE test_type_4 (g int, h frozen<test_type_2>)");
        session.execute("CREATE TABLE test_table_1 (e int primary key, f frozen<test_type_3>)");
        session.execute("CREATE TABLE test_table_2 (g int primary key, h frozen<test_type_4>)");
      };

  @Test
  public void should_invalidate_cache_entry_on_nested_udt_change_result_set() {
    invalidationResultSetTest(
        setupCacheEntryTestNested, ImmutableSet.of("test_type_2", "test_type_4"));
  }

  @Test
  public void should_invalidate_cache_entry_on_nested_udt_change_variable_defs() {
    invalidationVariableDefsTest(
        setupCacheEntryTestNested, false, ImmutableSet.of("test_type_2", "test_type_4"));
  }

  /* ========================= Infrastructure copied from PreparedStatementIT ========================= */
  private CqlSession sessionWithCacheSizeMetric() {
    return SessionUtils.newSession(
        ccmRule,
        sessionRule.keyspace(),
        SessionUtils.configLoaderBuilder()
            .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 2)
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
            .withStringList(
                DefaultDriverOption.METRICS_SESSION_ENABLED,
                ImmutableList.of(DefaultSessionMetric.CQL_PREPARED_CACHE_SIZE.getPath()))
            .build());
  }

  @SuppressWarnings("unchecked")
  private static long getPreparedCacheSize(CqlSession session) {
    return session
        .getMetrics()
        .flatMap(metrics -> metrics.getSessionMetric(DefaultSessionMetric.CQL_PREPARED_CACHE_SIZE))
        .map(metric -> ((Gauge<Long>) metric).getValue())
        .orElseThrow(
            () ->
                new AssertionError(
                    "Could not access metric "
                        + DefaultSessionMetric.CQL_PREPARED_CACHE_SIZE.getPath()));
  }
}
