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
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareAsyncProcessor;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareSyncProcessor;
import com.datastax.oss.driver.internal.core.metadata.schema.events.TypeChangeEvent;
import com.datastax.oss.driver.internal.core.session.BuiltInRequestProcessors;
import com.datastax.oss.driver.internal.core.session.RequestProcessor;
import com.datastax.oss.driver.internal.core.session.RequestProcessorRegistry;
import com.datastax.oss.driver.shaded.guava.common.cache.Cache;
import com.datastax.oss.driver.shaded.guava.common.cache.CacheBuilder;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(ParallelizableTests.class)
public class PreparedStatementCachingIT {

  private static final Logger LOG = LoggerFactory.getLogger(PreparedStatementCachingIT.class);

  private CcmRule ccmRule = CcmRule.getInstance();

  private SessionRule<CqlSession> sessionRule =
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
      if (o == null || getClass() != o.getClass()) return false;
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

    private static Function<
            Optional<? extends DefaultDriverContext>,
            Cache<PrepareRequest, CompletableFuture<PreparedStatement>>>
        buildCache =
            (contextOption) -> {

              // Default CqlPrepareAsyncProcessor uses weak values here as well.  We avoid doing so
              // to prevent cache
              // entries from unexpectedly disappearing mid-test.
              CacheBuilder builder = CacheBuilder.newBuilder();
              contextOption.ifPresent(
                  (ctx) -> {
                    builder.removalListener(
                        (evt) -> {
                          try {

                            CompletableFuture<PreparedStatement> future =
                                (CompletableFuture<PreparedStatement>) evt.getValue();
                            ByteBuffer queryId =
                                Uninterruptibles.getUninterruptibly(future).getId();
                            ctx.getEventBus().fire(new PreparedStatementRemovalEvent(queryId));
                          } catch (Exception e) {
                            LOG.error("Unable to register removal handler", e);
                          }
                        });
                  });
              return builder.build();
            };

    public TestCqlPrepareAsyncProcessor(@NonNull Optional<? extends DefaultDriverContext> context) {
      super(TestCqlPrepareAsyncProcessor.buildCache.apply(context), context);
    }
  }

  private static class TestDefaultDriverContext extends DefaultDriverContext {

    public TestDefaultDriverContext(
        DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
      super(configLoader, programmaticArguments);
    }

    @Override
    protected RequestProcessorRegistry buildRequestProcessorRegistry() {
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

    private final Logger LOG = LoggerFactory.getLogger(TestSessionBuilder.class);

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

  private void invalidationResultSetTest(Consumer<CqlSession> createFn) {

    try (CqlSession session = sessionWithCacheSizeMetric()) {

      assertThat(getPreparedCacheSize(session)).isEqualTo(0);
      createFn.accept(session);

      session.prepare("select f from test_table_1 where e = ?");
      ByteBuffer queryId2 = session.prepare("select h from test_table_2 where g = ?").getId();
      assertThat(getPreparedCacheSize(session)).isEqualTo(2);

      CountDownLatch latch = new CountDownLatch(1);
      DefaultDriverContext ctx = (DefaultDriverContext) session.getContext();
      AtomicReference<Optional<String>> changeRef = new AtomicReference<>(Optional.empty());
      AtomicReference<Optional<ByteBuffer>> idRef = new AtomicReference<>(Optional.empty());
      ctx.getEventBus()
          .register(
              TypeChangeEvent.class,
              (e) -> {
                if (!changeRef.compareAndSet(
                    Optional.empty(), Optional.of(e.oldType.getName().toString())))
                  // TODO: Note that we actually do see this error for tests around nested UDTs.
                  // What's happening is that
                  // we see an event for the changed type itself and then we get an event for the
                  // nesting type that contains
                  // it.  Upshot is that this logic is dependent on the order of event delivery; as
                  // long as the event for the
                  // type itself is first this test will behave as expected.
                  //
                  // We probably want something more robust here.
                  LOG.error("Unable to set reference for type change event " + e);
              });
      ctx.getEventBus()
          .register(
              PreparedStatementRemovalEvent.class,
              (e) -> {
                if (!idRef.compareAndSet(Optional.empty(), Optional.of(e.queryId)))
                  LOG.error("Unable to set reference for PS removal event");
                latch.countDown();
              });

      session.execute("ALTER TYPE test_type_2 add i blob");
      Uninterruptibles.awaitUninterruptibly(latch, 2, TimeUnit.SECONDS);

      /* Okay, the latch triggered so cache processing should now be done.  Let's validate :allthethings: */
      assertThat(changeRef.get()).isNotEmpty();
      assertThat(changeRef.get().get()).isEqualTo("test_type_2");
      assertThat(idRef.get()).isNotEmpty();
      assertThat(idRef.get().get()).isEqualTo(queryId2);
      assertThat(getPreparedCacheSize(session)).isEqualTo(1);
    }
  }

  private void invalidationVariableDefsTest(Consumer<CqlSession> createFn, boolean isCollection) {

    /* TODO: There's a lot more infrastructure in this test now, which means we're duplicating a lot of setup with
     *   the ResultSet test above.  Probably worth while to see if we can merge the two. */
    try (CqlSession session = sessionWithCacheSizeMetric()) {

      assertThat(getPreparedCacheSize(session)).isEqualTo(0);
      createFn.accept(session);

      String fStr = isCollection ? "f contains ?" : "f = ?";
      session.prepare(String.format("select e from test_table_1 where %s allow filtering", fStr));
      String hStr = isCollection ? "h contains ?" : "h = ?";
      ByteBuffer queryId2 =
          session
              .prepare(String.format("select g from test_table_2 where %s allow filtering", hStr))
              .getId();
      assertThat(getPreparedCacheSize(session)).isEqualTo(2);

      CountDownLatch latch = new CountDownLatch(1);
      DefaultDriverContext ctx = (DefaultDriverContext) session.getContext();
      AtomicReference<Optional<String>> changeRef = new AtomicReference<>(Optional.empty());
      AtomicReference<Optional<ByteBuffer>> idRef = new AtomicReference<>(Optional.empty());
      ctx.getEventBus()
          .register(
              TypeChangeEvent.class,
              (e) -> {
                if (!changeRef.compareAndSet(
                    Optional.empty(), Optional.of(e.oldType.getName().toString())))
                  // TODO: Note that we actually do see this error for tests around nested UDTs.
                  // What's happening is that
                  // we see an event for the changed type itself and then we get an event for the
                  // nesting type that contains
                  // it.  Upshot is that this logic is dependent on the order of event delivery; as
                  // long as the event for the
                  // type itself is first this test will behave as expected.
                  //
                  // We probably want something more robust here.
                  LOG.error("Unable to set reference for type change event " + e);
              });
      ctx.getEventBus()
          .register(
              PreparedStatementRemovalEvent.class,
              (e) -> {
                if (!idRef.compareAndSet(Optional.empty(), Optional.of(e.queryId)))
                  LOG.error("Unable to set reference for PS removal event");
                latch.countDown();
              });

      session.execute("ALTER TYPE test_type_2 add i blob");
      Uninterruptibles.awaitUninterruptibly(latch, 2, TimeUnit.SECONDS);

      /* Okay, the latch triggered so cache processing should now be done.  Let's validate :allthethings: */
      assertThat(changeRef.get()).isNotEmpty();
      assertThat(changeRef.get().get()).isEqualTo("test_type_2");
      assertThat(idRef.get()).isNotEmpty();
      assertThat(idRef.get().get()).isEqualTo(queryId2);
      assertThat(getPreparedCacheSize(session)).isEqualTo(1);
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
    invalidationResultSetTest(setupCacheEntryTestBasic);
  }

  @Test
  public void should_invalidate_cache_entry_on_basic_udt_change_variable_defs() {
    invalidationVariableDefsTest(setupCacheEntryTestBasic, false);
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
    invalidationResultSetTest(setupCacheEntryTestCollection);
  }

  @Test
  public void should_invalidate_cache_entry_on_collection_udt_change_variable_defs() {
    invalidationVariableDefsTest(setupCacheEntryTestCollection, true);
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
    invalidationResultSetTest(setupCacheEntryTestTuple);
  }

  @Test
  public void should_invalidate_cache_entry_on_tuple_udt_change_variable_defs() {
    invalidationVariableDefsTest(setupCacheEntryTestTuple, false);
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
    invalidationResultSetTest(setupCacheEntryTestNested);
  }

  @Test
  public void should_invalidate_cache_entry_on_nested_udt_change_variable_defs() {
    invalidationVariableDefsTest(setupCacheEntryTestNested, false);
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
