package com.datastax.oss.driver.core.cql;

import com.codahale.metrics.Gauge;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
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
import com.datastax.oss.driver.shaded.guava.common.cache.CacheBuilder;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

@Category(ParallelizableTests.class)
public class PreparedStatementCachingIT {

    private CcmRule ccmRule = CcmRule.getInstance();

    private SessionRule<CqlSession> sessionRule =
            SessionRule.builder(ccmRule)
                    .withConfigLoader(
                            SessionUtils.configLoaderBuilder()
                                    .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 2)
                                    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
                                    .build())
                    .build();

    @Rule
    public TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

    private static class TestCqlPrepareAsyncProcessor extends CqlPrepareAsyncProcessor {

        public TestCqlPrepareAsyncProcessor(@NonNull Optional<? extends DefaultDriverContext> context) {
            super(CacheBuilder.newBuilder().build(), context);
        }
    }


    private static class TestDefaultDriverContext extends DefaultDriverContext {

        public TestDefaultDriverContext(DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
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
        System.setProperty(SessionUtils.SESSION_BUILDER_CLASS_PROPERTY, PreparedStatementCachingIT.class.getName());
    }

    @AfterClass
    public static void teardown() {
        System.clearProperty(SessionUtils.SESSION_BUILDER_CLASS_PROPERTY);
    }

    public static SessionBuilder builder() { return new TestSessionBuilder(); }

    private void invalidationResultSetTest(Consumer<CqlSession> createFn) {

        try (CqlSession session = sessionWithCacheSizeMetric()) {

            assertThat(getPreparedCacheSize(session)).isEqualTo(0);
            createFn.accept(session);

            session.prepare("select f from test_table_1 where e = ?");
            session.prepare("select h from test_table_2 where g = ?");
            assertThat(getPreparedCacheSize(session)).isEqualTo(2);

            CountDownLatch latch = new CountDownLatch(1);
            DefaultDriverContext ctx = (DefaultDriverContext)session.getContext();
            ctx.getEventBus()
                    .register(
                            TypeChangeEvent.class,
                            (e) -> {
                                assertThat(e.oldType.getName().toString()).isEqualTo("test_type_2");
                                latch.countDown();
                            });

            session.execute("ALTER TYPE test_type_2 add i blob");
            Uninterruptibles.awaitUninterruptibly(latch, 2, TimeUnit.SECONDS);

            //Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);

            assertThat(getPreparedCacheSize(session)).isEqualTo(1);
        }
    }

    private void invalidationVariableDefsTest(Consumer<CqlSession> createFn, boolean isCollection) {

        try (CqlSession session = sessionWithCacheSizeMetric()) {

            assertThat(getPreparedCacheSize(session)).isEqualTo(0);
            createFn.accept(session);

            String fStr = isCollection ? "f contains ?" : "f = ?";
            session.prepare(String.format("select e from test_table_1 where %s allow filtering", fStr));
            String hStr = isCollection ? "h contains ?" : "h = ?";
            session.prepare(String.format("select g from test_table_2 where %s allow filtering", hStr));
            assertThat(getPreparedCacheSize(session)).isEqualTo(2);

            CountDownLatch latch = new CountDownLatch(1);
            DefaultDriverContext ctx = (DefaultDriverContext) session.getContext();
            ctx.getEventBus()
                    .register(
                            TypeChangeEvent.class,
                            (e) -> {
                                assertThat(e.oldType.getName().toString()).isEqualTo("test_type_2");
                                latch.countDown();
                            });

            session.execute("ALTER TYPE test_type_2 add i blob");
            Uninterruptibles.awaitUninterruptibly(latch, 2, TimeUnit.SECONDS);

            //Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);

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
