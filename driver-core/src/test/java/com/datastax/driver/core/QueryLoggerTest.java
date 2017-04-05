/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.core;

import com.datastax.driver.core.StatementWrapperTest.CustomStatement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static com.datastax.driver.core.BatchStatement.Type.COUNTER;
import static com.datastax.driver.core.BatchStatement.Type.UNLOGGED;
import static com.datastax.driver.core.QueryLogger.*;
import static com.datastax.driver.core.TestUtils.getFixedValue;
import static com.datastax.driver.core.TestUtils.ipOfNode;
import static org.apache.log4j.Level.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Main tests for {@link QueryLogger} using {@link com.datastax.driver.core.CCMBridge}.
 * More tests, specifically targeting slow and unsuccessful queries, can be found in
 * {@link QueryLoggerErrorsTest}.
 */
public class QueryLoggerTest extends CCMTestsSupport {

    private List<DataType> dataTypes;
    private List<Object> values;
    private String assignments;

    private Logger normal = Logger.getLogger(NORMAL_LOGGER.getName());
    private Logger slow = Logger.getLogger(SLOW_LOGGER.getName());
    private Logger error = Logger.getLogger(ERROR_LOGGER.getName());

    private MemoryAppender normalAppender;
    private MemoryAppender slowAppender;
    private MemoryAppender errorAppender;

    private QueryLogger queryLogger;
    private Level originalNormal;
    private Level originalSlow;
    private Level originalError;

    @Override
    public void onTestContextInitialized() {
        dataTypes = new ArrayList<DataType>(
                Sets.filter(TestUtils.allPrimitiveTypes(ccm().getProtocolVersion()), new Predicate<DataType>() {
                    @Override
                    public boolean apply(DataType type) {
                        return type != DataType.counter();
                    }
                }));

        values = Lists.transform(dataTypes, new Function<DataType, Object>() {
                    @Override
                    public Object apply(DataType type) {
                        return getFixedValue(type);
                    }
                }
        );

        String definitions = Joiner.on(", ").join(
                Lists.transform(dataTypes, new Function<DataType, String>() {
                            @Override
                            public String apply(DataType type) {
                                return "c_" + type + " " + type;
                            }
                        }
                )
        );

        assignments = Joiner.on(", ").join(
                Lists.transform(dataTypes, new Function<DataType, String>() {
                            @Override
                            public String apply(DataType type) {
                                return "c_" + type + " = ?";
                            }
                        }
                )
        );

        execute("CREATE TABLE test (pk int PRIMARY KEY, " + definitions + ")");
    }

    @BeforeMethod(groups = {"short", "unit"})
    public void startCapturingLogs() {
        originalNormal = normal.getLevel();
        originalSlow = slow.getLevel();
        originalError = error.getLevel();
        normal.setLevel(INFO);
        slow.setLevel(INFO);
        error.setLevel(INFO);
        normal.addAppender(normalAppender = new MemoryAppender());
        slow.addAppender(slowAppender = new MemoryAppender());
        error.addAppender(errorAppender = new MemoryAppender());
    }

    @AfterMethod(groups = {"short", "unit"}, alwaysRun = true)
    public void stopCapturingLogs() {
        normal.setLevel(originalNormal);
        slow.setLevel(originalSlow);
        error.setLevel(originalError);
        normal.removeAppender(normalAppender);
        slow.removeAppender(slowAppender);
        error.removeAppender(errorAppender);
    }

    @AfterMethod(groups = {"short", "unit"}, alwaysRun = true)
    public void unregisterQueryLogger() {
        if (cluster() != null && queryLogger != null) {
            cluster().unregister(queryLogger);
        }
    }

    // Tests for different types of statements (Regular, Bound, Batch)

    @Test(groups = "short")
    public void should_log_regular_statements() throws Exception {
        // given
        normal.setLevel(DEBUG);
        queryLogger = QueryLogger.builder()
                .withConstantThreshold(Long.MAX_VALUE)
                .build();
        cluster().register(queryLogger);
        String query = "SELECT c_text FROM test WHERE pk = 42";
        session().execute(query);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains(query)
                .doesNotContain("parameters");
    }

    @Test(groups = "short")
    public void should_log_bound_statements() throws Exception {
        // given
        normal.setLevel(DEBUG);
        queryLogger = QueryLogger.builder()
                .withConstantThreshold(Long.MAX_VALUE)
                .build();
        cluster().register(queryLogger);
        String query = "SELECT * FROM test where pk = ?";
        PreparedStatement ps = session().prepare(query);
        BoundStatement bs = ps.bind(42);
        session().execute(bs);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains(query)
                .doesNotContain("actual parameters");
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_log_batch_statements() throws Exception {
        // given
        normal.setLevel(DEBUG);
        queryLogger = QueryLogger.builder()
                .withConstantThreshold(Long.MAX_VALUE)
                .withMaxQueryStringLength(Integer.MAX_VALUE)
                .build();
        cluster().register(queryLogger);
        // when
        String query1 = "INSERT INTO test (pk) VALUES (?)";
        String query2 = "UPDATE test SET c_int = ? WHERE pk = 42";
        PreparedStatement ps1 = session().prepare(query1);
        PreparedStatement ps2 = session().prepare(query2);
        BatchStatement batch = new BatchStatement();
        batch.add(ps1.bind(42));
        batch.add(ps2.bind(1234));
        session().execute(batch);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains("BEGIN BATCH")
                .contains("APPLY BATCH")
                .contains(query1)
                .contains(query2)
                .doesNotContain("c_int:");
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_log_unlogged_batch_statements() throws Exception {
        // given
        normal.setLevel(DEBUG);
        queryLogger = QueryLogger.builder()
                .withConstantThreshold(Long.MAX_VALUE)
                .withMaxQueryStringLength(Integer.MAX_VALUE)
                .build();
        cluster().register(queryLogger);
        // when
        String query1 = "INSERT INTO test (pk) VALUES (?)";
        String query2 = "UPDATE test SET c_int = ? WHERE pk = 42";
        PreparedStatement ps1 = session().prepare(query1);
        PreparedStatement ps2 = session().prepare(query2);
        BatchStatement batch = new BatchStatement(UNLOGGED);
        batch.add(ps1.bind(42));
        batch.add(ps2.bind(1234));
        session().execute(batch);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains("BEGIN UNLOGGED BATCH")
                .contains("APPLY BATCH")
                .contains(query1)
                .contains(query2)
                .doesNotContain("c_int:");
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_log_counter_batch_statements() throws Exception {
        // Create a special table for testing with counters.
        session().execute(
                "CREATE TABLE IF NOT EXISTS counter_test (pk int PRIMARY KEY, c_count COUNTER, c_count2 COUNTER)");

        // given
        normal.setLevel(DEBUG);
        queryLogger = QueryLogger.builder()
                .withConstantThreshold(Long.MAX_VALUE)
                .withMaxQueryStringLength(Integer.MAX_VALUE)
                .build();
        cluster().register(queryLogger);
        // when
        String query1 = "UPDATE counter_test SET c_count = c_count + ? WHERE pk = 42";
        String query2 = "UPDATE counter_test SET c_count2 = c_count2 + ? WHERE pk = 42";
        PreparedStatement ps1 = session().prepare(query1);
        PreparedStatement ps2 = session().prepare(query2);
        BatchStatement batch = new BatchStatement(COUNTER);
        batch.add(ps1.bind(1234L));
        batch.add(ps2.bind(5678L));
        session().execute(batch);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains("BEGIN COUNTER BATCH")
                .contains("APPLY BATCH")
                .contains(query1)
                .contains(query2)
                .doesNotContain("c_count:");
    }

    @Test(groups = "unit")
    public void should_log_unknown_statements() throws Exception {
        // given
        normal.setLevel(DEBUG);
        Statement unknownStatement = new Statement() {
            @Override
            public ByteBuffer getRoutingKey(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
                return null;
            }

            @Override
            public String getKeyspace() {
                return null;
            }

            @Override
            public String toString() {
                return "weird statement";
            }
        };
        // when
        queryLogger = QueryLogger.builder().build();
        queryLogger.onRegister(mock(Cluster.class));
        queryLogger.update(null, unknownStatement, null, 0);
        // then
        String line = normalAppender.get();
        assertThat(line).contains("weird statement");
    }

    // Tests for different log levels

    @Test(groups = "unit")
    public void should_not_log_normal_if_level_higher_than_DEBUG() throws Exception {
        // given
        normal.setLevel(INFO);
        slow.setLevel(INFO);
        error.setLevel(INFO);
        // when
        queryLogger = QueryLogger.builder().build();
        queryLogger.onRegister(mock(Cluster.class));
        queryLogger.update(null, mock(BoundStatement.class), null, 0);
        // then
        assertThat(normalAppender.get()).isEmpty();
        assertThat(slowAppender.get()).isEmpty();
        assertThat(errorAppender.get()).isEmpty();
    }

    @Test(groups = "unit")
    public void should_not_log_slow_if_level_higher_than_DEBUG() throws Exception {
        // given
        normal.setLevel(INFO);
        slow.setLevel(INFO);
        error.setLevel(INFO);
        // when
        queryLogger = QueryLogger.builder().build();
        queryLogger.onRegister(mock(Cluster.class));
        queryLogger.update(null, mock(BoundStatement.class), null, DEFAULT_SLOW_QUERY_THRESHOLD_MS + 1);
        // then
        assertThat(normalAppender.get()).isEmpty();
        assertThat(slowAppender.get()).isEmpty();
        assertThat(errorAppender.get()).isEmpty();
    }

    @Test(groups = "unit")
    public void should_not_log_error_if_level_higher_than_DEBUG() throws Exception {
        // given
        normal.setLevel(INFO);
        slow.setLevel(INFO);
        error.setLevel(INFO);
        // when
        queryLogger = QueryLogger.builder().build();
        queryLogger.onRegister(mock(Cluster.class));
        queryLogger.update(null, mock(BoundStatement.class), new DriverException("booh"), 0);
        // then
        assertThat(normalAppender.get()).isEmpty();
        assertThat(slowAppender.get()).isEmpty();
        assertThat(errorAppender.get()).isEmpty();
    }

    // Tests for different query types (normal, slow, exception)

    @Test(groups = "short")
    public void should_log_normal_queries() throws Exception {
        // given
        normal.setLevel(DEBUG);
        queryLogger = QueryLogger.builder()
                .withConstantThreshold(Long.MAX_VALUE)
                .withMaxQueryStringLength(Integer.MAX_VALUE)
                .build();
        cluster().register(queryLogger);
        // when
        String query = "SELECT * FROM test where pk = ?";
        PreparedStatement ps = session().prepare(query);
        BoundStatement bs = ps.bind(42);
        session().execute(bs);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains(query)
                .doesNotContain("pk:42");
    }

    // Tests for slow and error queries are in QueryLoggerErrorsTest

    // Tests with query parameters (log level TRACE)

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_log_non_null_named_parameter_bound_statements() throws Exception {
        // given
        normal.setLevel(TRACE);
        queryLogger = QueryLogger.builder()
                .withConstantThreshold(Long.MAX_VALUE)
                .withMaxQueryStringLength(Integer.MAX_VALUE)
                .build();
        cluster().register(queryLogger);
        // when
        String query = "UPDATE test SET c_text = :param1 WHERE pk = :param2";
        PreparedStatement ps = session().prepare(query);
        BoundStatement bs = ps.bind();
        bs.setString("param1", "foo");
        bs.setInt("param2", 42);
        session().execute(bs);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains(query)
                .contains("param2:42")
                .contains("param1:'foo'");
    }

    @Test(groups = "short")
    public void should_log_non_null_positional_parameter_bound_statements() throws Exception {
        // given
        normal.setLevel(TRACE);
        queryLogger = QueryLogger.builder().build();
        cluster().register(queryLogger);
        // when
        String query = "UPDATE test SET c_text = ? WHERE pk = ?";
        PreparedStatement ps = session().prepare(query);
        BoundStatement bs = ps.bind();
        bs.setString("c_text", "foo");
        bs.setInt("pk", 42);
        session().execute(bs);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains(query)
                .contains("pk:42")
                .contains("c_text:'foo'");
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_log_non_null_positional_parameter_simple_statements() throws Exception {
        // given
        normal.setLevel(TRACE);
        queryLogger = QueryLogger.builder().build();
        cluster().register(queryLogger);
        // when
        String query = "UPDATE test SET c_text = ? WHERE pk = ?";
        SimpleStatement ss = new SimpleStatement(query, "foo", 42);
        session().execute(ss);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains(query)
                .contains("42")
                .contains("'foo'");
    }

    @Test(groups = "short")
    public void should_log_null_parameter_bound_statements() throws Exception {
        // given
        normal.setLevel(TRACE);
        queryLogger = QueryLogger.builder().build();
        cluster().register(queryLogger);
        // when
        String query = "UPDATE test SET c_text = ? WHERE pk = ?";
        PreparedStatement ps = session().prepare(query);
        BoundStatement bs = ps.bind();
        bs.setString("c_text", null);
        bs.setInt("pk", 42);
        session().execute(bs);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains(query)
                .contains("pk:42")
                .contains("c_text:NULL");
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_log_null_parameter_simple_statements() throws Exception {
        // given
        normal.setLevel(TRACE);
        queryLogger = QueryLogger.builder().build();
        cluster().register(queryLogger);
        // when
        String query = "UPDATE test SET c_text = ? WHERE pk = ?";
        SimpleStatement ss = new SimpleStatement(query, null, 42);
        session().execute(ss);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains(query)
                .contains("42")
                .contains("NULL");
    }

    @Test(groups = "short")
    @CassandraVersion("3.0")
    public void should_log_unset_parameter() throws Exception {
        // given
        normal.setLevel(TRACE);
        queryLogger = QueryLogger.builder().build();
        cluster().register(queryLogger);
        // when
        String query = "UPDATE test SET c_text = ? WHERE pk = ?";
        PreparedStatement ps = session().prepare(query);
        BoundStatement bs = ps.bind();
        bs.setInt("pk", 42);
        session().execute(bs);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains(query)
                .contains("pk:42")
                .contains("c_text:<UNSET>");
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_log_bound_statement_parameters_inside_batch_statement() throws Exception {
        // given
        normal.setLevel(TRACE);
        queryLogger = QueryLogger.builder().build();
        cluster().register(queryLogger);
        // when
        String query1 = "UPDATE test SET c_text = ? WHERE pk = ?";
        String query2 = "UPDATE test SET c_int = ? WHERE pk = ?";
        BatchStatement batch = new BatchStatement();
        batch.add(session().prepare(query1).bind("foo", 42));
        batch.add(session().prepare(query2).bind(12345, 43));
        session().execute(batch);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains(query1)
                .contains(query2)
                .contains("pk:42")
                .contains("pk:43")
                .contains("c_text:'foo'")
                .contains("c_int:12345");
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_log_simple_statement_parameters_inside_batch_statement() throws Exception {
        // given
        normal.setLevel(TRACE);
        queryLogger = QueryLogger.builder().build();
        cluster().register(queryLogger);
        // when
        String query1 = "UPDATE test SET c_text = ? WHERE pk = ?";
        String query2 = "UPDATE test SET c_int = ? WHERE pk = ?";
        BatchStatement batch = new BatchStatement();
        batch.add(new SimpleStatement(query1, "foo", 42));
        batch.add(new SimpleStatement(query2, 12345, 43));
        session().execute(batch);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains(query1)
                .contains(query2)
                .contains("42")
                .contains("43")
                .contains("'foo'")
                .contains("12345");
    }

    // Test different CQL types

    @Test(groups = "short")
    public void should_log_all_parameter_types_bound_statements() throws Exception {
        // given
        normal.setLevel(TRACE);
        queryLogger = QueryLogger.builder()
                .withMaxParameterValueLength(Integer.MAX_VALUE)
                .build();
        cluster().register(queryLogger);
        // when
        String query = "UPDATE test SET " + assignments + " WHERE pk = 42";
        PreparedStatement ps = session().prepare(query);
        BoundStatement bs = ps.bind(values.toArray());
        session().execute(bs);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains(query);
        CodecRegistry codecRegistry = cluster().getConfiguration().getCodecRegistry();
        for (DataType type : dataTypes) {
            TypeCodec<Object> codec = codecRegistry.codecFor(type);
            assertThat(line).contains(codec.format(getFixedValue(type)));
        }
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_log_all_parameter_types_simple_statements() throws Exception {
        // given
        normal.setLevel(TRACE);
        queryLogger = QueryLogger.builder()
                .withMaxParameterValueLength(Integer.MAX_VALUE)
                .build();
        cluster().register(queryLogger);
        // when
        String query = "UPDATE test SET " + assignments + " WHERE pk = 42";
        SimpleStatement ss = new SimpleStatement(query, values.toArray());
        session().execute(ss);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains(query);
        CodecRegistry codecRegistry = cluster().getConfiguration().getCodecRegistry();
        for (DataType type : dataTypes) {
            TypeCodec<Object> codec;
            if (type.equals(DataType.time())) {
                codec = codecRegistry.codecFor(DataType.bigint());
            } else {
                codec = codecRegistry.codecFor(type);
            }
            assertThat(line).contains(codec.format(getFixedValue(type)));
        }
    }

    // Tests for truncation of query strings and parameter values

    @Test(groups = "short")
    public void should_truncate_query_when_max_length_exceeded() throws Exception {
        // given
        normal.setLevel(DEBUG);
        queryLogger = QueryLogger.builder()
                .withMaxQueryStringLength(5)
                .build();
        cluster().register(queryLogger);
        // when
        String query = "SELECT * FROM test WHERE pk = 42";
        session().execute(query);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains("SELEC" + TRUNCATED_OUTPUT)
                .doesNotContain(query);
    }

    @CassandraVersion("2.0.0")
    @Test(groups = "short")
    public void should_show_total_statements_for_batches_even_if_query_truncated() throws Exception {
        // given
        normal.setLevel(DEBUG);
        queryLogger = QueryLogger.builder()
                .withMaxQueryStringLength(5)
                .build();
        cluster().register(queryLogger);
        // when
        String query1 = "UPDATE test SET c_text = ? WHERE pk = ?";
        String query2 = "UPDATE test SET c_int = ? WHERE pk = ?";
        BatchStatement batch = new BatchStatement();
        batch.add(session().prepare(query1).bind("foo", 42));
        batch.add(session().prepare(query2).bind(12345, 43));
        session().execute(batch);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains("BEGIN" + TRUNCATED_OUTPUT)
                .doesNotContain(query1)
                .doesNotContain(query2)
                .contains(" [2 statements");
    }

    @Test(groups = "short")
    public void should_not_truncate_query_when_max_length_unlimited() throws Exception {
        // given
        normal.setLevel(DEBUG);
        queryLogger = QueryLogger.builder()
                .withMaxQueryStringLength(-1)
                .build();
        cluster().register(queryLogger);
        // when
        String query = "SELECT * FROM test WHERE pk = 42";
        session().execute(query);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains(query)
                .doesNotContain(TRUNCATED_OUTPUT);
    }

    @CassandraVersion("2.0.0")
    @Test(groups = "short")
    public void should_truncate_parameter_when_max_length_exceeded_bound_statements() throws Exception {
        // given
        normal.setLevel(TRACE);
        queryLogger = QueryLogger.builder()
                .withMaxParameterValueLength(5)
                .build();
        cluster().register(queryLogger);
        // when
        String query = "UPDATE test SET c_int = ? WHERE pk = ?";
        PreparedStatement ps = session().prepare(query);
        BoundStatement bs = ps.bind();
        bs.setInt("c_int", 123456);
        bs.setInt("pk", 42);
        session().execute(bs);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains("c_int:12345" + TRUNCATED_OUTPUT)
                .doesNotContain("123456");
    }

    @CassandraVersion("2.0.0")
    @Test(groups = "short")
    public void should_truncate_parameter_when_max_length_exceeded_simple_statements() throws Exception {
        // given
        normal.setLevel(TRACE);
        queryLogger = QueryLogger.builder()
                .withMaxParameterValueLength(5)
                .build();
        cluster().register(queryLogger);
        // when
        String query = "UPDATE test SET c_int = ? WHERE pk = ?";
        SimpleStatement ss = new SimpleStatement(query, 123456, 42);
        session().execute(ss);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains("12345" + TRUNCATED_OUTPUT)
                .doesNotContain("123456");
    }

    @Test(groups = "short")
    public void should_truncate_blob_parameter_when_max_length_exceeded_bound_statements() throws Exception {
        // given
        normal.setLevel(TRACE);
        queryLogger = QueryLogger.builder()
                .withMaxParameterValueLength(6)
                .build();
        cluster().register(queryLogger);
        // when
        String query = "UPDATE test SET c_blob = ? WHERE pk = ?";
        PreparedStatement ps = session().prepare(query);
        BoundStatement bs = ps.bind();
        bs.setBytes("c_blob", ByteBuffer.wrap(Bytes.toArray(Lists.newArrayList(1, 2, 3))));
        bs.setInt("pk", 42);
        session().execute(bs);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains("c_blob:0x0102" + TRUNCATED_OUTPUT)
                .doesNotContain("0x010203");
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_truncate_blob_parameter_when_max_length_exceeded_simple_statements() throws Exception {
        // given
        normal.setLevel(TRACE);
        queryLogger = QueryLogger.builder()
                .withMaxParameterValueLength(6)
                .build();
        cluster().register(queryLogger);
        // when
        String query = "UPDATE test SET c_blob = ? WHERE pk = ?";
        SimpleStatement ss = new SimpleStatement(query, ByteBuffer.wrap(Bytes.toArray(Lists.newArrayList(1, 2, 3))), 42);
        session().execute(ss);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains("0x0102" + TRUNCATED_OUTPUT)
                .doesNotContain("0x010203");
    }

    @Test(groups = "short")
    public void should_not_truncate_parameter_when_max_length_unlimited_bound_statements() throws Exception {
        // given
        normal.setLevel(TRACE);
        queryLogger = QueryLogger.builder()
                .withMaxParameterValueLength(-1)
                .build();
        cluster().register(queryLogger);
        // when
        String query = "UPDATE test SET c_int = ? WHERE pk = ?";
        PreparedStatement ps = session().prepare(query);
        BoundStatement bs = ps.bind();
        bs.setInt("c_int", 123456);
        bs.setInt("pk", 42);
        session().execute(bs);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains("c_int:123456")
                .doesNotContain(TRUNCATED_OUTPUT);
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_not_truncate_parameter_when_max_length_unlimited_simple_statements() throws Exception {
        // given
        normal.setLevel(TRACE);
        queryLogger = QueryLogger.builder()
                .withMaxParameterValueLength(-1)
                .build();
        cluster().register(queryLogger);
        // when
        String query = "UPDATE test SET c_int = ? WHERE pk = ?";
        SimpleStatement ss = new SimpleStatement(query, 123456, 42);
        session().execute(ss);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains("123456")
                .doesNotContain(TRUNCATED_OUTPUT);
    }

    @Test(groups = "short")
    public void should_not_log_exceeding_number_of_parameters_bound_statements() throws Exception {
        // given
        normal.setLevel(TRACE);
        queryLogger = QueryLogger.builder()
                .withMaxLoggedParameters(1)
                .build();
        cluster().register(queryLogger);
        // when
        String query = "UPDATE test SET c_int = ? WHERE pk = ?";
        PreparedStatement ps = session().prepare(query);
        BoundStatement bs = ps.bind();
        bs.setInt("c_int", 123456);
        bs.setInt("pk", 42);
        session().execute(bs);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains("c_int:123456")
                .doesNotContain("pk:42")
                .contains(FURTHER_PARAMS_OMITTED);
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_not_log_exceeding_number_of_parameters_simple_statements() throws Exception {
        // given
        normal.setLevel(TRACE);
        queryLogger = QueryLogger.builder()
                .withMaxLoggedParameters(1)
                .build();
        cluster().register(queryLogger);
        // when
        String query = "UPDATE test SET c_int = ? WHERE pk = ?";
        SimpleStatement ss = new SimpleStatement(query, 123456, 42);
        session().execute(ss);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains("123456")
                .doesNotContain("123456, 42")
                .contains(FURTHER_PARAMS_OMITTED);
    }

    @Test(groups = "short")
    @CassandraVersion("2.1.0")
    public void should_not_log_exceeding_number_of_parameters_simple_statements_with_named_values() throws Exception {
        // given
        normal.setLevel(TRACE);
        queryLogger = QueryLogger.builder()
                .withMaxLoggedParameters(1)
                .build();
        cluster().register(queryLogger);
        // when
        String query = "UPDATE test SET c_int = :c_int WHERE pk = :pk";
        Map<String, Object> namedValues = new LinkedHashMap<String, Object>();
        namedValues.put("c_int", 123456);
        namedValues.put("pk", 42);
        SimpleStatement ss = new SimpleStatement(query, namedValues);
        session().execute(ss);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains("c_int:123456")
                .doesNotContain("pk:42")
                .contains(FURTHER_PARAMS_OMITTED);
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_not_log_exceeding_number_of_parameters_in_batch_statement_bound_statements() throws Exception {
        // given
        normal.setLevel(TRACE);
        queryLogger = QueryLogger.builder()
                .withMaxLoggedParameters(1)
                .build();
        cluster().register(queryLogger);
        // when
        String query1 = "UPDATE test SET c_text = ? WHERE pk = ?";
        String query2 = "UPDATE test SET c_int = ? WHERE pk = ?";
        BatchStatement batch = new BatchStatement();
        batch.add(session().prepare(query1).bind("foo", 42));
        batch.add(session().prepare(query2).bind(12345, 43));
        session().execute(batch);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains(query1)
                .contains(query2)
                .contains("c_text:'foo'")
                .doesNotContain("pk:42")
                .doesNotContain("c_int:12345")
                .doesNotContain("pk:43")
                .contains(FURTHER_PARAMS_OMITTED);
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_not_log_exceeding_number_of_parameters_in_batch_statement_simple_statements() throws Exception {
        // given
        normal.setLevel(TRACE);
        queryLogger = QueryLogger.builder()
                .withMaxLoggedParameters(1)
                .build();
        cluster().register(queryLogger);
        // when
        String query1 = "UPDATE test SET c_text = ? WHERE pk = ?";
        String query2 = "UPDATE test SET c_int = ? WHERE pk = ?";
        BatchStatement batch = new BatchStatement();
        batch.add(new SimpleStatement(query1, "foo", 42));
        batch.add(new SimpleStatement(query2, 12345, 43));
        session().execute(batch);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains(query1)
                .contains(query2)
                .contains("'foo'")
                .doesNotContain("42, 12345, 43")
                .contains(FURTHER_PARAMS_OMITTED);
    }

    @Test(groups = "short")
    public void should_log_all_parameters_when_max_unlimited_bound_statements() throws Exception {
        // given
        normal.setLevel(TRACE);
        queryLogger = QueryLogger.builder()
                .withMaxLoggedParameters(-1)
                .build();
        cluster().register(queryLogger);
        // when
        String query = "UPDATE test SET c_int = ? WHERE pk = ?";
        PreparedStatement ps = session().prepare(query);
        BoundStatement bs = ps.bind();
        bs.setInt("c_int", 123456);
        bs.setInt("pk", 42);
        session().execute(bs);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains("c_int:123456")
                .contains("pk:42");
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_log_all_parameters_when_max_unlimited_simple_statements() throws Exception {
        // given
        normal.setLevel(TRACE);
        queryLogger = QueryLogger.builder()
                .withMaxLoggedParameters(-1)
                .build();
        cluster().register(queryLogger);
        // when
        String query = "UPDATE test SET c_int = ? WHERE pk = ?";
        SimpleStatement ss = new SimpleStatement(query, 123456, 42);
        session().execute(ss);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains("123456")
                .contains("42");
    }

    @Test(groups = "short")
    @CassandraVersion("2.1.0")
    public void should_log_all_parameters_when_max_unlimited_simple_statements_with_named_values() throws Exception {
        // given
        normal.setLevel(TRACE);
        queryLogger = QueryLogger.builder()
                .withMaxLoggedParameters(-1)
                .build();
        cluster().register(queryLogger);
        // when
        String query = "UPDATE test SET c_int = :c_int WHERE pk = :pk";
        Map<String, Object> namedValues = new HashMap<String, Object>();
        namedValues.put("c_int", 123456);
        namedValues.put("pk", 42);
        SimpleStatement ss = new SimpleStatement(query, namedValues);
        session().execute(ss);
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains("123456")
                .contains("42");
    }

    @CassandraVersion("2.0.0")
    @Test(groups = "short")
    public void should_log_wrapped_bound_statement() throws Exception {
        // given
        normal.setLevel(TRACE);
        queryLogger = QueryLogger.builder()
                .withConstantThreshold(Long.MAX_VALUE)
                .withMaxQueryStringLength(Integer.MAX_VALUE)
                .build();
        cluster().register(queryLogger);
        // when
        String query = "UPDATE test SET c_text = :param1 WHERE pk = :param2";
        PreparedStatement ps = session().prepare(query);
        BoundStatement bs = ps.bind();
        bs.setString("param1", "foo");
        bs.setInt("param2", 42);
        session().execute(new CustomStatement(bs));
        // then
        String line = normalAppender.waitAndGet(10000);
        assertThat(line)
                .contains("Query completed normally")
                .contains(ipOfNode(1))
                .contains(query)
                .contains("param2:42")
                .contains("param1:'foo'");
    }

}
