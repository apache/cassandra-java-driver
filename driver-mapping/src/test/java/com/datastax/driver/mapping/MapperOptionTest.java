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
package com.datastax.driver.mapping;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.utils.CassandraVersion;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.testng.SkipException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static com.datastax.driver.core.ConsistencyLevel.*;
import static com.datastax.driver.core.ProtocolVersion.V1;
import static com.datastax.driver.mapping.Mapper.Option;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@SuppressWarnings("unused")
public class MapperOptionTest extends CCMTestsSupport {

    ProtocolVersion protocolVersion;
    Mapper<User> mapper;

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE user (key int primary key, v text)");
    }

    @BeforeMethod(groups = "short")
    public void setup() {
        mapper = new MappingManager(session()).mapper(User.class);
        protocolVersion = cluster().getConfiguration().getProtocolOptions().getProtocolVersion();
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    void should_use_save_options() throws Exception {
        Long tsValue = futureTimestamp();
        mapper.save(new User(42, "helloworld"), Option.timestamp(tsValue), Option.tracing(true));
        assertThat(mapper.get(42).getV()).isEqualTo("helloworld");
        Long tsReturned = session().execute("SELECT writetime(v) FROM user WHERE key=" + 42).one().getLong(0);
        assertThat(tsReturned).isEqualTo(tsValue);
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    void should_use_delete_options() {
        User todelete = new User(45, "todelete");
        mapper.save(todelete);
        Option opt = Option.timestamp(35);
        BoundStatement bs = (BoundStatement) mapper.deleteQuery(45, opt, Option.consistencyLevel(QUORUM));
        assertThat(bs.preparedStatement().getQueryString()).contains("USING TIMESTAMP");
        assertThat(bs.getConsistencyLevel()).isEqualTo(QUORUM);
    }

    @Test(groups = "short", expectedExceptions = {IllegalArgumentException.class})
    @CassandraVersion("2.0.0")
    void should_use_get_options() {
        User user = new User(45, "toget");
        mapper.save(user);

        BoundStatement bs = (BoundStatement) mapper.getQuery(45, Option.tracing(true), Option.consistencyLevel(ALL));
        assertThat(bs.isTracing()).isTrue();
        assertThat(bs.getConsistencyLevel()).isEqualTo(ALL);

        ResultSet rs = session().execute(bs);
        assertThat(rs.getExecutionInfo().getQueryTrace()).isNotNull();
        User us = mapper.map(rs).one();
        assertThat(us.getV()).isEqualTo("toget");

        mapper.getQuery(45, Option.timestamp(1337));
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    void should_use_options_only_once() {
        Long tsValue = futureTimestamp();
        mapper.save(new User(43, "helloworld"), Option.timestamp(tsValue));
        mapper.save(new User(44, "test"));
        Long tsReturned = session().execute("SELECT writetime(v) FROM user WHERE key=" + 44).one().getLong(0);
        // Assuming we cannot go back in time (yet) and execute the write at ts=1
        assertThat(tsReturned).isNotEqualTo(tsValue);
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    void should_use_default_options() {
        mapper.setDefaultSaveOptions(Option.timestamp(644746L), Option.ttl(76324));
        BoundStatement bs = (BoundStatement) mapper.saveQuery(new User(46, "rjhrgce"));
        assertThat(bs.preparedStatement().getQueryString()).contains("TIMESTAMP").contains("TTL");

        mapper.resetDefaultSaveOptions();
        bs = (BoundStatement) mapper.saveQuery(new User(47, "rjhrgce"));
        assertThat(bs.preparedStatement().getQueryString()).doesNotContain("TIMESTAMP").doesNotContain("TTL");

        mapper.setDefaultDeleteOptions(Option.timestamp(3245L), Option.tracing(true));
        bs = (BoundStatement) mapper.deleteQuery(47);
        assertThat(bs.preparedStatement().getQueryString()).contains("TIMESTAMP");
        assertThat(bs.isTracing()).isTrue();

        mapper.resetDefaultDeleteOptions();
        bs = (BoundStatement) mapper.deleteQuery(47);
        assertThat(bs.preparedStatement().getQueryString()).doesNotContain("TIMESTAMP");
        assertThat(bs.isTracing()).isFalse();

        bs = (BoundStatement) mapper.saveQuery(new User(46, "rjhrgce"), Option.timestamp(23), Option.consistencyLevel(ConsistencyLevel.ANY));
        assertThat(bs.getConsistencyLevel()).isEqualTo(ConsistencyLevel.ANY);

        mapper.setDefaultGetOptions(Option.tracing(true));
        bs = (BoundStatement) mapper.getQuery(46);
        assertThat(bs.isTracing()).isTrue();

        mapper.resetDefaultGetOptions();
        bs = (BoundStatement) mapper.getQuery(46);
        assertThat(bs.isTracing()).isFalse();
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    void should_prioritize_option_over_model_consistency() {
        // Generate Write Query and ensure User model writeConsistency is used.
        User user = new User(1859, "Steve");
        Statement saveDefault = mapper.saveQuery(user);
        assertThat(saveDefault.getConsistencyLevel()).isEqualTo(ONE);

        // Generate Write Query and ensure provided Option for consistencyLevel is used.
        Statement saveProvidedCL = mapper.saveQuery(user, Option.consistencyLevel(QUORUM));
        assertThat(saveProvidedCL.getConsistencyLevel()).isEqualTo(QUORUM);

        // Generate Read Query and ensure User model readConsistency is used.
        Statement readDefault = mapper.getQuery(1859);
        assertThat(readDefault.getConsistencyLevel()).isEqualTo(LOCAL_ONE);

        // Generate Ready Query and ensure provided Option for consistencyLevel is used.
        Statement readProvidedCL = mapper.getQuery(1859, Option.consistencyLevel(LOCAL_QUORUM));
        assertThat(readProvidedCL.getConsistencyLevel()).isEqualTo(LOCAL_QUORUM);
    }
    

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    void should_use_if_not_exists_option() {
        Pattern ifNotExistsPattern = Pattern.compile(".*\\sIF\\s+NOT\\s+EXISTS(\\s+)?;?(\\s+)?$", Pattern.CASE_INSENSITIVE);

        User user = new User(42, "Cin Ali");

        // test default
        BoundStatement saveDefault = (BoundStatement) mapper.saveQuery(user);
        DefaultPreparedStatement stmt = (DefaultPreparedStatement) saveDefault.preparedStatement();
        assertThat(stmt.getQueryString()).doesNotMatch(ifNotExistsPattern);

        // test disabled
        saveDefault = (BoundStatement) mapper.saveQuery(user, Option.ifNotExists(false));
        stmt = (DefaultPreparedStatement) saveDefault.preparedStatement();
        assertThat(stmt.getQueryString()).doesNotMatch(ifNotExistsPattern);

        // test enabled
        saveDefault = (BoundStatement) mapper.saveQuery(user, Option.ifNotExists(true));
        stmt = (DefaultPreparedStatement) saveDefault.preparedStatement();
        assertThat(stmt.getQueryString()).matches(ifNotExistsPattern);
        
        // test default enabled
        mapper.setDefaultSaveOptions(Option.ifNotExists(true));
        saveDefault = (BoundStatement) mapper.saveQuery(user);
        stmt = (DefaultPreparedStatement) saveDefault.preparedStatement();
        assertThat(stmt.getQueryString()).matches(ifNotExistsPattern);

        // test default disabled
        mapper.setDefaultSaveOptions(Option.ifNotExists(false));
        saveDefault = (BoundStatement) mapper.saveQuery(user);
        stmt = (DefaultPreparedStatement) saveDefault.preparedStatement();
        assertThat(stmt.getQueryString()).doesNotMatch(ifNotExistsPattern);
    }

    @Test(groups = "short", expectedExceptions = {IllegalArgumentException.class})
    void should_fail_when_using_if_not_exists_on_get_query() {
        mapper.get(new User(42, "Cin Ali"), Option.ifNotExists(true));
    }

    @Test(groups = "short", expectedExceptions = {IllegalArgumentException.class})
    void should_fail_when_using_if_not_exists_on_delete_query() {
        mapper.delete(new User(42, "Cin Ali"), Option.ifNotExists(true));
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    void should_use_explicit_options_over_default_options() {
        long defaultTimestamp = futureTimestamp();
        long explicitTimestamp = futureTimestamp();

        mapper.setDefaultSaveOptions(Option.timestamp(defaultTimestamp));

        mapper.save(new User(42, "helloworld"), Option.timestamp(explicitTimestamp));
        Long savedTimestamp = session().execute("SELECT writetime(v) FROM user WHERE key=" + 42).one().getLong(0);
        assertThat(savedTimestamp).isEqualTo(explicitTimestamp);
    }

    /**
     * Cover all versions of save() to check that methods that call each other properly propagate the options
     */
    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    void should_use_save_options_for_all_variants() throws ExecutionException, InterruptedException {
        Long timestamp = futureTimestamp();
        User user = new User(42, "helloworld");

        mapper.save(user, Option.timestamp(timestamp));
        assertThat(getWriteTime(user.getKey())).isEqualTo(timestamp);
        mapper.delete(user.getKey());

        mapper.saveAsync(user, Option.timestamp(timestamp)).get();
        assertThat(getWriteTime(user.getKey())).isEqualTo(timestamp);
        mapper.delete(user.getKey());

        session().execute(mapper.saveQuery(user, Option.timestamp(timestamp)));
        assertThat(getWriteTime(user.getKey())).isEqualTo(timestamp);
        mapper.delete(user.getKey());

        // Set as default and try optionless methods
        mapper.setDefaultSaveOptions(Option.timestamp(timestamp));

        mapper.save(user);
        assertThat(getWriteTime(user.getKey())).isEqualTo(timestamp);
        mapper.delete(user.getKey());

        mapper.saveAsync(user).get();
        assertThat(getWriteTime(user.getKey())).isEqualTo(timestamp);
        mapper.delete(user.getKey());

        session().execute(mapper.saveQuery(user));
        assertThat(getWriteTime(user.getKey())).isEqualTo(timestamp);
        mapper.delete(user.getKey());
    }

    private Long getWriteTime(int key) {
        Row row = session().execute("SELECT writetime(v) FROM user WHERE key=" + key).one();
        return row.getLong(0);
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    void should_use_get_options_for_all_variants() throws InterruptedException {
        try {
            mapper.get(42, Option.consistencyLevel(TWO));
            fail("Expected a NoHostAvailableException");
        } catch (NoHostAvailableException e) {
            assertFirstNodeUnavailable(e);
        }

        try {
            mapper.getAsync(42, Option.consistencyLevel(TWO)).get();
            fail("Expected an ExecutionException");
        } catch (ExecutionException e) {
            assertFirstNodeUnavailable(e);
        }

        Statement statement = mapper.getQuery(42, Option.consistencyLevel(TWO));
        assertThat(statement.getConsistencyLevel()).isEqualTo(TWO);

        // Set as default and try optionless methods
        mapper.setDefaultGetOptions(Option.consistencyLevel(TWO));

        try {
            mapper.get(42);
            fail("Expected a NoHostAvailableException");
        } catch (NoHostAvailableException e) {
            assertFirstNodeUnavailable(e);
        }

        try {
            mapper.getAsync(42).get();
            fail("Expected an ExecutionException");
        } catch (ExecutionException e) {
            assertFirstNodeUnavailable(e);
        }

        statement = mapper.getQuery(42);
        assertThat(statement.getConsistencyLevel()).isEqualTo(TWO);
    }

    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    void should_use_delete_options_for_all_variants() throws InterruptedException {
        User user = new User(42, "helloworld");

        try {
            mapper.delete(user, Option.consistencyLevel(TWO));
            fail("Expected a NoHostAvailableException");
        } catch (NoHostAvailableException e) {
            assertFirstNodeUnavailable(e);
        }

        try {
            mapper.deleteAsync(user, Option.consistencyLevel(TWO)).get();
            fail("Expected an ExecutionException");
        } catch (ExecutionException e) {
            assertFirstNodeUnavailable(e);
        }

        Statement statement = mapper.deleteQuery(user, Option.consistencyLevel(TWO));
        assertThat(statement.getConsistencyLevel()).isEqualTo(TWO);

        try {
            mapper.delete(user.getKey(), Option.consistencyLevel(TWO));
            fail("Expected a NoHostAvailableException");
        } catch (NoHostAvailableException e) {
            assertFirstNodeUnavailable(e);
        }

        try {
            mapper.deleteAsync(user.getKey(), Option.consistencyLevel(TWO)).get();
            fail("Expected an ExecutionException");
        } catch (ExecutionException e) {
            assertFirstNodeUnavailable(e);
        }

        statement = mapper.deleteQuery(user.getKey(), Option.consistencyLevel(TWO));
        assertThat(statement.getConsistencyLevel()).isEqualTo(TWO);

        // Set as default and try optionless methods
        mapper.setDefaultDeleteOptions(Option.consistencyLevel(TWO));

        try {
            mapper.delete(user);
            fail("Expected a NoHostAvailableException");
        } catch (NoHostAvailableException e) {
            assertFirstNodeUnavailable(e);
        }

        try {
            mapper.deleteAsync(user).get();
            fail("Expected an ExecutionException");
        } catch (ExecutionException e) {
            assertFirstNodeUnavailable(e);
        }

        statement = mapper.deleteQuery(user);
        assertThat(statement.getConsistencyLevel()).isEqualTo(TWO);

        try {
            mapper.delete(user.getKey());
            fail("Expected a NoHostAvailableException");
        } catch (NoHostAvailableException e) {
            assertFirstNodeUnavailable(e);
        }

        try {
            mapper.deleteAsync(user.getKey()).get();
            fail("Expected an ExecutionException");
        } catch (ExecutionException e) {
            assertFirstNodeUnavailable(e);
        }

        statement = mapper.deleteQuery(user.getKey());
        assertThat(statement.getConsistencyLevel()).isEqualTo(TWO);
    }

    @Test(groups = "short", expectedExceptions = IllegalArgumentException.class)
    void should_fail_if_option_does_not_apply_to_query() {
        mapper.get(42, Option.ttl(1));
    }

    @Test(groups = "short", expectedExceptions = IllegalArgumentException.class)
    void should_fail_when_using_ttl_with_protocol_v1() {
        if (protocolVersion.compareTo(V1) > 0)
            throw new SkipException("Skipped when protocol version > V1");

        mapper.saveQuery(new User(42, "helloworld"), Option.ttl(15));
    }

    @Test(groups = "short", expectedExceptions = IllegalArgumentException.class)
    void should_fail_when_using_timestamp_with_protocol_v1() {
        if (protocolVersion.compareTo(V1) > 0)
            throw new SkipException("Skipped when protocol version > V1");

        mapper.saveQuery(new User(42, "helloworld"), Option.timestamp(15));
    }

    private static void assertFirstNodeUnavailable(NoHostAvailableException e) {
        Throwable node1Error = e.getErrors().values().iterator().next();
        assertThat(node1Error).isInstanceOf(UnavailableException.class);
    }

    private static void assertFirstNodeUnavailable(ExecutionException e) {
        Throwable cause = e.getCause();
        assertThat(cause).isInstanceOf(NoHostAvailableException.class);
        assertFirstNodeUnavailable((NoHostAvailableException) cause);
    }

    private static long futureTimestamp() {
        return (System.currentTimeMillis() + 1000) * 1000;
    }

    @Table(name = "user", readConsistency = "LOCAL_ONE", writeConsistency = "ONE")
    public static class User {
        @PartitionKey
        private int key;
        private String v;

        public User() {
        }

        public User(int k, String val) {
            this.key = k;
            this.v = val;
        }

        public int getKey() {
            return this.key;
        }

        public void setKey(int pk) {
            this.key = pk;
        }

        public String getV() {
            return this.v;
        }

        public void setV(String val) {
            this.v = val;
        }
    }
}
