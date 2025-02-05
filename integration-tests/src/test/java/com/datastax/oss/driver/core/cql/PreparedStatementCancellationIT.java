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
import static org.junit.Assert.fail;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareAsyncProcessor;
import com.datastax.oss.driver.shaded.guava.common.base.Predicates;
import com.datastax.oss.driver.shaded.guava.common.cache.Cache;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;
import java.util.concurrent.CompletableFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(IsolatedTests.class)
public class PreparedStatementCancellationIT {

  private CustomCcmRule ccmRule = CustomCcmRule.builder().build();

  private SessionRule<CqlSession> sessionRule = SessionRule.builder(ccmRule).build();

  @Rule public TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  @Before
  public void setup() {

    CqlSession session = SessionUtils.newSession(ccmRule, sessionRule.keyspace());
    session.execute("DROP TABLE IF EXISTS test_table_1");
    session.execute("CREATE TABLE test_table_1 (k int primary key, v int)");
    session.execute("INSERT INTO test_table_1 (k,v) VALUES (1, 100)");
    session.execute("INSERT INTO test_table_1 (k,v) VALUES (2, 200)");
    session.execute("INSERT INTO test_table_1 (k,v) VALUES (3, 300)");
    session.close();
  }

  @After
  public void teardown() {

    CqlSession session = SessionUtils.newSession(ccmRule, sessionRule.keyspace());
    session.execute("DROP TABLE test_table_1");
    session.close();
  }

  private CompletableFuture<PreparedStatement> toCompletableFuture(CqlSession session, String cql) {

    return session.prepareAsync(cql).toCompletableFuture();
  }

  private CqlPrepareAsyncProcessor findProcessor(CqlSession session) {

    DefaultDriverContext context = (DefaultDriverContext) session.getContext();
    return (CqlPrepareAsyncProcessor)
        Iterables.find(
            context.getRequestProcessorRegistry().getProcessors(),
            Predicates.instanceOf(CqlPrepareAsyncProcessor.class));
  }

  @Test
  public void should_cache_valid_cql() throws Exception {

    CqlSession session = SessionUtils.newSession(ccmRule, sessionRule.keyspace());
    CqlPrepareAsyncProcessor processor = findProcessor(session);
    Cache<PrepareRequest, CompletableFuture<PreparedStatement>> cache = processor.getCache();
    assertThat(cache.size()).isEqualTo(0);

    // Make multiple CompletableFuture requests for the specified CQL, then wait until
    // the cached request finishes and confirm that all futures got the same values
    String cql = "select v from test_table_1 where k = ?";
    CompletableFuture<PreparedStatement> cf1 = toCompletableFuture(session, cql);
    CompletableFuture<PreparedStatement> cf2 = toCompletableFuture(session, cql);
    assertThat(cache.size()).isEqualTo(1);

    CompletableFuture<PreparedStatement> future = Iterables.get(cache.asMap().values(), 0);
    PreparedStatement stmt = future.get();

    assertThat(cf1.isDone()).isTrue();
    assertThat(cf2.isDone()).isTrue();

    assertThat(cf1.join()).isEqualTo(stmt);
    assertThat(cf2.join()).isEqualTo(stmt);
  }

  // A holdover from work done on JAVA-3055.  This probably isn't _desired_ behaviour but this test
  // documents the fact that the current driver impl will behave in this way.  We should probably
  // consider changing this in a future release, although it's worthwhile fully considering the
  // implications of such a change.
  @Test
  public void will_cache_invalid_cql() throws Exception {

    CqlSession session = SessionUtils.newSession(ccmRule, sessionRule.keyspace());
    CqlPrepareAsyncProcessor processor = findProcessor(session);
    Cache<PrepareRequest, CompletableFuture<PreparedStatement>> cache = processor.getCache();
    assertThat(cache.size()).isEqualTo(0);

    // Verify that we get the CompletableFuture even if the CQL is invalid but that nothing is
    // cached
    String cql = "select v fromfrom test_table_1 where k = ?";
    CompletableFuture<PreparedStatement> cf = toCompletableFuture(session, cql);

    // join() here should throw exceptions due to the invalid syntax... for purposes of this test we
    // can ignore this
    try {
      cf.join();
      fail();
    } catch (Exception e) {
    }

    assertThat(cache.size()).isEqualTo(1);
  }

  @Test
  public void should_not_affect_cache_if_returned_futures_are_cancelled() throws Exception {

    CqlSession session = SessionUtils.newSession(ccmRule, sessionRule.keyspace());
    CqlPrepareAsyncProcessor processor = findProcessor(session);
    Cache<PrepareRequest, CompletableFuture<PreparedStatement>> cache = processor.getCache();
    assertThat(cache.size()).isEqualTo(0);

    String cql = "select v from test_table_1 where k = ?";
    CompletableFuture<PreparedStatement> cf = toCompletableFuture(session, cql);

    assertThat(cf.isCancelled()).isFalse();
    assertThat(cf.cancel(false)).isTrue();
    assertThat(cf.isCancelled()).isTrue();
    assertThat(cf.isCompletedExceptionally()).isTrue();

    // Confirm that cancelling the CompletableFuture returned to the user does _not_ cancel the
    // future used within the cache.  CacheEntry very deliberately doesn't maintain a reference
    // to it's contained CompletableFuture so we have to get at this by secondary effects.
    assertThat(cache.size()).isEqualTo(1);
    CompletableFuture<PreparedStatement> future = Iterables.get(cache.asMap().values(), 0);
    PreparedStatement rv = future.get();
    assertThat(rv).isNotNull();
    assertThat(rv.getQuery()).isEqualTo(cql);
    assertThat(cache.size()).isEqualTo(1);
  }
}
