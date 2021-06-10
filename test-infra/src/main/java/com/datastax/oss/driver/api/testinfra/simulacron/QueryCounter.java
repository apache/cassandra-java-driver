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
package com.datastax.oss.driver.api.testinfra.simulacron;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.server.BoundNode;
import com.datastax.oss.simulacron.server.BoundTopic;
import com.datastax.oss.simulacron.server.listener.QueryListener;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * A convenience utility that keeps track of the number of queries matching a filter received by
 * {@link BoundNode}s.
 *
 * <p>One tricky thing about validating query counts in context of testing is in cases where you
 * don't require waiting for a node to respond. In this case it's possible that a user would check
 * count criteria before the node has even processed the message. This class offers the capability
 * to wait a specified amount of time when asserting query arrival counts on nodes.
 */
public class QueryCounter {

  private final long beforeTimeout;
  private final TimeUnit beforeUnit;
  private final AtomicInteger totalCount = new AtomicInteger(0);
  private final ConcurrentHashMap<Integer, Integer> countMap = new ConcurrentHashMap<>();

  public enum NotificationMode {
    BEFORE_PROCESSING,
    AFTER_PROCESSING
  }

  private QueryCounter(
      BoundTopic<?, ?> topic,
      NotificationMode notificationMode,
      Predicate<QueryLog> queryLogFilter,
      long beforeTimeout,
      TimeUnit beforeUnit) {
    this.beforeTimeout = beforeTimeout;
    this.beforeUnit = beforeUnit;
    QueryListener listener =
        (boundNode, queryLog) -> {
          totalCount.incrementAndGet();
          countMap.merge(boundNode.getId().intValue(), 1, Integer::sum);
        };
    topic.registerQueryListener(
        listener, notificationMode == NotificationMode.AFTER_PROCESSING, queryLogFilter);
  }

  /** Creates a builder that tracks counts for the given {@link BoundTopic} (cluster, dc, node). */
  public static QueryCounterBuilder builder(BoundTopic<?, ?> topic) {
    return new QueryCounterBuilder(topic);
  }

  /** Clears all counters. */
  public void clearCounts() {
    totalCount.set(0);
    countMap.clear();
  }

  /**
   * Asserts that the total number of requests received matching filter criteria matches the
   * expected count within the configured time period.
   */
  public void assertTotalCount(int expected) {
    await()
        .pollInterval(10, TimeUnit.MILLISECONDS)
        .atMost(beforeTimeout, beforeUnit)
        .untilAsserted(() -> assertThat(totalCount.get()).isEqualTo(expected));
  }

  /**
   * Asserts that the total number of requests received matcher filter criteria matches the expected
   * count for each node within the configured time period.
   *
   * @param counts The expected node counts, with the value at each index matching the expected
   *     count for that node id (i.e. index 0 = node id 0 expected count).
   */
  public void assertNodeCounts(int... counts) {
    Map<Integer, Integer> expectedCounts = new HashMap<>();
    for (int id = 0; id < counts.length; id++) {
      int count = counts[id];
      if (count > 0) {
        expectedCounts.put(id, counts[id]);
      }
    }
    await()
        .pollInterval(10, TimeUnit.MILLISECONDS)
        .atMost(beforeTimeout, beforeUnit)
        .untilAsserted(() -> assertThat(countMap).containsAllEntriesOf(expectedCounts));
  }

  public static class QueryCounterBuilder {

    @SuppressWarnings("UnnecessaryLambda")
    private static final Predicate<QueryLog> DEFAULT_FILTER = (q) -> !q.getQuery().isEmpty();

    private final BoundTopic<?, ?> topic;

    private Predicate<QueryLog> queryLogFilter = DEFAULT_FILTER;
    private NotificationMode notificationMode = NotificationMode.BEFORE_PROCESSING;
    private long beforeTimeout = 1;
    private TimeUnit beforeUnit = TimeUnit.SECONDS;

    private QueryCounterBuilder(BoundTopic<?, ?> topic) {
      this.topic = topic;
    }

    /**
     * The filter to apply to consider a message received by the node, if not provided we consider
     * all messages that are queries.
     */
    public QueryCounterBuilder withFilter(Predicate<QueryLog> queryLogFilter) {
      this.queryLogFilter = queryLogFilter;
      return this;
    }

    /** Whether or not simulacron should notify before or after the message is processed. */
    public QueryCounterBuilder withNotification(NotificationMode notificationMode) {
      this.notificationMode = notificationMode;
      return this;
    }

    /**
     * Up to how long we check counts to match. If counts don't match after this time, an exception
     * is thrown.
     */
    public QueryCounterBuilder before(long timeout, TimeUnit unit) {
      this.beforeTimeout = timeout;
      this.beforeUnit = unit;
      return this;
    }

    public QueryCounter build() {
      return new QueryCounter(topic, notificationMode, queryLogFilter, beforeTimeout, beforeUnit);
    }
  }
}
