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
package com.datastax.oss.driver.internal.core.util.concurrent;

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.graph.Graphs;
import com.datastax.oss.driver.shaded.guava.common.graph.MutableValueGraph;
import com.datastax.oss.driver.shaded.guava.common.graph.ValueGraphBuilder;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Detects cycles between a set of {@link LazyReference} instances. */
@ThreadSafe
public class CycleDetector {
  private static final boolean ENABLED =
      Boolean.getBoolean("com.datastax.oss.driver.DETECT_CYCLES");
  private static final Logger LOG = LoggerFactory.getLogger(CycleDetector.class);

  private final String errorMessage;
  private final boolean enabled;
  private final MutableValueGraph<String, String> graph;

  public CycleDetector(String errorMessage) {
    this(errorMessage, ENABLED);
  }

  @VisibleForTesting
  CycleDetector(String errorMessage, boolean enabled) {
    this.errorMessage = errorMessage;
    this.enabled = enabled;
    this.graph = enabled ? ValueGraphBuilder.directed().build() : null;
  }

  void onTryLock(LazyReference<?> reference) {
    if (enabled) {
      synchronized (this) {
        Thread me = Thread.currentThread();
        LOG.debug("{} wants to initialize {}", me, reference.getName());
        graph.putEdgeValue(me.getName(), reference.getName(), "wants to initialize");
        LOG.debug("{}", graph);
        if (Graphs.hasCycle(graph.asGraph())) {
          throw new IllegalStateException(errorMessage + " " + graph);
        }
      }
    }
  }

  void onLockAcquired(LazyReference<?> reference) {
    if (enabled) {
      synchronized (this) {
        Thread me = Thread.currentThread();
        LOG.debug("{} is initializing {}", me, reference.getName());
        String old = graph.removeEdge(me.getName(), reference.getName());
        assert "wants to initialize".equals(old);
        graph.putEdgeValue(reference.getName(), me.getName(), "is getting initialized by");
      }
    }
  }

  void onReleaseLock(LazyReference<?> reference) {
    if (enabled) {
      synchronized (this) {
        Thread me = Thread.currentThread();
        LOG.debug("{} is done initializing {}", me, reference.getName());
        graph.removeEdge(reference.getName(), me.getName());
      }
    }
  }
}
