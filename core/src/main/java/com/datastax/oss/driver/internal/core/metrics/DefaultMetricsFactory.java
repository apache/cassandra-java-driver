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
package com.datastax.oss.driver.internal.core.metrics;

import static com.datastax.oss.driver.internal.core.util.Dependency.DROPWIZARD;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.internal.core.util.DefaultDependencyChecker;
import java.util.Optional;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class DefaultMetricsFactory implements MetricsFactory {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetricsFactory.class);

  private final MetricsFactory delegate;

  @SuppressWarnings("unused")
  public DefaultMetricsFactory(DriverContext context) {
    if (DefaultDependencyChecker.isPresent(DROPWIZARD)) {
      this.delegate = new DropwizardMetricsFactory(context);
    } else {
      this.delegate = new NoopMetricsFactory(context);
    }
    LOG.debug("[{}] Using {}", context.getSessionName(), delegate.getClass().getSimpleName());
  }

  @Override
  public Optional<Metrics> getMetrics() {
    return delegate.getMetrics();
  }

  @Override
  public SessionMetricUpdater getSessionUpdater() {
    return delegate.getSessionUpdater();
  }

  @Override
  public NodeMetricUpdater newNodeUpdater(Node node) {
    return delegate.newNodeUpdater(node);
  }
}
