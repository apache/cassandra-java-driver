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
package com.datastax.oss.driver.internal.metrics.micrometer;

import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.shaded.guava.common.base.Ticker;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.micrometer.core.instrument.MeterRegistry;

/** Implementation of {@link DriverContext} that provides for a Micrometer {@link MeterRegistry}. */
public class MicrometerDriverContext extends DefaultDriverContext {

  private final MeterRegistry registry;

  public MicrometerDriverContext(
      @NonNull DriverConfigLoader configLoader,
      @NonNull ProgrammaticArguments programmaticArguments,
      @NonNull MeterRegistry registry) {
    super(configLoader, programmaticArguments);
    this.registry = registry;
  }

  @Override
  @NonNull
  protected MetricsFactory buildMetricsFactory() {
    return new MicrometerMetricsFactory(this, registry, Ticker.systemTicker());
  }
}
