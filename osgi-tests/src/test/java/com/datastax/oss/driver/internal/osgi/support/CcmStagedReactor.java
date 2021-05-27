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
package com.datastax.oss.driver.internal.osgi.support;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import java.util.List;
import java.util.Objects;
import net.jcip.annotations.GuardedBy;
import org.ops4j.pax.exam.TestContainer;
import org.ops4j.pax.exam.TestProbeBuilder;
import org.ops4j.pax.exam.spi.reactors.AllConfinedStagedReactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CcmStagedReactor extends AllConfinedStagedReactor {

  private static final Logger LOGGER = LoggerFactory.getLogger(CcmStagedReactor.class);

  public static final CcmBridge CCM_BRIDGE;

  public static final Version DSE_5_0 = Objects.requireNonNull(Version.parse("5.0"));

  static {
    CcmBridge.Builder builder = CcmBridge.builder().withNodes(1);
    if (CcmBridge.DSE_ENABLEMENT && CcmBridge.VERSION.compareTo(DSE_5_0) >= 0) {
      builder.withDseWorkloads("graph");
    }
    CCM_BRIDGE = builder.build();
  }

  @GuardedBy("this")
  private boolean running = false;

  public CcmStagedReactor(List<TestContainer> containers, List<TestProbeBuilder> mProbes) {
    super(containers, mProbes);
  }

  @Override
  public synchronized void beforeSuite() {
    if (!running) {
      boolean dse = CCM_BRIDGE.getDseVersion().isPresent();
      LOGGER.info(
          "Starting CCM, running {} version {}",
          dse ? "DSE" : "Cassandra",
          dse ? CCM_BRIDGE.getDseVersion().get() : CCM_BRIDGE.getCassandraVersion());
      CCM_BRIDGE.create();
      CCM_BRIDGE.start();
      LOGGER.info("CCM started");
      running = true;
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    try {
                      afterSuite();
                    } catch (Exception e) {
                      // silently remove as may have already been removed.
                    }
                  }));
    }
  }

  @Override
  public synchronized void afterSuite() {
    if (running) {
      LOGGER.info("Stopping CCM");
      CCM_BRIDGE.stop();
      CCM_BRIDGE.remove();
      running = false;
      LOGGER.info("CCM stopped");
    }
  }
}
