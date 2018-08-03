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
package com.datastax.driver.core;

import static com.datastax.driver.core.Assertions.assertThat;

import com.codahale.metrics.Meter;
import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.Test;

@CassandraVersion(
    "3.0") // Limit to recent Cassandra versions to avoid special-casing for old protocols
public class TrafficMetersTest extends CCMTestsSupport {

  @Test(groups = "short")
  public void should_measure_inbound_and_outbound_traffic() {
    Metrics metrics = session().getCluster().getMetrics();
    Meter bytesReceived = metrics.getBytesReceived();
    Meter bytesSent = metrics.getBytesSent();

    long bytesReceivedBefore = bytesReceived.getCount();
    long bytesSentBefore = bytesSent.getCount();

    SimpleStatement statement = new SimpleStatement("SELECT host_id FROM system.local");
    // Set serial CL to something non-default so request size estimate is accurate.
    statement.setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);
    int requestSize =
        statement.requestSizeInBytes(
            cluster().getConfiguration().getProtocolOptions().getProtocolVersion(),
            cluster().getConfiguration().getCodecRegistry());

    int responseSize =
        9 // header
            + 4 // kind (ROWS)
            + 4 // flags
            + 4 // column count
            + CBUtil.sizeOfString("system")
            + CBUtil.sizeOfString("local") // global table specs
            + CBUtil.sizeOfString("host_id") // column name
            + 2 // column type (uuid)
            + 4 // row count
            + 4
            + 16; // uuid length + uuid

    for (int i = 0; i < 1000; i++) {
      session().execute(statement);
    }

    // Do not check for an exact value, in case there were heartbeats or control queries
    assertThat(bytesSent.getCount()).isGreaterThanOrEqualTo(bytesSentBefore + requestSize * 1000);
    assertThat(bytesReceived.getCount())
        .isGreaterThanOrEqualTo(bytesReceivedBefore + responseSize * 1000);
  }
}
