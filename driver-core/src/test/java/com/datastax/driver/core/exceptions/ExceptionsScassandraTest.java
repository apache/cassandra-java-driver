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
package com.datastax.driver.core.exceptions;

import static com.datastax.driver.core.ConsistencyLevel.LOCAL_ONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.scassandra.http.client.Result.read_request_timeout;
import static org.scassandra.http.client.Result.unavailable;
import static org.scassandra.http.client.Result.write_request_timeout;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metrics;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ScassandraTestBase;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TestUtils;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.Result;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ExceptionsScassandraTest extends ScassandraTestBase {

  protected Cluster cluster;
  protected Metrics.Errors errors;
  protected Host host1;
  protected Session session;

  @BeforeMethod(groups = "short")
  public void beforeMethod() {
    cluster = createClusterBuilder().withRetryPolicy(FallthroughRetryPolicy.INSTANCE).build();
    session = cluster.connect();
    host1 = TestUtils.findHost(cluster, 1);
    errors = cluster.getMetrics().getErrorMetrics();
  }

  @Test(groups = "short")
  public void should_throw_proper_unavailable_exception() {
    simulateError(unavailable);
    try {
      query();
      fail("expected an UnavailableException");
    } catch (UnavailableException e) {
      assertThat(e.getMessage())
          .isEqualTo(
              "Not enough replicas available for query at consistency LOCAL_ONE (1 required but only 0 alive)");
      assertThat(e.getConsistencyLevel()).isEqualTo(LOCAL_ONE);
      assertThat(e.getAliveReplicas()).isEqualTo(0);
      assertThat(e.getRequiredReplicas()).isEqualTo(1);
      assertThat(e.getEndPoint()).isEqualTo(host1.getEndPoint());
    }
  }

  @Test(groups = "short")
  public void should_throw_proper_read_timeout_exception() {
    simulateError(read_request_timeout);
    try {
      query();
      fail("expected a ReadTimeoutException");
    } catch (ReadTimeoutException e) {
      assertThat(e.getMessage())
          .isEqualTo(
              "Cassandra timeout during read query at consistency LOCAL_ONE (1 responses were required but only 0 replica responded)");
      assertThat(e.getConsistencyLevel()).isEqualTo(LOCAL_ONE);
      assertThat(e.getReceivedAcknowledgements()).isEqualTo(0);
      assertThat(e.getRequiredAcknowledgements()).isEqualTo(1);
      assertThat(e.getEndPoint()).isEqualTo(host1.getEndPoint());
    }
  }

  @Test(groups = "short")
  public void should_throw_proper_write_timeout_exception() {
    simulateError(write_request_timeout);
    try {
      query();
      fail("expected a WriteTimeoutException");
    } catch (WriteTimeoutException e) {
      assertThat(e.getMessage())
          .isEqualTo(
              "Cassandra timeout during SIMPLE write query at consistency LOCAL_ONE (1 replica were required but only 0 acknowledged the write)");
      assertThat(e.getConsistencyLevel()).isEqualTo(LOCAL_ONE);
      assertThat(e.getReceivedAcknowledgements()).isEqualTo(0);
      assertThat(e.getRequiredAcknowledgements()).isEqualTo(1);
      assertThat(e.getWriteType()).isEqualTo(WriteType.SIMPLE);
      assertThat(e.getEndPoint()).isEqualTo(host1.getEndPoint());
    }
  }

  private void simulateError(Result result) {
    primingClient.prime(
        PrimingRequest.queryBuilder()
            .withQuery("mock query")
            .withThen(then().withResult(result))
            .build());
  }

  protected ResultSet query() {
    return session.execute("mock query");
  }
}
