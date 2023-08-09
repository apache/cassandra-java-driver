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
package com.datastax.oss.driver.internal.core.loadbalancing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

// TODO fix unnecessary stubbing of config option in parent class (and stop using "silent" runner)
@RunWith(MockitoJUnitRunner.Silent.class)
public class DcInferringLoadBalancingPolicyDistanceTest
    extends BasicLoadBalancingPolicyDistanceTest {

  @Override
  public void should_report_LOCAL_when_dc_agnostic() {
    // This policy cannot operate when contact points are from different DCs
    Throwable error = catchThrowable(super::should_report_LOCAL_when_dc_agnostic);
    assertThat(error)
        .isInstanceOfSatisfying(
            IllegalStateException.class,
            ise ->
                assertThat(ise)
                    .hasMessageContaining(
                        "No local DC was provided, but the contact points are from different DCs")
                    .hasMessageContaining("node1=null")
                    .hasMessageContaining("node2=dc1")
                    .hasMessageContaining("node3=dc2"));
  }

  @NonNull
  @Override
  protected BasicLoadBalancingPolicy createPolicy() {
    return new DcInferringLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME);
  }
}
