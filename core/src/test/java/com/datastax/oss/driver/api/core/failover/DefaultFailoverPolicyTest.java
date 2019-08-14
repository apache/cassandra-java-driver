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
package com.datastax.oss.driver.api.core.failover;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.InvalidKeyspaceException;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.failover.DefaultFailoverPolicy;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultFailoverPolicyTest {

  @Mock protected InternalDriverContext context;
  @Mock protected DriverConfig config;
  @Mock protected Statement statementOriginal;
  @Mock protected Statement statementNew;
  @Mock protected DriverExecutionProfile driverExecutionProfileOriginal;
  @Mock protected DriverExecutionProfile driverExecutionProfileNew;

  FailoverPolicy policy;

  @Before
  public void setup() {

    String originalProfile = "OriginalProfile";
    String failoverProfile = "NewProfile";

    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(driverExecutionProfileOriginal);
    when(config.getProfile(failoverProfile)).thenReturn(driverExecutionProfileNew);

    when(statementOriginal.getExecutionProfile()).thenReturn(driverExecutionProfileOriginal);
    when(statementOriginal.getExecutionProfileName()).thenReturn(originalProfile);
    when(statementOriginal.setExecutionProfile(driverExecutionProfileNew)).thenReturn(statementNew);

    when(driverExecutionProfileOriginal.isDefined(DefaultDriverOption.FAILOVER_POLICY_PROFILE))
        .thenReturn(true);
    when(driverExecutionProfileOriginal.getString(DefaultDriverOption.FAILOVER_POLICY_PROFILE))
        .thenReturn(failoverProfile);

    when(statementNew.getExecutionProfile()).thenReturn(driverExecutionProfileNew);

    this.policy = new DefaultFailoverPolicy(context, "ProfileName");
  }

  @Test
  public void should_return_true() {
    assertTrue(
        policy.shouldFailover(mock(AllNodesFailedException.class), mock(SimpleStatement.class)));
    assertTrue(
        policy.shouldFailover(mock(DriverTimeoutException.class), mock(SimpleStatement.class)));
    assertTrue(
        policy.shouldFailover(mock(InvalidKeyspaceException.class), mock(SimpleStatement.class)));
    assertTrue(
        policy.shouldFailover(mock(NoNodeAvailableException.class), mock(SimpleStatement.class)));
  }

  @Test
  public void should_return_false() {
    assertFalse(policy.shouldFailover(mock(AssertionError.class), mock(SimpleStatement.class)));
  }

  @Test
  public void should_change_profile() {
    assertEquals(
        policy.processRequest(statementOriginal).getExecutionProfile(), driverExecutionProfileNew);
  }
}
