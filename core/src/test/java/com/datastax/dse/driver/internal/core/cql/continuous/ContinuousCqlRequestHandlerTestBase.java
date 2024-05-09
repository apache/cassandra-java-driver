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
package com.datastax.dse.driver.internal.core.cql.continuous;

import static com.datastax.dse.driver.api.core.config.DseDriverOption.CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES;
import static com.datastax.dse.driver.api.core.config.DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_FIRST_PAGE;
import static com.datastax.dse.driver.api.core.config.DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_OTHER_PAGES;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.internal.core.cql.CqlRequestHandlerTestBase;
import com.datastax.oss.driver.internal.core.cql.RequestHandlerTestHarness;
import java.time.Duration;

public abstract class ContinuousCqlRequestHandlerTestBase extends CqlRequestHandlerTestBase {

  static final Duration TIMEOUT_FIRST_PAGE = Duration.ofSeconds(2);
  static final Duration TIMEOUT_OTHER_PAGES = Duration.ofSeconds(1);

  protected RequestHandlerTestHarness.Builder continuousHarnessBuilder() {
    return new RequestHandlerTestHarness.Builder() {
      @Override
      public RequestHandlerTestHarness build() {
        RequestHandlerTestHarness harness = super.build();
        DriverExecutionProfile config = harness.getContext().getConfig().getDefaultProfile();
        when(config.getDuration(CONTINUOUS_PAGING_TIMEOUT_FIRST_PAGE))
            .thenReturn(TIMEOUT_FIRST_PAGE);
        when(config.getDuration(CONTINUOUS_PAGING_TIMEOUT_OTHER_PAGES))
            .thenReturn(TIMEOUT_OTHER_PAGES);
        when(config.getInt(CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES)).thenReturn(4);
        return harness;
      }
    };
  }
}
