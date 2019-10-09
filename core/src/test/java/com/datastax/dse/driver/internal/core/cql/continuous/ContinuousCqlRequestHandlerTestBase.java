/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
