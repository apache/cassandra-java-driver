/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.testinfra;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.config.DseDriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.session.SessionBuilder;

public class DseSessionBuilderInstantiator {
  public static SessionBuilder<?, ?> builder() {
    return DseSession.builder();
  }

  public static ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder() {
    return DseDriverConfigLoader.programmaticBuilder();
  }
}
