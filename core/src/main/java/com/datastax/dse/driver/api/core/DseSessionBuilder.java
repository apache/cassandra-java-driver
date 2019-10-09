/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core;

import com.datastax.dse.driver.internal.core.session.DefaultDseSession;
import com.datastax.oss.driver.api.core.CqlSession;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.NotThreadSafe;

/**
 * Helper class to build a {@link DseSession} instance.
 *
 * <p>This class is mutable and not thread-safe.
 */
@NotThreadSafe
public class DseSessionBuilder extends DseSessionBuilderBase<DseSessionBuilder, DseSession> {

  @NonNull
  @Override
  protected DseSession wrap(@NonNull CqlSession defaultSession) {
    return new DefaultDseSession(defaultSession);
  }
}
