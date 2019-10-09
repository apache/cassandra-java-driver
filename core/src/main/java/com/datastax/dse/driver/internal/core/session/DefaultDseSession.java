/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.session;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.session.SessionWrapper;
import net.jcip.annotations.ThreadSafe;

/**
 * Implementation note: metadata methods perform unchecked casts, relying on the fact that the
 * metadata manager returns the appropriate runtime type.
 */
@ThreadSafe
public class DefaultDseSession extends SessionWrapper implements DseSession {

  public DefaultDseSession(Session delegate) {
    super(delegate);
  }
}
