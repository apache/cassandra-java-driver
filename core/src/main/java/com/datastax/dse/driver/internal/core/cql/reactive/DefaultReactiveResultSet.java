/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.cql.reactive;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class DefaultReactiveResultSet extends ReactiveResultSetBase<AsyncResultSet> {

  public DefaultReactiveResultSet(Callable<CompletionStage<AsyncResultSet>> firstPage) {
    super(firstPage);
  }
}
