/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.cql.continuous.reactive;

import com.datastax.dse.driver.api.core.cql.continuous.ContinuousAsyncResultSet;
import com.datastax.dse.driver.api.core.cql.continuous.reactive.ContinuousReactiveResultSet;
import com.datastax.dse.driver.internal.core.cql.reactive.ReactiveResultSetBase;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class DefaultContinuousReactiveResultSet
    extends ReactiveResultSetBase<ContinuousAsyncResultSet> implements ContinuousReactiveResultSet {

  public DefaultContinuousReactiveResultSet(
      Callable<CompletionStage<ContinuousAsyncResultSet>> firstPage) {
    super(firstPage);
  }
}
