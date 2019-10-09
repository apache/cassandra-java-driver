/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.servererrors;

import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A server-side error triggered when DSE can't send asynchronous results back to the client.
 *
 * <p>Currently, this is used when the client is unable to keep up with the rate during a continuous
 * paging session.
 *
 * <p>Note that the protocol specification refers to this error as {@code CLIENT_WRITE_FAILURE}; we
 * don't follow that terminology because it would be too misleading (this is not a client error, and
 * it doesn't occur while writing data to DSE).
 */
public class UnfitClientException extends CoordinatorException {

  public UnfitClientException(@NonNull Node coordinator, @NonNull String message) {
    this(coordinator, message, null, false);
  }

  private UnfitClientException(
      @NonNull Node coordinator,
      @NonNull String message,
      @Nullable ExecutionInfo executionInfo,
      boolean writableStackTrace) {
    super(coordinator, message, executionInfo, writableStackTrace);
  }

  @Override
  @NonNull
  public UnfitClientException copy() {
    return new UnfitClientException(getCoordinator(), getMessage(), getExecutionInfo(), true);
  }
}
