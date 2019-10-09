/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import net.jcip.annotations.Immutable;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnectionException;
import org.apache.tinkerpop.gremlin.process.remote.traversal.RemoteTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

@Immutable
public class DseGraphRemoteConnection implements RemoteConnection {

  private final DseSession dseSession;
  private final DriverExecutionProfile executionProfile;
  private final String executionProfileName;

  public DseGraphRemoteConnection(
      DseSession dseSession, DriverExecutionProfile executionProfile, String executionProfileName) {
    this.dseSession = dseSession;
    this.executionProfile = executionProfile;
    this.executionProfileName = executionProfileName;
  }

  @Override
  @SuppressWarnings("deprecation")
  public <E> Iterator<Traverser.Admin<E>> submit(Traversal<?, E> traversal)
      throws RemoteConnectionException {
    return submit(traversal.asAdmin().getBytecode());
  }

  @Override
  @SuppressWarnings({"deprecation", "unchecked"})
  public <E> RemoteTraversal<?, E> submit(Bytecode bytecode) throws RemoteConnectionException {
    try {
      return (RemoteTraversal<?, E>) submitAsync(bytecode).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RemoteConnectionException(e);
    }
  }

  @Override
  public <E> CompletableFuture<RemoteTraversal<?, E>> submitAsync(Bytecode bytecode)
      throws RemoteConnectionException {
    return dseSession
        .executeAsync(new BytecodeGraphStatement(bytecode, executionProfile, executionProfileName))
        .toCompletableFuture()
        .thenApply(DseGraphTraversal::new);
  }

  @Override
  public void close() throws Exception {
    // do not close the DseSession here.
  }
}
