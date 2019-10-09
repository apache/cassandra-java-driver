/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultScriptGraphStatement extends GraphStatementBase<ScriptGraphStatement>
    implements ScriptGraphStatement {

  private final String script;
  private final Boolean isSystemQuery;
  private final NullAllowingImmutableMap<String, Object> queryParams;

  public DefaultScriptGraphStatement(
      String script,
      Map<String, Object> queryParams,
      Boolean isSystemQuery,
      Boolean isIdempotent,
      Duration timeout,
      Node node,
      long timestamp,
      DriverExecutionProfile executionProfile,
      String executionProfileName,
      Map<String, ByteBuffer> customPayload,
      String graphName,
      String traversalSource,
      String subProtocol,
      ConsistencyLevel consistencyLevel,
      ConsistencyLevel readConsistencyLevel,
      ConsistencyLevel writeConsistencyLevel) {
    super(
        isIdempotent,
        timeout,
        node,
        timestamp,
        executionProfile,
        executionProfileName,
        customPayload,
        graphName,
        traversalSource,
        subProtocol,
        consistencyLevel,
        readConsistencyLevel,
        writeConsistencyLevel);
    this.script = script;
    this.isSystemQuery = isSystemQuery;
    this.queryParams = NullAllowingImmutableMap.copyOf(queryParams);
  }

  //// Script GraphStatement level options

  @NonNull
  @Override
  public String getScript() {
    return script;
  }

  @NonNull
  @Override
  public ScriptGraphStatement setSystemQuery(@Nullable Boolean newValue) {
    return new DefaultScriptGraphStatement(
        script,
        queryParams,
        newValue,
        isIdempotent(),
        getTimeout(),
        getNode(),
        getTimestamp(),
        getExecutionProfile(),
        getExecutionProfileName(),
        getCustomPayload(),
        getGraphName(),
        getTraversalSource(),
        getSubProtocol(),
        getConsistencyLevel(),
        getReadConsistencyLevel(),
        getWriteConsistencyLevel());
  }

  @Nullable
  @Override
  public Boolean isSystemQuery() {
    return isSystemQuery;
  }

  @NonNull
  @Override
  public Map<String, Object> getQueryParams() {
    return this.queryParams;
  }

  @NonNull
  @Override
  public ScriptGraphStatement setQueryParam(@NonNull String name, @Nullable Object value) {
    NullAllowingImmutableMap.Builder<String, Object> newQueryParamsBuilder =
        NullAllowingImmutableMap.builder();
    for (Map.Entry<String, Object> entry : queryParams.entrySet()) {
      if (!entry.getKey().equals(name)) {
        newQueryParamsBuilder.put(entry.getKey(), entry.getValue());
      }
    }
    newQueryParamsBuilder.put(name, value);
    return setQueryParams(newQueryParamsBuilder.build());
  }

  @NonNull
  @Override
  public ScriptGraphStatement removeQueryParam(@NonNull String name) {
    if (!queryParams.containsKey(name)) {
      return this;
    } else {
      NullAllowingImmutableMap.Builder<String, Object> newQueryParamsBuilder =
          NullAllowingImmutableMap.builder();
      for (Map.Entry<String, Object> entry : queryParams.entrySet()) {
        if (!entry.getKey().equals(name)) {
          newQueryParamsBuilder.put(entry.getKey(), entry.getValue());
        }
      }
      return setQueryParams(newQueryParamsBuilder.build());
    }
  }

  private ScriptGraphStatement setQueryParams(Map<String, Object> newQueryParams) {
    return new DefaultScriptGraphStatement(
        script,
        newQueryParams,
        isSystemQuery,
        isIdempotent(),
        getTimeout(),
        getNode(),
        getTimestamp(),
        getExecutionProfile(),
        getExecutionProfileName(),
        getCustomPayload(),
        getGraphName(),
        getTraversalSource(),
        getSubProtocol(),
        getConsistencyLevel(),
        getReadConsistencyLevel(),
        getWriteConsistencyLevel());
  }

  @Override
  protected ScriptGraphStatement newInstance(
      Boolean isIdempotent,
      Duration timeout,
      Node node,
      long timestamp,
      DriverExecutionProfile executionProfile,
      String executionProfileName,
      Map<String, ByteBuffer> customPayload,
      String graphName,
      String traversalSource,
      String subProtocol,
      ConsistencyLevel consistencyLevel,
      ConsistencyLevel readConsistencyLevel,
      ConsistencyLevel writeConsistencyLevel) {
    return new DefaultScriptGraphStatement(
        script,
        queryParams,
        isSystemQuery,
        isIdempotent,
        timeout,
        node,
        timestamp,
        executionProfile,
        executionProfileName,
        customPayload,
        graphName,
        traversalSource,
        subProtocol,
        consistencyLevel,
        readConsistencyLevel,
        writeConsistencyLevel);
  }
}
