/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.graph;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe
public abstract class GraphStatementBuilderBase<
    SelfT extends GraphStatementBuilderBase<SelfT, StatementT>,
    StatementT extends GraphStatement<StatementT>> {

  @SuppressWarnings({"unchecked"})
  private final SelfT self = (SelfT) this;

  protected Boolean isIdempotent;
  protected Duration timeout;
  protected Node node;
  protected long timestamp;
  protected DriverExecutionProfile executionProfile;
  protected String executionProfileName;
  private NullAllowingImmutableMap.Builder<String, ByteBuffer> customPayloadBuilder;
  protected String graphName;
  protected String traversalSource;
  protected String subProtocol;
  protected ConsistencyLevel consistencyLevel;
  protected ConsistencyLevel readConsistencyLevel;
  protected ConsistencyLevel writeConsistencyLevel;

  protected GraphStatementBuilderBase() {
    // nothing to do
  }

  protected GraphStatementBuilderBase(StatementT template) {
    this.isIdempotent = template.isIdempotent();
    this.timeout = template.getTimeout();
    this.node = template.getNode();
    this.timestamp = template.getTimestamp();
    this.executionProfile = template.getExecutionProfile();
    this.executionProfileName = template.getExecutionProfileName();
    if (!template.getCustomPayload().isEmpty()) {
      this.customPayloadBuilder =
          NullAllowingImmutableMap.<String, ByteBuffer>builder()
              .putAll(template.getCustomPayload());
    }
    this.graphName = template.getGraphName();
    this.traversalSource = template.getTraversalSource();
    this.subProtocol = template.getSubProtocol();
    this.consistencyLevel = template.getConsistencyLevel();
    this.readConsistencyLevel = template.getReadConsistencyLevel();
    this.writeConsistencyLevel = template.getWriteConsistencyLevel();
  }

  /** @see GraphStatement#setIdempotent(Boolean) */
  @NonNull
  public SelfT setIdempotence(@Nullable Boolean idempotent) {
    this.isIdempotent = idempotent;
    return self;
  }

  /** @see GraphStatement#setTimeout(Duration) */
  @NonNull
  public SelfT setTimeout(@Nullable Duration timeout) {
    this.timeout = timeout;
    return self;
  }

  /** @see GraphStatement#setNode(Node) */
  @NonNull
  public SelfT setNode(@Nullable Node node) {
    this.node = node;
    return self;
  }

  /** @see GraphStatement#setTimestamp(long) */
  @NonNull
  public SelfT setTimestamp(long timestamp) {
    this.timestamp = timestamp;
    return self;
  }

  /** @see GraphStatement#setExecutionProfileName(String) */
  @NonNull
  public SelfT setExecutionProfileName(@Nullable String executionProfileName) {
    this.executionProfileName = executionProfileName;
    return self;
  }

  /** @see GraphStatement#setExecutionProfile(DriverExecutionProfile) */
  @NonNull
  public SelfT setExecutionProfile(@Nullable DriverExecutionProfile executionProfile) {
    this.executionProfile = executionProfile;
    this.executionProfileName = null;
    return self;
  }

  /** @see GraphStatement#setCustomPayload(Map) */
  @NonNull
  public SelfT addCustomPayload(@NonNull String key, @Nullable ByteBuffer value) {
    if (customPayloadBuilder == null) {
      customPayloadBuilder = NullAllowingImmutableMap.builder();
    }
    customPayloadBuilder.put(key, value);
    return self;
  }

  /** @see GraphStatement#setCustomPayload(Map) */
  @NonNull
  public SelfT clearCustomPayload() {
    customPayloadBuilder = null;
    return self;
  }

  /** @see GraphStatement#setGraphName(String) */
  @NonNull
  public SelfT setGraphName(@Nullable String graphName) {
    this.graphName = graphName;
    return self;
  }

  /** @see GraphStatement#setTraversalSource(String) */
  @NonNull
  public SelfT setTraversalSource(@Nullable String traversalSource) {
    this.traversalSource = traversalSource;
    return self;
  }

  /** @see GraphStatement#setSubProtocol(String) */
  @NonNull
  public SelfT setSubProtocol(@Nullable String subProtocol) {
    this.subProtocol = subProtocol;
    return self;
  }

  /** @see GraphStatement#setConsistencyLevel(ConsistencyLevel) */
  @NonNull
  public SelfT setConsistencyLevel(@Nullable ConsistencyLevel consistencyLevel) {
    this.consistencyLevel = consistencyLevel;
    return self;
  }

  /** @see GraphStatement#setReadConsistencyLevel(ConsistencyLevel) */
  @NonNull
  public SelfT setReadConsistencyLevel(@Nullable ConsistencyLevel readConsistencyLevel) {
    this.readConsistencyLevel = readConsistencyLevel;
    return self;
  }

  /** @see GraphStatement#setWriteConsistencyLevel(ConsistencyLevel) */
  @NonNull
  public SelfT setWriteConsistencyLevel(@Nullable ConsistencyLevel writeConsistencyLevel) {
    this.writeConsistencyLevel = writeConsistencyLevel;
    return self;
  }

  @NonNull
  protected Map<String, ByteBuffer> buildCustomPayload() {
    return (customPayloadBuilder == null)
        ? NullAllowingImmutableMap.of()
        : customPayloadBuilder.build();
  }

  /** Create the statement with the configuration defined by this builder object. */
  @NonNull
  public abstract StatementT build();
}
