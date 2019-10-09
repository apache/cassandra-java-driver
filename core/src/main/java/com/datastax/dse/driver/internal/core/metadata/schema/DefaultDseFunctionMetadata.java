/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.metadata.schema;

import com.datastax.dse.driver.api.core.metadata.schema.DseFunctionMetadata;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultFunctionMetadata;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Objects;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultDseFunctionMetadata extends DefaultFunctionMetadata
    implements DseFunctionMetadata {

  private final boolean deterministic;
  private final boolean monotonic;
  @NonNull private final List<CqlIdentifier> monotonicArgumentNames;

  public DefaultDseFunctionMetadata(
      @NonNull CqlIdentifier keyspace,
      @NonNull FunctionSignature signature,
      @NonNull List<CqlIdentifier> parameterNames,
      @NonNull String body,
      boolean calledOnNullInput,
      @NonNull String language,
      @NonNull DataType returnType,
      boolean deterministic,
      boolean monotonic,
      @NonNull List<CqlIdentifier> monotonicArgumentNames) {
    super(keyspace, signature, parameterNames, body, calledOnNullInput, language, returnType);
    // set DSE extension attributes
    this.deterministic = deterministic;
    this.monotonic = monotonic;
    this.monotonicArgumentNames = monotonicArgumentNames;
  }

  @Override
  public boolean isDeterministic() {
    return this.deterministic;
  }

  @Override
  public boolean isMonotonic() {
    return this.monotonic;
  }

  @NonNull
  @Override
  public List<CqlIdentifier> getMonotonicArgumentNames() {
    return this.monotonicArgumentNames;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof DseFunctionMetadata) {
      DseFunctionMetadata that = (DseFunctionMetadata) other;
      return Objects.equals(this.getKeyspace(), that.getKeyspace())
          && Objects.equals(this.getSignature(), that.getSignature())
          && Objects.equals(this.getParameterNames(), that.getParameterNames())
          && Objects.equals(this.getBody(), that.getBody())
          && this.isCalledOnNullInput() == that.isCalledOnNullInput()
          && Objects.equals(this.getLanguage(), that.getLanguage())
          && Objects.equals(this.getReturnType(), that.getReturnType())
          && this.deterministic == that.isDeterministic()
          && this.monotonic == that.isMonotonic()
          && Objects.equals(this.monotonicArgumentNames, that.getMonotonicArgumentNames());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getKeyspace(),
        getSignature(),
        getParameterNames(),
        getBody(),
        isCalledOnNullInput(),
        getLanguage(),
        getReturnType(),
        isDeterministic(),
        isMonotonic(),
        getMonotonicArgumentNames());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Function Name: ").append(this.getSignature().getName().asCql(false));
    sb.append(", Keyspace: ").append(this.getKeyspace());
    sb.append(", Language: ").append(this.getLanguage());
    sb.append(", Protocol Code: ").append(this.getReturnType().getProtocolCode());
    sb.append(", Deterministic: ").append(this.isDeterministic());
    sb.append(", Monotonic: ").append(this.isMonotonic());
    sb.append(", Monotonic On: ")
        .append(this.monotonicArgumentNames.isEmpty() ? "" : this.monotonicArgumentNames.get(0));
    return sb.toString();
  }
}
