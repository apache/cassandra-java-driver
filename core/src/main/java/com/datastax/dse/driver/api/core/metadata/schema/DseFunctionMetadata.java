/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.metadata.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.metadata.schema.ScriptBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;

/**
 * Specialized function metadata for DSE.
 *
 * <p>It adds support for the DSE-specific {@link #isDeterministic() DETERMINISTIC} and {@link
 * #isMonotonic() MONOTONIC} keywords.
 */
public interface DseFunctionMetadata extends FunctionMetadata {

  /**
   * Indicates if this function is deterministic. A deterministic function means that given a
   * particular input, the function will always produce the same output.
   *
   * <p>NOTE: For versions of DSE older than 6.0.0, this method will always return false, regardless
   * of the actual function characteristics.
   *
   * @return Whether or not this function is deterministic.
   */
  boolean isDeterministic();

  /**
   * Indicates whether or not this function is monotonic on all of its arguments. This means that it
   * is either entirely non-increasing or non-decreasing.
   *
   * <p>A function can be either:
   *
   * <ul>
   *   <li>monotonic on all of its arguments. In that case, this method returns {@code true}, and
   *       {@link #getMonotonicArgumentNames()} returns all the arguments;
   *   <li>partially monotonic, meaning that partial application over some of the arguments is
   *       monotonic. Currently (DSE 6.0.0), CQL only allows partial monotonicity on <em>exactly one
   *       argument</em>. This may change in a future CQL version. In that case, this method returns
   *       {@code false}, and {@link #getMonotonicArgumentNames()} returns a singleton list;
   *   <li>not monotonic. In that case, this method return {@code false} and {@link
   *       #getMonotonicArgumentNames()} returns an empty list.
   * </ul>
   *
   * <p>Monotonicity is required to use the function in a GROUP BY clause.
   *
   * <p>NOTE: For versions of DSE older than 6.0.0, this method will always return false, regardless
   * of the actual function characteristics.
   *
   * @return whether or not this function is monotonic on all of its arguments.
   */
  boolean isMonotonic();

  /**
   * Returns a list of argument names that are monotonic.
   *
   * <p>See {@link #isMonotonic()} for explanations on monotonicity, and the possible values
   * returned by this method.
   *
   * <p>NOTE: For versions of DSE older than 6.0.0, this method will always return an empty list,
   * regardless of the actual function characteristics.
   *
   * @return the argument names that the function is monotonic on.
   */
  @NonNull
  List<CqlIdentifier> getMonotonicArgumentNames();

  @NonNull
  @Override
  default String describe(boolean pretty) {
    ScriptBuilder builder = new ScriptBuilder(pretty);
    builder
        .append("CREATE FUNCTION ")
        .append(getKeyspace())
        .append(".")
        .append(getSignature().getName())
        .append("(");
    boolean first = true;
    for (int i = 0; i < getSignature().getParameterTypes().size(); i++) {
      if (first) {
        first = false;
      } else {
        builder.append(",");
      }
      DataType type = getSignature().getParameterTypes().get(i);
      CqlIdentifier name = getParameterNames().get(i);
      builder.append(name).append(" ").append(type.asCql(false, pretty));
    }
    builder
        .append(")")
        .increaseIndent()
        .newLine()
        .append(isCalledOnNullInput() ? "CALLED ON NULL INPUT" : "RETURNS NULL ON NULL INPUT")
        .newLine()
        .append("RETURNS ")
        .append(getReturnType().asCql(false, true))
        .newLine();
    // handle deterministic and monotonic
    if (isDeterministic()) {
      builder.append("DETERMINISTIC").newLine();
    }
    if (isMonotonic()) {
      builder.append("MONOTONIC").newLine();
    } else if (!getMonotonicArgumentNames().isEmpty()) {
      builder.append("MONOTONIC ON ").append(getMonotonicArgumentNames().get(0)).newLine();
    }
    builder
        .append("LANGUAGE ")
        .append(getLanguage())
        .newLine()
        .append("AS '")
        .append(getBody())
        .append("';");
    return builder.build();
  }
}
