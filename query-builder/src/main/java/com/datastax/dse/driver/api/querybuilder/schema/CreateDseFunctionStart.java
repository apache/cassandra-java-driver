/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.querybuilder.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;

public interface CreateDseFunctionStart {

  /**
   * Adds IF NOT EXISTS to the create function specification. This indicates that the function
   * should not be created if it already exists.
   */
  @NonNull
  CreateDseFunctionStart ifNotExists();

  /**
   * Adds OR REPLACE to the create function specification. This indicates that the function should
   * replace an existing function with the same name if it exists.
   */
  @NonNull
  CreateDseFunctionStart orReplace();

  /**
   * Adds a parameter definition in the CREATE FUNCTION statement.
   *
   * <p>Parameter keys are added in the order of their declaration.
   *
   * <p>To create the data type, use the constants and static methods in {@link DataTypes}, or
   * {@link SchemaBuilder#udt(CqlIdentifier, boolean)}.
   */
  @NonNull
  CreateDseFunctionStart withParameter(
      @NonNull CqlIdentifier paramName, @NonNull DataType paramType);

  /**
   * Shortcut for {@link #withParameter(CqlIdentifier, DataType)
   * withParameter(CqlIdentifier.asCql(paramName), dataType)}.
   */
  @NonNull
  default CreateDseFunctionStart withParameter(
      @NonNull String paramName, @NonNull DataType paramType) {
    return withParameter(CqlIdentifier.fromCql(paramName), paramType);
  }

  /**
   * Adds RETURNS NULL ON NULL to the create function specification. This indicates that the body of
   * the function should be skipped when null input is provided.
   */
  @NonNull
  CreateDseFunctionWithNullOption returnsNullOnNull();

  /**
   * Adds CALLED ON NULL to the create function specification. This indicates that the body of the
   * function not be skipped when null input is provided.
   */
  @NonNull
  CreateDseFunctionWithNullOption calledOnNull();
}
