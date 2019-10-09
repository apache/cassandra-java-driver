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

public interface CreateDseAggregateStart {
  /**
   * Adds IF NOT EXISTS to the create aggregate specification. This indicates that the aggregate
   * should not be created if it already exists.
   */
  @NonNull
  CreateDseAggregateStart ifNotExists();

  /**
   * Adds OR REPLACE to the create aggregate specification. This indicates that the aggregate should
   * replace an existing aggregate with the same name if it exists.
   */
  @NonNull
  CreateDseAggregateStart orReplace();

  /**
   * Adds a parameter definition in the CREATE AGGREGATE statement.
   *
   * <p>Parameter keys are added in the order of their declaration.
   *
   * <p>To create the data type, use the constants and static methods in {@link DataTypes}, or
   * {@link SchemaBuilder#udt(CqlIdentifier, boolean)}.
   */
  @NonNull
  CreateDseAggregateStart withParameter(@NonNull DataType paramType);

  /** Adds SFUNC to the create aggregate specification. This is the state function for each row. */
  @NonNull
  CreateDseAggregateStateFunc withSFunc(@NonNull CqlIdentifier sfuncName);

  /** Shortcut for {@link #withSFunc(CqlIdentifier) withSFunc(CqlIdentifier.fromCql(sfuncName))}. */
  @NonNull
  default CreateDseAggregateStateFunc withSFunc(@NonNull String sfuncName) {
    return withSFunc(CqlIdentifier.fromCql(sfuncName));
  }
}
