/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.querybuilder.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import edu.umd.cs.findbugs.annotations.NonNull;

public interface CreateDseAggregateEnd extends BuildableQuery {

  /**
   * Adds INITCOND to the aggregate query. Defines the initial condition, values, of the first
   * parameter in the SFUNC.
   */
  @NonNull
  CreateDseAggregateEnd withInitCond(@NonNull Term term);

  /**
   * Adds FINALFUNC to the create aggregate query. This is used to specify what type is returned
   * from the state function.
   */
  @NonNull
  CreateDseAggregateEnd withFinalFunc(@NonNull CqlIdentifier finalFunc);

  /**
   * Shortcut for {@link #withFinalFunc(CqlIdentifier)
   * withFinalFunc(CqlIdentifier.fromCql(finalFuncName))}.
   */
  @NonNull
  default CreateDseAggregateEnd withFinalFunc(@NonNull String finalFuncName) {
    return withFinalFunc(CqlIdentifier.fromCql(finalFuncName));
  }

  /**
   * Adds "DETERMINISTIC" to create aggregate specification. This is used to specify that this
   * aggregate always returns the same output for a given input. Requires an initial condition and
   * returns a single value.
   */
  @NonNull
  CreateDseAggregateEnd deterministic();
}
