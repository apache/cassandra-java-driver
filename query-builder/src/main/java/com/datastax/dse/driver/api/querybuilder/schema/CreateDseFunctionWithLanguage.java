/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.querybuilder.schema;

import edu.umd.cs.findbugs.annotations.NonNull;

public interface CreateDseFunctionWithLanguage {

  /**
   * Adds AS to the create function specification. This is used to specify the body of the function.
   * Note that it is expected that the provided body is properly quoted as this method does not make
   * that decision for the user. For simple cases, one should wrap the input in single quotes, i.e.
   * <code>'myBody'</code>. If the body itself contains single quotes, one could use a
   * postgres-style string literal, which is surrounded in two dollar signs, i.e. <code>$$ myBody $$
   * </code>.
   */
  @NonNull
  CreateDseFunctionEnd as(@NonNull String functionBody);

  /**
   * Adds AS to the create function specification and quotes the function body. Assumes that if the
   * input body contains at least one single quote, to quote the body with two dollar signs, i.e.
   * <code>$$ myBody $$</code>, otherwise the body is quoted with single quotes, i.e. <code>
   * ' myBody '</code>. If the function body is already quoted {@link #as(String)} should be used
   * instead.
   */
  @NonNull
  default CreateDseFunctionEnd asQuoted(@NonNull String functionBody) {
    if (functionBody.contains("'")) {
      return as("$$ " + functionBody + " $$");
    } else {
      return as('\'' + functionBody + '\'');
    }
  }
}
