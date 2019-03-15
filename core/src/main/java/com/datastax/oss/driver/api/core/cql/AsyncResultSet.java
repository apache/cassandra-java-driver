/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.api.core.cql;

import com.datastax.oss.driver.api.core.AsyncPagingIterable;
import com.datastax.oss.driver.api.core.CqlSession;

/**
 * The result of an asynchronous CQL query.
 *
 * @see CqlSession#executeAsync(Statement)
 * @see CqlSession#executeAsync(String)
 */
public interface AsyncResultSet extends AsyncPagingIterable<Row, AsyncResultSet> {

  // overridden to amend the javadocs:
  /**
   * {@inheritDoc}
   *
   * <p>This is equivalent to calling:
   *
   * <pre>
   *   this.iterator().next().getBoolean("[applied]")
   * </pre>
   *
   * Except that this method peeks at the next row without consuming it.
   */
  @Override
  boolean wasApplied();
}
