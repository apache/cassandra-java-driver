/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.api.core.cql;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.PagingIterable;

/**
 * The result of a synchronous CQL query.
 *
 * <p>See {@link PagingIterable} for a few generic explanations about the behavior of this object;
 * in particular, implementations are <b>not thread-safe</b>. They can only be iterated by the
 * thread that invoked {@code session.execute}.
 *
 * @see CqlSession#execute(Statement)
 * @see CqlSession#execute(String)
 */
public interface ResultSet extends PagingIterable<Row> {

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
