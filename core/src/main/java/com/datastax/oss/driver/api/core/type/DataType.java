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
package com.datastax.oss.driver.api.core.type;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.detach.Detachable;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The type of a CQL column, field or function argument.
 *
 * <p>The default implementations returned by the driver are immutable and serializable. If you
 * write your own implementations, they should at least be thread-safe; serializability is not
 * mandatory, but recommended for use with some 3rd-party tools like Apache Spark &trade;.
 *
 * @see DataTypes
 */
public interface DataType extends Detachable {
  /** The code of the data type in the native protocol specification. */
  int getProtocolCode();

  /**
   * Builds an appropriate representation for use in a CQL query.
   *
   * @param includeFrozen whether to include the {@code frozen<...>} keyword if applicable. This
   *     will need to be set depending on where the result is used: for example, {@code CREATE
   *     TABLE} statements use the frozen keyword, whereas it should never appear in {@code CREATE
   *     FUNCTION}.
   * @param pretty whether to pretty-print UDT names (as described in {@link
   *     CqlIdentifier#asCql(boolean)}.
   */
  @NonNull
  String asCql(boolean includeFrozen, boolean pretty);
}
