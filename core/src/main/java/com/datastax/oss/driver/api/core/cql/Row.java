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

import com.datastax.oss.driver.api.core.data.GettableById;
import com.datastax.oss.driver.api.core.data.GettableByIndex;
import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.api.core.detach.Detachable;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A row from a CQL table.
 *
 * <p>The default implementation returned by the driver is immutable and serializable. If you write
 * your own implementation, it should at least be thread-safe; serializability is not mandatory, but
 * recommended for use with some 3rd-party tools like Apache Spark &trade;.
 */
public interface Row extends GettableByIndex, GettableByName, GettableById, Detachable {

  /** @return the column definitions contained in this result set. */
  @NonNull
  ColumnDefinitions getColumnDefinitions();

  /**
   * Returns a string representation of the contents of this row.
   *
   * <p>This produces a comma-separated list enclosed in square brackets. Each column is represented
   * by its name, followed by a column and the value as a CQL literal. For example:
   *
   * <pre>
   * [id:1, name:'test']
   * </pre>
   *
   * Notes:
   *
   * <ul>
   *   <li>This method does not sanitize its output in any way. In particular, no effort is made to
   *       limit output size: all columns are included, and large strings or blobs will be appended
   *       as-is.
   *   <li>Be mindful of how you expose the result. For example, in high-security environments, it
   *       might be undesirable to leak data in application logs.
   * </ul>
   */
  @NonNull
  default String getFormattedContents() {
    StringBuilder result = new StringBuilder("[");
    ColumnDefinitions definitions = getColumnDefinitions();
    for (int i = 0; i < definitions.size(); i++) {
      if (i > 0) {
        result.append(", ");
      }
      ColumnDefinition definition = definitions.get(i);
      String name = definition.getName().asCql(true);
      TypeCodec<Object> codec = codecRegistry().codecFor(definition.getType());
      Object value = codec.decode(getBytesUnsafe(i), protocolVersion());
      result.append(name).append(':').append(codec.format(value));
    }
    return result.append("]").toString();
  }

  /**
   * Returns an abstract representation of this object, <b>that may not include the row's
   * contents</b>.
   *
   * <p>The driver's built-in {@link Row} implementation returns the default format of {@link
   * Object#toString()}: the class name, followed by the at-sign and the hash code of the object.
   *
   * <p>Omitting the contents was a deliberate choice, because we feel it would make it too easy to
   * accidentally leak data (e.g. in application logs). If you want the contents, use {@link
   * #getFormattedContents()}.
   */
  @Override
  String toString();
}
