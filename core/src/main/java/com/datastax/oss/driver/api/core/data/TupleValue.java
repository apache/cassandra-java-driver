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
package com.datastax.oss.driver.api.core.data;

import com.datastax.oss.driver.api.core.detach.Detachable;
import com.datastax.oss.driver.api.core.type.TupleType;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Driver-side representation of a CQL {@code tuple} value.
 *
 * <p>It is an ordered set of anonymous, typed fields.
 *
 * <p>A tuple value is attached if and only if its type is attached (see {@link Detachable}).
 *
 * <p>The default implementation returned by the driver is mutable and serializable. If you write
 * your own implementation, serializability is not mandatory, but recommended for use with some
 * 3rd-party tools like Apache Spark &trade;.
 */
public interface TupleValue extends GettableByIndex, SettableByIndex<TupleValue> {

  @NonNull
  TupleType getType();

  /**
   * Returns a string representation of the contents of this tuple.
   *
   * <p>This produces a CQL literal, for example:
   *
   * <pre>
   * (1,'test')
   * </pre>
   *
   * Notes:
   *
   * <ul>
   *   <li>This method does not sanitize its output in any way. In particular, no effort is made to
   *       limit output size: all fields are included, and large strings or blobs will be appended
   *       as-is.
   *   <li>Be mindful of how you expose the result. For example, in high-security environments, it
   *       might be undesirable to leak data in application logs.
   * </ul>
   */
  @NonNull
  default String getFormattedContents() {
    return codecRegistry().codecFor(getType(), TupleValue.class).format(this);
  }

  /**
   * Returns an abstract representation of this object, <b>that may not include the tuple's
   * contents</b>.
   *
   * <p>The driver's built-in {@link TupleValue} implementation returns the default format of {@link
   * Object#toString()}: the class name, followed by the at-sign and the hash code of the object.
   *
   * <p>Omitting the contents was a deliberate choice, because we feel it would make it too easy to
   * accidentally leak data (e.g. in application logs). If you want the contents, use {@link
   * #getFormattedContents()}.
   */
  @Override
  String toString();
}
