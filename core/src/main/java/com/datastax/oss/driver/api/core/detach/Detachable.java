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
package com.datastax.oss.driver.api.core.detach;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.Data;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Defines the contract of an object that can be detached and reattached to a driver instance.
 *
 * <p>The driver's {@link Data data structure} types (such as rows, tuples and UDT values) store
 * their data as byte buffers, and only decode it on demand, when the end user accesses a particular
 * column or field.
 *
 * <p>Decoding requires a {@link ProtocolVersion} (because the encoded format might change across
 * versions), and a {@link CodecRegistry} (because the user might ask us to decode to a custom
 * type).
 *
 * <ul>
 *   <li>When a data container was obtained from a driver instance (for example, reading a row from
 *       a result set, or reading a value from a UDT column), it is <em>attached</em>: its protocol
 *       version and registry are those of the driver.
 *   <li>When it is created manually by the user (for example, creating an instance from a manually
 *       created {@link TupleType}), it is <em>detached</em>: it uses {@link
 *       ProtocolVersion#DEFAULT} and {@link CodecRegistry#DEFAULT}.
 * </ul>
 *
 * The only way an attached object can become detached is if it is serialized and deserialized
 * (referring to Java serialization).
 *
 * <p>A detached object can be reattached to a driver instance. This is done automatically if you
 * pass the object to one of the driver methods, for example if you use a manually created tuple as
 * a query parameter.
 */
public interface Detachable {
  boolean isDetached();

  void attach(@NonNull AttachmentPoint attachmentPoint);
}
