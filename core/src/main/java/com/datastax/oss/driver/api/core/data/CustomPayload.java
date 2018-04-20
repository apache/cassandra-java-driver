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

import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.detach.Detachable;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.data.DefaultCustomPayload;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * A custom payload to be sent alongside the request.
 *
 * <p>This is used to exchange extra information with the server. By default, Cassandra doesn't do
 * anything with this, you'll only need it if you have a custom request handler on the server-side.
 */
public interface CustomPayload
    extends GettableByName, SettableByName<CustomPayload>, Detachable, Serializable {

  /** Creates an instance of the default implementation, with no {@link AttachmentPoint}. */
  static CustomPayload newInstance(Map<String, DataType> columns) {
    return new DefaultCustomPayload(columns, AttachmentPoint.NONE);
  }

  /** Creates an instance of the default implementation. */
  static CustomPayload newInstance(Map<String, DataType> columns, AttachmentPoint attachmentPoint) {
    return new DefaultCustomPayload(columns, attachmentPoint);
  }

  // TODO builder

  /** The values to send, in their serialized form. */
  Map<String, ByteBuffer> getValues();
}
