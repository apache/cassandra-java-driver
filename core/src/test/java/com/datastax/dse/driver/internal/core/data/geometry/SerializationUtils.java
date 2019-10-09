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
package com.datastax.dse.driver.internal.core.data.geometry;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.data.geometry.Geometry;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class SerializationUtils {

  public static Object serializeAndDeserialize(Geometry geometry)
      throws IOException, ClassNotFoundException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(baos);

    out.writeObject(geometry);

    byte[] bytes = baos.toByteArray();
    if (!(geometry instanceof Distance)) {
      byte[] wkb = Bytes.getArray(geometry.asWellKnownBinary());
      assertThat(bytes).containsSequence(wkb);
    }
    ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes));
    return in.readObject();
  }
}
