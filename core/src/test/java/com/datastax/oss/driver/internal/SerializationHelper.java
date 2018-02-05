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
package com.datastax.oss.driver.internal;

import static org.assertj.core.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public abstract class SerializationHelper {

  public static <T> byte[] serialize(T t) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      ObjectOutputStream out = new ObjectOutputStream(bytes);
      out.writeObject(t);
      return bytes.toByteArray();
    } catch (Exception e) {
      fail("Unexpected error", e);
      throw new AssertionError(); // never reached
    }
  }

  // the calling code performs validations on the result, so this doesn't matter
  @SuppressWarnings("TypeParameterUnusedInFormals")
  public static <T> T deserialize(byte[] bytes) {
    try {
      ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes));
      @SuppressWarnings("unchecked")
      T t = (T) in.readObject();
      return t;
    } catch (Exception e) {
      fail("Unexpected error", e);
      throw new AssertionError(); // never reached
    }
  }

  public static <T> T serializeAndDeserialize(T t) {
    return deserialize(serialize(t));
  }
}
