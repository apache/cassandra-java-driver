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
package com.datastax.oss.driver.internal.mapper.processor.util;

import java.beans.Introspector;
import java.util.Objects;

public class Capitalizer {

  /**
   * Lower cases the first character of a name, for example when inferring the name of a field from
   * the name of a getter stripped of its {@code get} prefix.
   *
   * <p>This method respects a weird corner case of the JavaBeans conventions: "in the (unusual)
   * special case when there is more than one character and both the first and second characters are
   * upper case, we leave it alone. Thus {@code FooBah} becomes {@code fooBah} and {@code X} becomes
   * {@code x}, but {@code URL} stays as {@code URL}.".
   */
  public static String decapitalize(String name) {
    return Introspector.decapitalize(Objects.requireNonNull(name));
  }

  /**
   * Upper cases the first character of a name, for example when inferring the name of a setter from
   * the name of a field.
   *
   * <p>Mirroring the behavior of {@link #decapitalize(String)}, this method returns the string
   * unchanged not only if the first character is uppercase, but also if the <em>second</em> is. For
   * example, if a field is named {@code cId}, we want to produce the setter name {@code setcId()},
   * not {@code setCId()}. Otherwise applying the process in reverse would produce the field name
   * {@code CId}.
   */
  public static String capitalize(String name) {
    Objects.requireNonNull(name);
    if (name.isEmpty()
        || Character.isUpperCase(name.charAt(0))
        || (name.length() > 1 && Character.isUpperCase(name.charAt(1)))) {
      return name;
    } else {
      char[] chars = name.toCharArray();
      chars[0] = Character.toUpperCase(chars[0]);
      return new String(chars);
    }
  }
}
