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
package com.datastax.oss.driver.internal.mapper.processor.entity;

import com.datastax.oss.driver.api.mapper.entity.naming.NameConverter;
import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import com.datastax.oss.driver.internal.core.util.Strings;
import com.datastax.oss.driver.shaded.guava.common.base.CaseFormat;

/**
 * Handles the {@link NamingConvention built-in naming conventions}.
 *
 * <p>Unlike user-provided {@link NameConverter}s, this code is invoked at compile time by the
 * mapper processor (built-in conventions are applied directly in the generated code).
 */
public class BuiltInNameConversions {

  public static String toCassandraName(String javaName, NamingConvention convention) {
    switch (convention) {
      case CASE_INSENSITIVE:
        return javaName;
      case EXACT_CASE:
        return Strings.doubleQuote(javaName);
      case LOWER_CAMEL_CASE:
        // Piggy-back on Guava's CaseFormat. Note that we indicate that the input is upper-camel
        // when in reality it can be lower-camel for a property name, but CaseFormat is lenient and
        // handles that correctly.
        return Strings.doubleQuote(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL, javaName));
      case UPPER_CAMEL_CASE:
        return Strings.doubleQuote(CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, javaName));
      case SNAKE_CASE_INSENSITIVE:
        return CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, javaName);
      case UPPER_SNAKE_CASE:
        return Strings.doubleQuote(
            CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, javaName));
      case UPPER_CASE:
        return Strings.doubleQuote(javaName.toUpperCase());
      default:
        throw new AssertionError("Unsupported convention: " + convention);
    }
  }
}
