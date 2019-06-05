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
package com.datastax.oss.driver.internal.mapper.processor.dao;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import javax.lang.model.element.Modifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingGenerator {
  private final boolean logsEnabled;

  public LoggingGenerator(boolean logsEnabled) {
    this.logsEnabled = logsEnabled;
  }

  /**
   * Generates a logger in a constant, such as:
   *
   * <pre>
   *   private static final Logger LOG = LoggerFactory.getLogger(Foobar.class);
   * </pre>
   *
   * @param classBuilder where to generate.
   * @param className the name of the class ({@code Foobar}).
   */
  public void addLoggerField(TypeSpec.Builder classBuilder, ClassName className) {
    if (logsEnabled) {
      classBuilder.addField(
          FieldSpec.builder(
                  ClassName.get(Logger.class),
                  "LOG",
                  Modifier.PRIVATE,
                  Modifier.FINAL,
                  Modifier.STATIC)
              .initializer("$T.getLogger($T.class)", LoggerFactory.class, className)
              .build());
    }
  }

  /**
   * Generates a debug log statement, such as:
   *
   * <pre>
   *   LOG.debug("setting {} = {}", key, value);
   * </pre>
   *
   * <p>This assumes that {@link #addLoggerField(TypeSpec.Builder, ClassName)} has already been
   * called for the class where this is generated.
   *
   * @param builder where to generate.
   * @param template the message ({@code "setting {} = {}"}).
   * @param arguments the arguments ({@code key} and {@code value}).
   */
  public void debug(MethodSpec.Builder builder, String template, CodeBlock... arguments) {
    if (logsEnabled) {
      builder.addCode("$[LOG.debug($S", template);
      for (CodeBlock argument : arguments) {
        builder.addCode(",\n$L", argument);
      }
      builder.addCode(");$]\n");
    }
  }
}
