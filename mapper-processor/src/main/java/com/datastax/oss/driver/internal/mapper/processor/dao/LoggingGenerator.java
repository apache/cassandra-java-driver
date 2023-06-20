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

public class LoggingGenerator {

  // Reference these types by name. They are in the classpath but that is more of a workaround in
  // case they get accidentally referenced via driver core types (see JAVA-2863), the mapper
  // processor does not directly "use" SLF4J.
  private static final ClassName LOGGER_FACTORY_CLASS_NAME =
      ClassName.get("org.slf4j", "LoggerFactory");
  private static final ClassName LOGGER_CLASS_NAME = ClassName.get("org.slf4j", "Logger");

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
                  LOGGER_CLASS_NAME, "LOG", Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
              .initializer("$T.getLogger($T.class)", LOGGER_FACTORY_CLASS_NAME, className)
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
    log("debug", builder, template, arguments);
  }

  /**
   * Generates a warn log statement, such as:
   *
   * <pre>
   *   LOG.warn("setting {} = {}", key, value);
   * </pre>
   *
   * <p>This assumes that {@link #addLoggerField(TypeSpec.Builder, ClassName)} has already been
   * called for the class where this is generated.
   *
   * @param builder where to generate.
   * @param template the message ({@code "setting {} = {}"}).
   * @param arguments the arguments ({@code key} and {@code value}).
   */
  public void warn(MethodSpec.Builder builder, String template, CodeBlock... arguments) {
    log("warn", builder, template, arguments);
  }

  public void log(
      String logLevel, MethodSpec.Builder builder, String template, CodeBlock... arguments) {
    if (logsEnabled) {
      builder.addCode("$[LOG.$L($S", logLevel, template);
      for (CodeBlock argument : arguments) {
        builder.addCode(",\n$L", argument);
      }
      builder.addCode(");$]\n");
    }
  }
}
