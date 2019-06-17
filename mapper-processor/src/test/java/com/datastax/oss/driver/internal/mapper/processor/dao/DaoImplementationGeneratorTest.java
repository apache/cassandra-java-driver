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

import com.datastax.oss.driver.api.core.cql.Row;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import javax.lang.model.element.Modifier;
import org.junit.Test;

public class DaoImplementationGeneratorTest extends DaoMethodGeneratorTest {

  @Test
  public void should_fail_if_method_is_not_annotated() {
    should_fail_with_expected_error(
        MethodSpec.methodBuilder("get")
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ParameterSpec.builder(Row.class, "source").build())
            .returns(ENTITY_CLASS_NAME)
            .build(),
        "Unrecognized method signature: no implementation will be generated");
  }

  @Test
  public void should_ignore_static_methods() {
    should_succeed_without_warnings(
        MethodSpec.methodBuilder("doNothing")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .build());
  }

  @Test
  public void should_ignore_default_methods() {
    should_succeed_without_warnings(
        MethodSpec.methodBuilder("doNothing")
            .addModifiers(Modifier.PUBLIC, Modifier.DEFAULT)
            .build());
  }
}
