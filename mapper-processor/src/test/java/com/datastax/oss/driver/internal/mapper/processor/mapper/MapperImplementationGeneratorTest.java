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
package com.datastax.oss.driver.internal.mapper.processor.mapper;

import com.squareup.javapoet.MethodSpec;
import javax.lang.model.element.Modifier;
import org.junit.Test;

public class MapperImplementationGeneratorTest extends MapperMethodGeneratorTest {

  @Test
  public void should_fail_if_method_is_not_annotated() {
    should_fail_with_expected_error(
        "Unrecognized method signature: no implementation will be generated",
        MethodSpec.methodBuilder("productDao")
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(DAO_CLASS_NAME)
            .build());
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
