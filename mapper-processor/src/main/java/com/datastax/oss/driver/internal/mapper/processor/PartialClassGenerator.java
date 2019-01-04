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
package com.datastax.oss.driver.internal.mapper.processor;

import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;

/**
 * A construct that affects multiple elements in an existing generated file.
 *
 * <p>For example, a final field with the corresponding initialization statements in the constructor
 * and getter.
 */
public interface PartialClassGenerator {

  void addMembers(TypeSpec.Builder classBuilder);

  void addConstructorInstructions(MethodSpec.Builder constructorBuilder);
}
