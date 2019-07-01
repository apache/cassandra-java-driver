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
package com.datastax.oss.driver.internal.mapper.processor.util.generation;

import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.api.core.data.SettableByName;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.util.NameIndex;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;

/**
 * Shared class-level code for {@link MethodGenerator} components that generate code that
 * manipulates {@link SettableByName} or {@link GettableByName} objects.
 *
 * <p>This allows method generators to create fields, ensuring that they will get reused if another
 * method generator also needs them.
 */
public interface BindableHandlingSharedCode {

  NameIndex getNameIndex();

  /**
   * Requests the generation of a constant holding the {@link GenericType} for the given type.
   *
   * <p>If this is called multiple times, only a single constant will be created.
   *
   * @return the name of the constant.
   */
  String addGenericTypeConstant(TypeName type);

  /**
   * Requests the generation of a field holding the {@link EntityHelper} that was generated for the
   * given entity class, along with the initializing code (where appropriate for this class).
   *
   * <p>If this is called multiple times, only a single field will be created.
   *
   * @return the name of the field.
   */
  String addEntityHelperField(ClassName entityClassName);
}
