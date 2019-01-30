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

import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.mapper.processor.GeneratedNames;
import com.datastax.oss.driver.internal.mapper.processor.util.NameIndex;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.lang.model.element.Modifier;

/** Helper class for {@link BindableHandlingSharedCode#addGenericTypeConstant(TypeName)}. */
public class GenericTypeConstantGenerator {

  private final NameIndex nameIndex;

  public GenericTypeConstantGenerator(NameIndex nameIndex) {
    this.nameIndex = nameIndex;
  }

  private final Map<TypeName, String> typeConstantNames = new LinkedHashMap<>();

  public String add(TypeName type) {
    return typeConstantNames.computeIfAbsent(
        type, k -> nameIndex.uniqueField(GeneratedNames.GENERIC_TYPE_CONSTANT));
  }

  public void generate(TypeSpec.Builder classBuilder) {
    for (Map.Entry<TypeName, String> entry : typeConstantNames.entrySet()) {
      TypeName typeParameter = entry.getKey();
      String name = entry.getValue();
      ParameterizedTypeName type =
          ParameterizedTypeName.get(ClassName.get(GenericType.class), typeParameter);
      classBuilder.addField(
          FieldSpec.builder(type, name, Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
              .initializer("new $T(){}", type)
              .build());
    }
  }
}
