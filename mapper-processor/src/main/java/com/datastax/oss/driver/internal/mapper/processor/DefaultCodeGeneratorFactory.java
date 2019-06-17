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

import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.internal.mapper.processor.dao.DaoImplementationGenerator;
import com.datastax.oss.driver.internal.mapper.processor.entity.EntityHelperGenerator;
import com.datastax.oss.driver.internal.mapper.processor.mapper.DaoFactoryMethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.mapper.MapperBuilderGenerator;
import com.datastax.oss.driver.internal.mapper.processor.mapper.MapperGenerator;
import com.datastax.oss.driver.internal.mapper.processor.mapper.MapperImplementationGenerator;
import com.datastax.oss.driver.internal.mapper.processor.mapper.MapperImplementationSharedCode;
import java.util.Optional;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;

public class DefaultCodeGeneratorFactory implements CodeGeneratorFactory {

  private final ProcessorContext context;

  public DefaultCodeGeneratorFactory(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public CodeGenerator newEntity(TypeElement classElement) {
    return new EntityHelperGenerator(classElement, context);
  }

  @Override
  public CodeGenerator newMapper(TypeElement interfaceElement) {
    return new MapperGenerator(interfaceElement, context);
  }

  @Override
  public CodeGenerator newMapperImplementation(TypeElement interfaceElement) {
    return new MapperImplementationGenerator(interfaceElement, context);
  }

  @Override
  public Optional<MethodGenerator> newMapperImplementationMethod(
      ExecutableElement methodElement, MapperImplementationSharedCode enclosingClass) {
    if (methodElement.getAnnotation(DaoFactory.class) != null) {
      return Optional.of(new DaoFactoryMethodGenerator(methodElement, enclosingClass, context));
    } else {
      return Optional.empty();
    }
  }

  @Override
  public CodeGenerator newMapperBuilder(TypeElement interfaceElement) {
    return new MapperBuilderGenerator(interfaceElement, context);
  }

  @Override
  public CodeGenerator newDao(TypeElement interfaceElement) {
    return new DaoImplementationGenerator(interfaceElement, context);
  }
}
