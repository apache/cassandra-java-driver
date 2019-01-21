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

import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.internal.mapper.processor.dao.DaoImplementationGenerator;
import com.datastax.oss.driver.internal.mapper.processor.mapper.DaoFactoryMethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.mapper.MapperBuilderGenerator;
import com.datastax.oss.driver.internal.mapper.processor.mapper.MapperGenerator;
import com.datastax.oss.driver.internal.mapper.processor.mapper.MapperImplementationGenerator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

public class DefaultCodeGeneratorFactory implements CodeGeneratorFactory {

  private final ProcessorContext context;

  public DefaultCodeGeneratorFactory(ProcessorContext context) {
    this.context = context;
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
  public PartialClassGenerator newDaoMethodFactory(ExecutableElement methodElement) {
    TypeMirror returnType = methodElement.getReturnType();
    if (returnType.getKind() != TypeKind.DECLARED) {
      return null;
    }
    DeclaredType declaredReturnType = (DeclaredType) returnType;
    if (declaredReturnType.getTypeArguments().isEmpty()) {
      Element returnTypeElement = declaredReturnType.asElement();
      return (returnTypeElement.getKind() != ElementKind.INTERFACE
              || returnTypeElement.getAnnotation(Dao.class) == null)
          ? null
          : new DaoFactoryMethodGenerator(
              methodElement,
              GeneratedNames.daoImplementation(((TypeElement) returnTypeElement)),
              false,
              context);
    } else if (isFuture(declaredReturnType.asElement())
        && declaredReturnType.getTypeArguments().size() == 1) {
      TypeMirror typeArgument = declaredReturnType.getTypeArguments().get(0);
      if (typeArgument.getKind() != TypeKind.DECLARED) {
        return null;
      }
      Element typeArgumentElement = ((DeclaredType) typeArgument).asElement();
      return (typeArgumentElement.getKind() != ElementKind.INTERFACE
              || typeArgumentElement.getAnnotation(Dao.class) == null)
          ? null
          : new DaoFactoryMethodGenerator(
              methodElement,
              GeneratedNames.daoImplementation(((TypeElement) typeArgumentElement)),
              true,
              context);
    } else {
      return null;
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

  private boolean isFuture(Element element) {
    return context.getClassUtils().isSame(element, CompletionStage.class)
        || context.getClassUtils().isSame(element, CompletableFuture.class);
  }
}
