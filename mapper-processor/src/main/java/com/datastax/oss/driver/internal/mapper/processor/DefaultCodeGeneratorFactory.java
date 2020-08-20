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
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.datastax.oss.driver.api.mapper.annotations.GetEntity;
import com.datastax.oss.driver.api.mapper.annotations.Increment;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.QueryProvider;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.SetEntity;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.driver.internal.mapper.processor.dao.DaoDeleteMethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.dao.DaoGetEntityMethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.dao.DaoImplementationGenerator;
import com.datastax.oss.driver.internal.mapper.processor.dao.DaoImplementationSharedCode;
import com.datastax.oss.driver.internal.mapper.processor.dao.DaoIncrementMethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.dao.DaoInsertMethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.dao.DaoQueryMethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.dao.DaoQueryProviderMethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.dao.DaoReturnTypeParser;
import com.datastax.oss.driver.internal.mapper.processor.dao.DaoSelectMethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.dao.DaoSetEntityMethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.dao.DaoUpdateMethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeParser;
import com.datastax.oss.driver.internal.mapper.processor.entity.EntityHelperGenerator;
import com.datastax.oss.driver.internal.mapper.processor.mapper.MapperBuilderGenerator;
import com.datastax.oss.driver.internal.mapper.processor.mapper.MapperDaoFactoryMethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.mapper.MapperGenerator;
import com.datastax.oss.driver.internal.mapper.processor.mapper.MapperImplementationGenerator;
import com.datastax.oss.driver.internal.mapper.processor.mapper.MapperImplementationSharedCode;
import java.util.Map;
import java.util.Optional;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;

public class DefaultCodeGeneratorFactory implements CodeGeneratorFactory {

  protected final ProcessorContext context;
  private final DaoReturnTypeParser daoReturnTypeParser;

  public DefaultCodeGeneratorFactory(ProcessorContext context) {
    this.context = context;
    this.daoReturnTypeParser = new DefaultDaoReturnTypeParser(context);
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
      ExecutableElement methodElement,
      TypeElement processedType,
      MapperImplementationSharedCode enclosingClass) {
    if (methodElement.getAnnotation(DaoFactory.class) != null) {
      return Optional.of(
          new MapperDaoFactoryMethodGenerator(methodElement, enclosingClass, context));
    } else {
      return Optional.empty();
    }
  }

  @Override
  public CodeGenerator newMapperBuilder(TypeElement interfaceElement) {
    return new MapperBuilderGenerator(interfaceElement, context);
  }

  @Override
  public CodeGenerator newDaoImplementation(TypeElement interfaceElement) {
    return new DaoImplementationGenerator(interfaceElement, context);
  }

  @Override
  public Optional<MethodGenerator> newDaoImplementationMethod(
      ExecutableElement methodElement,
      Map<Name, TypeElement> typeParameters,
      TypeElement processedType,
      DaoImplementationSharedCode enclosingClass) {
    if (methodElement.getAnnotation(SetEntity.class) != null) {
      return Optional.of(
          new DaoSetEntityMethodGenerator(
              methodElement, typeParameters, processedType, enclosingClass, context));
    } else if (methodElement.getAnnotation(Insert.class) != null) {
      return Optional.of(
          new DaoInsertMethodGenerator(
              methodElement, typeParameters, processedType, enclosingClass, context));
    } else if (methodElement.getAnnotation(GetEntity.class) != null) {
      return Optional.of(
          new DaoGetEntityMethodGenerator(
              methodElement, typeParameters, processedType, enclosingClass, context));
    } else if (methodElement.getAnnotation(Select.class) != null) {
      return Optional.of(
          new DaoSelectMethodGenerator(
              methodElement, typeParameters, processedType, enclosingClass, context));
    } else if (methodElement.getAnnotation(Delete.class) != null) {
      return Optional.of(
          new DaoDeleteMethodGenerator(
              methodElement, typeParameters, processedType, enclosingClass, context));
    } else if (methodElement.getAnnotation(Query.class) != null) {
      return Optional.of(
          new DaoQueryMethodGenerator(
              methodElement, typeParameters, processedType, enclosingClass, context));
    } else if (methodElement.getAnnotation(Update.class) != null) {
      return Optional.of(
          new DaoUpdateMethodGenerator(
              methodElement, typeParameters, processedType, enclosingClass, context));
    } else if (methodElement.getAnnotation(QueryProvider.class) != null) {
      return Optional.of(
          new DaoQueryProviderMethodGenerator(
              methodElement, typeParameters, processedType, enclosingClass, context));
    } else if (methodElement.getAnnotation(Increment.class) != null) {
      return Optional.of(
          new DaoIncrementMethodGenerator(
              methodElement, typeParameters, processedType, enclosingClass, context));
    } else {
      return Optional.empty();
    }
  }

  @Override
  public DaoReturnTypeParser getDaoReturnTypeParser() {
    return daoReturnTypeParser;
  }
}
