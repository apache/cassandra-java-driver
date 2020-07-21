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

import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.util.concurrent.LazyReference;
import com.datastax.oss.driver.internal.mapper.processor.dao.LoggingGenerator;
import com.datastax.oss.driver.internal.mapper.processor.entity.DefaultEntityFactory;
import com.datastax.oss.driver.internal.mapper.processor.entity.EntityFactory;
import com.datastax.oss.driver.internal.mapper.processor.util.Classes;
import javax.annotation.processing.Filer;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

/** This follows the same principles as {@link DefaultDriverContext}. */
public class DefaultProcessorContext implements ProcessorContext {

  private final LazyReference<CodeGeneratorFactory> codeGeneratorFactoryRef =
      new LazyReference<>(this::buildCodeGeneratorFactory);

  private final LazyReference<EntityFactory> entityFactoryRef =
      new LazyReference<>(this::buildEntityFactory);

  private final DecoratedMessager messager;
  private final Types typeUtils;
  private final Elements elementUtils;
  private final Classes classUtils;
  private final JavaPoetFiler filer;
  private final LoggingGenerator loggingGenerator;
  private final boolean customResultsEnabled;

  public DefaultProcessorContext(
      DecoratedMessager messager,
      Types typeUtils,
      Elements elementUtils,
      Filer filer,
      String indent,
      boolean logsEnabled,
      boolean customResultsEnabled) {
    this.messager = messager;
    this.typeUtils = typeUtils;
    this.elementUtils = elementUtils;
    this.classUtils = new Classes(typeUtils, elementUtils);
    this.filer = new JavaPoetFiler(filer, indent);
    this.loggingGenerator = new LoggingGenerator(logsEnabled);
    this.customResultsEnabled = customResultsEnabled;
  }

  protected CodeGeneratorFactory buildCodeGeneratorFactory() {
    return new DefaultCodeGeneratorFactory(this);
  }

  protected EntityFactory buildEntityFactory() {
    return new DefaultEntityFactory(this);
  }

  @Override
  public DecoratedMessager getMessager() {
    return messager;
  }

  @Override
  public Types getTypeUtils() {
    return typeUtils;
  }

  @Override
  public Elements getElementUtils() {
    return elementUtils;
  }

  @Override
  public Classes getClassUtils() {
    return classUtils;
  }

  @Override
  public JavaPoetFiler getFiler() {
    return filer;
  }

  @Override
  public CodeGeneratorFactory getCodeGeneratorFactory() {
    return codeGeneratorFactoryRef.get();
  }

  @Override
  public EntityFactory getEntityFactory() {
    return entityFactoryRef.get();
  }

  @Override
  public LoggingGenerator getLoggingGenerator() {
    return loggingGenerator;
  }

  @Override
  public boolean areCustomResultsEnabled() {
    return customResultsEnabled;
  }
}
