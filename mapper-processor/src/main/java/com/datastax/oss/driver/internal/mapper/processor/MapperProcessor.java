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

import com.datastax.oss.driver.api.mapper.annotations.MappingManager;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.google.auto.service.AutoService;
import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;

@AutoService(Processor.class)
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class MapperProcessor extends AbstractProcessor {

  private DecoratedMessager messager;
  private Filer filer;

  @Override
  public synchronized void init(ProcessingEnvironment processingEnvironment) {
    super.init(processingEnvironment);
    messager = new DecoratedMessager(processingEnvironment.getMessager());
    filer = processingEnvironment.getFiler();
  }

  @Override
  public boolean process(
      Set<? extends TypeElement> annotations, RoundEnvironment roundEnvironment) {
    for (Element element : roundEnvironment.getElementsAnnotatedWith(MappingManager.class)) {
      if (element.getKind() != ElementKind.INTERFACE) {
        messager.error(
            element,
            "Only interfaces can be annotated with %s",
            MappingManager.class.getSimpleName());
      } else {
        // Safe cast given that we checked the kind above
        TypeElement typeElement = (TypeElement) element;
        try {
          new ManagerGenerator(typeElement).generate(filer);
        } catch (Exception e) {
          messager.error(
              element, "Unexpected error while writing generated code: %s", e.toString());
        }
      }
    }
    return true;
  }

  @Override
  public Set<String> getSupportedAnnotationTypes() {
    return ImmutableSet.of(MappingManager.class.getName());
  }
}
