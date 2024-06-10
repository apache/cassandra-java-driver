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

import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.internal.mapper.processor.CodeGenerator;
import com.datastax.oss.driver.internal.mapper.processor.CodeGeneratorFactory;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import javax.lang.model.element.TypeElement;

/** Entry point to generate all the types related to a {@link Mapper}-annotated interface. */
public class MapperGenerator implements CodeGenerator {

  private final TypeElement interfaceElement;
  private final ProcessorContext context;

  public MapperGenerator(TypeElement interfaceElement, ProcessorContext context) {
    this.interfaceElement = interfaceElement;
    this.context = context;
  }

  @Override
  public void generate() {
    CodeGeneratorFactory factory = context.getCodeGeneratorFactory();
    factory.newMapperBuilder(interfaceElement).generate();
    factory.newMapperImplementation(interfaceElement).generate();
  }
}
