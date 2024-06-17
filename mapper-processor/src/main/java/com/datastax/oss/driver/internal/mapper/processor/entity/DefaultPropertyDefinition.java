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
package com.datastax.oss.driver.internal.mapper.processor.entity;

import com.datastax.oss.driver.internal.mapper.processor.util.generation.PropertyType;
import com.squareup.javapoet.CodeBlock;
import java.util.Optional;

public class DefaultPropertyDefinition implements PropertyDefinition {

  private final String javaName;
  private final CodeBlock selector;
  private final CodeBlock cqlName;
  private final String getterName;
  private final String setterName;
  private final PropertyType type;

  public DefaultPropertyDefinition(
      String javaName,
      Optional<String> customCqlName,
      Optional<String> computedFormula,
      String getterName,
      String setterName,
      PropertyType type,
      CqlNameGenerator cqlNameGenerator) {
    this.javaName = javaName;

    this.cqlName =
        customCqlName
            .map(n -> CodeBlock.of("$S", n))
            .orElse(cqlNameGenerator.buildCqlName(javaName));

    /*
     * If computed formula is present, this property does not map to a particular column,
     * but rather a computed result.  In this case, we need to use column aliasing
     * i.e. 'count(*) as X' as the name is not deterministic from the computed formula.
     * In this case we use the cqlName (or customCqlName if present) as the aliased name,
     * and the formula as the selector.
     */
    this.selector = computedFormula.map(n -> CodeBlock.of("$S", n)).orElse(cqlName);

    this.getterName = getterName;
    this.setterName = setterName;
    this.type = type;
  }

  @Override
  public String getJavaName() {
    return javaName;
  }

  @Override
  public CodeBlock getSelector() {
    return selector;
  }

  @Override
  public CodeBlock getCqlName() {
    return cqlName;
  }

  @Override
  public String getGetterName() {
    return getterName;
  }

  @Override
  public String getSetterName() {
    return setterName;
  }

  @Override
  public PropertyType getType() {
    return type;
  }
}
