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

import com.datastax.oss.driver.api.mapper.annotations.NamingStrategy;
import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import com.squareup.javapoet.CodeBlock;
import javax.lang.model.type.TypeMirror;

/**
 * Generates the code blocks for CQL names according to the strategy defined by the {@link
 * NamingStrategy} annotation on an entity class.
 *
 * <p>If the entity uses a built-in convention, we convert right away and the code blocks are just
 * simple strings:
 *
 * <pre>
 * target.set("product_id", entity.getProductId(), UUID.class);
 * </pre>
 *
 * If it uses a custom converter class, we must instantiate it and invoke it at runtime:
 *
 * <pre>
 * target.set(context.getNameConverter(SomeCustomConverter.class).toCassandraName("productId"),
 *            entity.getProductId(), UUID.class);
 * </pre>
 */
public class CqlNameGenerator {

  /** The default (when an entity is not annotated). */
  public static final CqlNameGenerator DEFAULT =
      new CqlNameGenerator(NamingConvention.SNAKE_CASE_INSENSITIVE);

  private final NamingConvention namingConvention;
  private final TypeMirror converterClassMirror;

  public CqlNameGenerator(NamingConvention namingConvention) {
    this(namingConvention, null);
  }

  public CqlNameGenerator(TypeMirror converterClassMirror) {
    this(null, converterClassMirror);
  }

  private CqlNameGenerator(NamingConvention namingConvention, TypeMirror converterClassMirror) {
    assert namingConvention == null ^ converterClassMirror == null;
    this.namingConvention = namingConvention;
    this.converterClassMirror = converterClassMirror;
  }

  public CodeBlock buildCqlName(String javaName) {
    if (namingConvention != null) {
      return CodeBlock.of("$S", BuiltInNameConversions.toCassandraName(javaName, namingConvention));
    } else {
      return CodeBlock.of(
          "context.getNameConverter($T.class).toCassandraName($S)", converterClassMirror, javaName);
    }
  }
}
