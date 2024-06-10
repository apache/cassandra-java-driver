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
package com.datastax.oss.driver.api.core.type.reflect;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import net.jcip.annotations.Immutable;

/**
 * Captures a free type variable that can be used in {@link GenericType#where(GenericTypeParameter,
 * GenericType)}.
 */
@Immutable
@SuppressWarnings("unused") // for T (unfortunately has to cover the whole class)
public class GenericTypeParameter<T> {
  private final TypeVariable<?> typeVariable;

  protected GenericTypeParameter() {
    Type superclass = getClass().getGenericSuperclass();
    Preconditions.checkArgument(
        superclass instanceof ParameterizedType, "%s isn't parameterized", superclass);
    this.typeVariable =
        (TypeVariable<?>) ((ParameterizedType) superclass).getActualTypeArguments()[0];
  }

  @NonNull
  public TypeVariable<?> getTypeVariable() {
    return typeVariable;
  }
}
