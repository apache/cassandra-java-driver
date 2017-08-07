/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.api.core.type.reflect;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Captures a free type variable that can be used in {@link GenericType#where(GenericTypeParameter,
 * GenericType)}.
 */
@SuppressWarnings("unused") // for T (unfortunately has to cover the whole class)
public class GenericTypeParameter<T> {
  private final TypeVariable<?> typeVariable;

  protected GenericTypeParameter() {
    Type superclass = getClass().getGenericSuperclass();
    checkArgument(superclass instanceof ParameterizedType, "%s isn't parameterized", superclass);
    this.typeVariable =
        (TypeVariable<?>) ((ParameterizedType) superclass).getActualTypeArguments()[0];
  }

  public TypeVariable<?> getTypeVariable() {
    return typeVariable;
  }
}
