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
package com.datastax.oss.driver.api.types.reflect;

import com.google.common.reflect.TypeToken;

/**
 * Runtime representation of a generic Java type.
 *
 * <p>This is used by type codecs to indicate which Java types they accept, and by generic getters
 * and setters in the driver's query API.
 *
 * <p>To create an instance, use one of the static factory methods, or create an anonymous class:
 *
 * <pre>{@code
 * GenericType<Foo<Bar>> fooBarType = new GenericType<Foo<Bar>>(){};
 * }</pre>
 *
 * You are encouraged to store and reuse these objects.
 */
public abstract class GenericType<T> {

  /** Creates a new instance representing a raw Java class. */
  public static <T> GenericType<T> of(Class<T> type) {
    return new SimpleGenericType<>(type);
  }

  // This wraps -- and delegates most of the work to -- a Guava type token. The reason we don't
  // expose that type directly is because we shade Guava.
  private final TypeToken<T> token;

  private GenericType(TypeToken<T> token) {
    this.token = token;
  }

  protected GenericType() {
    this.token = new TypeToken<T>(getClass()) {};
  }

  /**
   * This method is for internal use, <b>DO NOT use it from client code</b>.
   *
   * <p>It leaks a shaded type. This should be part of the internal API, but due to internal
   * implementation details it has to be exposed here.
   */
  public TypeToken<T> __getToken() {
    return token;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof GenericType) {
      GenericType that = (GenericType) other;
      return this.token.equals(that.token);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return token.hashCode();
  }

  @Override
  public String toString() {
    return token.toString();
  }

  private static class SimpleGenericType<T> extends GenericType<T> {
    SimpleGenericType(Class<T> type) {
      super(TypeToken.of(type));
    }
  }
}
