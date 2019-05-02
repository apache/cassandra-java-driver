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
package com.datastax.oss.driver.internal.mapper.processor.dao;

import com.datastax.oss.driver.api.mapper.annotations.GetEntity;
import com.datastax.oss.driver.api.mapper.annotations.SetEntity;

/**
 * A kind of return type supported by by DAO query methods (excluding {@link GetEntity} and {@link
 * SetEntity}).
 *
 * <p>Not all kinds are supported by all methods, see the additional checks in each method
 * generator.
 */
public enum ReturnTypeKind {
  VOID(false),
  BOOLEAN(false),
  ENTITY(false),
  RESULT_SET(false),
  PAGING_ITERABLE(false),

  FUTURE_OF_VOID(true),
  FUTURE_OF_BOOLEAN(true),
  FUTURE_OF_ENTITY(true),
  FUTURE_OF_ASYNC_RESULT_SET(true),
  FUTURE_OF_ASYNC_PAGING_ITERABLE(true),

  UNSUPPORTED(false),
  ;

  final boolean isAsync;

  ReturnTypeKind(boolean isAsync) {
    this.isAsync = isAsync;
  }
}
