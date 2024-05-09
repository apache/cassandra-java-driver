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
package com.datastax.oss.driver.api.mapper;

import com.datastax.oss.driver.api.core.DriverException;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A runtime issue with the object mapper.
 *
 * <p>Most configuration issues (e.g. misuse of the annotations) can be detected at compile-time,
 * and will be reported as compiler errors instead. This exception is reserved for things that can
 * only be checked at runtime, for example session state (protocol version, schema, etc).
 *
 * <p>{@link #getExecutionInfo()} always returns {@code null} for this type.
 */
public class MapperException extends DriverException {

  public MapperException(@NonNull String message, @Nullable Throwable cause) {
    super(message, null, cause, true);
  }

  public MapperException(@NonNull String message) {
    this(message, null);
  }

  @NonNull
  @Override
  public DriverException copy() {
    return new MapperException(getMessage(), this);
  }
}
