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
package com.datastax.oss.driver.internal.core.os;

import java.util.Optional;

/** A no-op NativeImpl implementation; useful if we can't load one of the others */
public class EmptyLibc implements Libc {

  @Override
  public boolean available() {
    return false;
  }

  @Override
  public Optional<Long> gettimeofday() {
    return Optional.empty();
  }

  @Override
  public Optional<Integer> getpid() {
    return Optional.empty();
  }
}
