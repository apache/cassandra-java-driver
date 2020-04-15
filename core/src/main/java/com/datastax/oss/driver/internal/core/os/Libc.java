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
package com.datastax.oss.driver.internal.core.os;

import java.util.Optional;

public interface Libc {

  /* Maintained to allow Native.isXAvailable() functionality without trying to make a native call if
   * the underlying support _is_ available. */
  boolean available();

  Optional<Long> gettimeofday();

  Optional<Integer> getpid();
}
