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

public class GraalLibc implements Libc {

  @Override
  public boolean available() {
    return true;
  }

  /* Substrate includes a substitution for Linux + Darwin which redefines System.nanoTime() to use
   * gettimeofday() (unless platform-specific higher-res clocks are available, which is even better). */
  @Override
  public Optional<Long> gettimeofday() {
    return Optional.of(Math.round(System.nanoTime() / 1_000d));
  }

  @Override
  public Optional<Integer> getpid() {
    return Optional.of(GraalGetpid.getpid());
  }
}
