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

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import java.util.Optional;

/**
 * Add an explicit Graal substitution for {@link JnrLibc}. If we don't implement something like this
 * the analysis done at Graal native image build time will discover the jnr-posix references in
 * JnrLibc even though they won't be used at runtime. By default jnr-ffi (used by jnr-posix to do
 * it's work) will use {@link ClassLoader#defineClass(String, byte[], int, int)} which isn't
 * supported by Graal. This behaviour can be changed with a system property but the cleanest
 * solution is simply to remove the references to jnr-posix code via a Graal substitution.
 */
@TargetClass(JnrLibc.class)
@Substitute
final class JnrLibcSubstitution implements Libc {

  @Substitute
  public JnrLibcSubstitution() {}

  @Substitute
  @Override
  public boolean available() {
    return false;
  }

  @Substitute
  @Override
  public Optional<Long> gettimeofday() {
    return Optional.empty();
  }

  @Substitute
  @Override
  public Optional<Integer> getpid() {
    return Optional.empty();
  }
}
