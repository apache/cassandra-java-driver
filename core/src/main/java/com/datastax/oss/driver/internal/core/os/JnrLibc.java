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
import java.util.function.Consumer;
import jnr.posix.POSIX;
import jnr.posix.POSIXFactory;
import jnr.posix.Timeval;
import jnr.posix.util.DefaultPOSIXHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JnrLibc implements Libc {

  private static final Logger LOG = LoggerFactory.getLogger(JnrLibc.class);

  private final Optional<POSIX> posix;

  public JnrLibc() {

    this.posix = loadPosix();
  }

  @Override
  public Optional<Long> gettimeofday() {

    return this.posix.flatMap(this::gettimeofdayImpl);
  }

  @Override
  public Optional<Integer> getpid() {

    return this.posix.map(POSIX::getpid);
  }

  @Override
  public boolean available() {
    return this.posix.isPresent();
  }

  private Optional<POSIX> loadPosix() {

    try {
      return Optional.of(POSIXFactory.getPOSIX(new DefaultPOSIXHandler(), true))
          .flatMap(p -> catchAll(p, posix -> posix.getpid(), "Error calling getpid()"))
          .flatMap(p -> catchAll(p, this::gettimeofdayImpl, "Error calling gettimeofday()"));
    } catch (Throwable t) {
      LOG.debug("Error loading POSIX", t);
      return Optional.empty();
    }
  }

  private Optional<POSIX> catchAll(POSIX posix, Consumer<POSIX> fn, String debugStr) {
    try {
      fn.accept(posix);
      return Optional.of(posix);
    } catch (Throwable t) {

      LOG.debug(debugStr, t);
      return Optional.empty();
    }
  }

  private Optional<Long> gettimeofdayImpl(POSIX posix) {

    Timeval tv = posix.allocateTimeval();
    int rv = posix.gettimeofday(tv);
    if (rv != 0) {
      LOG.debug("Expected 0 return value from gettimeofday(), observed " + rv);
      return Optional.empty();
    }
    return Optional.of(tv.sec() * 1_000_000 + tv.usec());
  }
}
