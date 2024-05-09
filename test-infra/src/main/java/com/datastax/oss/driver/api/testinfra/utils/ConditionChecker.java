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
package com.datastax.oss.driver.api.testinfra.utils;

import static org.assertj.core.api.Fail.fail;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

/**
 * @deprecated We've replaced this home-grown utility by Awaitility in our tests. We're preserving
 *     it because it was part of the public test infrastructure API, but it won't be maintained
 *     anymore, and removed in the next major version.
 * @see <a href="https://github.com/awaitility/awaitility">Awaitility homepage</a>
 */
@Deprecated
public class ConditionChecker {

  private static final int DEFAULT_PERIOD_MILLIS = 500;

  private static final int DEFAULT_TIMEOUT_MILLIS = 60000;

  /** @deprecated see {@link ConditionChecker} */
  @Deprecated
  public static class ConditionCheckerBuilder {

    private long timeout = DEFAULT_TIMEOUT_MILLIS;

    private TimeUnit timeoutUnit = TimeUnit.MILLISECONDS;

    private long period = DEFAULT_PERIOD_MILLIS;

    private TimeUnit periodUnit = TimeUnit.MILLISECONDS;

    private final Object predicate;

    private String description;

    ConditionCheckerBuilder(BooleanSupplier predicate) {
      this.predicate = predicate;
    }

    public ConditionCheckerBuilder(Runnable predicate) {
      this.predicate = predicate;
    }

    public ConditionCheckerBuilder every(long period, TimeUnit unit) {
      this.period = period;
      periodUnit = unit;
      return this;
    }

    public ConditionCheckerBuilder every(long periodMillis) {
      period = periodMillis;
      periodUnit = TimeUnit.MILLISECONDS;
      return this;
    }

    public ConditionCheckerBuilder before(long timeout, TimeUnit unit) {
      this.timeout = timeout;
      timeoutUnit = unit;
      return this;
    }

    public ConditionCheckerBuilder before(long timeoutMillis) {
      timeout = timeoutMillis;
      timeoutUnit = TimeUnit.MILLISECONDS;
      return this;
    }

    public ConditionCheckerBuilder as(String description) {
      this.description = description;
      return this;
    }

    public void becomesTrue() {
      new ConditionChecker(predicate, true, period, periodUnit, description)
          .await(timeout, timeoutUnit);
    }

    public void becomesFalse() {
      new ConditionChecker(predicate, false, period, periodUnit, description)
          .await(timeout, timeoutUnit);
    }
  }

  public static ConditionCheckerBuilder checkThat(BooleanSupplier predicate) {
    return new ConditionCheckerBuilder(predicate);
  }

  public static ConditionCheckerBuilder checkThat(Runnable predicate) {
    return new ConditionCheckerBuilder(predicate);
  }

  private final Object predicate;
  private final boolean expectedOutcome;
  private final String description;
  private final Lock lock;
  private final Condition condition;
  private final Timer timer;
  private Throwable lastFailure;

  public ConditionChecker(
      Object predicate,
      boolean expectedOutcome,
      long period,
      TimeUnit periodUnit,
      String description) {
    this.predicate = predicate;
    this.expectedOutcome = expectedOutcome;
    this.description = (description != null) ? description : this.toString();
    lock = new ReentrantLock();
    condition = lock.newCondition();
    timer = new Timer("condition-checker", true);
    timer.schedule(
        new TimerTask() {
          @Override
          public void run() {
            checkCondition();
          }
        },
        0,
        periodUnit.toMillis(period));
  }

  /** Waits until the predicate becomes true, or a timeout occurs, whichever happens first. */
  public void await(long timeout, TimeUnit unit) {
    boolean interrupted = false;
    long nanos = unit.toNanos(timeout);
    lock.lock();
    try {
      while (!evalCondition()) {
        if (nanos <= 0L) {
          String msg =
              String.format(
                  "Timeout after %s %s while waiting for '%s'",
                  timeout, unit.toString().toLowerCase(), description);
          if (lastFailure != null) {
            fail(msg, lastFailure);
          } else {
            fail(msg);
          }
        }
        try {
          nanos = condition.awaitNanos(nanos);
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      timer.cancel();
      if (interrupted) Thread.currentThread().interrupt();
    }
  }

  private void checkCondition() {
    lock.lock();
    try {
      if (evalCondition()) {
        condition.signal();
      }
    } finally {
      lock.unlock();
    }
  }

  private boolean evalCondition() {
    if (predicate instanceof BooleanSupplier) {
      return ((BooleanSupplier) predicate).getAsBoolean() == expectedOutcome;
    } else if (predicate instanceof Runnable) {
      boolean succeeded = true;
      try {
        ((Runnable) predicate).run();
      } catch (Throwable t) {
        succeeded = false;
        lastFailure = t;
      }
      return succeeded == expectedOutcome;
    } else {
      throw new AssertionError("Unsupported predicate type " + predicate.getClass());
    }
  }
}
