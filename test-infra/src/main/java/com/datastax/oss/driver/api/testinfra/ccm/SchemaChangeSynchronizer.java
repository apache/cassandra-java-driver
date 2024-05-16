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
package com.datastax.oss.driver.api.testinfra.ccm;

import java.util.concurrent.Semaphore;

/**
 * Running multiple parallel integration tests may fail due to query timeout when trying to apply
 * several schema changes at once. Limit concurrently executed DDLs to 5.
 */
public class SchemaChangeSynchronizer {
  private static final Semaphore lock = new Semaphore(5);

  public static void withLock(Runnable callback) {
    try {
      lock.acquire();
      try {
        callback.run();
      } finally {
        lock.release();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Thread interrupted wile waiting to obtain DDL lock", e);
    }
  }
}
