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
package com.datastax.oss.driver.api.testinfra;

import com.datastax.oss.driver.api.testinfra.requirement.BackendType;

/**
 * Common interface for Cassandra backend bridges (CCM and Astra).
 *
 * <p>This interface defines the lifecycle methods and common operations that both {@link
 * com.datastax.oss.driver.api.testinfra.ccm.CcmBridge} and {@link
 * com.datastax.oss.driver.api.testinfra.astra.AstraBridge} implement.
 */
public interface CassandraBridge extends AutoCloseable {

  /**
   * Creates the Cassandra backend (cluster or database).
   *
   * <p>For CCM, this creates a local cluster. For Astra, this creates a cloud database.
   */
  void create();

  /**
   * Starts the Cassandra backend.
   *
   * <p>For CCM, this starts the cluster nodes. For Astra, this ensures the database is active.
   */
  void start();

  /**
   * Stops the Cassandra backend.
   *
   * <p>For CCM, this stops the cluster nodes. For Astra, this initiates database termination.
   */
  void stop();

  /**
   * Returns the backend type (distribution) of this bridge.
   *
   * @return the backend type
   */
  BackendType getDistribution();

  /**
   * Closes this bridge and releases any resources.
   *
   * <p>For CCM, this removes the cluster. For Astra, this terminates the database.
   */
  @Override
  void close();
}
