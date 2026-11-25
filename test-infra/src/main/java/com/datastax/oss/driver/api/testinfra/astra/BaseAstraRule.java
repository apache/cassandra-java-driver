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
package com.datastax.oss.driver.api.testinfra.astra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirementRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import java.io.File;
import java.util.Collections;
import java.util.Set;
import org.junit.AssumptionViolatedException;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.LoggerFactory;

public abstract class BaseAstraRule extends CcmRule {

  protected final AstraBridge astraBridge;

  // Reusable session for table cleanup operations
  private volatile CqlSession cleanupSession;

  BaseAstraRule(AstraBridge astraBridge) {
    super();
    this.astraBridge = astraBridge;
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    closeCleanupSession();
                    astraBridge.close();
                  } catch (Exception e) {
                    // silently remove as may have already been removed.
                  }
                }));
  }

  @Override
  protected synchronized void before() {
    astraBridge.create();
    astraBridge.start();
  }

  @Override
  public Statement apply(Statement base, Description description) {
    if (BackendRequirementRule.meetsDescriptionRequirements(description)) {
      // @ClassRule: Wrap the base statement to drop all tables after test suite execution
      Statement wrappedStatement =
          new Statement() {
            @Override
            public void evaluate() throws Throwable {
              try {
                base.evaluate();
              } finally {
                // Drop all tables in the keyspace after the test suite completes
                dropAllTablesInKeyspace();
              }
            }
          };
      return super.apply(wrappedStatement, description);
    } else {
      // requirements not met, throw reasoning assumption to skip test
      return new Statement() {
        @Override
        public void evaluate() {
          throw new AssumptionViolatedException(
              BackendRequirementRule.buildReasonString(description));
        }
      };
    }
  }

  /**
   * Gets or creates a reusable CQL session for cleanup operations. This session is created lazily
   * and reused across all table cleanup operations to avoid the overhead of creating/closing
   * sessions after each test suite.
   *
   * @return a CQL session connected to the Astra database
   */
  private synchronized CqlSession getOrCreateCleanupSession() {
    if (cleanupSession == null || cleanupSession.isClosed()) {
      String keyspaceName = astraBridge.getKeyspace();
      cleanupSession =
          CqlSession.builder()
              .withCloudSecureConnectBundle(astraBridge.getSecureConnectBundle().toPath())
              .withAuthCredentials("token", astraBridge.getToken())
              .withKeyspace(keyspaceName)
              .build();
      LoggerFactory.getLogger(BaseAstraRule.class)
          .error("Created reusable cleanup session for keyspace '{}'", keyspaceName);
    }
    return cleanupSession;
  }

  /**
   * Closes the cleanup session if it exists. This is called when the rule is done (either in
   * after() or in the shutdown hook).
   */
  private synchronized void closeCleanupSession() {
    if (cleanupSession != null && !cleanupSession.isClosed()) {
      try {
        LoggerFactory.getLogger(BaseAstraRule.class)
            .error("Closing cleanup session for keyspace '{}'", astraBridge.getKeyspace());
        cleanupSession.close();
      } catch (Exception e) {
        LoggerFactory.getLogger(BaseAstraRule.class)
            .error("Failed to close cleanup session: {}", e.getMessage(), e);
      }
      cleanupSession = null;
    }
  }

  /**
   * Drops all user-created tables in the keyspace after a test suite completes. This is called
   * automatically after each test class when running against Astra to avoid the expensive operation
   * of creating/dropping keyspaces.
   *
   * <p>This method uses a reusable session that is created once and reused across all cleanup
   * operations.
   */
  protected void dropAllTablesInKeyspace() {
    String keyspaceName = astraBridge.getKeyspace();
    if (keyspaceName == null || keyspaceName.isEmpty()) {
      return; // No keyspace to clean
    }

    try {
      CqlSession session = getOrCreateCleanupSession();
      astraBridge.dropAllTablesInKeyspace(keyspaceName, session);
    } catch (Exception e) {
      // Log but don't fail - this is cleanup
      LoggerFactory.getLogger(BaseAstraRule.class)
          .error("Failed to drop tables in keyspace '{}': {}", keyspaceName, e.getMessage(), e);
    }
  }

  @Override
  public BackendType getDistribution() {
    return AstraBridge.DISTRIBUTION;
  }

  @Override
  public boolean isDistributionOf(BackendType type) {
    return AstraBridge.isDistributionOf(type);
  }

  @Override
  public Version getDistributionVersion() {
    return AstraBridge.getDistributionVersion();
  }

  @Override
  public Version getCassandraVersion() {
    return AstraBridge.getCassandraVersion();
  }

  @Override
  public ProtocolVersion getHighestProtocolVersion() {
    // Astra supports protocol version V4
    return DefaultProtocolVersion.V4;
  }

  @Override
  public Set<EndPoint> getContactPoints() {
    // Astra uses Secure Connect Bundle instead of contact points
    return Collections.emptySet();
  }

  /**
   * Returns the Secure Connect Bundle file for connecting to Astra.
   *
   * @return the Secure Connect Bundle file
   */
  public File getSecureConnectBundle() {
    return astraBridge.getSecureConnectBundle();
  }

  public AstraBridge getAstraBridge() {
    return astraBridge;
  }
}
