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
package com.datastax.oss.driver.core.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import com.google.common.io.Files;
import java.io.File;
import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(ParallelizableTests.class)
public class DescribeIT {

  private static final Logger LOG = LoggerFactory.getLogger(DescribeIT.class);

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(CCM_RULE)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
                  // disable debouncer to speed up test.
                  .withDuration(DefaultDriverOption.METADATA_SCHEMA_WINDOW, Duration.ofSeconds(0))
                  .build())
          .build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static final Splitter STATEMENT_SPLITTER =
      // Use a regex to ignore semicolons in function scripts
      Splitter.on(Pattern.compile(";\n")).omitEmptyStrings();

  private Version serverVersion;
  private boolean isDse;

  @Before
  public void setup() {
    Optional<Version> dseVersion = CCM_RULE.getDseVersion();
    isDse = dseVersion.isPresent();
    serverVersion =
        isDse ? dseVersion.get().nextStable() : CCM_RULE.getCassandraVersion().nextStable();
  }

  @Test
  public void describe_output_should_match_creation_script() throws Exception {
    CqlSession session = SESSION_RULE.session();

    File scriptFile = getScriptFile();
    String scriptContents =
        Files.asCharSource(scriptFile, Charsets.UTF_8)
            .read()
            .trim()
            .replaceAll("ks_0", SESSION_RULE.keyspace().asCql(true));
    List<String> statements = STATEMENT_SPLITTER.splitToList(scriptContents);

    // Skip the first statement (CREATE KEYSPACE), we already have a keyspace
    for (int i = 1; i < statements.size(); i++) {
      String statement = statements.get(i);
      try {
        session.execute(statement);
      } catch (Exception e) {
        fail("Error executing statement %s (%s)", statement, e);
      }
    }

    KeyspaceMetadata keyspaceMetadata =
        session.getMetadata().getKeyspace(SESSION_RULE.keyspace()).orElseThrow(AssertionError::new);
    String describeOutput = keyspaceMetadata.describeWithChildren(true).trim();

    assertThat(describeOutput)
        .as(
            "Describe output doesn't match create statements, "
                + "maybe you need to add a new script in integration-tests/src/test/resources. "
                + "Server version = %s %s, used script = %s",
            isDse ? "DSE" : "Cassandra", serverVersion, scriptFile)
        .isEqualTo(scriptContents);
  }

  /**
   * Find a creation script in our test resources that matches the current server version. If we
   * don't have an exact match, use the closest version below it.
   */
  private File getScriptFile() {
    URL logbackTestUrl = DescribeIT.class.getResource("/logback-test.xml");
    if (logbackTestUrl == null || logbackTestUrl.getFile().isEmpty()) {
      fail(
          "Expected to use logback-test.xml to determine location of "
              + "target/test-classes, but got URL %s",
          logbackTestUrl);
    }
    File resourcesDir = new File(logbackTestUrl.getFile()).getParentFile();
    File scriptsDir = new File(resourcesDir, isDse ? "DescribeIT/dse" : "DescribeIT/oss");
    LOG.debug("Looking for a matching script in directory {}", scriptsDir);

    File[] candidates = scriptsDir.listFiles();
    assertThat(candidates).isNotNull();

    File bestFile = null;
    Version bestVersion = null;
    for (File candidate : candidates) {
      String fileName = candidate.getName();
      String candidateVersionString = fileName.substring(0, fileName.lastIndexOf('.'));
      Version candidateVersion = Version.parse(candidateVersionString);
      LOG.debug("Considering {}, which resolves to version {}", fileName, candidateVersion);
      if (candidateVersion.compareTo(serverVersion) > 0) {
        LOG.debug("too high, discarding");
      } else if (bestVersion != null && bestVersion.compareTo(candidateVersion) >= 0) {
        LOG.debug("not higher than {}, discarding", bestVersion);
      } else {
        LOG.debug("best so far");
        bestVersion = candidateVersion;
        bestFile = candidate;
      }
    }
    assertThat(bestFile)
        .as("Could not find create script with version <= %s in %s", serverVersion, scriptsDir)
        .isNotNull();

    LOG.info(
        "Using {} to test against {} {}", bestFile, isDse ? "DSE" : "Cassandra", serverVersion);
    return bestFile;
  }
}
