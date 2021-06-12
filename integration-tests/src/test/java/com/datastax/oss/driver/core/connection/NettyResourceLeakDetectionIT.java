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
package com.datastax.oss.driver.core.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.datastax.oss.driver.shaded.guava.common.base.Strings;
import com.datastax.oss.protocol.internal.Segment;
import com.datastax.oss.protocol.internal.util.Bytes;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.LoggerFactory;

@Category(IsolatedTests.class)
@RunWith(MockitoJUnitRunner.class)
public class NettyResourceLeakDetectionIT {

  static {
    ResourceLeakDetector.setLevel(Level.PARANOID);
  }

  private static final CustomCcmRule CCM_RULE = CustomCcmRule.builder().build();

  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static final ByteBuffer LARGE_PAYLOAD =
      Bytes.fromHexString("0x" + Strings.repeat("ab", Segment.MAX_PAYLOAD_LENGTH + 100));

  @Mock private Appender<ILoggingEvent> appender;

  @BeforeClass
  public static void createTables() {
    CqlSession session = SESSION_RULE.session();
    DriverExecutionProfile slowProfile = SessionUtils.slowProfile(session);
    session.execute(
        SimpleStatement.newInstance(
                "CREATE TABLE IF NOT EXISTS leak_test_small (key int PRIMARY KEY, value int)")
            .setExecutionProfile(slowProfile));
    session.execute(
        SimpleStatement.newInstance(
                "CREATE TABLE IF NOT EXISTS leak_test_large (key int PRIMARY KEY, value blob)")
            .setExecutionProfile(slowProfile));
  }

  @Before
  public void setupLogger() {
    Logger logger = (Logger) LoggerFactory.getLogger(ResourceLeakDetector.class);
    logger.setLevel(ch.qos.logback.classic.Level.ERROR);
    logger.addAppender(appender);
  }

  @After
  public void resetLogger() {
    Logger logger = (Logger) LoggerFactory.getLogger(ResourceLeakDetector.class);
    logger.detachAppender(appender);
  }

  @Test
  public void should_not_leak_uncompressed() {
    doLeakDetectionTest(SESSION_RULE.session());
  }

  @Test
  public void should_not_leak_compressed_lz4() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.PROTOCOL_COMPRESSION, "lz4")
            .build();
    try (CqlSession session = SessionUtils.newSession(CCM_RULE, SESSION_RULE.keyspace(), loader)) {
      doLeakDetectionTest(session);
    }
  }

  @Test
  public void should_not_leak_compressed_snappy() {
    Assume.assumeTrue(
        "Snappy is not supported in OSS C* 4.0+ with protocol v5",
        CCM_RULE.getDseVersion().isPresent()
            || CCM_RULE.getCassandraVersion().nextStable().compareTo(Version.V4_0_0) < 0);
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.PROTOCOL_COMPRESSION, "snappy")
            .build();
    try (CqlSession session = SessionUtils.newSession(CCM_RULE, SESSION_RULE.keyspace(), loader)) {
      doLeakDetectionTest(session);
    }
  }

  private void doLeakDetectionTest(CqlSession session) {
    for (int i = 0; i < 10; i++) {
      testSmallMessages(session);
      verify(appender, never()).doAppend(any());
      System.gc();
      testLargeMessages(session);
      verify(appender, never()).doAppend(any());
      System.gc();
    }
  }

  private void testSmallMessages(CqlSession session) {
    // trigger some activity using small requests and responses; in v5, these messages should fit in
    // one single, self-contained segment
    for (int i = 0; i < 1000; i++) {
      session.execute("INSERT INTO leak_test_small (key, value) VALUES (?,?)", i, i);
    }
    List<Row> rows = session.execute("SELECT value FROM leak_test_small").all();
    assertThat(rows).hasSize(1000);
    for (Row row : rows) {
      assertThat(row).isNotNull();
      int actual = row.getInt(0);
      assertThat(actual).isGreaterThanOrEqualTo(0).isLessThan(1000);
    }
  }

  private void testLargeMessages(CqlSession session) {
    // trigger some activity using large requests and responses; in v5, these messages are likely to
    // be split in multiple segments
    for (int i = 0; i < 100; i++) {
      session.execute(
          "INSERT INTO leak_test_large (key, value) VALUES (?,?)", i, LARGE_PAYLOAD.duplicate());
    }
    List<Row> rows = session.execute("SELECT value FROM leak_test_large").all();
    assertThat(rows).hasSize(100);
    for (Row row : rows) {
      assertThat(row).isNotNull();
      ByteBuffer actual = row.getByteBuffer(0);
      assertThat(actual).isEqualTo(LARGE_PAYLOAD.duplicate());
    }
  }
}
