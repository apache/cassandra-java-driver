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
package com.datastax.oss.driver.osgi;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.datastax.oss.driver.osgi.support.BundleOptions;
import com.datastax.oss.driver.osgi.support.OsgiSimpleTests;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;
import org.slf4j.LoggerFactory;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
@Category(IsolatedTests.class)
@DseRequirement(min = "4.7")
public class OsgiVanillaIT implements OsgiSimpleTests {

  @ClassRule
  public static final CustomCcmRule CCM_RULE = CustomCcmRule.builder().withNodes(1).build();

  @Configuration
  public Option[] config() {
    // this configuration purposely excludes bundles whose resolution is optional:
    // ESRI, Reactive Streams and Tinkerpop. This allows to validate that the driver can still
    // work properly in an OSGi container as long as the missing packages are not accessed.
    return CoreOptions.options(
        BundleOptions.driverCoreBundle(),
        BundleOptions.driverQueryBuilderBundle(),
        BundleOptions.baseOptions(),
        BundleOptions.jacksonBundles());
  }

  @Before
  public void addTestAppender() {
    Logger logger = (Logger) LoggerFactory.getLogger("com.datastax.oss.driver");
    Level oldLevel = logger.getLevel();
    logger.getLoggerContext().putObject("oldLevel", oldLevel);
    logger.setLevel(Level.INFO);
    TestAppender appender = new TestAppender();
    logger.addAppender(appender);
    appender.start();
  }

  @After
  public void removeTestAppender() {
    Logger logger = (Logger) LoggerFactory.getLogger("com.datastax.oss.driver");
    logger.detachAppender("test");
    Level oldLevel = (Level) logger.getLoggerContext().getObject("oldLevel");
    logger.setLevel(oldLevel);
  }

  @Test
  public void should_connect_and_query_simple() {
    connectAndQuerySimple();
    assertLogMessagesPresent();
  }

  private void assertLogMessagesPresent() {
    Logger logger = (Logger) LoggerFactory.getLogger("com.datastax.oss.driver");
    TestAppender appender = (TestAppender) logger.getAppender("test");
    List<String> infoLogs =
        appender.events.stream()
            .filter(event -> event.getLevel().toInt() == Level.INFO.toInt())
            .map(ILoggingEvent::getFormattedMessage)
            .collect(Collectors.toList());
    assertThat(infoLogs)
        .anySatisfy(
            msg ->
                assertThat(msg)
                    .contains(
                        "Could not register Geo codecs; this is normal if ESRI was explicitly "
                            + "excluded from classpath"))
        .anySatisfy(
            msg ->
                assertThat(msg)
                    .contains(
                        "Could not register Reactive extensions; this is normal if Reactive "
                            + "Streams was explicitly excluded from classpath"))
        .anySatisfy(
            msg ->
                assertThat(msg)
                    .contains(
                        "Could not register Graph extensions; this is normal if Tinkerpop was "
                            + "explicitly excluded from classpath"));
  }

  private static class TestAppender extends AppenderBase<ILoggingEvent> {

    private final List<ILoggingEvent> events = new CopyOnWriteArrayList<>();

    private TestAppender() {
      name = "test";
    }

    @Override
    protected void append(ILoggingEvent event) {
      events.add(event);
    }
  }
}
