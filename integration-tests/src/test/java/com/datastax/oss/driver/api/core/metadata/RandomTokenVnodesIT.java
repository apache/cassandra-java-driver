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
package com.datastax.oss.driver.api.core.metadata;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.internal.core.metadata.token.RandomToken;
import java.time.Duration;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

public class RandomTokenVnodesIT extends TokenITBase {

  private static CustomCcmRule ccmRule =
      CustomCcmRule.builder()
          .withNodes(3)
          .withCreateOption("-p RandomPartitioner")
          .withCreateOption("--vnodes")
          .build();

  private static SessionRule<CqlSession> sessionRule =
      SessionRule.builder(ccmRule)
          .withKeyspace(false)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
                  .build())
          .build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  public RandomTokenVnodesIT() {
    super("org.apache.cassandra.dht.RandomPartitioner", RandomToken.class, true);
  }

  @Override
  protected CqlSession session() {
    return sessionRule.session();
  }

  @BeforeClass
  public static void createSchema() {
    TokenITBase.createSchema(sessionRule.session());
  }
}
