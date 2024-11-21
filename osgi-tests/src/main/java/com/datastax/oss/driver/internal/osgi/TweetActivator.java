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
package com.datastax.oss.driver.internal.osgi;

import com.datastax.oss.driver.api.osgi.service.TweetService;
import com.datastax.oss.driver.internal.osgi.service.TweetServiceImpl;
import java.util.Dictionary;
import java.util.Hashtable;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TweetActivator extends BaseActivator {

  private static final Logger LOGGER = LoggerFactory.getLogger(TweetActivator.class);

  @Override
  protected void registerService(BundleContext context) {
    TweetServiceImpl tweet = new TweetServiceImpl(session, keyspace);
    tweet.init();
    @SuppressWarnings("JdkObsolete")
    Dictionary<String, String> properties = new Hashtable<>();
    context.registerService(TweetService.class.getName(), tweet, properties);
    LOGGER.info("Tweet Service successfully initialized");
  }
}
