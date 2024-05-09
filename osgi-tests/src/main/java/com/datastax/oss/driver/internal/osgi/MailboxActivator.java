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

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.internal.core.graph.GraphProtocol;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.osgi.service.MailboxService;
import com.datastax.oss.driver.internal.osgi.service.MailboxServiceImpl;
import com.datastax.oss.driver.internal.osgi.service.geo.GeoMailboxServiceImpl;
import com.datastax.oss.driver.internal.osgi.service.graph.GraphMailboxServiceImpl;
import com.datastax.oss.driver.internal.osgi.service.reactive.ReactiveMailboxServiceImpl;
import java.net.InetSocketAddress;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.wiring.BundleWiring;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MailboxActivator implements BundleActivator {

  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxActivator.class);

  private CqlSession session;
  private CqlIdentifier keyspace;
  private String graphName;

  @Override
  public void start(BundleContext context) {
    buildSession(context);
    registerService(context);
  }

  private void buildSession(BundleContext context) {

    Bundle bundle = context.getBundle();
    BundleWiring bundleWiring = bundle.adapt(BundleWiring.class);
    ClassLoader classLoader = bundleWiring.getClassLoader();

    LOGGER.info("Application class loader: {}", classLoader);

    // Use the application bundle class loader to load classes by reflection when
    // they are located in the application bundle. This is not strictly required
    // as the driver has a "Dynamic-Import:*" directive which makes it capable
    // of loading classes outside its bundle.
    CqlSessionBuilder builder = CqlSession.builder().withClassLoader(classLoader);

    // Use the application bundle class loader to load configuration resources located
    // in the application bundle. This is required, otherwise these resources will
    // not be found.
    ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder =
        DriverConfigLoader.programmaticBuilder(classLoader);

    String contactPointsStr = context.getProperty("cassandra.contactpoints");
    if (contactPointsStr == null) {
      contactPointsStr = "127.0.0.1";
    }
    LOGGER.info("Contact points: {}", contactPointsStr);

    String portStr = context.getProperty("cassandra.port");
    if (portStr == null) {
      portStr = "9042";
    }
    LOGGER.info("Port: {}", portStr);
    int port = Integer.parseInt(portStr);

    List<InetSocketAddress> contactPoints =
        Stream.of(contactPointsStr.split(","))
            .map((String host) -> InetSocketAddress.createUnresolved(host, port))
            .collect(Collectors.toList());
    builder.addContactPoints(contactPoints);

    String keyspaceStr = context.getProperty("cassandra.keyspace");
    if (keyspaceStr == null) {
      keyspaceStr = "mailbox";
    }
    LOGGER.info("Keyspace: {}", keyspaceStr);
    keyspace = CqlIdentifier.fromCql(keyspaceStr);

    String lbp = context.getProperty("cassandra.lbp");
    if (lbp != null) {
      LOGGER.info("Custom LBP: " + lbp);
      configLoaderBuilder.withString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, lbp);
    } else {
      LOGGER.info("Custom LBP: NO");
    }

    String datacenter = context.getProperty("cassandra.datacenter");
    if (datacenter != null) {
      LOGGER.info("Custom datacenter: " + datacenter);
      configLoaderBuilder.withString(
          DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, datacenter);
    } else {
      LOGGER.info("Custom datacenter: NO");
    }

    String compression = context.getProperty("cassandra.compression");
    if (compression != null) {
      LOGGER.info("Compression: {}", compression);
      configLoaderBuilder.withString(DefaultDriverOption.PROTOCOL_COMPRESSION, compression);
    } else {
      LOGGER.info("Compression: NONE");
    }

    graphName = context.getProperty("cassandra.graph.name");
    if (graphName != null) {
      LOGGER.info("Graph name: {}", graphName);
      configLoaderBuilder.withString(DseDriverOption.GRAPH_NAME, graphName);
      configLoaderBuilder.withString(
          DseDriverOption.GRAPH_SUB_PROTOCOL, GraphProtocol.GRAPH_BINARY_1_0.toInternalCode());
    } else {
      LOGGER.info("Graph: NONE");
    }

    builder.withConfigLoader(configLoaderBuilder.build());

    LOGGER.info("Initializing session");
    session = builder.build();
    LOGGER.info("Session initialized");
  }

  private void registerService(BundleContext context) {
    MailboxServiceImpl mailbox;
    if ("true".equalsIgnoreCase(context.getProperty("cassandra.reactive"))) {
      mailbox = new ReactiveMailboxServiceImpl(session, keyspace);
    } else if ("true".equalsIgnoreCase(context.getProperty("cassandra.geo"))) {
      mailbox = new GeoMailboxServiceImpl(session, keyspace);
    } else if ("true".equalsIgnoreCase(context.getProperty("cassandra.graph"))) {
      mailbox = new GraphMailboxServiceImpl(session, keyspace, graphName);
    } else {
      mailbox = new MailboxServiceImpl(session, keyspace);
    }
    mailbox.init();
    @SuppressWarnings("JdkObsolete")
    Dictionary<String, String> properties = new Hashtable<>();
    context.registerService(MailboxService.class.getName(), mailbox, properties);
    LOGGER.info("Mailbox Service successfully initialized");
  }

  @Override
  public void stop(BundleContext context) {
    if (session != null) {
      LOGGER.info("Closing session");
      session.close();
      session = null;
      LOGGER.info("Session closed");
    }
  }
}
