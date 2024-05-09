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
package com.datastax.oss.driver.internal.core;

import com.datastax.oss.driver.api.core.MavenCoordinates;
import com.datastax.oss.driver.api.core.Version;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMavenCoordinates implements MavenCoordinates {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultMavenCoordinates.class);

  public static MavenCoordinates buildFromResourceAndPrint(URL resource) {
    MavenCoordinates info = buildFromResource(resource);
    LOG.info("{}", info);
    return info;
  }

  public static DefaultMavenCoordinates buildFromResource(URL resource) {
    // The resource is assumed to be a properties file, but
    // encoded in UTF-8, not ISO-8859-1 as required by the Java specs,
    // since our build tool (Maven) produces UTF-8-encoded resources.
    try (InputStreamReader reader =
        new InputStreamReader(resource.openStream(), StandardCharsets.UTF_8)) {
      Properties props = new Properties();
      props.load(reader);
      String name = props.getProperty("driver.name");
      String groupId = props.getProperty("driver.groupId");
      String artifactId = props.getProperty("driver.artifactId");
      String version = props.getProperty("driver.version");
      if (ByteBuf.class.getPackage().getName().contains("com.datastax.oss.driver.shaded")) {
        artifactId += "-shaded";
      }
      return new DefaultMavenCoordinates(name, groupId, artifactId, Version.parse(version));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private final String name;
  private final String groupId;
  private final String artifactId;
  private final Version version;

  public DefaultMavenCoordinates(String name, String groupId, String artifactId, Version version) {
    this.name = name;
    this.groupId = groupId;
    this.artifactId = artifactId;
    this.version = version;
  }

  @NonNull
  @Override
  public String getName() {
    return name;
  }

  @NonNull
  @Override
  public String getGroupId() {
    return groupId;
  }

  @NonNull
  @Override
  public String getArtifactId() {
    return artifactId;
  }

  @NonNull
  @Override
  public Version getVersion() {
    return version;
  }

  @Override
  public String toString() {
    return String.format("%s (%s:%s) version %s", name, groupId, artifactId, version);
  }
}
