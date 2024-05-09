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
package com.datastax.oss.driver.examples.mapper;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.examples.mapper.killrvideo.KillrVideoMapper;
import com.datastax.oss.driver.examples.mapper.killrvideo.user.User;
import com.datastax.oss.driver.examples.mapper.killrvideo.user.UserDao;
import com.datastax.oss.driver.examples.mapper.killrvideo.video.LatestVideo;
import com.datastax.oss.driver.examples.mapper.killrvideo.video.UserVideo;
import com.datastax.oss.driver.examples.mapper.killrvideo.video.Video;
import com.datastax.oss.driver.examples.mapper.killrvideo.video.VideoByTag;
import com.datastax.oss.driver.examples.mapper.killrvideo.video.VideoDao;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Uses the driver's object mapper to interact with a schema.
 *
 * <p>We use the data model of the <a href="https://killrvideo.github.io/">KillrVideo</a> sample
 * application. The mapped entities and DAOs are in the {@link
 * com.datastax.oss.driver.examples.mapper.killrvideo} package. We only cover a subset of the data
 * model (ratings, stats, recommendations and comments are not covered).
 *
 * <p>Preconditions:
 *
 * <ul>
 *   <li>An Apache Cassandra(R) cluster is running and accessible through the contacts points
 *       identified by basic.contact-points (see application.conf).
 * </ul>
 *
 * <p>Side effects:
 *
 * <ul>
 *   <li>creates a new keyspace "killrvideo" in the session. If a keyspace with this name already
 *       exists, it will be reused;
 *   <li>creates the tables of the KillrVideo data model, if they don't already exist;
 *   <li>inserts a new user, or reuse the existing one if the email address is already taken;
 *   <li>inserts a video for that user.
 * </ul>
 *
 * @see <a href="https://docs.datastax.com/en/developer/java-driver/latest/manual/mapper/">Java
 *     Driver Mapper manual</a>
 */
@SuppressWarnings("CatchAndPrintStackTrace")
public class KillrVideoMapperExample {

  private static final CqlIdentifier KEYSPACE_ID = CqlIdentifier.fromCql("killrvideo");

  public static void main(String[] args) {

    try (CqlSession session = CqlSession.builder().build()) {

      maybeCreateSchema(session);

      KillrVideoMapper mapper =
          KillrVideoMapper.builder(session).withDefaultKeyspace(KEYSPACE_ID).build();

      // Create a new user
      UserDao userDao = mapper.userDao();

      User user = new User(Uuids.random(), "test", "user", "testuser@example.com", Instant.now());

      if (userDao.create(user, "fakePasswordForTests".toCharArray())) {
        System.out.println("Created " + user);
      } else {
        user = userDao.getByEmail("testuser@example.com");
        System.out.println("Reusing existing " + user);
      }

      // Creating another user with the same email should fail
      assert !userDao.create(
          new User(Uuids.random(), "test2", "user", "testuser@example.com", Instant.now()),
          "fakePasswordForTests2".toCharArray());

      // Simulate login attempts
      tryLogin(userDao, "testuser@example.com", "fakePasswordForTests");
      tryLogin(userDao, "testuser@example.com", "fakePasswordForTests2");

      // Insert a video
      VideoDao videoDao = mapper.videoDao();

      Video video = new Video();
      video.setUserid(user.getUserid());
      video.setName(
          "Getting Started with DataStax Apache Cassandra as a Service on DataStax Astra");
      video.setLocation("https://www.youtube.com/watch?v=68xzKpcZURA");
      Set<String> tags = new HashSet<>();
      tags.add("apachecassandra");
      tags.add("nosql");
      tags.add("hybridcloud");
      video.setTags(tags);

      videoDao.create(video);
      System.out.printf("Created video [%s] %s%n", video.getVideoid(), video.getName());

      // Check that associated denormalized tables have also been updated:
      PagingIterable<UserVideo> userVideos = videoDao.getByUser(user.getUserid());
      System.out.printf("Videos for %s %s:%n", user.getFirstname(), user.getLastname());
      for (UserVideo userVideo : userVideos) {
        System.out.printf("  [%s] %s%n", userVideo.getVideoid(), userVideo.getName());
      }

      PagingIterable<LatestVideo> latestVideos = videoDao.getLatest(todaysTimestamp());
      System.out.println("Latest videos:");
      for (LatestVideo latestVideo : latestVideos) {
        System.out.printf("  [%s] %s%n", latestVideo.getVideoid(), latestVideo.getName());
      }

      PagingIterable<VideoByTag> videosByTag = videoDao.getByTag("apachecassandra");
      System.out.println("Videos tagged with apachecassandra:");
      for (VideoByTag videoByTag : videosByTag) {
        System.out.printf("  [%s] %s%n", videoByTag.getVideoid(), videoByTag.getName());
      }

      // Update the existing video:
      Video template = new Video();
      template.setVideoid(video.getVideoid());
      template.setName(
          "Getting Started with DataStax Apache Cassandra® as a Service on DataStax Astra");
      videoDao.update(template);
      // Reload the whole entity and check the fields
      video = videoDao.get(video.getVideoid());
      System.out.printf("Updated name for video %s: %s%n", video.getVideoid(), video.getName());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void tryLogin(UserDao userDao, String email, String password) {
    Optional<User> maybeUser = userDao.login(email, password.toCharArray());
    System.out.printf(
        "Logging in with %s/%s: %s%n",
        email, password, maybeUser.isPresent() ? "Success" : "Failure");
  }

  private static void maybeCreateSchema(CqlSession session) throws Exception {
    session.execute(
        SimpleStatement.newInstance(
                "CREATE KEYSPACE IF NOT EXISTS killrvideo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
            .setExecutionProfileName("slow"));
    session.execute("USE killrvideo");
    for (String statement : getStatements("killrvideo_schema.cql")) {
      session.execute(SimpleStatement.newInstance(statement).setExecutionProfileName("slow"));
    }
  }

  private static List<String> getStatements(String fileName) throws Exception {
    Path path = Paths.get(ClassLoader.getSystemResource(fileName).toURI());
    String contents = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    return Arrays.stream(contents.split(";"))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }

  /**
   * KillrVideo uses a textual timestamp to partition recent video. Build the timestamp for today to
   * fetch our latest insertions.
   */
  private static String todaysTimestamp() {
    return DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC).format(Instant.now());
  }
}
