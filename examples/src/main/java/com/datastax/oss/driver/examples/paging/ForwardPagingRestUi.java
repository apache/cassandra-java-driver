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
package com.datastax.oss.driver.examples.paging;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.type.codec.DateCodec;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

/**
 * A stateless REST service (backed by <a href="https://jersey.java.net/">Jersey</a>, <a
 * href="https://hk2.java.net/">HK2</a> and the JDK HttpServer) that displays paginated results for
 * a CQL query.
 *
 * <p>Conversion to and from JSON is made through <a
 * href="https://jersey.java.net/documentation/latest/media.html#json.jackson">Jersey Jackson
 * providers</a>.
 *
 * <p>Navigation is forward-only. The implementation relies on the paging state returned by
 * Cassandra, and encodes it in HTTP URLs.
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
 *   <li>creates a new keyspace "examples" in the cluster. If a keyspace with this name already
 *       exists, it will be reused;
 *   <li>creates a table "examples.forward_paging_rest_ui". If it already exists, it will be reused;
 *   <li>inserts data in the table;
 *   <li>launches a REST server listening on HTTP_PORT.
 * </ul>
 */
public class ForwardPagingRestUi {

  private static final int HTTP_PORT = 8080;

  private static final int ITEMS_PER_PAGE = 10;

  private static final URI BASE_URI =
      UriBuilder.fromUri("http://localhost/").path("").port(HTTP_PORT).build();

  public static void main(String[] args) throws Exception {

    try (CqlSession session = CqlSession.builder().addTypeCodecs(new DateCodec()).build()) {
      createSchema(session);
      populateSchema(session);
      startRestService(session);
    }
  }

  // Creates a table storing videos by users, in a typically denormalized way
  private static void createSchema(CqlSession session) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS examples "
            + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
    session.execute(
        "CREATE TABLE IF NOT EXISTS examples.forward_paging_rest_ui("
            + "userid int, username text, "
            + "added timestamp, "
            + "videoid int, title text, "
            + "PRIMARY KEY (userid, added, videoid)"
            + ") WITH CLUSTERING ORDER BY (added DESC, videoid ASC)");
  }

  private static void populateSchema(CqlSession session) {
    PreparedStatement prepare =
        session.prepare(
            "INSERT INTO examples.forward_paging_rest_ui (userid, username, added, videoid, title) VALUES (?, ?, ?, ?, ?)");
    // 3 users
    for (int i = 0; i < 3; i++) {
      // 49 videos each
      for (int j = 0; j < 49; j++) {
        int videoid = i * 100 + j;
        session.execute(
            prepare.bind(
                i, "user " + i, Instant.ofEpochMilli(j * 100000), videoid, "video " + videoid));
      }
    }
  }

  // starts the REST server using JDK HttpServer (com.sun.net.httpserver.HttpServer)
  private static void startRestService(CqlSession session)
      throws IOException, InterruptedException {

    final HttpServer server =
        JdkHttpServerFactory.createHttpServer(BASE_URI, new VideoApplication(session), false);
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    server.setExecutor(executor);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  System.out.println();
                  System.out.println("Stopping REST Service");
                  server.stop(0);
                  executor.shutdownNow();
                  System.out.println("REST Service stopped");
                }));
    server.start();

    System.out.println();
    System.out.printf(
        "REST Service started on http://localhost:%d/users, press CTRL+C to stop%n", HTTP_PORT);
    System.out.println(
        "To explore this example, start with the following request and walk from there:");
    System.out.printf("curl -i http://localhost:%d/users/1/videos%n", HTTP_PORT);
    System.out.println();

    Thread.currentThread().join();
  }

  /**
   * Configures the REST application and handles injection of custom objects, such as the driver
   * session.
   *
   * <p>This is also the place where you would normally configure JSON serialization, etc.
   *
   * <p>Note that in this example, we rely on the automatic discovery and configuration of Jackson
   * through {@code org.glassfish.jersey.jackson.JacksonFeature}.
   */
  public static class VideoApplication extends ResourceConfig {

    public VideoApplication(final CqlSession session) {
      super(UserService.class);
      // AbstractBinder is provided by HK2
      register(
          new AbstractBinder() {

            @Override
            protected void configure() {
              bind(session).to(CqlSession.class);
            }
          });
    }
  }

  /**
   * A typical REST service, handling requests involving users.
   *
   * <p>Typically, this service would contain methods for listing and searching for users, and
   * methods to retrieve user details. Here, for brevity, only one method, listing videos by user,
   * is implemented.
   */
  @Singleton
  @Path("/users")
  @Produces("application/json")
  public static class UserService {

    @Inject private CqlSession session;

    @Context private UriInfo uri;

    private PreparedStatement videosByUser;

    @PostConstruct
    @SuppressWarnings("unused")
    public void init() {
      this.videosByUser =
          session.prepare(
              "SELECT videoid, title, added FROM examples.forward_paging_rest_ui WHERE userid = ?");
    }

    /**
     * Returns a paginated list of all the videos created by the given user.
     *
     * @param userid the user ID.
     * @param page the page to request, or {@code null} to get the first page.
     */
    @GET
    @Path("/{userid}/videos")
    public UserVideosResponse getUserVideos(
        @PathParam("userid") int userid, @QueryParam("page") String page) {

      BoundStatementBuilder statementBuilder =
          videosByUser.boundStatementBuilder(userid).setPageSize(ITEMS_PER_PAGE);
      if (page != null) {
        statementBuilder.setPagingState(Bytes.fromHexString(page));
      }

      ResultSet rs = session.execute(statementBuilder.build());
      String nextPage = Bytes.toHexString(rs.getExecutionInfo().getPagingState());

      int remaining = rs.getAvailableWithoutFetching();
      List<UserVideo> videos = new ArrayList<>(remaining);

      if (remaining > 0) {
        for (Row row : rs) {

          UserVideo video =
              new UserVideo(row.getInt("videoid"), row.getString("title"), row.getInstant("added"));
          videos.add(video);

          // Make sure we don't go past the current page (we don't want the driver to fetch the next
          // one)
          if (--remaining == 0) {
            break;
          }
        }
      }

      URI next = null;
      if (nextPage != null)
        next = uri.getAbsolutePathBuilder().queryParam("page", nextPage).build();

      return new UserVideosResponse(videos, next);
    }
  }

  @SuppressWarnings("unused")
  public static class UserVideosResponse {

    private final List<UserVideo> videos;

    private final URI nextPage;

    public UserVideosResponse(List<UserVideo> videos, URI nextPage) {
      this.videos = videos;
      this.nextPage = nextPage;
    }

    public List<UserVideo> getVideos() {
      return videos;
    }

    public URI getNextPage() {
      return nextPage;
    }
  }

  @SuppressWarnings("unused")
  public static class UserVideo {

    private final int videoid;

    private final String title;

    private final Instant added;

    public UserVideo(int videoid, String title, Instant added) {
      this.videoid = videoid;
      this.title = title;
      this.added = added;
    }

    public int getVideoid() {
      return videoid;
    }

    public String getTitle() {
      return title;
    }

    public Instant getAdded() {
      return added;
    }
  }
}
