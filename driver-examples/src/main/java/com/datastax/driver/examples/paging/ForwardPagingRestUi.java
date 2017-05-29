/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.examples.paging;

import com.datastax.driver.core.*;
import com.sun.net.httpserver.HttpServer;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A stateless REST service (backed by
 * <a href="https://jersey.java.net/">Jersey</a>,
 * <a href="https://hk2.java.net/">HK2</a> and
 * the JDK HttpServer) that displays paginated results for a CQL query.
 * <p/>
 * Conversion to and from JSON is made through
 * <a href="https://jersey.java.net/documentation/latest/media.html#json.jackson">Jersey Jackson providers</a>.
 * <p/>
 * Navigation is forward-only.
 * The implementation relies on the paging state returned by Cassandra, and encodes it in HTTP URLs.
 * <p/>
 * Preconditions:
 * - a Cassandra cluster is running and accessible through the contacts points identified by CONTACT_POINTS and
 * CASSANDRA_PORT;
 * - port HTTP_PORT is available.
 * <p/>
 * Side effects:
 * - creates a new keyspace "examples" in the cluster. If a keyspace with this name already exists, it will be reused;
 * - creates a table "examples.forward_paging_rest_ui". If it already exists, it will be reused;
 * - inserts data in the table;
 * - launches a REST server listening on HTTP_PORT.
 */
public class ForwardPagingRestUi {

    static final String[] CONTACT_POINTS = {"127.0.0.1"};

    static final int CASSANDRA_PORT = 9042;

    static final int HTTP_PORT = 8080;

    static final int ITEMS_PER_PAGE = 10;

    static final URI BASE_URI = UriBuilder.fromUri("http://localhost/").path("").port(HTTP_PORT).build();

    public static void main(String[] args) throws Exception {

        Cluster cluster = null;
        try {

            cluster = Cluster.builder()
                    .addContactPoints(CONTACT_POINTS).withPort(CASSANDRA_PORT)
                    .build();
            Session session = cluster.connect();

            createSchema(session);
            populateSchema(session);
            startRestService(session);

        } finally {
            if (cluster != null) cluster.close();
        }

    }

    // Creates a table storing videos by users, in a typically denormalized way
    private static void createSchema(Session session) {
        session.execute("CREATE KEYSPACE IF NOT EXISTS examples " +
                "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        session.execute("CREATE TABLE IF NOT EXISTS examples.forward_paging_rest_ui(" +
                "userid int, username text, " +
                "added timestamp, " +
                "videoid int, title text, " +
                "PRIMARY KEY (userid, added, videoid)" +
                ") WITH CLUSTERING ORDER BY (added DESC, videoid ASC)");
    }

    private static void populateSchema(Session session) {
        // 3 users
        for (int i = 0; i < 3; i++) {
            // 49 videos each
            for (int j = 0; j < 49; j++) {
                int videoid = i * 100 + j;
                session.execute("INSERT INTO examples.forward_paging_rest_ui (userid, username, added, videoid, title) VALUES (?, ?, ?, ?, ?)",
                        i, "user " + i, new Date(j * 100000), videoid, "video " + videoid);
            }
        }
    }

    // starts the REST server using JDK HttpServer (com.sun.net.httpserver.HttpServer)
    private static void startRestService(Session session) throws IOException, InterruptedException {

        final HttpServer server = JdkHttpServerFactory.createHttpServer(BASE_URI, new VideoApplication(session), false);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        server.setExecutor(executor);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println();
                System.out.println("Stopping REST Service");
                server.stop(0);
                executor.shutdownNow();
                System.out.println("REST Service stopped");
            }
        }));
        server.start();

        System.out.println();
        System.out.printf("REST Service started on http://localhost:%d/users, press CTRL+C to stop%n", HTTP_PORT);
        System.out.println("To explore this example, start with the following request and walk from there:");
        System.out.printf("curl -i http://localhost:%d/users/1/videos%n", HTTP_PORT);
        System.out.println();

        Thread.currentThread().join();

    }

    /**
     * Configures the REST application and handles injection of custom objects, such
     * as the driver session.
     * <p>
     * This is also the place where you would normally configure JSON serialization, etc.
     * <p>
     * Note that in this example, we rely on the automatic discovery and configuration of
     * Jackson through {@code org.glassfish.jersey.jackson.JacksonFeature}.
     */
    public static class VideoApplication extends ResourceConfig {

        public VideoApplication(final Session session) {
            super(UserService.class);
            // AbstractBinder is provided by HK2
            register(new AbstractBinder() {

                @Override
                protected void configure() {
                    bind(session).to(Session.class);
                }

            });
        }

    }

    /**
     * A typical REST service, handling requests involving users.
     * <p/>
     * Typically, this service would contain methods for listing and searching for users,
     * and methods to retrieve user details. Here, for brevity,
     * only one method, listing videos by user, is implemented.
     */
    @Singleton
    @Path("/users")
    @Produces("application/json")
    public static class UserService {

        @Inject
        private Session session;

        @Context
        private UriInfo uri;

        private PreparedStatement videosByUser;

        @PostConstruct
        @SuppressWarnings("unused")
        public void init() {
            this.videosByUser = session.prepare("SELECT videoid, title, added FROM examples.forward_paging_rest_ui WHERE userid = ?");
        }

        /**
         * Returns a paginated list of all the videos created by the given user.
         *
         * @param userid the user ID.
         * @param page the page to request, or {@code null} to get the first page.
         */
        @GET
        @Path("/{userid}/videos")
        public UserVideosResponse getUserVideos(@PathParam("userid") int userid, @QueryParam("page") String page) {

            Statement statement = videosByUser.bind(userid).setFetchSize(ITEMS_PER_PAGE);
            if (page != null)
                statement.setPagingState(PagingState.fromString(page));

            ResultSet rs = session.execute(statement);
            PagingState nextPage = rs.getExecutionInfo().getPagingState();

            int remaining = rs.getAvailableWithoutFetching();
            List<UserVideo> videos = new ArrayList<UserVideo>(remaining);

            if (remaining > 0) {
                for (Row row : rs) {

                    UserVideo video = new UserVideo(
                            row.getInt("videoid"),
                            row.getString("title"),
                            row.getTimestamp("added"));
                    videos.add(video);

                    // Make sure we don't go past the current page (we don't want the driver to fetch the next one)
                    if (--remaining == 0)
                        break;
                }
            }

            URI next = null;
            if (nextPage != null)
                next = uri.getAbsolutePathBuilder().queryParam("page", nextPage).build();

            return new UserVideosResponse(videos, next);
        }

    }

    public static class UserVideosResponse {

        private final List<UserVideo> videos;

        private final URI nextPage;

        public UserVideosResponse(List<UserVideo> videos, URI nextPage) {
            this.videos = videos;
            this.nextPage = nextPage;
        }

        @SuppressWarnings("unused")
        public List<UserVideo> getVideos() {
            return videos;
        }

        @SuppressWarnings("unused")
        public URI getNextPage() {
            return nextPage;
        }

    }

    public static class UserVideo {

        private final int videoid;

        private final String title;

        private final Date added;

        public UserVideo(int videoid, String title, Date added) {
            this.videoid = videoid;
            this.title = title;
            this.added = added;
        }

        @SuppressWarnings("unused")
        public int getVideoid() {
            return videoid;
        }

        public String getTitle() {
            return title;
        }

        @SuppressWarnings("unused")
        public Date getAdded() {
            return added;
        }
    }

}
