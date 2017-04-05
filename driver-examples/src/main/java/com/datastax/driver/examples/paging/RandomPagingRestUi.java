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
import java.util.Collections;
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
 * Navigation is bidirectional, and you can jump to a random page (by modifying the URL).
 * Cassandra does not support offset queries (see https://issues.apache.org/jira/browse/CASSANDRA-6511), so we emulate
 * it by restarting from the beginning each time, and iterating through the results until we reach the requested page.
 * This is fundamentally inefficient (O(n) in the number of rows skipped), but the tradeoff might be acceptable for some
 * use cases; for example, if you show 10 results per page and you think users are unlikely to browse past page 10,
 * you only need to retrieve at most 100 rows.
 * <p/>
 * Preconditions:
 * - a Cassandra cluster is running and accessible through the contacts points identified by CONTACT_POINTS and
 * CASSANDRA_PORT;
 * - port HTTP_PORT is available.
 * <p/>
 * Side effects:
 * - creates a new keyspace "examples" in the cluster. If a keyspace with this name already exists, it will be reused;
 * - creates a table "examples.random_paging_rest_ui". If it already exists, it will be reused;
 * - inserts data in the table;
 * - launches a REST server listening on HTTP_PORT.
 */
public class RandomPagingRestUi {

    static final String[] CONTACT_POINTS = {"127.0.0.1"};

    static final int CASSANDRA_PORT = 9042;

    static final int HTTP_PORT = 8080;

    static final int ITEMS_PER_PAGE = 10;
    // How many rows the driver will retrieve at a time.
    // This is set artificially low for the sake of this example. Unless your rows are very large, you can probably use
    // a much higher value (the driver's default is 5000).
    static final int FETCH_SIZE = 60;

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
        session.execute("CREATE TABLE IF NOT EXISTS examples.random_paging_rest_ui(" +
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
                session.execute("INSERT INTO examples.random_paging_rest_ui (userid, username, added, videoid, title) VALUES (?, ?, ?, ?, ?)",
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
     * <p/>
     * This is also the place where you would normally configure JSON serialization, etc.
     * <p/>
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
        private Pager pager;

        @PostConstruct
        @SuppressWarnings("unused")
        public void init() {
            this.pager = new Pager(session, ITEMS_PER_PAGE);
            this.videosByUser = session.prepare("SELECT videoid, title, added FROM examples.random_paging_rest_ui WHERE userid = ?");
        }

        /**
         * Returns a paginated list of all the videos created by the given user.
         *
         * @param userid the user ID.
         * @param page   the page to request, or {@code null} to get the first page.
         */
        @GET
        @Path("/{userid}/videos")
        public UserVideosResponse getUserVideos(@PathParam("userid") int userid, @QueryParam("page") Integer page) {

            Statement statement = videosByUser.bind(userid).setFetchSize(FETCH_SIZE);

            if (page == null) page = 1;
            ResultSet rs = pager.skipTo(statement, page);

            List<UserVideo> videos;
            boolean empty = rs.isExhausted();
            if (empty) {
                videos = Collections.emptyList();
            } else {
                int remaining = ITEMS_PER_PAGE;
                videos = new ArrayList<UserVideo>(remaining);
                for (Row row : rs) {
                    UserVideo video = new UserVideo(
                            row.getInt("videoid"),
                            row.getString("title"),
                            row.getTimestamp("added"));
                    videos.add(video);

                    if (--remaining == 0)
                        break;
                }
            }

            URI previous = (page == 1) ? null
                    : uri.getAbsolutePathBuilder().queryParam("page", page - 1).build();
            URI next = (empty) ? null
                    : uri.getAbsolutePathBuilder().queryParam("page", page + 1).build();
            return new UserVideosResponse(videos, previous, next);
        }

    }

    public static class UserVideosResponse {

        private final List<UserVideo> videos;

        private final URI previousPage;

        private final URI nextPage;

        public UserVideosResponse(List<UserVideo> videos, URI previousPage, URI nextPage) {
            this.videos = videos;
            this.previousPage = previousPage;
            this.nextPage = nextPage;
        }

        @SuppressWarnings("unused")
        public List<UserVideo> getVideos() {
            return videos;
        }

        @SuppressWarnings("unused")
        public URI getPreviousPage() {
            return previousPage;
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

    /**
     * Helper class to emulate random paging.
     * <p/>
     * Note that it MUST be stateless, because it is cached as a field in our HTTP handler.
     */
    static class Pager {
        private final Session session;
        private final int pageSize;

        Pager(Session session, int pageSize) {
            this.session = session;
            this.pageSize = pageSize;
        }

        ResultSet skipTo(Statement statement, int displayPage) {
            // Absolute index of the first row we want to display on the web page. Our goal is that rs.next() returns
            // that row.
            int targetRow = (displayPage - 1) * pageSize;

            ResultSet rs = session.execute(statement);
            // Absolute index of the next row returned by rs (if it is not exhausted)
            int currentRow = 0;
            int fetchedSize = rs.getAvailableWithoutFetching();
            byte[] nextState = rs.getExecutionInfo().getPagingStateUnsafe();

            // Skip protocol pages until we reach the one that contains our target row.
            // For example, if the first query returned 60 rows and our target is row number 90, we know we can skip
            // those 60 rows directly without even iterating through them.
            // This part is optional, we could simply iterate through the rows with the for loop below, but that's
            // slightly less efficient because iterating each row involves a bit of internal decoding.
            while (fetchedSize > 0 && nextState != null && currentRow + fetchedSize < targetRow) {
                statement.setPagingStateUnsafe(nextState);
                rs = session.execute(statement);
                currentRow += fetchedSize;
                fetchedSize = rs.getAvailableWithoutFetching();
                nextState = rs.getExecutionInfo().getPagingStateUnsafe();
            }

            if (currentRow < targetRow) {
                for (@SuppressWarnings("unused") Row row : rs) {
                    if (++currentRow == targetRow) break;
                }
            }
            // If targetRow is past the end, rs will be exhausted.
            // This means you can request a page past the end in the web UI (e.g. request page 12 while there are only
            // 10 pages), and it will show up as empty.
            // One improvement would be to detect that and take a different action, for example redirect to page 10 or
            // show an error message, this is left as an exercise for the reader.
            return rs;
        }
    }
}
