package com.datastax.driver.examples.basic;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This example shows how to create schema objects,
 * load data into Cassandra then retrieve and display them.
 *
 * @see <a href="http://datastax.github.io/java-driver/manual/">Java driver online manual</a>
 */
public class Example0003 {

    private static final Logger logger = LoggerFactory.getLogger("com.datastax.driver.examples");

    public static void main(String[] args) {

        Example0003 client = new Example0003();

        try {

            client.connect("127.0.0.1");
            client.createSchema();
            client.loadData();
            client.querySchema();

        } finally {
            client.close();
        }
    }

    private Cluster cluster;

    private Session session;

    /**
     * Initiates a connection to the cluster
     * specified by the given contact point.
     *
     * @param node The contact point to use
     */
    public void connect(String node) {

        cluster = Cluster.builder()
                .addContactPoint(node)
                .build();

        Metadata metadata = cluster.getMetadata();

        logger.info("Connected to cluster: {}", metadata.getClusterName());

        session = cluster.connect();
    }

    /**
     * Creates the schema (keyspace) and tables
     * for this example.
     */
    public void createSchema() {

        session.execute("CREATE KEYSPACE IF NOT EXISTS simplex WITH replication " +
                "= {'class':'SimpleStrategy', 'replication_factor':3};");

        session.execute(
                "CREATE TABLE IF NOT EXISTS simplex.songs (" +
                        "id uuid PRIMARY KEY," +
                        "title text," +
                        "album text," +
                        "artist text," +
                        "tags set<text>," +
                        "data blob" +
                        ");");

        session.execute(
                "CREATE TABLE IF NOT EXISTS simplex.playlists (" +
                        "id uuid," +
                        "title text," +
                        "album text, " +
                        "artist text," +
                        "song_id uuid," +
                        "PRIMARY KEY (id, title, album, artist)" +
                        ");");
    }

    /**
     * Inserts data into the tables.
     */
    public void loadData() {

        session.execute(
                "INSERT INTO simplex.songs (id, title, album, artist, tags) " +
                        "VALUES (" +
                        "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                        "'La Petite Tonkinoise'," +
                        "'Bye Bye Blackbird'," +
                        "'Joséphine Baker'," +
                        "{'jazz', '2013'})" +
                        ";");

        session.execute(
                "INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
                        "VALUES (" +
                        "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
                        "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                        "'La Petite Tonkinoise'," +
                        "'Bye Bye Blackbird'," +
                        "'Joséphine Baker'" +
                        ");");
    }

    /**
     * Queries and displays data.
     */
    public void querySchema() {

        ResultSet results = session.execute(
                "SELECT * FROM simplex.playlists " +
                        "WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");

        logger.info(String.format("%-30s\t%-20s\t%-20s", "title", "album", "artist"));
        logger.info("-------------------------------+-----------------------+--------------------");

        for (Row row : results) {

            logger.info(String.format("%-30s\t%-20s\t%-20s",
                    row.getString("title"),
                    row.getString("album"),
                    row.getString("artist")));

        }

    }

    /**
     * Closes the session and the cluster.
     */
    public void close() {
        session.close();
        cluster.close();
    }

}
