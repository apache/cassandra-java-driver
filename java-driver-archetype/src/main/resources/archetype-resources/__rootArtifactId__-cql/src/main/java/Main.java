package ${package};

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger LOG = LoggerFactory.getLogger(Main.class);
  private static final String WELCOME =
      "This is a CQL demo. " +
      "See https://docs.datastax.com/en/cql/3.3/cql/cql_reference/cqlCommandsTOC.html " +
      "for more info on CQL commands. Type 'EXIT' to quit.";

  private final PrintStream output;

  private Main() {
    this.output = System.out;
  }

  private void promptForQuery(String msg) {
    if (msg != null) {
      output.println();
      output.println(msg);
    }
    output.println();
    output.print("cql-demo> ");
  }

  private void printKeyspaceTables(KeyspaceMetadata ksMetadata) {
    System.out.println();
    final String ksOutput = "Keyspace " + ksMetadata.getName().asInternal();
    System.out.println(ksOutput);
    for (int i = 0; i < ksOutput.length(); ++i) {
      System.out.print("-");
    }
    System.out.println();
    for (CqlIdentifier cqlId : ksMetadata.getTables().keySet()) {
      System.out.println(cqlId.asInternal());
    }
  }

  private void handleDescribe(CqlSession session, String query) {
    // strip the describe command off the query
    String describeTarget = query.substring("describe".length() + 1);
    // get the metadata
    Metadata metadata = session.getMetadata();
    if (describeTarget.startsWith("keyspaces") || describeTarget.startsWith("KEYSPACES")) {
      System.out.println();
      for (CqlIdentifier cqlId : metadata.getKeyspaces().keySet()) {
        System.out.print(cqlId.asInternal() + "  ");
      }
      System.out.println();
    } else if (describeTarget.startsWith("tables") || describeTarget.startsWith("TABLES")) {
      // dump all tables from the current keyspace or all keyspaces if no current keyspace
      if (session.getKeyspace().isPresent()) {
        // get the tables from the current keyspace
        printKeyspaceTables(metadata.getKeyspace(session.getKeyspace().get()).get());
      } else {
        for (KeyspaceMetadata ksMetadata : metadata.getKeyspaces().values()) {
          printKeyspaceTables(ksMetadata);
        }
      }
    } else {
      System.out.println("\nDescribe target not implemented: '" + describeTarget + "'");
    }
  }

  private void executeQuery(CqlSession session, String query) {
    try {
      ResultSet rs = session.execute(query);
      Iterator<Row> iterator = rs.iterator();
      while (iterator.hasNext()) {
        Row row = iterator.next();
        ColumnDefinitions cd = row.getColumnDefinitions();
        for (int i = 0; i < cd.size(); ++i) {
          System.out.println(cd.get(i).getName() + ":  " + row.getObject(i).toString());
        }
      }
    } catch (Exception ex) {
      // something went wrong with the query, just dump the stacktrace to the output
      ex.printStackTrace(output);
    }
  }
  /**
   * Basic cqlsh-like prompt. It loops until the user types "EXIT", executing queries and dumping
   * the response to the console (System.out).
   */
  private void cqlshLite(CqlSession session, LineNumberReader input) throws IOException {
    // provide a prompt
    promptForQuery(WELCOME);
    // get the query
    String query = input.readLine();
    // execute the query
    while (!"exit".equalsIgnoreCase(query)) {
      if (query.startsWith("describe") || query.startsWith("DESCRIBE")) {
        handleDescribe(session, query);
      } else {
        executeQuery(session, query);
      }
      // provide another prompt
      promptForQuery(null);
      // get the next query
      query = input.readLine();
    }
  }

  public static void main(String[] args) {
    Main main = new Main();
    CqlSessionBuilder builder = CqlSession.builder();
    // Set the host and port of the Cassandra server here
    builder.addContactPoint(new InetSocketAddress("127.0.0.1", 9042));
    try (CqlSession session = builder.build();
        LineNumberReader commandLine = new LineNumberReader(new InputStreamReader(System.in))) {
      ResultSet rs = session.execute("SELECT release_version FROM system.local");
      LOG.info("Cassandra release version: {}", rs.one().getString(0));
      // run the cqlsh demo
      main.cqlshLite(session, commandLine);
      // demo exited
      System.out.println("\nGood Bye!");
    } catch (IOException ioe) {
      ioe.printStackTrace(main.output);
    }
  }
}
