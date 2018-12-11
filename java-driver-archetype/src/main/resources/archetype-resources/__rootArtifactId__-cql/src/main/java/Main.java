package ${package};

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
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

  /**
   * Basic cqlsh-like prompt. It loops until the user types "EXIT", executing queries and dumping
   * the response to the console (System.out).
   */
  private void cqlshLite(CqlSession session, LineNumberReader input) throws IOException {
    // provide a prompt
    promptForQuery(WELCOME);
    // get the query
    String query = input.readLine();
    ResultSet rs;
    // execute the query
    while (!"exit".equalsIgnoreCase(query)) {
      try {
        rs = session.execute(query);
        Iterator<Row> iterator = rs.iterator();
        while (iterator.hasNext()) {
          Row row = iterator.next();
          System.out.println("Response: '" + row.getObject(0).toString() + "'");
        }
      } catch (Exception ex) {
        // something went wrong with the query, just dump the stacktrace to the output
        ex.printStackTrace(output);
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
      System.out.println();
      System.out.println("Done. Good Bye!");
    } catch (IOException ioe) {
      ioe.printStackTrace(main.output);
    }
  }
}
