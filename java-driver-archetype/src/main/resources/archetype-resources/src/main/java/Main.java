package ${package};

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    CqlSessionBuilder builder = CqlSession.builder();
    // Set the host and port of the Cassandra server here
    builder.addContactPoint(InetSocketAddress.createUnresolved("${cassandra-host}", ${cassandra-port}));
    try (CqlSession session = builder.build()) {
      ResultSet rs = session.execute("SELECT release_version FROM system.local");
      LOG.info("Cassandra release version: {}", rs.one().getString(0));
    }
  }
}
