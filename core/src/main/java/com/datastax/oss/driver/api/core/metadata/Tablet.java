package com.datastax.oss.driver.api.core.metadata;

import com.datastax.oss.driver.shaded.guava.common.annotations.Beta;
import java.util.Set;

/**
 * Represents a tablet as described in tablets-routing-v1 protocol extension with some additional
 * fields for ease of use.
 */
@Beta
public interface Tablet extends Comparable<Tablet> {
  /**
   * Returns left endpoint of an interval. This interval is left-open, meaning the tablet does not
   * own the token equal to the first token.
   *
   * @return {@code long} value representing first token.
   */
  public long getFirstToken();

  /**
   * Returns right endpoint of an interval. This interval is right-closed, which means that last
   * token is owned by this tablet.
   *
   * @return {@code long} value representing last token.
   */
  public long getLastToken();

  public Set<Node> getReplicaNodes();

  /**
   * Looks up the shard number for specific replica Node.
   *
   * @param node one of the replica nodes of this tablet.
   * @return Shard number for the replica or -1 if no such Node found.
   */
  public int getShardForNode(Node node);
}
