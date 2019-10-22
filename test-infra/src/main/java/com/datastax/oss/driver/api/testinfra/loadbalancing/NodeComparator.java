package com.datastax.oss.driver.api.testinfra.loadbalancing;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.shaded.guava.common.primitives.UnsignedBytes;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Comparator;

public final class NodeComparator implements Comparator<Node> {

  public static final NodeComparator INSTANCE = new NodeComparator();

  private static final byte[] EMPTY = {};

  private NodeComparator() {}

  @Override
  public int compare(Node node1, Node node2) {
    // compare address bytes, byte by byte.
    byte[] address1 =
        node1
            .getBroadcastAddress()
            .map(InetSocketAddress::getAddress)
            .map(InetAddress::getAddress)
            .orElse(EMPTY);
    byte[] address2 =
        node2
            .getBroadcastAddress()
            .map(InetSocketAddress::getAddress)
            .map(InetAddress::getAddress)
            .orElse(EMPTY);

    int result = UnsignedBytes.lexicographicalComparator().compare(address1, address2);
    if (result != 0) {
      return result;
    }

    int port1 = node1.getBroadcastAddress().map(InetSocketAddress::getPort).orElse(0);
    int port2 = node2.getBroadcastAddress().map(InetSocketAddress::getPort).orElse(0);
    return port1 - port2;
  }
}
