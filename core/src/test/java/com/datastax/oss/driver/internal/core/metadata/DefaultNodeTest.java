package com.datastax.oss.driver.internal.core.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.context.MockedDriverContextFactory;
import java.net.InetSocketAddress;
import java.util.UUID;
import org.junit.Test;

public class DefaultNodeTest {

  @Test
  public void should_have_expected_string_representation() {

    String uuidStr = "1e4687e6-f94e-432e-a792-216f89ef265f";
    UUID hostId = UUID.fromString(uuidStr);
    EndPoint endPoint = new DefaultEndPoint(new InetSocketAddress("localhost", 9042));
    DefaultNode node = new DefaultNode(endPoint, MockedDriverContextFactory.defaultDriverContext());
    node.hostId = hostId;

    String expected = uuidStr + "@" + "localhost/127.0.0.1:9042";
    assertThat(node.toString()).isEqualTo(expected);
  }
}
