/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.api.core.metadata.DseNodeProperties;
import com.datastax.dse.driver.internal.core.context.DseDriverContext;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** Utility for common Graph Unit testing. */
class GraphTestUtil {

  // Default GraphSON sub protocol version differs based on DSE version, so test with a version less
  // than DSE 6.8 as well as DSE 6.8.
  static final Version DSE_6_7_0 = Objects.requireNonNull(Version.parse("6.7.0"));
  static final Version DSE_6_8_0 = Objects.requireNonNull(Version.parse("6.8.0"));

  static DseDriverContext mockContext(boolean treatNullAsMissing, Version... dseVersions) {
    DseDriverContext mockContext = mock(DseDriverContext.class);
    return mockContext(mockContext, treatNullAsMissing, dseVersions);
  }

  static DseDriverContext mockContext(
      DseDriverContext context, boolean treatNullAsMissing, Version... dseVersions) {
    // mock bits of the context
    MetadataManager metadataManager = mock(MetadataManager.class);
    Metadata metadata = mock(Metadata.class);
    Map<UUID, Node> nodeMap = new HashMap<>((dseVersions != null) ? dseVersions.length : 1);
    if (dseVersions == null) {
      Node node = mock(Node.class);
      Map<String, Object> nodeExtras = new HashMap<>(1);
      if (!treatNullAsMissing) {
        // put an explicit null in for DSE_VERSION
        nodeExtras.put(DseNodeProperties.DSE_VERSION, null);
      }
      nodeMap.put(UUID.randomUUID(), node);
      when(node.getExtras()).thenReturn(nodeExtras);
    } else {
      for (Version dseVersion : dseVersions) {
        // create a node with DSE version in its extra data
        Node node = mock(Node.class);
        Map<String, Object> nodeExtras = new HashMap<>(1);
        if (dseVersion != null || !treatNullAsMissing) {
          nodeExtras.put(DseNodeProperties.DSE_VERSION, dseVersion);
        }
        nodeMap.put(UUID.randomUUID(), node);
        when(node.getExtras()).thenReturn(nodeExtras);
      }
    }
    // return mocked data when requested
    when(metadata.getNodes()).thenReturn(nodeMap);
    when(metadataManager.getMetadata()).thenReturn(metadata);
    when(context.getMetadataManager()).thenReturn(metadataManager);
    return context;
  }
}
