package com.datastax.oss.driver.internal.core.metadata;

import static org.mockito.Mockito.mock;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.KeyspaceTableNamePair;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.Tablet;
import com.datastax.oss.driver.api.core.metadata.TabletMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.Test;
import org.testng.Assert;

public class DefaultTabletMapTest {

  @Test
  public void should_remove_overlapping_tablets() {
    TabletMap tabletMap = DefaultTabletMap.emptyMap();
    Tablet tablet1 =
        new DefaultTabletMap.DefaultTablet(0, 1, Collections.emptySet(), Collections.emptyMap());
    Tablet tablet2 =
        new DefaultTabletMap.DefaultTablet(1, 2, Collections.emptySet(), Collections.emptyMap());
    Tablet tablet3 =
        new DefaultTabletMap.DefaultTablet(2, 3, Collections.emptySet(), Collections.emptyMap());
    Tablet tablet4 =
        new DefaultTabletMap.DefaultTablet(
            -100, 100, Collections.emptySet(), Collections.emptyMap());

    Tablet tablet5 =
        new DefaultTabletMap.DefaultTablet(-10, 10, Collections.emptySet(), Collections.emptyMap());
    Tablet tablet6 =
        new DefaultTabletMap.DefaultTablet(9, 20, Collections.emptySet(), Collections.emptyMap());

    KeyspaceTableNamePair key1 =
        new KeyspaceTableNamePair(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab"));

    tabletMap.addTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab"), tablet1);
    Assert.assertEquals(tabletMap.getMapping().size(), 1);
    Assert.assertEquals(tabletMap.getMapping().get(key1).size(), 1);

    tabletMap.addTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab"), tablet2);
    Assert.assertEquals(tabletMap.getMapping().size(), 1);
    Assert.assertEquals(tabletMap.getMapping().get(key1).size(), 2);

    tabletMap.addTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab"), tablet3);
    Assert.assertEquals(tabletMap.getMapping().size(), 1);
    Assert.assertEquals(tabletMap.getMapping().get(key1).size(), 3);

    tabletMap.addTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab"), tablet4);
    Assert.assertEquals(tabletMap.getMapping().size(), 1);
    Assert.assertEquals(tabletMap.getMapping().get(key1).size(), 1);

    KeyspaceTableNamePair key2 =
        new KeyspaceTableNamePair(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab2"));

    tabletMap.addTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab2"), tablet5);
    Assert.assertEquals(tabletMap.getMapping().size(), 2);
    Assert.assertEquals(tabletMap.getMapping().get(key2).size(), 1);

    tabletMap.addTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab2"), tablet6);
    Assert.assertEquals(tabletMap.getMapping().size(), 2);
    Assert.assertEquals(tabletMap.getMapping().get(key2).size(), 1);
    Assert.assertTrue(tabletMap.getMapping().get(key2).contains(tablet6));
    Assert.assertEquals(
        tablet6,
        tabletMap.getTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab2"), 10L));
  }

  @Test
  public void tablet_range_should_not_include_first_token() {
    TabletMap tabletMap = DefaultTabletMap.emptyMap();
    Tablet tablet1 =
        new DefaultTabletMap.DefaultTablet(
            -123, 123, Collections.emptySet(), Collections.emptyMap());
    tabletMap.addTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab"), tablet1);
    Tablet result =
        tabletMap.getTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab"), -123);
    Assert.assertEquals(result, null);
  }

  @Test
  public void tablet_range_should_include_last_token() {
    TabletMap tabletMap = DefaultTabletMap.emptyMap();
    Tablet tablet1 =
        new DefaultTabletMap.DefaultTablet(
            -123, 456, Collections.emptySet(), Collections.emptyMap());
    tabletMap.addTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab"), tablet1);
    Tablet result =
        tabletMap.getTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab"), 456);
    Assert.assertEquals(result, tablet1);
  }

  @Test
  public void should_return_correct_shard() {
    Node node1 = mock(DefaultNode.class);
    Node node2 = mock(DefaultNode.class);
    Set<Node> replicaNodes = new HashSet<Node>();
    replicaNodes.add(node1);
    replicaNodes.add(node2);
    Map<Node, Integer> replicaShards = new HashMap<>();
    replicaShards.put(node1, 1);
    replicaShards.put(node2, 2);
    TabletMap tabletMap = DefaultTabletMap.emptyMap();
    Tablet tablet1 = new DefaultTabletMap.DefaultTablet(-123, 456, replicaNodes, replicaShards);
    tabletMap.addTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab"), tablet1);
    Tablet result =
        tabletMap.getTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab"), 456);
    Assert.assertEquals(result.getShardForNode(node1), 1);
    Assert.assertEquals(result.getShardForNode(node2), 2);
  }
}
