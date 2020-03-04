/*
 * Copyright DataStax, Inc.
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
package com.datastax.dse.driver.internal.core.graph.schema.refresh;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.api.core.metadata.schema.DseEdgeMetadata;
import com.datastax.dse.driver.api.core.metadata.schema.DseGraphTableMetadata;
import com.datastax.dse.driver.api.core.metadata.schema.DseVertexMetadata;
import com.datastax.dse.driver.internal.core.metadata.schema.DefaultDseEdgeMetadata;
import com.datastax.dse.driver.internal.core.metadata.schema.DefaultDseKeyspaceMetadata;
import com.datastax.dse.driver.internal.core.metadata.schema.DefaultDseTableMetadata;
import com.datastax.dse.driver.internal.core.metadata.schema.DefaultDseVertexMetadata;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.channel.ChannelFactory;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultMetadata;
import com.datastax.oss.driver.internal.core.metadata.MetadataRefresh;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultColumnMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.events.KeyspaceChangeEvent;
import com.datastax.oss.driver.internal.core.metadata.schema.events.TableChangeEvent;
import com.datastax.oss.driver.internal.core.metadata.schema.refresh.SchemaRefresh;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Collections;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GraphSchemaRefreshTest {

  private static final DefaultDseTableMetadata OLD_TABLE =
      newTable(
          CqlIdentifier.fromInternal("ks_with_engine"),
          CqlIdentifier.fromInternal("tbl"),
          null,
          null);
  private static final DefaultDseKeyspaceMetadata OLD_KS1 = newKeyspace("ks1", null);
  private static final DefaultDseKeyspaceMetadata KS_WITH_ENGINE =
      newKeyspace(
          CqlIdentifier.fromInternal("ks_with_engine"),
          "Core",
          ImmutableMap.of(CqlIdentifier.fromInternal("tbl"), OLD_TABLE));

  @Mock private InternalDriverContext context;
  @Mock private ChannelFactory channelFactory;
  private DefaultMetadata oldMetadata;

  @Before
  public void setup() {
    when(context.getChannelFactory()).thenReturn(channelFactory);
    oldMetadata =
        DefaultMetadata.EMPTY.withSchema(
            ImmutableMap.of(OLD_KS1.getName(), OLD_KS1, KS_WITH_ENGINE.getName(), KS_WITH_ENGINE),
            false,
            context);
  }

  @Test
  public void should_detect_created_keyspace_without_graph_engine() {
    DefaultDseKeyspaceMetadata ks2 = newKeyspace("ks2", null);
    SchemaRefresh refresh =
        new SchemaRefresh(
            ImmutableMap.of(
                OLD_KS1.getName(),
                OLD_KS1,
                KS_WITH_ENGINE.getName(),
                KS_WITH_ENGINE,
                ks2.getName(),
                ks2));
    MetadataRefresh.Result result = refresh.compute(oldMetadata, false, context);
    assertThat(result.newMetadata.getKeyspaces()).hasSize(3);
    assertThat(result.events).containsExactly(KeyspaceChangeEvent.created(ks2));
  }

  @Test
  public void should_detect_created_keyspace_with_graph_engine() {
    DefaultDseKeyspaceMetadata ks2 = newKeyspace("ks2", "Core");
    SchemaRefresh refresh =
        new SchemaRefresh(
            ImmutableMap.of(
                OLD_KS1.getName(),
                OLD_KS1,
                KS_WITH_ENGINE.getName(),
                KS_WITH_ENGINE,
                ks2.getName(),
                ks2));
    MetadataRefresh.Result result = refresh.compute(oldMetadata, false, context);
    assertThat(result.newMetadata.getKeyspaces()).hasSize(3);
    assertThat(result.events).containsExactly(KeyspaceChangeEvent.created(ks2));
  }

  @Test
  public void should_detect_top_level_graph_engine_update_in_keyspace() {
    // Change only one top-level option (graph_engine)
    DefaultDseKeyspaceMetadata newKs1 = newKeyspace("ks1", "Core");
    SchemaRefresh refresh =
        new SchemaRefresh(
            ImmutableMap.of(KS_WITH_ENGINE.getName(), KS_WITH_ENGINE, OLD_KS1.getName(), newKs1));
    MetadataRefresh.Result result = refresh.compute(oldMetadata, false, context);
    assertThat(result.newMetadata.getKeyspaces()).hasSize(2);
    assertThat(result.events).containsExactly(KeyspaceChangeEvent.updated(OLD_KS1, newKs1));
  }

  @Test
  public void should_detect_adding_and_renaming_and_removing_vertex_label() {
    DefaultDseTableMetadata newTable =
        newTable(
            KS_WITH_ENGINE.getName(),
            CqlIdentifier.fromInternal("tbl"),
            new DefaultDseVertexMetadata(CqlIdentifier.fromInternal("someLabel")),
            null);
    DefaultDseKeyspaceMetadata ks =
        newKeyspace(
            KS_WITH_ENGINE.getName(),
            "Core",
            ImmutableMap.of(CqlIdentifier.fromInternal("tbl"), newTable));
    SchemaRefresh refresh =
        new SchemaRefresh(
            ImmutableMap.of(KS_WITH_ENGINE.getName(), ks, OLD_KS1.getName(), OLD_KS1));
    MetadataRefresh.Result result = refresh.compute(oldMetadata, false, context);
    assertThat(result.newMetadata.getKeyspaces()).hasSize(2);
    assertThat(result.events).containsExactly(TableChangeEvent.updated(OLD_TABLE, newTable));
    assertThat(result.newMetadata.getKeyspaces().get(KS_WITH_ENGINE.getName())).isNotNull();
    assertThat(
            ((DseGraphTableMetadata)
                    result
                        .newMetadata
                        .getKeyspaces()
                        .get(KS_WITH_ENGINE.getName())
                        .getTable("tbl")
                        .get())
                .getVertex())
        .isNotNull();
    assertThat(
            ((DseGraphTableMetadata)
                    result
                        .newMetadata
                        .getKeyspaces()
                        .get(KS_WITH_ENGINE.getName())
                        .getTable("tbl")
                        .get())
                .getVertex()
                .get()
                .getLabelName()
                .asInternal())
        .isEqualTo("someLabel");

    // now rename the vertex label
    newTable =
        newTable(
            KS_WITH_ENGINE.getName(),
            CqlIdentifier.fromInternal("tbl"),
            new DefaultDseVertexMetadata(CqlIdentifier.fromInternal("someNewLabel")),
            null);
    ks =
        newKeyspace(
            KS_WITH_ENGINE.getName(),
            "Core",
            ImmutableMap.of(CqlIdentifier.fromInternal("tbl"), newTable));
    refresh =
        new SchemaRefresh(
            ImmutableMap.of(KS_WITH_ENGINE.getName(), ks, OLD_KS1.getName(), OLD_KS1));
    result = refresh.compute(oldMetadata, false, context);
    assertThat(result.newMetadata.getKeyspaces()).hasSize(2);
    assertThat(result.events).containsExactly(TableChangeEvent.updated(OLD_TABLE, newTable));
    assertThat(
            ((DseGraphTableMetadata)
                    result
                        .newMetadata
                        .getKeyspaces()
                        .get(KS_WITH_ENGINE.getName())
                        .getTable("tbl")
                        .get())
                .getVertex()
                .get()
                .getLabelName()
                .asInternal())
        .isEqualTo("someNewLabel");

    // now remove the vertex label from the table
    DefaultMetadata metadataWithVertexLabel = result.newMetadata;
    DefaultDseTableMetadata tableWithRemovedLabel =
        newTable(KS_WITH_ENGINE.getName(), CqlIdentifier.fromInternal("tbl"), null, null);
    ks =
        newKeyspace(
            KS_WITH_ENGINE.getName(),
            "Core",
            ImmutableMap.of(CqlIdentifier.fromInternal("tbl"), tableWithRemovedLabel));
    refresh =
        new SchemaRefresh(
            ImmutableMap.of(KS_WITH_ENGINE.getName(), ks, OLD_KS1.getName(), OLD_KS1));
    result = refresh.compute(metadataWithVertexLabel, false, context);
    assertThat(result.newMetadata.getKeyspaces()).hasSize(2);
    assertThat(result.events)
        .containsExactly(TableChangeEvent.updated(newTable, tableWithRemovedLabel));
    assertThat(
            ((DseGraphTableMetadata)
                    result
                        .newMetadata
                        .getKeyspaces()
                        .get(KS_WITH_ENGINE.getName())
                        .getTable("tbl")
                        .get())
                .getVertex()
                .isPresent())
        .isFalse();
  }

  @Test
  public void should_detect_adding_and_renaming_and_removing_edge_label() {
    DefaultDseTableMetadata newTable =
        newTable(
            KS_WITH_ENGINE.getName(),
            CqlIdentifier.fromInternal("tbl"),
            null,
            newEdgeMetadata(
                CqlIdentifier.fromInternal("created"),
                CqlIdentifier.fromInternal("person"),
                CqlIdentifier.fromInternal("software")));
    DefaultDseKeyspaceMetadata ks =
        newKeyspace(
            KS_WITH_ENGINE.getName(),
            "Core",
            ImmutableMap.of(CqlIdentifier.fromInternal("tbl"), newTable));
    SchemaRefresh refresh =
        new SchemaRefresh(
            ImmutableMap.of(KS_WITH_ENGINE.getName(), ks, OLD_KS1.getName(), OLD_KS1));
    MetadataRefresh.Result result = refresh.compute(oldMetadata, false, context);
    assertThat(result.newMetadata.getKeyspaces()).hasSize(2);
    assertThat(result.events).containsExactly(TableChangeEvent.updated(OLD_TABLE, newTable));
    assertThat(result.newMetadata.getKeyspaces().get(KS_WITH_ENGINE.getName())).isNotNull();
    assertThat(
            ((DseGraphTableMetadata)
                    result
                        .newMetadata
                        .getKeyspaces()
                        .get(KS_WITH_ENGINE.getName())
                        .getTable("tbl")
                        .get())
                .getVertex())
        .isNotNull();
    assertThat(
            ((DseGraphTableMetadata)
                    result
                        .newMetadata
                        .getKeyspaces()
                        .get(KS_WITH_ENGINE.getName())
                        .getTable("tbl")
                        .get())
                .getEdge()
                .get()
                .getLabelName()
                .asInternal())
        .isEqualTo("created");

    // now rename the edge label
    newTable =
        newTable(
            KS_WITH_ENGINE.getName(),
            CqlIdentifier.fromInternal("tbl"),
            null,
            newEdgeMetadata(
                CqlIdentifier.fromInternal("CHANGED"),
                CqlIdentifier.fromInternal("person"),
                CqlIdentifier.fromInternal("software")));
    ks =
        newKeyspace(
            KS_WITH_ENGINE.getName(),
            "Core",
            ImmutableMap.of(CqlIdentifier.fromInternal("tbl"), newTable));
    refresh =
        new SchemaRefresh(
            ImmutableMap.of(KS_WITH_ENGINE.getName(), ks, OLD_KS1.getName(), OLD_KS1));
    result = refresh.compute(oldMetadata, false, context);
    assertThat(result.newMetadata.getKeyspaces()).hasSize(2);
    assertThat(result.events).containsExactly(TableChangeEvent.updated(OLD_TABLE, newTable));
    assertThat(
            ((DseGraphTableMetadata)
                    result
                        .newMetadata
                        .getKeyspaces()
                        .get(KS_WITH_ENGINE.getName())
                        .getTable("tbl")
                        .get())
                .getEdge()
                .get()
                .getLabelName()
                .asInternal())
        .isEqualTo("CHANGED");

    // now remove the edge label from the table
    DefaultMetadata metadataWithEdgeLabel = result.newMetadata;
    DefaultDseTableMetadata tableWithRemovedLabel =
        newTable(KS_WITH_ENGINE.getName(), CqlIdentifier.fromInternal("tbl"), null, null);
    ks =
        newKeyspace(
            KS_WITH_ENGINE.getName(),
            "Core",
            ImmutableMap.of(CqlIdentifier.fromInternal("tbl"), tableWithRemovedLabel));
    refresh =
        new SchemaRefresh(
            ImmutableMap.of(KS_WITH_ENGINE.getName(), ks, OLD_KS1.getName(), OLD_KS1));
    result = refresh.compute(metadataWithEdgeLabel, false, context);
    assertThat(result.newMetadata.getKeyspaces()).hasSize(2);
    assertThat(result.events)
        .containsExactly(TableChangeEvent.updated(newTable, tableWithRemovedLabel));
    assertThat(
            ((DseGraphTableMetadata)
                    result
                        .newMetadata
                        .getKeyspaces()
                        .get(KS_WITH_ENGINE.getName())
                        .getTable("tbl")
                        .get())
                .getEdge()
                .isPresent())
        .isFalse();
  }

  private static DefaultDseKeyspaceMetadata newKeyspace(String name, String graphEngine) {
    return new DefaultDseKeyspaceMetadata(
        CqlIdentifier.fromInternal(name),
        false,
        false,
        graphEngine,
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap());
  }

  private static DefaultDseKeyspaceMetadata newKeyspace(
      CqlIdentifier name, String graphEngine, @NonNull Map<CqlIdentifier, TableMetadata> tables) {
    return new DefaultDseKeyspaceMetadata(
        name,
        false,
        false,
        graphEngine,
        Collections.emptyMap(),
        Collections.emptyMap(),
        tables,
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap());
  }

  private static DefaultDseTableMetadata newTable(
      @NonNull CqlIdentifier keyspace,
      @NonNull CqlIdentifier name,
      @Nullable DseVertexMetadata vertex,
      @Nullable DseEdgeMetadata edge) {
    ImmutableList<ColumnMetadata> cols =
        ImmutableList.of(
            new DefaultColumnMetadata(
                keyspace,
                CqlIdentifier.fromInternal("parent"),
                CqlIdentifier.fromInternal("id"),
                DataTypes.INT,
                false));
    return new DefaultDseTableMetadata(
        keyspace,
        name,
        null,
        false,
        false,
        cols,
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        vertex,
        edge);
  }

  private static DefaultDseEdgeMetadata newEdgeMetadata(
      @NonNull CqlIdentifier labelName,
      @NonNull CqlIdentifier fromTable,
      @NonNull CqlIdentifier toTable) {
    return new DefaultDseEdgeMetadata(
        labelName,
        fromTable,
        fromTable,
        Collections.emptyList(),
        Collections.emptyList(),
        toTable,
        toTable,
        Collections.emptyList(),
        Collections.emptyList());
  }
}
