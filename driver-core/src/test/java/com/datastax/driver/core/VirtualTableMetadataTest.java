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
package com.datastax.driver.core;

import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.Test;

import java.util.UUID;

import static com.datastax.driver.core.Assertions.assertThat;

@CassandraVersion("4.0.0")
@CCMConfig()
public class VirtualTableMetadataTest extends CCMTestsSupport {

    @Test(groups = "short")
    public void should_parse_virtual_metadata() {
        KeyspaceMetadata km=session().getCluster().getMetadata().getKeyspace("system_views");
        assertThat(km.getTables().size()>=2);
        assertThat(km.isVirtual()).isTrue();
        assertThat(km.getTable("clients")).isNotNull();
        assertThat(km.isDurableWrites()).isFalse();
        assertThat(km.getName()).isEqualTo("system_views");
        assertThat(km.getUserTypes().size()).isEqualTo(0);
        assertThat(km.getFunctions().size()).isEqualTo(0);
        assertThat(km.getMaterializedViews().size()).isEqualTo(0);
        assertThat(km.getAggregates().size()).isEqualTo(0);
        TableMetadata tm = km.getTable("clients");
        assertThat(tm).isNotNull();
        assertThat(tm.getName()).isEqualTo("clients");
        assertThat(tm.isVirtual()).isTrue();
        assertThat(tm.getColumns().size()).isEqualTo(12);
        assertThat(tm.getIndexes().size()).isEqualTo(0);
        assertThat(tm.getViews().size()).isEqualTo(0);
        assertThat(tm.getClusteringColumns().size()).isEqualTo(0);
        assertThat(tm.getClusteringOrder().size()).isEqualTo(0);
        assertThat(tm.getId()).isEqualTo( new UUID(0L, 0L));
        assertThat(tm.getOptions()).isNull();
        assertThat(tm.getKeyspace()).isEqualTo(km);
        ColumnMetadata cm= tm.getColumn("driver_name");
        assertThat(cm).isNotNull();
        assertThat(cm.getParent()).isEqualTo(tm);
        assertThat(cm.getType()).isEqualTo(DataType.text());
        assertThat(cm.getName()).isEqualTo("driver_name");
    }
}
