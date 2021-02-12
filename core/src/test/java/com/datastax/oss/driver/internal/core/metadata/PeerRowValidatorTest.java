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

package com.datastax.oss.driver.internal.core.metadata;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class PeerRowValidatorTest {

  @DataProvider
  public static Object[][] nullColumnsV1() {
    return new Object[][] {
      {"rpc_address"}, {"host_id"}, {"data_center"}, {"rack"}, {"tokens"}, {"schema_version"}
    };
  }

  @DataProvider
  public static Object[][] nullColumnsV2() {
    return new Object[][] {
      {"native_address"},
      {"native_port"},
      {"host_id"},
      {"data_center"},
      {"rack"},
      {"tokens"},
      {"schema_version"}
    };
  }

  @Test
  @UseDataProvider("nullColumnsV1")
  public void should_fail_for_invalid_peer_v1(String nullColumn) {
    assertThat(PeerRowValidator.isValid(mockRowV1(nullColumn))).isFalse();
  }

  @Test
  @UseDataProvider("nullColumnsV2")
  public void should_fail_for_invalid_peer_v2(String nullColumn) {
    assertThat(PeerRowValidator.isValid(mockRowV2(nullColumn))).isFalse();
  }

  @Test
  public void should_succeed_for_valid_peer_v1() {
    AdminRow peerRow = mock(AdminRow.class);
    when(peerRow.isNull("host_id")).thenReturn(false);
    when(peerRow.isNull("rpc_address")).thenReturn(false);
    when(peerRow.isNull("native_address")).thenReturn(true);
    when(peerRow.isNull("native_port")).thenReturn(true);
    when(peerRow.isNull("data_center")).thenReturn(false);
    when(peerRow.isNull("rack")).thenReturn(false);
    when(peerRow.isNull("tokens")).thenReturn(false);
    when(peerRow.isNull("schema_version")).thenReturn(false);

    assertThat(PeerRowValidator.isValid(peerRow)).isTrue();
  }

  @Test
  public void should_succeed_for_valid_peer_v2() {
    AdminRow peerRow = mock(AdminRow.class);
    when(peerRow.isNull("host_id")).thenReturn(false);
    when(peerRow.isNull("rpc_address")).thenReturn(true);
    when(peerRow.isNull("native_address")).thenReturn(false);
    when(peerRow.isNull("native_port")).thenReturn(false);
    when(peerRow.isNull("data_center")).thenReturn(false);
    when(peerRow.isNull("rack")).thenReturn(false);
    when(peerRow.isNull("tokens")).thenReturn(false);
    when(peerRow.isNull("schema_version")).thenReturn(false);

    assertThat(PeerRowValidator.isValid(peerRow)).isTrue();
  }

  private AdminRow mockRowV1(String nullColumn) {
    AdminRow peerRow = mock(AdminRow.class);
    when(peerRow.isNull("host_id")).thenReturn(nullColumn.equals("host_id"));
    when(peerRow.isNull("rpc_address")).thenReturn(nullColumn.equals("rpc_address"));
    when(peerRow.isNull("native_address")).thenReturn(true);
    when(peerRow.isNull("native_port")).thenReturn(true);
    when(peerRow.isNull("data_center")).thenReturn(nullColumn.equals("data_center"));
    when(peerRow.isNull("rack")).thenReturn(nullColumn.equals("rack"));
    when(peerRow.isNull("tokens")).thenReturn(nullColumn.equals("tokens"));
    when(peerRow.isNull("schema_version")).thenReturn(nullColumn.equals("schema_version"));

    return peerRow;
  }

  private AdminRow mockRowV2(String nullColumn) {
    AdminRow peerRow = mock(AdminRow.class);
    when(peerRow.isNull("host_id")).thenReturn(nullColumn.equals("host_id"));
    when(peerRow.isNull("native_address")).thenReturn(nullColumn.equals("native_address"));
    when(peerRow.isNull("native_port")).thenReturn(nullColumn.equals("native_port"));
    when(peerRow.isNull("rpc_address")).thenReturn(true);
    when(peerRow.isNull("data_center")).thenReturn(nullColumn.equals("data_center"));
    when(peerRow.isNull("rack")).thenReturn(nullColumn.equals("rack"));
    when(peerRow.isNull("tokens")).thenReturn(nullColumn.equals("tokens"));
    when(peerRow.isNull("schema_version")).thenReturn(nullColumn.equals("schema_version"));

    return peerRow;
  }
}
