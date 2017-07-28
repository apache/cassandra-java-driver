/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.api.core.type.codec.registry;

import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterRule;
import com.datastax.oss.driver.internal.core.type.codec.IntCodec;
import java.nio.ByteBuffer;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;

import static org.assertj.core.api.Assertions.assertThat;

public class CodecRegistryIT {

  @ClassRule public static CcmRule ccm = CcmRule.getInstance();

  @ClassRule public static ClusterRule cluster = new ClusterRule(ccm);

  @Rule public TestName name = new TestName();

  @Rule public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void createSchema() {
    // table with simple primary key, single cell.
    cluster
        .session()
        .execute(
            SimpleStatement.builder("CREATE TABLE IF NOT EXISTS test (k text primary key, v int)")
                .withConfigProfile(cluster.slowProfile())
                .build());
  }

  // A simple codec that allows float values to be used for cassandra int column type.
  private static class FloatCIntCodec implements TypeCodec<Float> {

    private static final IntCodec intCodec = new IntCodec();

    @Override
    public GenericType<Float> getJavaType() {
      return GenericType.of(Float.class);
    }

    @Override
    public DataType getCqlType() {
      return DataTypes.INT;
    }

    @Override
    public ByteBuffer encode(Float value, ProtocolVersion protocolVersion) {
      return intCodec.encode(value.intValue(), protocolVersion);
    }

    @Override
    public Float decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
      return intCodec.decode(bytes, protocolVersion).floatValue();
    }

    @Override
    public String format(Float value) {
      return intCodec.format(value.intValue());
    }

    @Override
    public Float parse(String value) {
      return intCodec.parse(value).floatValue();
    }
  }

  @Test
  public void should_throw_exception_if_no_codec_registered_for_type_set() {
    PreparedStatement prepared = cluster.session().prepare("INSERT INTO test (k, v) values (?, ?)");

    thrown.expect(CodecNotFoundException.class);

    // float value for int column should not work since no applicable codec.
    prepared.boundStatementBuilder().setString(0, name.getMethodName()).setFloat(1, 3.14f).build();
  }

  @Test
  public void should_throw_exception_if_no_codec_registered_for_type_get() {
    PreparedStatement prepared = cluster.session().prepare("INSERT INTO test (k, v) values (?, ?)");

    BoundStatement insert =
        prepared.boundStatementBuilder().setString(0, name.getMethodName()).setInt(1, 2).build();
    cluster.session().execute(insert);

    ResultSet result =
        cluster
            .session()
            .execute(
                SimpleStatement.builder("SELECT v from TEST where k = ?")
                    .addPositionalValue(name.getMethodName())
                    .build());

    assertThat(result.getAvailableWithoutFetching()).isEqualTo(1);

    // should not be able to access int column as float as no codec is registered to handle that.
    Row row = result.iterator().next();

    thrown.expect(CodecNotFoundException.class);

    assertThat(row.getFloat("v")).isEqualTo(3.0f);
  }

  @Test
  public void should_be_able_to_register_and_use_custom_codec() {
    // create a cluster with a registered codec from Float <-> cql int.
    try (Cluster codecCluster =
        Cluster.builder()
            .addTypeCodecs(new FloatCIntCodec())
            .addContactPoints(ccm.getContactPoints())
            .build()) {
      Session session = codecCluster.connect(cluster.keyspace());

      PreparedStatement prepared = session.prepare("INSERT INTO test (k, v) values (?, ?)");

      // float value for int column should work.
      BoundStatement insert =
          prepared
              .boundStatementBuilder()
              .setString(0, name.getMethodName())
              .setFloat(1, 3.14f)
              .build();
      session.execute(insert);

      ResultSet result =
          session.execute(
              SimpleStatement.builder("SELECT v from TEST where k = ?")
                  .addPositionalValue(name.getMethodName())
                  .build());

      assertThat(result.getAvailableWithoutFetching()).isEqualTo(1);

      // should be able to retrieve value back as float, some precision is lost due to going from int -> float.
      Row row = result.iterator().next();
      assertThat(row.getFloat("v")).isEqualTo(3.0f);
      assertThat(row.getFloat(0)).isEqualTo(3.0f);
    }
  }
}
