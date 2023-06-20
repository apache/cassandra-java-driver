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
package com.datastax.oss.driver.mapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.GetEntity;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class GetEntityIT extends InventoryITBase {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static final UUID PRODUCT_2D_ID = UUID.randomUUID();

  private static ProductDao dao;

  @BeforeClass
  public static void setup() {
    CqlSession session = SESSION_RULE.session();

    for (String query : createStatements(CCM_RULE)) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(SESSION_RULE.slowProfile()).build());
    }

    UserDefinedType dimensions2d =
        session
            .getKeyspace()
            .flatMap(ks -> session.getMetadata().getKeyspace(ks))
            .flatMap(ks -> ks.getUserDefinedType("dimensions2d"))
            .orElseThrow(AssertionError::new);
    session.execute(
        "INSERT INTO product2d (id, description, dimensions) VALUES (?, ?, ?)",
        PRODUCT_2D_ID,
        "2D product",
        dimensions2d.newValue(12, 34));

    InventoryMapper inventoryMapper = new GetEntityIT_InventoryMapperBuilder(session).build();
    dao = inventoryMapper.productDao(SESSION_RULE.keyspace());

    dao.save(FLAMETHROWER);
    dao.save(MP3_DOWNLOAD);
  }

  @Test
  public void should_get_entity_from_complete_row() {
    CqlSession session = SESSION_RULE.session();
    ResultSet rs =
        session.execute(
            SimpleStatement.newInstance(
                "SELECT id, description, dimensions, now() FROM product WHERE id = ?",
                FLAMETHROWER.getId()));
    Row row = rs.one();
    assertThat(row).isNotNull();

    Product product = dao.get(row);
    assertThat(product).isEqualTo(FLAMETHROWER);
  }

  @Test
  public void should_not_get_entity_from_partial_row_when_not_lenient() {
    CqlSession session = SESSION_RULE.session();
    ResultSet rs =
        session.execute(
            SimpleStatement.newInstance(
                "SELECT id, description, now() FROM product WHERE id = ?", FLAMETHROWER.getId()));
    Row row = rs.one();
    assertThat(row).isNotNull();

    Throwable error = catchThrowable(() -> dao.get(row));
    assertThat(error).hasMessage("dimensions is not a column in this row");
  }

  @Test
  public void should_get_entity_from_partial_row_when_lenient() {
    CqlSession session = SESSION_RULE.session();
    ResultSet rs =
        session.execute(
            SimpleStatement.newInstance(
                "SELECT id, dimensions FROM product2d WHERE id = ?", PRODUCT_2D_ID));
    Row row = rs.one();
    assertThat(row).isNotNull();

    Product product = dao.getLenient(row);
    assertThat(product.getId()).isEqualTo(PRODUCT_2D_ID);
    assertThat(product.getDescription()).isNull();
    assertThat(product.getDimensions()).isNotNull();
    assertThat(product.getDimensions().getWidth()).isEqualTo(12);
    assertThat(product.getDimensions().getHeight()).isEqualTo(34);
    assertThat(product.getDimensions().getLength()).isZero();
  }

  @Test
  public void should_get_entity_from_complete_udt_value() {
    CqlSession session = SESSION_RULE.session();
    ResultSet rs =
        session.execute(
            SimpleStatement.newInstance(
                "SELECT dimensions FROM product WHERE id = ?", FLAMETHROWER.getId()));
    Row row = rs.one();
    assertThat(row).isNotNull();

    Dimensions dimensions = dao.get(row.getUdtValue(0));
    assertThat(dimensions).isEqualTo(FLAMETHROWER.getDimensions());
  }

  @Test
  public void should_not_get_entity_from_partial_udt_value_when_not_lenient() {
    CqlSession session = SESSION_RULE.session();
    ResultSet rs =
        session.execute(
            SimpleStatement.newInstance(
                "SELECT dimensions FROM product2d WHERE id = ?", PRODUCT_2D_ID));
    Row row = rs.one();
    assertThat(row).isNotNull();

    Throwable error = catchThrowable(() -> dao.get(row.getUdtValue(0)));
    assertThat(error).hasMessage("length is not a field in this UDT");
  }

  @Test
  public void should_get_entity_from_partial_udt_value_when_lenient() {
    CqlSession session = SESSION_RULE.session();
    ResultSet rs =
        session.execute(
            SimpleStatement.newInstance(
                "SELECT dimensions FROM product2d WHERE id = ?", PRODUCT_2D_ID));
    Row row = rs.one();
    assertThat(row).isNotNull();

    Dimensions dimensions = dao.getLenient(row.getUdtValue(0));
    assertThat(dimensions).isNotNull();
    assertThat(dimensions.getWidth()).isEqualTo(12);
    assertThat(dimensions.getHeight()).isEqualTo(34);
    assertThat(dimensions.getLength()).isZero();
  }

  @Test
  public void should_get_entity_from_first_row_of_result_set() {
    CqlSession session = SESSION_RULE.session();
    ResultSet rs = session.execute("SELECT * FROM product");

    Product product = dao.getOne(rs);
    // The order depends on the IDs, which are generated dynamically. This is good enough:
    assertThat(product.equals(FLAMETHROWER) || product.equals(MP3_DOWNLOAD)).isTrue();
  }

  @Test
  public void should_get_entity_from_first_row_of_async_result_set() {
    CqlSession session = SESSION_RULE.session();
    AsyncResultSet rs =
        CompletableFutures.getUninterruptibly(session.executeAsync("SELECT * FROM product"));

    Product product = dao.getOne(rs);
    // The order depends on the IDs, which are generated dynamically. This is good enough:
    assertThat(product.equals(FLAMETHROWER) || product.equals(MP3_DOWNLOAD)).isTrue();
  }

  @Test
  public void should_get_iterable_from_result_set() {
    CqlSession session = SESSION_RULE.session();
    ResultSet rs = session.execute("SELECT * FROM product");
    PagingIterable<Product> products = dao.get(rs);
    assertThat(Sets.newHashSet(products)).containsOnly(FLAMETHROWER, MP3_DOWNLOAD);
  }

  @Test
  public void should_get_stream_from_result_set() {
    CqlSession session = SESSION_RULE.session();
    ResultSet rs = session.execute("SELECT * FROM product");
    Stream<Product> products = dao.getAsStream(rs);
    assertThat(products).containsOnly(FLAMETHROWER, MP3_DOWNLOAD);
  }

  @Test
  public void should_get_async_iterable_from_async_result_set() {
    CqlSession session = SESSION_RULE.session();
    AsyncResultSet rs =
        CompletableFutures.getUninterruptibly(session.executeAsync("SELECT * FROM product"));
    MappedAsyncPagingIterable<Product> products = dao.get(rs);
    assertThat(Sets.newHashSet(products.currentPage())).containsOnly(FLAMETHROWER, MP3_DOWNLOAD);
    assertThat(products.hasMorePages()).isFalse();
  }

  @Mapper
  public interface InventoryMapper {
    @DaoFactory
    ProductDao productDao(@DaoKeyspace CqlIdentifier keyspace);
  }

  @Dao
  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  public interface ProductDao {

    @GetEntity
    Product get(Row row);

    @GetEntity(lenient = true)
    Product getLenient(Row row);

    @GetEntity
    Dimensions get(UdtValue row);

    @GetEntity(lenient = true)
    Dimensions getLenient(UdtValue row);

    @GetEntity
    PagingIterable<Product> get(ResultSet resultSet);

    @GetEntity
    Stream<Product> getAsStream(ResultSet resultSet);

    @GetEntity
    MappedAsyncPagingIterable<Product> get(AsyncResultSet resultSet);

    @GetEntity
    Product getOne(ResultSet resultSet);

    @GetEntity
    Product getOne(AsyncResultSet resultSet);

    @Insert
    void save(Product product);
  }
}
