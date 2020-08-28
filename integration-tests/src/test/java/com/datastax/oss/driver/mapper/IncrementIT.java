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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Increment;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import java.util.Objects;
import java.util.UUID;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class IncrementIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static ProductRatingDao dao;

  @BeforeClass
  public static void setup() {
    CqlSession session = SESSION_RULE.session();

    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE product_rating(product_id uuid PRIMARY KEY, "
                    + "one_star counter, two_star counter, three_star counter, "
                    + "four_star counter, five_star counter)")
            .setExecutionProfile(SESSION_RULE.slowProfile())
            .build());

    InventoryMapper inventoryMapper = new IncrementIT_InventoryMapperBuilder(session).build();
    dao = inventoryMapper.productRatingDao(SESSION_RULE.keyspace());
  }

  @Test
  public void should_increment_counters() {
    UUID productId1 = UUID.randomUUID();
    UUID productId2 = UUID.randomUUID();

    dao.incrementFiveStar(productId1, 1);
    dao.incrementFiveStar(productId1, 1);
    dao.incrementFourStar(productId1, 1);

    dao.incrementTwoStar(productId2, 1);
    dao.incrementThreeStar(productId2, 1);
    dao.incrementOneStar(productId2, 1);

    ProductRating product1Totals = dao.get(productId1);
    assertThat(product1Totals.getFiveStar()).isEqualTo(2);
    assertThat(product1Totals.getFourStar()).isEqualTo(1);
    assertThat(product1Totals.getThreeStar()).isEqualTo(0);
    assertThat(product1Totals.getTwoStar()).isEqualTo(0);
    assertThat(product1Totals.getOneStar()).isEqualTo(0);

    ProductRating product2Totals = dao.get(productId2);
    assertThat(product2Totals.getFiveStar()).isEqualTo(0);
    assertThat(product2Totals.getFourStar()).isEqualTo(0);
    assertThat(product2Totals.getThreeStar()).isEqualTo(1);
    assertThat(product2Totals.getTwoStar()).isEqualTo(1);
    assertThat(product2Totals.getOneStar()).isEqualTo(1);
  }

  @Mapper
  public interface InventoryMapper {
    @DaoFactory
    ProductRatingDao productRatingDao(@DaoKeyspace CqlIdentifier keyspace);
  }

  @Dao
  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  public interface ProductRatingDao {
    @Select
    ProductRating get(UUID productId);

    @Increment(entityClass = ProductRating.class)
    void incrementOneStar(UUID productId, long oneStar);

    @Increment(entityClass = ProductRating.class)
    void incrementTwoStar(UUID productId, long twoStar);

    @Increment(entityClass = ProductRating.class)
    void incrementThreeStar(UUID productId, long threeStar);

    @Increment(entityClass = ProductRating.class)
    void incrementFourStar(UUID productId, long fourStar);

    @Increment(entityClass = ProductRating.class)
    void incrementFiveStar(UUID productId, long fiveStar);
  }

  @Entity
  public static class ProductRating {

    @PartitionKey private UUID productId;
    private long oneStar;
    private long twoStar;
    private long threeStar;
    private long fourStar;
    private long fiveStar;

    public ProductRating() {}

    public UUID getProductId() {
      return productId;
    }

    public void setProductId(UUID productId) {
      this.productId = productId;
    }

    public long getOneStar() {
      return oneStar;
    }

    public void setOneStar(long oneStar) {
      this.oneStar = oneStar;
    }

    public long getTwoStar() {
      return twoStar;
    }

    public void setTwoStar(long twoStar) {
      this.twoStar = twoStar;
    }

    public long getThreeStar() {
      return threeStar;
    }

    public void setThreeStar(long threeStar) {
      this.threeStar = threeStar;
    }

    public long getFourStar() {
      return fourStar;
    }

    public void setFourStar(long fourStar) {
      this.fourStar = fourStar;
    }

    public long getFiveStar() {
      return fiveStar;
    }

    public void setFiveStar(long fiveStar) {
      this.fiveStar = fiveStar;
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      } else if (other instanceof ProductRating) {
        ProductRating that = (ProductRating) other;
        return Objects.equals(this.productId, that.productId)
            && this.oneStar == that.oneStar
            && this.twoStar == that.twoStar
            && this.threeStar == that.threeStar
            && this.fourStar == that.fourStar
            && this.fiveStar == that.fiveStar;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(productId, oneStar, twoStar, threeStar, fourStar, fiveStar);
    }

    @Override
    public String toString() {
      return String.format(
          "ProductRating(id=%s, 1*=%d, 2*=%d, 3*=%d, 4*=%d, 5*=%d)",
          productId, oneStar, twoStar, threeStar, fourStar, fiveStar);
    }
  }
}
