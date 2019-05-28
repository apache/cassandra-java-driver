package com.datastax.oss.driver.mapper;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.Update;
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
public class SchemaValidationIT {

  private static CcmRule ccm = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccm).build();

  private static InventoryMapper mapper;

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccm).around(sessionRule);

  @BeforeClass
  public static void setup() {
    CqlSession session = sessionRule.session();
    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE product_simple(id uuid PRIMARY KEY, description text)")
            .setExecutionProfile(sessionRule.slowProfile())
            .build());

    mapper = new SchemaValidationIT_InventoryMapperBuilder(session).build();
  }

  @Test
  public void should_throw_when_use_not_properly_mapped_entity() {
    // Given
    ProductSimpleDao dao = mapper.productDao(sessionRule.keyspace());

    // When, Then
    dao.update(new ProductSimple(UUID.randomUUID(), "desc"));
  }

  @Mapper
  public interface InventoryMapper {
    @DaoFactory
    ProductSimpleDao productDao(@DaoKeyspace CqlIdentifier keyspace);
  }

  @Dao
  public interface ProductSimpleDao {

    @Update
    void update(ProductSimple product);

    @Select
    ProductSimple findById(UUID productId);
  }

  @Entity
  public static class ProductSimple {
    @PartitionKey private UUID id;
    private String descriptionWithIncorrectName;

    public ProductSimple() {}

    public ProductSimple(UUID id, String descriptionWithIncorrectName) {
      this.id = id;
      this.descriptionWithIncorrectName = descriptionWithIncorrectName;
    }

    public UUID getId() {
      return id;
    }

    public void setId(UUID id) {
      this.id = id;
    }

    public String getDescriptionWithIncorrectName() {
      return descriptionWithIncorrectName;
    }

    public void setDescriptionWithIncorrectName(String descriptionWithIncorrectName) {
      this.descriptionWithIncorrectName = descriptionWithIncorrectName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ProductSimple that = (ProductSimple) o;
      return Objects.equals(id, that.id)
          && Objects.equals(descriptionWithIncorrectName, that.descriptionWithIncorrectName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, descriptionWithIncorrectName);
    }

    @Override
    public String toString() {
      return "ProductSimple{"
          + "id="
          + id
          + ", descriptionWithIncorrectName='"
          + descriptionWithIncorrectName
          + '\''
          + '}';
    }
  }
}
