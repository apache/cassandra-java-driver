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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.junit.Before;
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
    for (String query :
        ImmutableList.of(
            "CREATE TABLE product_simple(id uuid PRIMARY KEY, description text, unmapped text)",
            "CREATE TYPE type1(s text)",
            "CREATE TYPE type2(i int)",
            "CREATE TABLE container(id uuid PRIMARY KEY, "
                + "list frozen<list<type1>>, "
                + "map1 frozen<map<text, list<type1>>>, "
                + "map2 frozen<map<type1, set<list<type2>>>>,"
                + "map3 frozen<map<type1, map<text, set<type2>>>>"
                + ")")) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(sessionRule.slowProfile()).build());
    }
    mapper = new SchemaValidationIT_InventoryMapperBuilder(session).build();
  }

  @Before
  public void clearData() {
    CqlSession session = sessionRule.session();
    session.execute(
        SimpleStatement.builder("TRUNCATE container")
            .setExecutionProfile(sessionRule.slowProfile())
            .build());
    session.execute(
        SimpleStatement.builder("TRUNCATE product_simple")
            .setExecutionProfile(sessionRule.slowProfile())
            .build());
  }

  @Test
  public void should_throw_when_use_not_properly_mapped_entity() {
    assertThatThrownBy(() -> mapper.productDao(sessionRule.keyspace()))
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasStackTraceContaining(
            String.format(
                "The CQL ks.table: %s.product_simple has missing columns: [description_with_incorrect_name, some_other_not_mapped_field] that are defined in the entity class: com.datastax.oss.driver.mapper.SchemaValidationIT.ProductSimple",
                sessionRule.keyspace()));
  }

  @Test
  public void should_throw_when_use_not_properly_mapped_entity_with_udts() {
    assertThatThrownBy(() -> mapper.containerDao(sessionRule.keyspace()))
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasStackTraceContaining(
            String.format(
                "The CQL ks.table: %s.container has missing columns: [list_not_present] that are defined in the entity class: com.datastax.oss.driver.mapper.SchemaValidationIT.Container",
                sessionRule.keyspace()));
  }

  @Test
  public void should_throw_when_entity_has_no_corresponding_cql_table() {
    assertThatThrownBy(() -> mapper.productCqlTableMissingDao(sessionRule.keyspace()))
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasStackTraceContaining(
            String.format(
                "There is no ks.table: %s.product_cql_table_missing for the entity class: com.datastax.oss.driver.mapper.SchemaValidationIT.ProductCqlTableMissing",
                sessionRule.keyspace()));
  }

  @Test
  public void should_throw_general_driver_exception_when_schema_validation_check_is_disabled() {
    assertThatThrownBy(() -> mapper.productDaoValidationDisabled(sessionRule.keyspace()))
        .isInstanceOf(InvalidQueryException.class)
        .hasStackTraceContaining("Undefined column name description_with_incorrect_name");
  }

  @Mapper
  public interface InventoryMapper {
    @DaoFactory
    ProductSimpleDao productDao(@DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    ProductSimpleCqlTableMissingDao productCqlTableMissingDao(@DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    ProductSimpleDaoValidationDisabledDao productDaoValidationDisabled(
        @DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    ContainerDao containerDao(@DaoKeyspace CqlIdentifier keyspace);
  }

  @Dao
  public interface ProductSimpleDao {

    @Select
    ProductSimple findById(UUID productId);
  }

  @Dao(enableEntitySchemaValidation = false)
  public interface ProductSimpleDaoValidationDisabledDao {

    @Select
    ProductSimple findById(UUID productId);
  }

  @Dao
  public interface ProductSimpleCqlTableMissingDao {

    @Select
    ProductCqlTableMissing findById(UUID productId);
  }

  @Dao
  public interface ContainerDao {
    @Select
    Container loadByPk(UUID id);
  }

  @Entity
  public static class ProductCqlTableMissing {
    @PartitionKey private UUID id;

    public ProductCqlTableMissing() {}

    public UUID getId() {
      return id;
    }

    public void setId(UUID id) {
      this.id = id;
    }
  }

  @Entity
  public static class ProductSimple {
    @PartitionKey private UUID id;
    private String descriptionWithIncorrectName;
    private Integer someOtherNotMappedField;

    public ProductSimple() {}

    public ProductSimple(
        UUID id, String descriptionWithIncorrectName, Integer someOtherNotMappedField) {
      this.id = id;
      this.descriptionWithIncorrectName = descriptionWithIncorrectName;
      this.someOtherNotMappedField = someOtherNotMappedField;
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

    public Integer getSomeOtherNotMappedField() {
      return someOtherNotMappedField;
    }

    public void setSomeOtherNotMappedField(Integer someOtherNotMappedField) {
      this.someOtherNotMappedField = someOtherNotMappedField;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ProductSimple that = (ProductSimple) o;
      return Objects.equals(id, that.id)
          && Objects.equals(descriptionWithIncorrectName, that.descriptionWithIncorrectName)
          && Objects.equals(someOtherNotMappedField, that.someOtherNotMappedField);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, descriptionWithIncorrectName, someOtherNotMappedField);
    }

    @Override
    public String toString() {
      return "ProductSimple{"
          + "id="
          + id
          + ", descriptionWithIncorrectName='"
          + descriptionWithIncorrectName
          + '\''
          + ", someOtherNotMappedField="
          + someOtherNotMappedField
          + '}';
    }
  }

  @Entity
  public static class Container {

    @PartitionKey private UUID id;
    private List<Type1> listNotPresent;
    private Map<String, List<Type1>> map1;
    private Map<Type1, Set<List<Type2>>> map2;
    private Map<Type1, Map<String, Set<Type2>>> map3;

    public Container() {}

    public Container(
        UUID id,
        List<Type1> listNotPresent,
        Map<String, List<Type1>> map1,
        Map<Type1, Set<List<Type2>>> map2,
        Map<Type1, Map<String, Set<Type2>>> map3) {
      this.id = id;
      this.listNotPresent = listNotPresent;
      this.map1 = map1;
      this.map2 = map2;
      this.map3 = map3;
    }

    public UUID getId() {
      return id;
    }

    public void setId(UUID id) {
      this.id = id;
    }

    public List<Type1> getListNotPresent() {
      return listNotPresent;
    }

    public void setListNotPresent(List<Type1> listNotPresent) {
      this.listNotPresent = listNotPresent;
    }

    public Map<String, List<Type1>> getMap1() {
      return map1;
    }

    public void setMap1(Map<String, List<Type1>> map1) {
      this.map1 = map1;
    }

    public Map<Type1, Set<List<Type2>>> getMap2() {
      return map2;
    }

    public void setMap2(Map<Type1, Set<List<Type2>>> map2) {
      this.map2 = map2;
    }

    public Map<Type1, Map<String, Set<Type2>>> getMap3() {
      return map3;
    }

    public void setMap3(Map<Type1, Map<String, Set<Type2>>> map3) {
      this.map3 = map3;
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      } else if (other instanceof Container) {
        Container that = (Container) other;
        return Objects.equals(this.id, that.id)
            && Objects.equals(this.listNotPresent, that.listNotPresent)
            && Objects.equals(this.map1, that.map1)
            && Objects.equals(this.map2, that.map2)
            && Objects.equals(this.map3, that.map3);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, listNotPresent, map1, map2, map3);
    }
  }

  @Entity
  public static class Type1 {
    private String s;

    public Type1() {}

    public Type1(String s) {
      this.s = s;
    }

    public String getS() {
      return s;
    }

    public void setS(String s) {
      this.s = s;
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      } else if (other instanceof Type1) {
        Type1 that = (Type1) other;
        return Objects.equals(this.s, that.s);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return s == null ? 0 : s.hashCode();
    }
  }

  @Entity
  public static class Type2 {
    private int i;

    public Type2() {}

    public Type2(int i) {
      this.i = i;
    }

    public int getI() {
      return i;
    }

    public void setI(int i) {
      this.i = i;
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      } else if (other instanceof Type2) {
        Type2 that = (Type2) other;
        return this.i == that.i;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return i;
    }
  }
}
