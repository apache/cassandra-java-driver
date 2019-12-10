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

import static com.datastax.oss.driver.assertions.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.Computed;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DaoTable;
import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.GetEntity;
import com.datastax.oss.driver.api.mapper.annotations.HierarchyScanStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.SetEntity;
import com.datastax.oss.driver.api.mapper.annotations.Transient;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

@Category(ParallelizableTests.class)
@RunWith(DataProviderRunner.class)
public class EntityPolymorphismIT {
  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static TestMapper mapper;

  @BeforeClass
  public static void setup() {
    CqlSession session = SESSION_RULE.session();
    for (String query :
        ImmutableList.of(
            "CREATE TYPE point2d (\"X\" int, \"Y\" int)",
            "CREATE TYPE point3d (\"X\" int, \"Y\" int, \"Z\" int)",
            "CREATE TABLE circles (circle_id uuid PRIMARY KEY, center2d frozen<point2d>, radius "
                + "double, tags set<text>)",
            "CREATE TABLE rectangles (rect_id uuid PRIMARY KEY, bottom_left frozen<point2d>, top_right frozen<point2d>, tags set<text>)",
            "CREATE TABLE squares (square_id uuid PRIMARY KEY, bottom_left frozen<point2d>, top_right frozen<point2d>, tags set<text>)",
            "CREATE TABLE spheres (sphere_id uuid PRIMARY KEY, center3d frozen<point3d>, radius "
                + "double, tags set<text>)",
            "CREATE TABLE devices (device_id uuid PRIMARY KEY, name text)",
            "CREATE TABLE tracked_devices (device_id uuid PRIMARY KEY, name text, location text)",
            "CREATE TABLE simple_devices (id uuid PRIMARY KEY, in_use boolean)")) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(SESSION_RULE.slowProfile()).build());
    }
    mapper = new EntityPolymorphismIT_TestMapperBuilder(session).build();
  }

  // define parent interface with dao methods.
  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  interface BaseDao<T> {
    @Insert
    void save(T t);

    @Select
    T findById(UUID id);

    @Delete
    void delete(T t);

    @SetEntity
    void bind(T t, BoundStatementBuilder builder);

    @GetEntity
    T one(ResultSet result);

    @Update
    void update(T t);
  }

  // another parent interface with dao methods
  interface WriteTimeDao<Y extends WriteTimeProvider> extends BaseDao<Y> {
    @Insert(timestamp = ":writeTime")
    void saveWithTime(Y y, long writeTime);
  }

  @Dao
  interface RectangleDao extends BaseDao<Rectangle> {}

  // Define an intermediate interface with same type variable name to ensure
  // this doesn't cause any issue in code generation.
  interface ArbitraryInterface<Y extends Number> {
    default long increment(Y input) {
      return input.longValue() + 1;
    }
  }

  @Dao
  interface SquareDao extends WriteTimeDao<Square>, ArbitraryInterface<Float> {}

  @Dao
  interface CircleDao extends WriteTimeDao<Circle> {}

  @Dao
  interface SphereDao extends WriteTimeDao<Sphere> {}

  interface NamedDeviceDao<Y extends Device> extends BaseDao<Y> {
    @Query("UPDATE ${qualifiedTableId} SET name = :name WHERE device_id = :id")
    void updateName(String name, UUID id);

    @Query("SELECT * FROM ${qualifiedTableId} WHERE device_id = :id")
    CompletableFuture<Y> findByIdQueryAsync(UUID id);
  }

  @Dao
  interface DeviceDao extends NamedDeviceDao<Device> {}

  @Dao
  interface TrackedDeviceDao extends NamedDeviceDao<TrackedDevice> {}

  @Dao
  interface SimpleDeviceDao extends BaseDao<SimpleDevice> {}

  @Mapper
  public interface TestMapper {
    @DaoFactory
    CircleDao circleDao(@DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    RectangleDao rectangleDao(@DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    SquareDao squareDao(@DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    SphereDao sphereDao(@DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    DeviceDao deviceDao(@DaoKeyspace CqlIdentifier keyspace, @DaoTable CqlIdentifier table);

    @DaoFactory
    TrackedDeviceDao trackedDeviceDao(
        @DaoKeyspace CqlIdentifier keyspace, @DaoTable CqlIdentifier table);

    @DaoFactory
    SimpleDeviceDao simpleDeviceDao(@DaoKeyspace CqlIdentifier keyspace);
  }

  @DataProvider
  public static Object[][] setAndGetProvider() {
    Function<CqlIdentifier, CircleDao> circleDao = keyspace -> mapper.circleDao(keyspace);
    Function<CqlIdentifier, RectangleDao> rectangleDao = keyspace -> mapper.rectangleDao(keyspace);
    Function<CqlIdentifier, SquareDao> squareDao = keyspace -> mapper.squareDao(keyspace);
    Function<CqlIdentifier, SphereDao> sphereDao = keyspace -> mapper.sphereDao(keyspace);
    return new Object[][] {
      {
        new Rectangle(new Point2D(20, 30), new Point2D(50, 60)),
        rectangleDao,
        (Consumer<Rectangle>) (Rectangle r) -> r.setTopRight(new Point2D(21, 31)),
        SimpleStatement.newInstance(
            "insert into rectangles (rect_id, bottom_left, top_right, "
                + "tags) values (?, ?, ?, ?)"),
        SimpleStatement.newInstance("select * from rectangles where rect_id = :id limit 1")
      },
      {
        new Circle(new Point2D(11, 22), 12.34),
        circleDao,
        (Consumer<Circle>) (Circle c) -> c.setRadius(13.33),
        SimpleStatement.newInstance(
            "insert into circles (circle_id, center2d, radius, tags) " + "values (?, ?, ?, ?)"),
        SimpleStatement.newInstance(
            "select circle_id, center2d, radius, tags, writetime(radius) "
                + "as write_time from circles where circle_id = :id limit 1")
      },
      {
        new Square(new Point2D(20, 30), new Point2D(50, 60)),
        squareDao,
        (Consumer<Square>) (Square s) -> s.setBottomLeft(new Point2D(10, 20)),
        SimpleStatement.newInstance(
            "insert into squares (square_id, bottom_left, top_right, "
                + "tags) values (?, ?, ?, ?)"),
        SimpleStatement.newInstance(
            "select square_id, bottom_left, top_right, tags, writetime"
                + "(bottom_left) as write_time from squares where square_id = :id limit 1")
      },
      {
        new Sphere(new Point3D(11, 22, 33), 34.56),
        sphereDao,
        (Consumer<Sphere>) (Sphere s) -> s.setCenter(new Point3D(10, 20, 30)),
        SimpleStatement.newInstance(
            "insert into spheres (sphere_id, center3d, radius, tags) " + "values (?, ?, ?, ?)"),
        SimpleStatement.newInstance(
            "select sphere_id, center3d, radius, tags, writetime(radius) "
                + "as write_time from spheres where sphere_id = :id limit 1")
      },
    };
  }

  @UseDataProvider("setAndGetProvider")
  @Test
  public <T extends Shape> void should_set_and_get_entity_then_update_then_delete(
      T t,
      Function<CqlIdentifier, BaseDao<T>> daoProvider,
      Consumer<T> updater,
      SimpleStatement insertStatement,
      SimpleStatement selectStatement) {
    BaseDao<T> dao = daoProvider.apply(SESSION_RULE.keyspace());
    CqlSession session = SESSION_RULE.session();
    PreparedStatement prepared = session.prepare(insertStatement);

    BoundStatementBuilder bs = prepared.boundStatementBuilder();
    dao.bind(t, bs);

    session.execute(bs.build());

    PreparedStatement selectPrepared = session.prepare(selectStatement);
    BoundStatement selectBs = selectPrepared.bind(t.getId());
    T retrieved = dao.one(session.execute(selectBs));
    assertThat(retrieved).isEqualTo(t);

    // update value
    updater.accept(t);
    dao.update(t);

    // retrieve value to ensure update worked correctly.
    assertThat(dao.one(session.execute(selectBs))).isEqualTo(t);

    // delete value and ensure it's gone
    dao.delete(t);
    assertThat(dao.one(session.execute(selectBs))).isNull();
  }

  @Test
  public void should_save_and_retrieve_circle() {
    // verifies the inheritance behavior around Circle.
    // * CqlName("circle_id") on getId renames id property to circle_id
    // * annotations, but these are primarily used for
    //   verifying inheritance behavior in Sphere.
    // * verifies writeTime is set.
    CircleDao dao = mapper.circleDao(SESSION_RULE.keyspace());

    long writeTime = System.currentTimeMillis() - 1000;
    Circle circle = new Circle(new Point2D(11, 22), 12.34);
    dao.saveWithTime(circle, writeTime);

    Circle retrievedCircle = dao.findById(circle.getId());
    assertThat(retrievedCircle).isEqualTo(circle);
    assertThat(retrievedCircle.getWriteTime()).isEqualTo(writeTime);
  }

  @Test
  public void should_save_and_retrieve_rectangle() {
    // verifies the inheritance behavior around Rectangle:
    // * CqlName("rect_id") on getId renames id property to rect_id
    // * annotations work, but these are primarily used for
    //   verifying inheritance behavior in Square.
    RectangleDao dao = mapper.rectangleDao(SESSION_RULE.keyspace());

    Rectangle rectangle = new Rectangle(new Point2D(20, 30), new Point2D(50, 60));
    dao.save(rectangle);

    assertThat(dao.findById(rectangle.getId())).isEqualTo(rectangle);
  }

  @Test
  public void should_save_and_retrieve_square() {
    // verifies the inheritance behavior around Square:
    // * CqlName("square_id") on getId renames id property to square_id
    // * height remains transient even though we define field/getter/setter
    // * getBottomLeft() retains CqlName from parent.
    // * verifies writeTime is set.
    SquareDao dao = mapper.squareDao(SESSION_RULE.keyspace());

    long writeTime = System.currentTimeMillis() - 1000;
    Square square = new Square(new Point2D(20, 30), new Point2D(50, 60));
    dao.saveWithTime(square, writeTime);

    Square retrievedSquare = dao.findById(square.getId());
    assertThat(retrievedSquare).isEqualTo(square);
    assertThat(retrievedSquare.getWriteTime()).isEqualTo(writeTime);
  }

  @Test
  public void should_save_and_retrieve_sphere() {
    // verifies the inheritance behavior around Circle:
    // * @CqlName("sphere_id") on getId renames id property to sphere_id
    // * @CqlName("center3d") on getCenter renames center property from center2d
    //   which was renamed on the parent field for Circle
    // * That getCenter returns Point3D influences get/set behavior to use Point3D
    // * Override setRadius to return Sphere causes no issues.
    // * Interface method getVolume() is skipped because no field exists.
    // * WriteTime is inherited, so queried and set.
    SphereDao dao = mapper.sphereDao(SESSION_RULE.keyspace());

    long writeTime = System.currentTimeMillis() - 1000;
    Sphere sphere = new Sphere(new Point3D(11, 22, 33), 34.56);
    dao.saveWithTime(sphere, writeTime);

    Sphere retrievedSphere = dao.findById(sphere.getId());
    assertThat(retrievedSphere).isEqualTo(sphere);
    assertThat(retrievedSphere.getWriteTime()).isEqualTo(writeTime);
  }

  @Test
  public void should_save_and_retrieve_device() throws Exception {
    // verifies the hierarchy scanner behavior around Device:
    // * by virtue of Assert setting highestAncestor to Asset.class, location property from
    //   LocatableItem should not be included
    DeviceDao dao = mapper.deviceDao(SESSION_RULE.keyspace(), CqlIdentifier.fromCql("devices"));

    // save should be successful as location property omitted.
    Device device = new Device("my device", "New York");
    dao.save(device);

    Device retrievedDevice = dao.findById(device.getId());
    assertThat(retrievedDevice.getId()).isEqualTo(device.getId());
    assertThat(retrievedDevice.getName()).isEqualTo(device.getName());
    // location should be null.
    assertThat(retrievedDevice.getLocation()).isNull();

    // should be able to use @Query with update
    String name = "my new name";
    dao.updateName(name, device.getId());

    // should be able to use @Query returning Entity and name should be applied.
    retrievedDevice = dao.findByIdQueryAsync(device.getId()).get();
    assertThat(retrievedDevice.getName()).isEqualTo(name);
  }

  @Test
  public void should_save_and_retrieve_tracked_device() throws Exception {
    // verifies the hierarchy scanner behavior around TrackedDevice:
    // * Since TrackedDevice defines a default @HierarchyScanStrategy it should
    //   include LocatableItem's location property, even though Asset defines
    //   a strategy that excludes it.
    TrackedDeviceDao dao =
        mapper.trackedDeviceDao(SESSION_RULE.keyspace(), CqlIdentifier.fromCql("tracked_devices"));

    TrackedDevice device = new TrackedDevice("my device", "New York");
    dao.save(device);

    // location property should be present, thus should equal saved item.
    TrackedDevice retrievedDevice = dao.findById(device.getId());
    assertThat(retrievedDevice).isEqualTo(device);

    // should be able to use @Query with update
    String name = "my new name";
    dao.updateName(name, device.getId());

    // should be able to use @Query returning Entity and name should be applied.
    retrievedDevice = dao.findByIdQueryAsync(device.getId()).get();
    assertThat(retrievedDevice.getName()).isEqualTo(name);
  }

  @Test
  public void should_save_and_retrieve_simple_device() {
    // verifies the hierarchy scanner behavior around SimpleDevice:
    // * Since SimpleDevice defines a @HierarchyScanStrategy that prevents
    //   scanning of ancestors, only its properties (id, inUse) should be included.
    SimpleDeviceDao dao = mapper.simpleDeviceDao(SESSION_RULE.keyspace());

    SimpleDevice device = new SimpleDevice(true);
    dao.save(device);

    SimpleDevice retrievedDevice = dao.findById(device.getId());
    assertThat(retrievedDevice.getId()).isEqualTo(device.getId());
    assertThat(retrievedDevice.getInUse()).isEqualTo(device.getInUse());
    // location and name should be null
    assertThat(retrievedDevice.getLocation()).isNull();
    assertThat(retrievedDevice.getName()).isNull();
  }

  @Entity
  static class Point2D {
    private int x;
    private int y;

    public Point2D() {}

    public Point2D(int x, int y) {
      this.x = x;
      this.y = y;
    }

    @CqlName("\"X\"")
    public int getX() {
      return x;
    }

    public void setX(int x) {
      this.x = x;
    }

    @CqlName("\"Y\"")
    public int getY() {
      return y;
    }

    public void setY(int y) {
      this.y = y;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      else if (other instanceof Point2D) {
        Point2D that = (Point2D) other;
        return this.x == that.x && this.y == that.y;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(x, y);
    }
  }

  @Entity
  static class Point3D extends Point2D {
    private int z;

    public Point3D() {}

    public Point3D(int x, int y, int z) {
      super(x, y);
      this.z = z;
    }

    @CqlName("\"Z\"")
    public int getZ() {
      return z;
    }

    public void setZ(int z) {
      this.z = z;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } else if (other instanceof Point3D) {
        Point3D that = (Point3D) other;
        return super.equals(that) && this.z == that.z;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), z);
    }
  }

  interface Shape2D {
    Set<String> getTags();

    // test annotation on interface method, should get inherited everywhere
    @Transient
    double getArea();
  }

  interface Shape3D {
    double getVolume();
  }

  abstract static class Shape implements Shape2D {

    @PartitionKey // annotated field on superclass; annotation will get inherited in all subclasses
    protected UUID id;

    protected Set<String> tags;

    protected String location;

    public Shape() {
      this.id = UUID.randomUUID();
      this.tags = Sets.newHashSet("cool", "awesome");
    }

    @CqlName("wrong")
    public abstract UUID getId();

    public void setId(UUID id) {
      this.id = id;
    }

    @Override
    public Set<String> getTags() {
      return tags;
    }

    public void setTags(Set<String> tags) {
      this.tags = tags;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } else if (other instanceof Shape) {
        Shape that = (Shape) other;
        return Objects.equals(id, that.id) && Objects.equals(tags, that.tags);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, tags);
    }
  }

  interface WriteTimeProvider {
    long getWriteTime();
  }

  @CqlName("circles")
  @Entity
  static class Circle extends Shape implements WriteTimeProvider {

    @CqlName("center2d")
    protected Point2D center;

    protected double radius;

    private long writeTime;

    public Circle() {}

    public Circle(Point2D center, double radius) {
      super();
      this.center = center;
      this.radius = radius;
    }

    @Override
    @CqlName("circle_id")
    public UUID getId() {
      return id;
    }

    @Override
    public double getArea() {
      return Math.PI * Math.pow(getRadius(), 2);
    }

    public double getRadius() {
      return this.radius;
    }

    public Circle setRadius(double radius) {
      this.radius = radius;
      return this;
    }

    @Computed("writetime(radius)")
    @Override
    public long getWriteTime() {
      return writeTime;
    }

    public void setWriteTime(long writeTime) {
      this.writeTime = writeTime;
    }

    public Point2D getCenter() {
      return center;
    }

    public void setCenter(Point2D center) {
      this.center = center;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } else if (other instanceof Circle) {
        Circle that = (Circle) other;
        return super.equals(that)
            && Double.compare(that.radius, radius) == 0
            && center.equals(that.center);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), center, radius);
    }
  }

  @CqlName("rectangles")
  @Entity
  static class Rectangle extends Shape {
    private Point2D bottomLeft;
    private Point2D topRight;

    public Rectangle() {}

    public Rectangle(Point2D bottomLeft, Point2D topRight) {
      super();
      this.bottomLeft = bottomLeft;
      this.topRight = topRight;
    }

    @CqlName("rect_id")
    @Override
    public UUID getId() {
      return id;
    }

    @CqlName("bottom_left")
    public Point2D getBottomLeft() {
      return bottomLeft;
    }

    public void setBottomLeft(Point2D bottomLeft) {
      this.bottomLeft = bottomLeft;
    }

    @CqlName("top_right")
    public Point2D getTopRight() {
      return topRight;
    }

    public void setTopRight(Point2D topRight) {
      this.topRight = topRight;
    }

    // test annotation in class method
    @Transient
    public double getWidth() {
      return Math.abs(topRight.getX() - bottomLeft.getX());
    }

    @Transient
    public double getHeight() {
      return Math.abs(topRight.getY() - bottomLeft.getY());
    }

    @Override
    public double getArea() {
      return getWidth() * getHeight();
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } else if (other instanceof Rectangle) {
        Rectangle that = (Rectangle) other;
        return super.equals(that)
            && bottomLeft.equals(that.bottomLeft)
            && topRight.equals(that.topRight);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), bottomLeft, topRight);
    }
  }

  @CqlName("squares")
  @Entity
  static class Square extends Rectangle implements WriteTimeProvider {

    @Computed("writetime(bottom_left)")
    private long writeTime;

    public Square() {}

    public Square(Point2D bottomLeft, Point2D topRight) {
      super(bottomLeft, topRight);
      assert getHeight() == getWidth();
    }

    @CqlName("square_id")
    @Override
    public UUID getId() {
      return id;
    }

    @Override
    // inherits @CqlName
    public Point2D getBottomLeft() {
      return super.getBottomLeft();
    }

    @Override
    // inherits @Transient
    public double getHeight() {
      return getWidth();
    }

    public void setHeight(Point2D height) {
      throw new IllegalArgumentException("This method should never be called");
    }

    @Override
    public long getWriteTime() {
      return writeTime;
    }

    public void setWriteTime(long writeTime) {
      this.writeTime = writeTime;
    }
  }

  @CqlName("spheres")
  @Entity
  static class Sphere extends Circle implements Shape3D {

    // ignored field
    private long writeTime;

    public Sphere() {}

    public Sphere(Point3D center, double radius) {
      super(center, radius);
    }

    @CqlName("sphere_id")
    @Override
    public UUID getId() {
      return id;
    }

    // overrides field annotation in Circle,
    // note that the property type is narrowed down to Point3D
    @CqlName("center3d")
    @Override
    public Point3D getCenter() {
      return (Point3D) center;
    }

    @Override
    public void setCenter(Point2D center) {
      assert center instanceof Point3D;
      this.center = center;
    }

    // overridden builder-style setter
    @Override
    public Sphere setRadius(double radius) {
      super.setRadius(radius);
      return this;
    }

    @Override
    public double getVolume() {
      return 4d / 3d * Math.PI * Math.pow(getRadius(), 3);
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } else if (other instanceof Sphere) {
        Sphere that = (Sphere) other;
        return super.equals(that) && writeTime == that.writeTime;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), writeTime);
    }
  }

  abstract static class LocatableItem {

    protected String location;

    LocatableItem() {}

    LocatableItem(String location) {
      this.location = location;
    }

    public String getLocation() {
      return location;
    }

    public void setLocation(String location) {
      this.location = location;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } else if (other instanceof LocatableItem) {
        LocatableItem that = (LocatableItem) other;
        return Objects.equals(this.location, that.location);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(location);
    }
  }

  // define strategy that stops scanning at Asset, meaning LocatableItem's location will
  // not be considered a property.
  @HierarchyScanStrategy(highestAncestor = Asset.class, includeHighestAncestor = true)
  abstract static class Asset extends LocatableItem {
    protected String name;

    Asset() {}

    Asset(String name, String location) {
      super(location);
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } else if (other instanceof Asset) {
        Asset that = (Asset) other;
        return super.equals(that) && Objects.equals(this.name, that.name);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), name);
    }
  }

  // should inherit scan strategy from Asset, thus location will not be present.
  @Entity
  @CqlName("devices")
  static class Device extends Asset {

    @PartitionKey protected UUID id;

    Device() {}

    Device(String name, String location) {
      super(name, location);
      this.id = UUID.randomUUID();
    }

    // rename to device_id, if Device not included in scanning, this won't be used.
    @CqlName("device_id")
    public UUID getId() {
      return id;
    }

    public void setId(UUID id) {
      this.id = id;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } else if (other instanceof Device) {
        Device that = (Device) other;
        return super.equals(that) && this.id.equals(that.id);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), id);
    }
  }

  // use default strategy, which should override Assert's strategy, and thus location
  // should be considered a property.
  @HierarchyScanStrategy
  @Entity
  @CqlName("tracked_devices")
  static class TrackedDevice extends Device {
    TrackedDevice() {}

    TrackedDevice(String name, String location) {
      super(name, location);
    }
  }

  // do not scan ancestors, so only id and inUse should be considered properties.
  @HierarchyScanStrategy(scanAncestors = false)
  @Entity
  @CqlName("simple_devices")
  static class SimpleDevice extends Device {
    boolean inUse;

    // suppress error prone warning as we are doing this with intent
    @SuppressWarnings("HidingField")
    @PartitionKey
    private UUID id;

    SimpleDevice() {}

    SimpleDevice(boolean inUse) {
      super(null, null);
      this.id = UUID.randomUUID();
      this.inUse = inUse;
    }

    public boolean getInUse() {
      return inUse;
    }

    public void setInUse(boolean inUse) {
      this.inUse = inUse;
    }

    @Override
    public UUID getId() {
      return id;
    }

    @Override
    public void setId(UUID id) {
      this.id = id;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } else if (other instanceof SimpleDevice) {
        SimpleDevice that = (SimpleDevice) other;
        return super.equals(that) && this.inUse == that.inUse && this.id.equals(that.id);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), inUse, id);
    }
  }
}
