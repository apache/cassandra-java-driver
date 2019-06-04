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
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.Computed;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.Transient;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class EntityPolymorphismIT {
  private static CcmRule ccm = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccm).build();

  private static TestMapper mapper;

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccm).around(sessionRule);

  @BeforeClass
  public static void setup() {
    CqlSession session = sessionRule.session();
    for (String query :
        ImmutableList.of(
            "CREATE TYPE point2d (\"X\" int, \"Y\" int)",
            "CREATE TYPE point3d (\"X\" int, \"Y\" int, \"Z\" int)",
            "CREATE TABLE circles (circle_id uuid PRIMARY KEY, center2d frozen<point2d>, radius "
                + "double, tags set<text>)",
            "CREATE TABLE rectangles (rect_id uuid PRIMARY KEY, bottom_left frozen<point2d>, top_right frozen<point2d>, tags set<text>)",
            "CREATE TABLE squares (square_id uuid PRIMARY KEY, bottom_left frozen<point2d>, top_right frozen<point2d>, tags set<text>)",
            "CREATE TABLE spheres (sphere_id uuid PRIMARY KEY, center3d frozen<point3d>, radius "
                + "double, tags set<text>)")) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(sessionRule.slowProfile()).build());
    }
    mapper = new EntityPolymorphismIT_TestMapperBuilder(session).build();
  }

  // define parent interface with dao methods.
  interface ShapeDao<T> {
    @Insert
    void save(T t);

    @Select
    T findById(UUID id);
  }

  // another parent interface with dao methods
  interface WriteTimeDao<Y extends WriteTimeProvider> extends ShapeDao<Y> {
    @Insert(timestamp = ":writeTime")
    void saveWithTime(Y y, long writeTime);
  };

  // TODO: Get even crazier by having an interface with two parent interfaces that are general.

  @Dao
  interface RectangleDao extends ShapeDao<Rectangle> {};

  @Dao
  interface SquareDao extends WriteTimeDao<Square> {};

  @Dao
  interface CircleDao extends WriteTimeDao<Circle> {}

  @Dao
  interface SphereDao extends WriteTimeDao<Sphere> {};

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
  }

  @Test
  public void should_save_and_retrieve_circle() {
    // verifies the inheritance behavior around Circle.
    // * CqlName("circle_id") on getId renames id property to circle_id
    // * annotations, but these are primarily used for
    //   verifying inheritance behavior in Sphere.
    // * verifies writeTime is set.
    CircleDao dao = mapper.circleDao(sessionRule.keyspace());

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
    RectangleDao dao = mapper.rectangleDao(sessionRule.keyspace());

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
    SquareDao dao = mapper.squareDao(sessionRule.keyspace());

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
    SphereDao dao = mapper.sphereDao(sessionRule.keyspace());

    long writeTime = System.currentTimeMillis() - 1000;
    Sphere sphere = new Sphere(new Point3D(11, 22, 33), 34.56);
    dao.saveWithTime(sphere, writeTime);

    Sphere retrievedSphere = dao.findById(sphere.getId());
    assertThat(retrievedSphere).isEqualTo(sphere);
    assertThat(retrievedSphere.getWriteTime()).isEqualTo(writeTime);
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
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Point2D point2D = (Point2D) o;
      return x == point2D.x && y == point2D.y;
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
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      Point3D point3D = (Point3D) o;
      return z == point3D.z;
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

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Shape shape = (Shape) o;
      return Objects.equals(id, shape.id) && Objects.equals(tags, shape.tags);
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
      return Math.PI * (Math.pow(getRadius(), 2));
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
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      Circle circle = (Circle) o;
      return Double.compare(circle.radius, radius) == 0 && center.equals(circle.center);
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
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      Rectangle rectangle = (Rectangle) o;
      return bottomLeft.equals(rectangle.bottomLeft) && topRight.equals(rectangle.topRight);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), bottomLeft, topRight);
    }
  }

  @CqlName("squares")
  @Entity
  static class Square extends Rectangle implements WriteTimeProvider {

    private Point2D height;

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
      this.center = center;
      this.radius = radius;
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
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      Sphere sphere = (Sphere) o;
      return writeTime == sphere.writeTime;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), writeTime);
    }
  }
}
