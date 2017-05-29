/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.mapping;

import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.core.utils.CassandraVersion;
import com.datastax.driver.mapping.annotations.*;
import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for
 * JAVA-541 Add polymorphism support to object mapper
 * JAVA-636 Allow @Column annotations on getters/setters as well as fields
 */
@SuppressWarnings({"unused", "WeakerAccess"})
@CassandraVersion("2.1.0")
public class MapperPolymorphismTest extends CCMTestsSupport {

    Circle circle = new Circle(new Point2D(11, 22), 12.34);
    Rectangle rectangle = new Rectangle(new Point2D(20, 30), new Point2D(50, 60));
    Square square = new Square(new Point2D(20, 30), new Point2D(50, 60));
    Sphere sphere = new Sphere(new Point3D(11, 22, 33), 34.56);

    @Override
    public void onTestContextInitialized() {
        execute(
                "CREATE TYPE point2d (\"X\" int, \"Y\" int)",
                "CREATE TYPE point3d (\"X\" int, \"Y\" int, \"Z\" int)",
                "CREATE TABLE circles (circle_id uuid PRIMARY KEY, center2d frozen<point2d>, radius double, tags set<text>)",
                "CREATE TABLE rectangles (rect_id uuid PRIMARY KEY, bottom_left frozen<point2d>, top_right frozen<point2d>, tags set<text>)",
                "CREATE TABLE squares (square_id uuid PRIMARY KEY, bottom_left frozen<point2d>, top_right frozen<point2d>, tags set<text>)",
                "CREATE TABLE spheres (sphere_id uuid PRIMARY KEY, center3d frozen<point3d>, radius double, tags set<text>)");
    }

    @AfterMethod(groups = "short")
    public void clean() {
        execute("TRUNCATE circles", "TRUNCATE rectangles", "TRUNCATE squares", "TRUNCATE spheres");
    }

    @UDT(name = "point2d")
    static class Point2D {

        // test mix of field and getter - getter should win
        @Field(name = "wrong")
        private int x;

        private int y;

        // test private constructor + "immutability"
        private Point2D() {
        }

        public Point2D(int x, int y) {
            this.x = x;
            this.y = y;
        }

        @Field(name = "X", caseSensitive = true)
        public int getX() {
            return x;
        }

        @Field(name = "Y", caseSensitive = true)
        public int getY() {
            return y;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Point2D point2D = (Point2D) o;
            return getX() == point2D.getX() &&
                    getY() == point2D.getY();
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getX(), getY());
        }
    }

    @UDT(name = "point3d")
    static class Point3D extends Point2D {

        private int z;

        // test private constructor + "immutability"
        private Point3D() {
        }

        public Point3D(int x, int y, int z) {
            super(x, y);
            this.z = z;
        }

        @Field(name = "Z", caseSensitive = true)
        public int getZ() {
            return z;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Point3D point3D = (Point3D) o;
            return getZ() == point3D.getZ();
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(super.hashCode(), getZ());
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

    static abstract class Shape implements Shape2D {

        @PartitionKey // annotated field on superclass; annotation will get inherited in all subclasses
        protected UUID id;

        protected Set<String> tags;

        public Shape() {
            this.id = UUID.randomUUID();
            this.tags = Sets.newHashSet("cool", "awesome");
        }

        @Column(name = "wrong") // gets overridden in all subclasses
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
            return Objects.equal(getId(), shape.getId());
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getId());
        }
    }

    @Table(name = "circles")
    static class Circle extends Shape {

        @Column(name = "center2d") // overridden by a getter in Sphere
        @Frozen
        protected Point2D center;

        // tests unusual field name (i.e. does not correspond to getter/setter)
        // will be considered as a separate "property" with no getter nor setter;
        // thus needs to be annotated with @Transient
        @Transient
        private double _radius;

        private long writeTime;

        public Circle() {
        }

        public Circle(Point2D center, double radius) {
            this.center = center;
            this._radius = radius;
        }

        @Column(name = "circle_id")
        @Override
        public UUID getId() {
            return id;
        }

        public Point2D getCenter() {
            return center;
        }

        public void setCenter(Point2D center) {
            this.center = center;
        }

        public double getRadius() {
            return _radius;
        }

        // builder-style setter; because the field isn't named in a standard way,
        // if this setter is not detected the test would fail
        public Circle setRadius(double radius) {
            this._radius = radius;
            return this;
        }

        @Override
        // inherits @Transient
        public double getArea() {
            return Math.PI * (Math.pow(getRadius(), 2));
        }

        // tests computed columns - no setter
        @Computed(value = "writetime(\"radius\")")
        public long getWriteTime() {
            return writeTime;
        }

        // mismatched setter - getter is declared in Shape
        public void setTags(Set<String> tags) {
            this.tags = tags;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Circle circle = (Circle) o;
            return Double.compare(circle.getRadius(), getRadius()) == 0 &&
                    Objects.equal(getCenter(), circle.getCenter());
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(super.hashCode(), getCenter(), getRadius());
        }
    }

    @Table(name = "rectangles")
    static class Rectangle extends Shape {

        private Point2D bottomLeft;

        private Point2D topRight;

        public Rectangle() {
        }

        public Rectangle(Point2D bottomLeft, Point2D topRight) {
            this.bottomLeft = bottomLeft;
            this.topRight = topRight;
        }

        @Column(name = "rect_id")
        @Override
        public UUID getId() {
            return id;
        }

        @Column(name = "bottom_left")
        @Frozen
        public Point2D getBottomLeft() {
            return bottomLeft;
        }

        public void setBottomLeft(Point2D bottomLeft) {
            this.bottomLeft = bottomLeft;
        }

        @Column(name = "top_right")
        @Frozen
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
        // inherits @Transient
        public double getArea() {
            return getWidth() * getHeight();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Rectangle rectangle = (Rectangle) o;
            return Objects.equal(getBottomLeft(), rectangle.getBottomLeft()) &&
                    Objects.equal(getTopRight(), rectangle.getTopRight());
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(super.hashCode(), getBottomLeft(), getTopRight());
        }
    }

    @Table(name = "squares")
    static class Square extends Rectangle {

        public Square() {
        }

        public Square(Point2D bottomLeft, Point2D topRight) {
            super(bottomLeft, topRight);
            assert getHeight() == getWidth();
        }

        @Column(name = "square_id")
        @Override
        public UUID getId() {
            return id;
        }

        @Override
        // inherits @Column and @Frozen
        public Point2D getBottomLeft() {
            return super.getBottomLeft();
        }

        @Override
        // inherits @Transient
        public double getHeight() {
            return getWidth();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Square square = (Square) o;
            return Objects.equal(getBottomLeft(), square.getBottomLeft()) &&
                    Objects.equal(getTopRight(), square.getTopRight());
        }

    }

    @Table(name = "spheres")
    static class Sphere extends Circle implements Shape3D {

        private long writeTime;

        public Sphere() {
        }

        public Sphere(Point3D center, double radius) {
            this.center = center;
        }

        @Column(name = "sphere_id")
        @Override
        public UUID getId() {
            return id;
        }

        // overrides field annotation in Circle,
        // note that the property type is narrowed down to Point3D
        @Column(name = "center3d")
        @Frozen
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
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Sphere sphere = (Sphere) o;
            return Objects.equal(getCenter(), sphere.getCenter());
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(super.hashCode(), getCenter());
        }

        // test annotation on implementation
        @Override
        @Transient
        public double getVolume() {
            return 4d / 3d * Math.PI * Math.pow(getRadius(), 3);
        }

    }

    /**
     * Validates that fields and methods from a parent class {@link Shape} are inherited for a mapped entity
     * class {@link Circle}.
     * <p/>
     * It also ensures that methods overridden in {@link Circle} are given precedence and that their annotations are
     * used.   In the case of circle, the {@link Circle#getId()} method overrides the parent and specifies a
     * {@link Column} annotation that will be used instead of the parents annotation ('wrong').
     * <p/>
     * Lastly, it defines another column 'radius' that is defined by having {@link Circle#getRadius} and
     * {@link Circle#setRadius} properties, while it has a {@link Transient} field '_radius' that should not be pulled
     * in by the mapper.
     *
     * @test_category object_mapper
     * @jira_ticket JAVA-541, JAVA-636
     */
    @Test(groups = "short")
    public void should_save_and_retrieve_circle() throws Exception {
        MappingManager mappingManager = new MappingManager(session());
        Mapper<Circle> circleMapper = mappingManager.mapper(Circle.class);
        circleMapper.save(circle);
        assertThat(circleMapper.get(circle.getId())).isEqualTo(circle);
    }

    /**
     * Validates that property methods such as {@link Rectangle#getWidth} and {@link Rectangle#getHeight} are not
     * considered as they are marked as {@link Transient}.  It also ensures that overridden method
     * {@link Rectangle#getArea} inherits its parent's {@link Transient} annotation.
     *
     * @test_category object_mapper
     * @jira_ticket JAVA-541, JAVA-636
     */
    @Test(groups = "short")
    public void should_save_and_retrieve_rectangle() throws Exception {
        MappingManager mappingManager = new MappingManager(session());
        Mapper<Rectangle> rectangleMapper = mappingManager.mapper(Rectangle.class);
        rectangleMapper.save(rectangle);
        assertThat(rectangleMapper.get(rectangle.getId())).isEqualTo(rectangle);
    }

    /**
     * Validates that {@link Square} inherits its properties from its parent class {@link Rectangle} and its parent's
     * class {@link Shape} and that its methods {@link Square#getBottomLeft} and {@link Square#getHeight} inherit
     * annotations from its parent.
     *
     * @test_category object_mapper
     * @jira_ticket JAVA-541, JAVA-636
     */
    @Test(groups = "short")
    public void should_save_and_retrieve_square() throws Exception {
        MappingManager mappingManager = new MappingManager(session());
        Mapper<Square> squareMapper = mappingManager.mapper(Square.class);
        squareMapper.save(square);
        assertThat(squareMapper.get(square.getId())).isEqualTo(square);
    }

    /**
     * Validates that {@link Sphere} inherits its properties from its parent class {@link Circle} and most importantly
     * can specialize the type of {@link Sphere#center} to a {@link Point3D} instead of a {@link Point2D}.
     *
     * @test_category object_mapper
     * @jira_ticket JAVA-541, JAVA-636
     */
    @Test(groups = "short")
    public void should_save_and_retrieve_sphere() throws Exception {
        MappingManager mappingManager = new MappingManager(session());
        Mapper<Sphere> sphereMapper = mappingManager.mapper(Sphere.class);
        sphereMapper.save(sphere);
        assertThat(sphereMapper.get(sphere.getId())).isEqualTo(sphere);
    }

}
