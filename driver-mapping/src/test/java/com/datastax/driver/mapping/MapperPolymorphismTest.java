/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.mapping;

import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.UDT;
import com.google.common.base.Objects;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("unused")
public class MapperPolymorphismTest extends CCMTestsSupport {

    private final Circle circle = new Circle(new Point2D(11, 22), 12.34);
    private final Rectangle rectangle = new Rectangle(new Point2D(20, 30), new Point2D(50, 60));
    private final Sphere sphere = new Sphere(new Point3D(11, 22, 33), 34.56);
    private Mapper<Circle> circleMapper;
    private Mapper<Rectangle> rectangleMapper;
    private Mapper<Sphere> sphereMapper;

    @Override
    public void onTestContextInitialized() {
        execute(
                "CREATE TYPE point2d (x int, y int)",
                "CREATE TYPE point3d (x int, y int, z int)",
                "CREATE TABLE circles (id uuid PRIMARY KEY, center frozen<point2d>, radius double)",
                "CREATE TABLE rectangles (id uuid PRIMARY KEY, bottom_left frozen<point2d>, top_right frozen<point2d>)",
                "CREATE TABLE spheres (id uuid PRIMARY KEY, center frozen<point3d>, radius double)");
    }

    @BeforeMethod(groups = "short")
    public void clean() {
        execute("TRUNCATE circles", "TRUNCATE rectangles", "TRUNCATE spheres");
    }

    @UDT(name = "point2d")
    static class Point2D {

        private int x;

        private int y;

        public Point2D() {
        }

        public Point2D(int x, int y) {
            this.x = x;
            this.y = y;
        }

        public int getX() {
            return x;
        }

        public void setX(int x) {
            this.x = x;
        }

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

        public Point3D() {
        }

        public Point3D(int x, int y, int z) {
            super(x, y);
            this.z = z;
        }

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
            return getZ() == point3D.getZ();
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(super.hashCode(), getZ());
        }
    }

    static class Shape {

        @PartitionKey
        private UUID id;

        public Shape() {
            this.id = UUID.randomUUID();
        }

        public UUID getId() {
            return id;
        }

        public void setId(UUID id) {
            this.id = id;
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

        private Point2D center;

        private double radius;

        public Circle() {
        }

        public Circle(Point2D center, double radius) {
            this.center = center;
            this.radius = radius;
        }

        public Point2D getCenter() {
            return center;
        }

        public void setCenter(Point2D center) {
            this.center = center;
        }

        public double getRadius() {
            return radius;
        }

        public void setRadius(double radius) {
            this.radius = radius;
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

        @Column(name = "bottom_left")
        private Point2D bottomLeft;

        @Column(name = "top_right")
        private Point2D topRight;

        public Rectangle() {
        }

        public Rectangle(Point2D bottomLeft, Point2D topRight) {
            this.bottomLeft = bottomLeft;
            this.topRight = topRight;
        }

        public Point2D getBottomLeft() {
            return bottomLeft;
        }

        public void setBottomLeft(Point2D bottomLeft) {
            this.bottomLeft = bottomLeft;
        }

        public Point2D getTopRight() {
            return topRight;
        }

        public void setTopRight(Point2D topRight) {
            this.topRight = topRight;
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

    @Table(name = "spheres")
    static class Sphere extends Circle {

        private Point3D center;

        public Sphere() {
        }

        public Sphere(Point3D center, double radius) {
            this.center = center;
        }

        @Override
        public Point3D getCenter() {
            return center;
        }

        public void setCenter(Point3D center) {
            this.center = center;
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
    }

    @BeforeMethod(groups = "short")
    public void createMappers() throws Exception {
        MappingManager mappingManager = new MappingManager(session());
        circleMapper = mappingManager.mapper(Circle.class);
        rectangleMapper = mappingManager.mapper(Rectangle.class);
        sphereMapper = mappingManager.mapper(Sphere.class);
    }

    @Test(groups = "short")
    public void should_save_and_retrieve_circle() throws Exception {
        circleMapper.save(circle);
        assertThat(circleMapper.get(circle.getId())).isEqualTo(circle);
    }

    @Test(groups = "short")
    public void should_save_and_retrieve_rectangle() throws Exception {
        rectangleMapper.save(rectangle);
        assertThat(rectangleMapper.get(rectangle.getId())).isEqualTo(rectangle);
    }

    @Test(groups = "short")
    public void should_save_and_retrieve_sphere() throws Exception {
        sphereMapper.save(sphere);
        assertThat(sphereMapper.get(sphere.getId())).isEqualTo(sphere);
    }

}
