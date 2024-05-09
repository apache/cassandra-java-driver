<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

## Geospatial types

The driver comes with client-side representations of the DSE geospatial data types: [Point],
[LineString] and [Polygon].

Note: geospatial types require the [ESRI] library version 1.2 to be present on the classpath. The
DSE driver has a non-optional dependency on that library, but if your application does not use
geotypes at all, it is possible to exclude it to minimize the number of runtime dependencies (see
the [Integration>Driver dependencies](../../integration/#driver-dependencies) section for
more details). If the library cannot be found at runtime, geospatial types won't be available and a
warning will be logged, but the driver will otherwise operate normally (this is also valid for OSGi
deployments).

### Usage in requests

Geospatial types can be retrieved from query results like any other value; use the "typed" getter
that takes the class as a second argument: 

```java
// Schema: CREATE TABLE poi(id int PRIMARY KEY, location 'PointType', description text);

CqlSession session = CqlSession.builder().build()

Row row = session.execute("SELECT location FROM poi WHERE id = 1").one();
Point location = row.get(0, Point.class);
```

The corresponding setter can be used for insertions:

```java
PreparedStatement pst =
    session.prepare("INSERT INTO poi (id, location, description) VALUES (?, ?, ?)");
session.execute(
    pst.boundStatementBuilder()
        .setInt("id", 2)
        .set("location", Point.fromCoordinates(2.2945, 48.8584), Point.class)
        .setString("description", "Eiffel Tower")
        .build());
```

This also works with the vararg syntax where target CQL types are inferred:

```java
session.execute(pst.bind(2, Point.fromCoordinates(2.2945, 48.8584), "Eiffel Tower"));
``` 

### Client-side API

The driver provides methods to create instances or inspect existing ones.

[Point] is a trivial pair of coordinates:

```java
Point point = Point.fromCoordinates(2.2945, 48.8584);
System.out.println(point.X());
System.out.println(point.Y());
```

[LineString] is a series of 2 or more points: 

```java
LineString lineString =
    LineString.fromPoints(
        Point.fromCoordinates(30, 10),
        Point.fromCoordinates(10, 30),
        Point.fromCoordinates(40, 40));

for (Point point : lineString.getPoints()) {
  System.out.println(point);
}
```

[Polygon] is a planar surface in a two-dimensional XY-plane. You can build a simple polygon from a
list of points:

```java
Polygon polygon =
    Polygon.fromPoints(
        Point.fromCoordinates(30, 10),
        Point.fromCoordinates(10, 20),
        Point.fromCoordinates(20, 40),
        Point.fromCoordinates(40, 40));
```
 
In addition to its exterior boundary, a polygon can have an arbitrary number of interior rings,
possibly nested (the first level defines "lakes" in the shape, the next level "islands" in those
lakes, etc). To create such complex polygons, use the builder:

```java
Polygon polygon =
    Polygon.builder()
        .addRing(
            Point.fromCoordinates(0, 0),
            Point.fromCoordinates(0, 3),
            Point.fromCoordinates(5, 3),
            Point.fromCoordinates(5, 0))
        .addRing(
            Point.fromCoordinates(1, 1),
            Point.fromCoordinates(1, 2),
            Point.fromCoordinates(2, 2),
            Point.fromCoordinates(2, 1))
        .addRing(
            Point.fromCoordinates(3, 1),
            Point.fromCoordinates(3, 2),
            Point.fromCoordinates(4, 2),
            Point.fromCoordinates(4, 1))
        .build();
```

You can then retrieve all the points with the following methods:

```java
List<Point> exteriorRing = polygon.getExteriorRing();

for (List<Point> interiorRing : polygon.getInteriorRings()) {
  ...
}
```

Note that all rings (exterior or interior) are defined with the same builder method: you can provide
them in any order, the implementation will figure out which is the exterior one. In addition, points
are always ordered counterclockwise for the exterior ring, clockwise for the first interior level,
counterclockwise for the second level, etc. Again, this is done automatically, so you don't need to
sort them beforehand; however, be prepared to get a different order when you read them back:

```java
Polygon polygon =
    Polygon.fromPoints(
            // Clockwise:
            Point.fromCoordinates(0, 0),
            Point.fromCoordinates(0, 3),
            Point.fromCoordinates(5, 3),
            Point.fromCoordinates(5, 0));

System.out.println(polygon);
// Counterclockwise:
// POLYGON ((0 0, 5 0, 5 3, 0 3, 0 0))
```

All geospatial types interoperate with three standard formats:

* [Well-known text]\:

    ```java
    Point point = Point.fromWellKnownText("POINT (0 1)");
    System.out.println(point.asWellKnownText());
    ```

* [Well-known binary]\:

    ```java
    import com.datastax.oss.protocol.internal.util.Bytes;

    Point point =
        Point.fromWellKnownBinary(
            Bytes.fromHexString("0x01010000000000000000000000000000000000f03f"));
    System.out.println(Bytes.toHexString(point.asWellKnownBinary()));
    ```

* [GeoJSON]\:

    ```java
    Point point = Point.fromGeoJson("{\"type\":\"Point\",\"coordinates\":[0.0,1.0]}");
    System.out.println(point.asGeoJson());
    ```

[ESRI]: https://github.com/Esri/geometry-api-java

[LineString]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/dse/driver/api/core/data/geometry/LineString.html
[Point]:      https://docs.datastax.com/en/drivers/java/4.17/com/datastax/dse/driver/api/core/data/geometry/Point.html
[Polygon]:    https://docs.datastax.com/en/drivers/java/4.17/com/datastax/dse/driver/api/core/data/geometry/Polygon.html

[Well-known text]: https://en.wikipedia.org/wiki/Well-known_text
[Well-known binary]: https://en.wikipedia.org/wiki/Well-known_text#Well-known_binary
[GeoJSON]: https://tools.ietf.org/html/rfc7946
