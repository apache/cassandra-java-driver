/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.api.core.data;

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.base.Predicates;
import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;
import com.datastax.oss.driver.shaded.guava.common.collect.Streams;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Representation of a vector as defined in CQL.
 *
 * <p>A CQL vector is a fixed-length array of non-null numeric values. These properties don't map
 * cleanly to an existing class in the standard JDK Collections hierarchy so we provide this value
 * object instead. Like other value object collections returned by the driver instances of this
 * class are not immutable; think of these value objects as a representation of a vector stored in
 * the database as an initial step in some additional computation.
 *
 * <p>While we don't implement any Collection APIs we do implement Iterable. We also attempt to play
 * nice with the Streams API in order to better facilitate integration with data pipelines. Finally,
 * where possible we've tried to make the API of this class similar to the equivalent methods on
 * {@link List}.
 */
public class CqlVector<T> implements Iterable<T>, Serializable {

  /**
   * Create a new CqlVector containing the specified values.
   *
   * @param vals the collection of values to wrap.
   * @return a CqlVector wrapping those values
   */
  public static <V> CqlVector<V> newInstance(V... vals) {

    // Note that Array.asList() guarantees the return of an array which implements RandomAccess
    return new CqlVector(Arrays.asList(vals));
  }

  /**
   * Create a new CqlVector that "wraps" an existing ArrayList. Modifications to the passed
   * ArrayList will also be reflected in the returned CqlVector.
   *
   * @param list the collection of values to wrap.
   * @return a CqlVector wrapping those values
   */
  public static <V> CqlVector<V> newInstance(List<V> list) {
    Preconditions.checkArgument(list != null, "Input list should not be null");
    return new CqlVector(list);
  }

  /**
   * Create a new CqlVector instance from the specified string representation. Note that this method
   * is intended to mirror {@link #toString()}; passing this method the output from a <code>toString
   * </code> call on some CqlVector should return a CqlVector that is equal to the origin instance.
   *
   * @param str a String representation of a CqlVector
   * @param subtypeCodec
   * @return a new CqlVector built from the String representation
   */
  public static <V> CqlVector<V> from(@NonNull String str, @NonNull TypeCodec<V> subtypeCodec) {
    Preconditions.checkArgument(str != null, "Cannot create CqlVector from null string");
    Preconditions.checkArgument(!str.isEmpty(), "Cannot create CqlVector from empty string");
    ArrayList<V> vals =
        Streams.stream(Splitter.on(", ").split(str.substring(1, str.length() - 1)))
            .map(subtypeCodec::parse)
            .collect(Collectors.toCollection(ArrayList::new));
    return new CqlVector(vals);
  }

  private final List<T> list;

  private CqlVector(@NonNull List<T> list) {

    Preconditions.checkArgument(
        Iterables.all(list, Predicates.notNull()), "CqlVectors cannot contain null values");
    this.list = list;
  }

  /**
   * Retrieve the value at the specified index. Modelled after {@link List#get(int)}
   *
   * @param idx the index to retrieve
   * @return the value at the specified index
   */
  public T get(int idx) {
    return list.get(idx);
  }

  /**
   * Update the value at the specified index. Modelled after {@link List#set(int, Object)}
   *
   * @param idx the index to set
   * @param val the new value for the specified index
   * @return the old value for the specified index
   */
  public T set(int idx, T val) {
    return list.set(idx, val);
  }

  /**
   * Return the size of this vector. Modelled after {@link List#size()}
   *
   * @return the vector size
   */
  public int size() {
    return this.list.size();
  }

  /**
   * Return a CqlVector consisting of the contents of a portion of this vector. Modelled after
   * {@link List#subList(int, int)}
   *
   * @param from the index to start from (inclusive)
   * @param to the index to end on (exclusive)
   * @return a new CqlVector wrapping the sublist
   */
  public CqlVector<T> subVector(int from, int to) {
    return new CqlVector<T>(this.list.subList(from, to));
  }

  /**
   * Return a boolean indicating whether the vector is empty. Modelled after {@link List#isEmpty()}
   *
   * @return true if the list is empty, false otherwise
   */
  public boolean isEmpty() {
    return this.list.isEmpty();
  }

  /**
   * Create an {@link Iterator} for this vector
   *
   * @return the generated iterator
   */
  @Override
  public Iterator<T> iterator() {
    return this.list.iterator();
  }

  /**
   * Create a {@link Stream} of the values in this vector
   *
   * @return the Stream instance
   */
  public Stream<T> stream() {
    return this.list.stream();
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    } else if (o instanceof CqlVector) {
      CqlVector that = (CqlVector) o;
      return this.list.equals(that.list);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(list);
  }

  @Override
  public String toString() {
    return Iterables.toString(this.list);
  }

  /**
   * Serialization proxy for CqlVector. Allows serialization regardless of implementation of list
   * field.
   *
   * @param <T> inner type of CqlVector, assume Number is always Serializable.
   */
  private static class SerializationProxy<T> implements Serializable {

    private static final long serialVersionUID = 1;

    private transient List<T> list;

    SerializationProxy(CqlVector<T> vector) {
      this.list = vector.list;
    }

    // Reconstruct CqlVector's list of elements.
    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
      stream.defaultReadObject();

      int size = stream.readInt();
      list = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        list.add((T) stream.readObject());
      }
    }

    // Return deserialized proxy object as CqlVector.
    private Object readResolve() throws ObjectStreamException {
      return new CqlVector(list);
    }

    // Write size of CqlVector followed by items in order.
    private void writeObject(ObjectOutputStream stream) throws IOException {
      stream.defaultWriteObject();

      stream.writeInt(list.size());
      for (T item : list) {
        stream.writeObject(item);
      }
    }
  }

  /** @serialData The number of elements in the vector, followed by each element in-order. */
  private Object writeReplace() {
    return new SerializationProxy(this);
  }

  private void readObject(@SuppressWarnings("unused") ObjectInputStream stream)
      throws InvalidObjectException {
    // Should never be called since we serialized a proxy
    throw new InvalidObjectException("Proxy required");
  }
}
