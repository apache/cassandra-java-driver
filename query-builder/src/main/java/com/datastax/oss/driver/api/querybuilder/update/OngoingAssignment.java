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
package com.datastax.oss.driver.api.querybuilder.update;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Arrays;

public interface OngoingAssignment {

  /**
   * Adds an assignment to this statement, as in {@code UPDATE foo SET v=1}.
   *
   * <p>To create the argument, use one of the factory methods in {@link Assignment}, for example
   * Assignment{@link #setColumn(CqlIdentifier, Term)}. This type also provides shortcuts to create
   * and add the assignment in one call, for example {@link #setColumn(CqlIdentifier, Term)}.
   *
   * <p>If you add multiple assignments as one, consider {@link #set(Iterable)} as a more efficient
   * alternative.
   */
  @NonNull
  UpdateWithAssignments set(@NonNull Assignment assignment);

  /**
   * Adds multiple assignments at once.
   *
   * <p>This is slightly more efficient than adding the assignments one by one (since the underlying
   * implementation of this object is immutable).
   *
   * <p>To create the argument, use one of the factory methods in {@link Assignment}, for example
   * Assignment{@link #setColumn(CqlIdentifier, Term)}.
   */
  @NonNull
  UpdateWithAssignments set(@NonNull Iterable<Assignment> additionalAssignments);

  /** Var-arg equivalent of {@link #set(Iterable)}. */
  @NonNull
  default UpdateWithAssignments set(@NonNull Assignment... assignments) {
    return set(Arrays.asList(assignments));
  }

  /**
   * Assigns a value to a column, as in {@code SET c=1}.
   *
   * <p>This is a shortcut for {@link #set(Assignment) set(Assignment.setColumn(columnId, value))}.
   *
   * @see Assignment#setColumn(CqlIdentifier, Term)
   */
  @NonNull
  default UpdateWithAssignments setColumn(@NonNull CqlIdentifier columnId, @NonNull Term value) {
    return set(Assignment.setColumn(columnId, value));
  }

  /**
   * Shortcut for {@link #setColumn(CqlIdentifier, Term)
   * setColumn(CqlIdentifier.fromCql(columnName), value)}.
   *
   * @see Assignment#setColumn(String, Term)
   */
  @NonNull
  default UpdateWithAssignments setColumn(@NonNull String columnName, @NonNull Term value) {
    return setColumn(CqlIdentifier.fromCql(columnName), value);
  }

  /**
   * Assigns a value to a field of a UDT, as in {@code SET address.zip=?}.
   *
   * <p>This is a shortcut for {@link #set(Assignment) set(Assignment.setField(columnId, fieldId,
   * value))}.
   *
   * @see Assignment#setField(CqlIdentifier, CqlIdentifier, Term)
   */
  @NonNull
  default UpdateWithAssignments setField(
      @NonNull CqlIdentifier columnId, @NonNull CqlIdentifier fieldId, @NonNull Term value) {
    return set(Assignment.setField(columnId, fieldId, value));
  }

  /**
   * Shortcut for {@link #setField(CqlIdentifier, CqlIdentifier, Term)
   * setField(CqlIdentifier.fromCql(columnName), CqlIdentifier.fromCql(fieldName), value)}.
   *
   * @see Assignment#setField(String, String, Term)
   */
  @NonNull
  default UpdateWithAssignments setField(
      @NonNull String columnName, @NonNull String fieldName, @NonNull Term value) {
    return setField(CqlIdentifier.fromCql(columnName), CqlIdentifier.fromCql(fieldName), value);
  }

  /**
   * Assigns a value to an entry in a map column, as in {@code SET map[?]=?}.
   *
   * <p>This is a shortcut for {@link #set(Assignment) set(Assignment.setMapValue(columnId, key,
   * value))}.
   *
   * @see Assignment#setMapValue(CqlIdentifier, Term, Term)
   */
  @NonNull
  default UpdateWithAssignments setMapValue(
      @NonNull CqlIdentifier columnId, @NonNull Term key, @NonNull Term value) {
    return set(Assignment.setMapValue(columnId, key, value));
  }

  /**
   * Shortcut for {@link #setMapValue(CqlIdentifier, Term, Term)
   * setMapValue(CqlIdentifier.fromCql(columnName), key, value)}.
   *
   * @see Assignment#setMapValue(String, Term, Term)
   */
  @NonNull
  default UpdateWithAssignments setMapValue(
      @NonNull String columnName, @NonNull Term key, @NonNull Term value) {
    return setMapValue(CqlIdentifier.fromCql(columnName), key, value);
  }

  /**
   * Assigns a value to an index in a list column, as in {@code SET list[?]=?}.
   *
   * <p>This is a shortcut for {@link #set(Assignment) set(Assignment.setListValue(columnId, index,
   * value))}.
   *
   * @see Assignment#setListValue(CqlIdentifier, Term, Term)
   */
  @NonNull
  default UpdateWithAssignments setListValue(
      @NonNull CqlIdentifier columnId, @NonNull Term index, @NonNull Term value) {
    return set(Assignment.setListValue(columnId, index, value));
  }

  /**
   * Shortcut for {@link #setListValue(CqlIdentifier, Term, Term)
   * setListValue(CqlIdentifier.fromCql(columnName), index, value)}.
   *
   * @see Assignment#setListValue(String, Term, Term)
   */
  @NonNull
  default UpdateWithAssignments setListValue(
      @NonNull String columnName, @NonNull Term index, @NonNull Term value) {
    return setListValue(CqlIdentifier.fromCql(columnName), index, value);
  }

  /**
   * Increments a counter, as in {@code SET c+=?}.
   *
   * <p>This is a shortcut for {@link #set(Assignment) set(Assignment.increment(columnId, amount))}.
   *
   * @see Assignment#increment(CqlIdentifier, Term)
   */
  @NonNull
  default UpdateWithAssignments increment(@NonNull CqlIdentifier columnId, @NonNull Term amount) {
    return set(Assignment.increment(columnId, amount));
  }

  /**
   * Shortcut for {@link #increment(CqlIdentifier, Term)
   * increment(CqlIdentifier.fromCql(columnName), amount)}
   *
   * @see Assignment#increment(String, Term)
   */
  @NonNull
  default UpdateWithAssignments increment(@NonNull String columnName, @NonNull Term amount) {
    return increment(CqlIdentifier.fromCql(columnName), amount);
  }

  /**
   * Increments a counter by 1, as in {@code SET c+=1} .
   *
   * <p>This is a shortcut for {@link #increment(CqlIdentifier, Term)} increment(columnId,
   * QueryBuilder.literal(1))}.
   *
   * @see Assignment#increment(CqlIdentifier)
   */
  @NonNull
  default UpdateWithAssignments increment(@NonNull CqlIdentifier columnId) {
    return increment(columnId, QueryBuilder.literal(1));
  }

  /**
   * Shortcut for {@link #increment(CqlIdentifier) CqlIdentifier.fromCql(columnName)}.
   *
   * @see Assignment#increment(CqlIdentifier)
   */
  @NonNull
  default UpdateWithAssignments increment(@NonNull String columnName) {
    return increment(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Decrements a counter, as in {@code SET c-=?}.
   *
   * <p>This is a shortcut for {@link #set(Assignment) set(Assignment.decrement(columnId, amount))}.
   *
   * @see Assignment#decrement(CqlIdentifier, Term)
   */
  @NonNull
  default UpdateWithAssignments decrement(@NonNull CqlIdentifier columnId, @NonNull Term amount) {
    return set(Assignment.decrement(columnId, amount));
  }

  /**
   * Shortcut for {@link #decrement(CqlIdentifier, Term)
   * decrement(CqlIdentifier.fromCql(columnName), amount)}
   *
   * @see Assignment#decrement(String, Term)
   */
  @NonNull
  default UpdateWithAssignments decrement(@NonNull String columnName, @NonNull Term amount) {
    return decrement(CqlIdentifier.fromCql(columnName), amount);
  }

  /**
   * Decrements a counter by 1, as in {@code SET c-=1}.
   *
   * <p>This is a shortcut for {@link #decrement(CqlIdentifier, Term)} decrement(columnId, 1)}.
   *
   * @see Assignment#decrement(CqlIdentifier)
   */
  @NonNull
  default UpdateWithAssignments decrement(@NonNull CqlIdentifier columnId) {
    return decrement(columnId, QueryBuilder.literal(1));
  }

  /**
   * Shortcut for {@link #decrement(CqlIdentifier) CqlIdentifier.fromCql(columnName)}.
   *
   * @see Assignment#decrement(String)
   */
  @NonNull
  default UpdateWithAssignments decrement(@NonNull String columnName) {
    return decrement(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Appends to a collection column, as in {@code SET l=l+?}.
   *
   * <p>The term must be a collection of the same type as the column.
   *
   * <p>This is a shortcut for {@link #set(Assignment) set(Assignment.append(columnId, suffix))}.
   *
   * @see Assignment#append(CqlIdentifier, Term)
   */
  @NonNull
  default UpdateWithAssignments append(@NonNull CqlIdentifier columnId, @NonNull Term suffix) {
    return set(Assignment.append(columnId, suffix));
  }

  /**
   * Shortcut for {@link #append(CqlIdentifier, Term) append(CqlIdentifier.fromCql(columnName),
   * suffix)}.
   *
   * @see Assignment#append(String, Term)
   */
  @NonNull
  default UpdateWithAssignments append(@NonNull String columnName, @NonNull Term suffix) {
    return append(CqlIdentifier.fromCql(columnName), suffix);
  }

  /**
   * Appends a single element to a list column, as in {@code SET l=l+[?]}.
   *
   * <p>The term must be of the same type as the column's elements.
   *
   * <p>This is a shortcut for {@link #set(Assignment) set(Assignment.appendListElement(columnId,
   * suffix))}.
   *
   * @see Assignment#appendListElement(CqlIdentifier, Term)
   */
  @NonNull
  default UpdateWithAssignments appendListElement(
      @NonNull CqlIdentifier columnId, @NonNull Term suffix) {
    return set(Assignment.appendListElement(columnId, suffix));
  }

  /**
   * Shortcut for {@link #appendListElement(CqlIdentifier, Term)
   * appendListElement(CqlIdentifier.fromCql(columnName), suffix)}.
   *
   * @see Assignment#appendListElement(String, Term)
   */
  @NonNull
  default UpdateWithAssignments appendListElement(
      @NonNull String columnName, @NonNull Term suffix) {
    return appendListElement(CqlIdentifier.fromCql(columnName), suffix);
  }

  /**
   * Appends a single element to a set column, as in {@code SET s=s+{?}}.
   *
   * <p>The term must be of the same type as the column's elements.
   *
   * <p>This is a shortcut for {@link #set(Assignment) set(Assignment.appendSetElement(columnId,
   * suffix))}.
   *
   * @see Assignment#appendSetElement(CqlIdentifier, Term)
   */
  @NonNull
  default UpdateWithAssignments appendSetElement(
      @NonNull CqlIdentifier columnId, @NonNull Term suffix) {
    return set(Assignment.appendSetElement(columnId, suffix));
  }

  /**
   * Shortcut for {@link #appendSetElement(CqlIdentifier, Term)
   * appendSetElement(CqlIdentifier.fromCql(columnName), suffix)}.
   */
  @NonNull
  default UpdateWithAssignments appendSetElement(@NonNull String columnName, @NonNull Term suffix) {
    return appendSetElement(CqlIdentifier.fromCql(columnName), suffix);
  }

  /**
   * Appends a single entry to a map column, as in {@code SET m=m+{?:?}}.
   *
   * <p>The terms must be of the same type as the column's keys and values respectively.
   *
   * <p>This is a shortcut for {@link #set(Assignment) set(Assignment.appendMapEntry(columnId, key,
   * value)}.
   *
   * @see Assignment#appendMapEntry(CqlIdentifier, Term, Term)
   */
  @NonNull
  default UpdateWithAssignments appendMapEntry(
      @NonNull CqlIdentifier columnId, @NonNull Term key, @NonNull Term value) {
    return set(Assignment.appendMapEntry(columnId, key, value));
  }

  /**
   * Shortcut for {@link #appendMapEntry(CqlIdentifier, Term, Term)
   * appendMapEntry(CqlIdentifier.fromCql(columnName), key, value)}.
   *
   * @see Assignment#appendMapEntry(String, Term, Term)
   */
  @NonNull
  default UpdateWithAssignments appendMapEntry(
      @NonNull String columnName, @NonNull Term key, @NonNull Term value) {
    return appendMapEntry(CqlIdentifier.fromCql(columnName), key, value);
  }

  /**
   * Prepends to a collection column, as in {@code SET l=[1,2,3]+l}.
   *
   * <p>The term must be a collection of the same type as the column.
   *
   * <p>This is a shortcut for {@link #set(Assignment) set(Assignment.prepend(columnId, prefix))}.
   *
   * @see Assignment#prepend(CqlIdentifier, Term)
   */
  @NonNull
  default UpdateWithAssignments prepend(@NonNull CqlIdentifier columnId, @NonNull Term prefix) {
    return set(Assignment.prepend(columnId, prefix));
  }

  /**
   * Shortcut for {@link #prepend(CqlIdentifier, Term) prepend(CqlIdentifier.fromCql(columnName),
   * prefix)}.
   *
   * @see Assignment#prepend(String, Term)
   */
  @NonNull
  default UpdateWithAssignments prepend(@NonNull String columnName, @NonNull Term prefix) {
    return prepend(CqlIdentifier.fromCql(columnName), prefix);
  }

  /**
   * Prepends a single element to a list column, as in {@code SET l=[?]+l}.
   *
   * <p>The term must be of the same type as the column's elements.
   *
   * <p>This is a shortcut for {@link #set(Assignment) set(Assignment.prependListElement(columnId,
   * suffix))}.
   *
   * @see Assignment#prependListElement(CqlIdentifier, Term)
   */
  @NonNull
  default UpdateWithAssignments prependListElement(
      @NonNull CqlIdentifier columnId, @NonNull Term suffix) {
    return set(Assignment.prependListElement(columnId, suffix));
  }

  /**
   * Shortcut for {@link #prependListElement(CqlIdentifier, Term)
   * prependListElement(CqlIdentifier.fromCql(columnName), suffix)}.
   *
   * @see Assignment#prependListElement(String, Term)
   */
  @NonNull
  default UpdateWithAssignments prependListElement(
      @NonNull String columnName, @NonNull Term suffix) {
    return prependListElement(CqlIdentifier.fromCql(columnName), suffix);
  }

  /**
   * Prepends a single element to a set column, as in {@code SET s={?}+s}.
   *
   * <p>The term must be of the same type as the column's elements.
   *
   * <p>This is a shortcut for {@link #set(Assignment) set(Assignment.prependSetElement(columnId,
   * suffix))}.
   *
   * @see Assignment#prependSetElement(CqlIdentifier, Term)
   */
  @NonNull
  default UpdateWithAssignments prependSetElement(
      @NonNull CqlIdentifier columnId, @NonNull Term suffix) {
    return set(Assignment.prependSetElement(columnId, suffix));
  }

  /**
   * Shortcut for {@link #prependSetElement(CqlIdentifier, Term)
   * prependSetElement(CqlIdentifier.fromCql(columnName), suffix)}.
   *
   * @see Assignment#prependSetElement(String, Term)
   */
  @NonNull
  default UpdateWithAssignments prependSetElement(
      @NonNull String columnName, @NonNull Term suffix) {
    return prependSetElement(CqlIdentifier.fromCql(columnName), suffix);
  }

  /**
   * Prepends a single entry to a map column, as in {@code SET m={?:?}+m}.
   *
   * <p>The terms must be of the same type as the column's keys and values respectively.
   *
   * <p>This is a shortcut for {@link #set(Assignment) set(Assignment.prependMapEntry(columnId, key,
   * value))}.
   *
   * @see Assignment#prependMapEntry(CqlIdentifier, Term, Term)
   */
  @NonNull
  default UpdateWithAssignments prependMapEntry(
      @NonNull CqlIdentifier columnId, @NonNull Term key, @NonNull Term value) {
    return set(Assignment.prependMapEntry(columnId, key, value));
  }

  /**
   * Shortcut for {@link #prependMapEntry(CqlIdentifier, Term, Term)
   * prependMapEntry(CqlIdentifier.fromCql(columnName), key, value)}.
   *
   * @see Assignment#prependMapEntry(String, Term, Term)
   */
  @NonNull
  default UpdateWithAssignments prependMapEntry(
      @NonNull String columnName, @NonNull Term key, @NonNull Term value) {
    return prependMapEntry(CqlIdentifier.fromCql(columnName), key, value);
  }

  /**
   * Removes elements from a collection, as in {@code SET l=l-[1,2,3]}.
   *
   * <p>The term must be a collection of the same type as the column.
   *
   * <p><b>DO NOT USE THIS TO DECREMENT COUNTERS.</b> Use the dedicated {@link
   * #decrement(CqlIdentifier, Term)} methods instead. While the operator is technically the same,
   * and it would be possible to generate an expression such as {@code counter-=1} with this method,
   * a collection removal is idempotent while a counter decrement isn't.
   *
   * <p>This is a shortcut for {@link #set(Assignment) set(Assignment.remove(columnId,
   * collectionToRemove))}.
   *
   * @see Assignment#remove(CqlIdentifier, Term)
   */
  @NonNull
  default UpdateWithAssignments remove(
      @NonNull CqlIdentifier columnId, @NonNull Term collectionToRemove) {
    return set(Assignment.remove(columnId, collectionToRemove));
  }

  /**
   * Shortcut for {@link #remove(CqlIdentifier, Term) remove(CqlIdentifier.fromCql(columnName),
   * collectionToRemove)}.
   *
   * @see Assignment#remove(String, Term)
   */
  @NonNull
  default UpdateWithAssignments remove(
      @NonNull String columnName, @NonNull Term collectionToRemove) {
    return remove(CqlIdentifier.fromCql(columnName), collectionToRemove);
  }

  /**
   * Removes a single element to a list column, as in {@code SET l=l-[?]}.
   *
   * <p>The term must be of the same type as the column's elements.
   *
   * <p>This is a shortcut for {@link #set(Assignment) set(Assignment.removeListElement(columnId,
   * suffix))}.
   *
   * @see Assignment#removeListElement(CqlIdentifier, Term)
   */
  @NonNull
  default UpdateWithAssignments removeListElement(
      @NonNull CqlIdentifier columnId, @NonNull Term suffix) {
    return set(Assignment.removeListElement(columnId, suffix));
  }

  /**
   * Shortcut for {@link #removeListElement(CqlIdentifier, Term)
   * removeListElement(CqlIdentifier.fromCql(columnName), suffix)}.
   *
   * @see Assignment#removeListElement(String, Term)
   */
  @NonNull
  default UpdateWithAssignments removeListElement(
      @NonNull String columnName, @NonNull Term suffix) {
    return removeListElement(CqlIdentifier.fromCql(columnName), suffix);
  }

  /**
   * Removes a single element to a set column, as in {@code SET s=s-{?}}.
   *
   * <p>The term must be of the same type as the column's elements.
   *
   * <p>This is a shortcut for {@link #set(Assignment) set(Assignment.removeSetElement(columnId,
   * suffix))}.
   *
   * @see Assignment#removeSetElement(CqlIdentifier, Term)
   */
  @NonNull
  default UpdateWithAssignments removeSetElement(
      @NonNull CqlIdentifier columnId, @NonNull Term suffix) {
    return set(Assignment.removeSetElement(columnId, suffix));
  }

  /**
   * Shortcut for {@link #removeSetElement(CqlIdentifier, Term)
   * removeSetElement(CqlIdentifier.fromCql(columnName), suffix)}.
   */
  @NonNull
  default UpdateWithAssignments removeSetElement(@NonNull String columnName, @NonNull Term suffix) {
    return removeSetElement(CqlIdentifier.fromCql(columnName), suffix);
  }

  /**
   * Removes a single entry to a map column, as in {@code SET m=m-{?:?}}.
   *
   * <p>The terms must be of the same type as the column's keys and values respectively.
   *
   * <p>This is a shortcut for {@link #set(Assignment) set(Assignment.removeMapEntry(columnId, key,
   * value)}.
   *
   * @see Assignment#removeMapEntry(CqlIdentifier, Term, Term)
   */
  @NonNull
  default UpdateWithAssignments removeMapEntry(
      @NonNull CqlIdentifier columnId, @NonNull Term key, @NonNull Term value) {
    return set(Assignment.removeMapEntry(columnId, key, value));
  }

  /**
   * Shortcut for {@link #removeMapEntry(CqlIdentifier, Term, Term)
   * removeMapEntry(CqlIdentifier.fromCql(columnName), key, value)}.
   *
   * @see Assignment#removeMapEntry(String, Term, Term)
   */
  @NonNull
  default UpdateWithAssignments removeMapEntry(
      @NonNull String columnName, @NonNull Term key, @NonNull Term value) {
    return removeMapEntry(CqlIdentifier.fromCql(columnName), key, value);
  }
}
