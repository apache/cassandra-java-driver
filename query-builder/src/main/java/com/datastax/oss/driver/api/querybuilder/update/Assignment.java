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
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.api.querybuilder.CqlSnippet;
import com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.querybuilder.lhs.ColumnComponentLeftOperand;
import com.datastax.oss.driver.internal.querybuilder.lhs.ColumnLeftOperand;
import com.datastax.oss.driver.internal.querybuilder.lhs.FieldLeftOperand;
import com.datastax.oss.driver.internal.querybuilder.update.AppendAssignment;
import com.datastax.oss.driver.internal.querybuilder.update.AppendListElementAssignment;
import com.datastax.oss.driver.internal.querybuilder.update.AppendMapEntryAssignment;
import com.datastax.oss.driver.internal.querybuilder.update.AppendSetElementAssignment;
import com.datastax.oss.driver.internal.querybuilder.update.CounterAssignment;
import com.datastax.oss.driver.internal.querybuilder.update.DefaultAssignment;
import com.datastax.oss.driver.internal.querybuilder.update.PrependAssignment;
import com.datastax.oss.driver.internal.querybuilder.update.PrependListElementAssignment;
import com.datastax.oss.driver.internal.querybuilder.update.PrependMapEntryAssignment;
import com.datastax.oss.driver.internal.querybuilder.update.PrependSetElementAssignment;
import com.datastax.oss.driver.internal.querybuilder.update.RemoveListElementAssignment;
import com.datastax.oss.driver.internal.querybuilder.update.RemoveMapEntryAssignment;
import com.datastax.oss.driver.internal.querybuilder.update.RemoveSetElementAssignment;

/** An assignment that appears after the SET keyword in an UPDATE statement. */
public interface Assignment extends CqlSnippet {

  /** Assigns a value to a column, as in {@code SET c=?}. */
  static Assignment setColumn(CqlIdentifier columnId, Term value) {
    return new DefaultAssignment(new ColumnLeftOperand(columnId), "=", value);
  }

  /**
   * Shortcut for {@link #setColumn(CqlIdentifier, Term)
   * setColumn(CqlIdentifier.fromCql(columnName), value)}.
   */
  static Assignment setColumn(String columnName, Term value) {
    return setColumn(CqlIdentifier.fromCql(columnName), value);
  }

  /** Assigns a value to a field of a UDT, as in {@code SET address.zip=?}. */
  static Assignment setField(CqlIdentifier columnId, CqlIdentifier fieldId, Term value) {
    return new DefaultAssignment(new FieldLeftOperand(columnId, fieldId), "=", value);
  }

  /**
   * Shortcut for {@link #setField(CqlIdentifier, CqlIdentifier, Term)
   * setField(CqlIdentifier.fromCql(columnName), CqlIdentifier.fromCql(fieldName), value)}.
   */
  static Assignment setField(String columnName, String fieldName, Term value) {
    return setField(CqlIdentifier.fromCql(columnName), CqlIdentifier.fromCql(fieldName), value);
  }

  /** Assigns a value to an entry in a map column, as in {@code SET map[?]=?}. */
  static Assignment setMapValue(CqlIdentifier columnId, Term index, Term value) {
    return new DefaultAssignment(new ColumnComponentLeftOperand(columnId, index), "=", value);
  }

  /**
   * Shortcut for {@link #setMapValue(CqlIdentifier, Term, Term)
   * setMapValue(CqlIdentifier.fromCql(columnName), index, value)}.
   */
  static Assignment setMapValue(String columnName, Term index, Term value) {
    return setMapValue(CqlIdentifier.fromCql(columnName), index, value);
  }

  /** Increments a counter, as in {@code SET c+=?}. */
  static Assignment increment(CqlIdentifier columnId, Term amount) {
    return new CounterAssignment(new ColumnLeftOperand(columnId), "+=", amount);
  }

  /**
   * Shortcut for {@link #increment(CqlIdentifier, Term)
   * increment(CqlIdentifier.fromCql(columnName), amount)}
   */
  static Assignment increment(String columnName, Term amount) {
    return increment(CqlIdentifier.fromCql(columnName), amount);
  }

  /** Increments a counter by 1, as in {@code SET c+=1} . */
  static Assignment increment(CqlIdentifier columnId) {
    return increment(columnId, QueryBuilderDsl.literal(1));
  }

  /** Shortcut for {@link #increment(CqlIdentifier) CqlIdentifier.fromCql(columnName)}. */
  static Assignment increment(String columnName) {
    return increment(CqlIdentifier.fromCql(columnName));
  }

  /** Decrements a counter, as in {@code SET c-=?}. */
  static Assignment decrement(CqlIdentifier columnId, Term amount) {
    return new CounterAssignment(new ColumnLeftOperand(columnId), "-=", amount);
  }

  /**
   * Shortcut for {@link #decrement(CqlIdentifier, Term)
   * decrement(CqlIdentifier.fromCql(columnName), amount)}
   */
  static Assignment decrement(String columnName, Term amount) {
    return decrement(CqlIdentifier.fromCql(columnName), amount);
  }

  /** Decrements a counter by 1, as in {@code SET c-=1} . */
  static Assignment decrement(CqlIdentifier columnId) {
    return decrement(columnId, QueryBuilderDsl.literal(1));
  }

  /** Shortcut for {@link #decrement(CqlIdentifier) CqlIdentifier.fromCql(columnName)}. */
  static Assignment decrement(String columnName) {
    return decrement(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Appends to a collection column, as in {@code SET l+=?}.
   *
   * <p>The term must be a collection of the same type as the column.
   */
  static Assignment append(CqlIdentifier columnId, Term suffix) {
    return new AppendAssignment(new ColumnLeftOperand(columnId), suffix);
  }

  /**
   * Shortcut for {@link #append(CqlIdentifier, Term) append(CqlIdentifier.fromCql(columnName),
   * suffix)}.
   */
  static Assignment append(String columnName, Term suffix) {
    return append(CqlIdentifier.fromCql(columnName), suffix);
  }

  /**
   * Appends a single element to a list column, as in {@code SET l+=[?]}.
   *
   * <p>The term must be of the same type as the column's elements.
   */
  static Assignment appendListElement(CqlIdentifier columnId, Term suffix) {
    return new AppendListElementAssignment(columnId, suffix);
  }

  /**
   * Shortcut for {@link #appendListElement(CqlIdentifier, Term)
   * appendListElement(CqlIdentifier.fromCql(columnName), suffix)}.
   */
  static Assignment appendListElement(String columnName, Term suffix) {
    return appendListElement(CqlIdentifier.fromCql(columnName), suffix);
  }

  /**
   * Appends a single element to a set column, as in {@code SET s+={?}}.
   *
   * <p>The term must be of the same type as the column's elements.
   */
  static Assignment appendSetElement(CqlIdentifier columnId, Term suffix) {
    return new AppendSetElementAssignment(columnId, suffix);
  }

  /**
   * Shortcut for {@link #appendSetElement(CqlIdentifier, Term)
   * appendSetElement(CqlIdentifier.fromCql(columnName), suffix)}.
   */
  static Assignment appendSetElement(String columnName, Term suffix) {
    return appendSetElement(CqlIdentifier.fromCql(columnName), suffix);
  }

  /**
   * Appends a single entry to a map column, as in {@code SET m+={?:?}}.
   *
   * <p>The terms must be of the same type as the column's keys and values respectively.
   */
  static Assignment appendMapEntry(CqlIdentifier columnId, Term key, Term value) {
    return new AppendMapEntryAssignment(columnId, key, value);
  }

  /**
   * Shortcut for {@link #appendMapEntry(CqlIdentifier, Term, Term)
   * appendMapEntry(CqlIdentifier.fromCql(columnName), key, value)}.
   */
  static Assignment appendMapEntry(String columnName, Term key, Term value) {
    return appendMapEntry(CqlIdentifier.fromCql(columnName), key, value);
  }

  /**
   * Prepends to a collection column, as in {@code SET l=[1,2,3]+l}.
   *
   * <p>The term must be a collection of the same type as the column.
   */
  static Assignment prepend(CqlIdentifier columnId, Term prefix) {
    return new PrependAssignment(columnId, prefix);
  }

  /**
   * Shortcut for {@link #prepend(CqlIdentifier, Term) prepend(CqlIdentifier.fromCql(columnName),
   * prefix)}.
   */
  static Assignment prepend(String columnName, Term prefix) {
    return prepend(CqlIdentifier.fromCql(columnName), prefix);
  }

  /**
   * Prepends a single element to a list column, as in {@code SET l=[?]+l}.
   *
   * <p>The term must be of the same type as the column's elements.
   */
  static Assignment prependListElement(CqlIdentifier columnId, Term suffix) {
    return new PrependListElementAssignment(columnId, suffix);
  }

  /**
   * Shortcut for {@link #prependListElement(CqlIdentifier, Term)
   * prependListElement(CqlIdentifier.fromCql(columnName), suffix)}.
   */
  static Assignment prependListElement(String columnName, Term suffix) {
    return prependListElement(CqlIdentifier.fromCql(columnName), suffix);
  }

  /**
   * Prepends a single element to a set column, as in {@code SET s={?}+s}.
   *
   * <p>The term must be of the same type as the column's elements.
   */
  static Assignment prependSetElement(CqlIdentifier columnId, Term suffix) {
    return new PrependSetElementAssignment(columnId, suffix);
  }

  /**
   * Shortcut for {@link #prependSetElement(CqlIdentifier, Term)
   * prependSetElement(CqlIdentifier.fromCql(columnName), suffix)}.
   */
  static Assignment prependSetElement(String columnName, Term suffix) {
    return prependSetElement(CqlIdentifier.fromCql(columnName), suffix);
  }

  /**
   * Prepends a single entry to a map column, as in {@code SET m={?:?}+m}.
   *
   * <p>The terms must be of the same type as the column's keys and values respectively.
   */
  static Assignment prependMapEntry(CqlIdentifier columnId, Term key, Term value) {
    return new PrependMapEntryAssignment(columnId, key, value);
  }

  /**
   * Shortcut for {@link #prependMapEntry(CqlIdentifier, Term, Term)
   * prependMapEntry(CqlIdentifier.fromCql(columnName), key, value)}.
   */
  static Assignment prependMapEntry(String columnName, Term key, Term value) {
    return prependMapEntry(CqlIdentifier.fromCql(columnName), key, value);
  }

  /**
   * Removes elements from a collection, as in {@code SET l-=[1,2,3]}.
   *
   * <p>The term must be a collection of the same type as the column.
   *
   * <p><b>DO NOT USE THIS TO DECREMENT COUNTERS.</b> Use the dedicated {@link
   * #decrement(CqlIdentifier, Term)} methods instead. While the operator is technically the same,
   * and it would be possible to generate an expression such as {@code counter-=1} with this method,
   * a collection removal is idempotent while a counter decrement isn't.
   */
  static Assignment remove(CqlIdentifier columnId, Term collectionToRemove) {
    return new DefaultAssignment(new ColumnLeftOperand(columnId), "-=", collectionToRemove);
  }

  /**
   * Shortcut for {@link #remove(CqlIdentifier, Term) remove(CqlIdentifier.fromCql(columnName),
   * collectionToRemove)}.
   */
  static Assignment remove(String columnName, Term collectionToRemove) {
    return remove(CqlIdentifier.fromCql(columnName), collectionToRemove);
  }

  /**
   * Removes a single element to a list column, as in {@code SET l-=[?]}.
   *
   * <p>The term must be of the same type as the column's elements.
   */
  static Assignment removeListElement(CqlIdentifier columnId, Term suffix) {
    return new RemoveListElementAssignment(columnId, suffix);
  }

  /**
   * Shortcut for {@link #removeListElement(CqlIdentifier, Term)
   * removeListElement(CqlIdentifier.fromCql(columnName), suffix)}.
   */
  static Assignment removeListElement(String columnName, Term suffix) {
    return removeListElement(CqlIdentifier.fromCql(columnName), suffix);
  }

  /**
   * Removes a single element to a set column, as in {@code SET s-={?}}.
   *
   * <p>The term must be of the same type as the column's elements.
   */
  static Assignment removeSetElement(CqlIdentifier columnId, Term suffix) {
    return new RemoveSetElementAssignment(columnId, suffix);
  }

  /**
   * Shortcut for {@link #removeSetElement(CqlIdentifier, Term)
   * removeSetElement(CqlIdentifier.fromCql(columnName), suffix)}.
   */
  static Assignment removeSetElement(String columnName, Term suffix) {
    return removeSetElement(CqlIdentifier.fromCql(columnName), suffix);
  }

  /**
   * Removes a single entry to a map column, as in {@code SET m-={?:?}}.
   *
   * <p>The terms must be of the same type as the column's keys and values respectively.
   */
  static Assignment removeMapEntry(CqlIdentifier columnId, Term key, Term value) {
    return new RemoveMapEntryAssignment(columnId, key, value);
  }

  /**
   * Shortcut for {@link #removeMapEntry(CqlIdentifier, Term, Term)
   * removeMapEntry(CqlIdentifier.fromCql(columnName), key, value)}.
   */
  static Assignment removeMapEntry(String columnName, Term key, Term value) {
    return removeMapEntry(CqlIdentifier.fromCql(columnName), key, value);
  }

  /**
   * Whether this assignment is idempotent.
   *
   * <p>That is, whether it always sets its target column to the same value when used multiple
   * times. For example, {@code UPDATE ... SET c=1} is idempotent, {@code SET l=l+[1]} isn't.
   *
   * <p>This is used internally by the query builder to compute the {@link Statement#isIdempotent()}
   * flag on the UPDATE statements generated by {@link BuildableQuery#build()}. If an assignment is
   * ambiguous (for example a raw snippet or a call to a user function in the right operands), the
   * builder is pessimistic and assumes the term is not idempotent.
   */
  boolean isIdempotent();
}
