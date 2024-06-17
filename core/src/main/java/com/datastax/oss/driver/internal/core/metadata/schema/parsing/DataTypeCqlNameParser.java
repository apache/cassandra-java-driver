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
package com.datastax.oss.driver.internal.core.metadata.schema.parsing;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.schema.ShallowUserDefinedType;
import com.datastax.oss.driver.internal.core.type.DefaultTupleType;
import com.datastax.oss.driver.internal.core.type.DefaultVectorType;
import com.datastax.oss.driver.internal.core.type.codec.ParseUtils;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import net.jcip.annotations.ThreadSafe;

/**
 * Parses data types from schema tables, for Cassandra 3.0 and above.
 *
 * <p>In these versions, data types appear as string literals, like "ascii" or
 * "tuple&lt;int,int&gt;".
 */
@ThreadSafe
public class DataTypeCqlNameParser implements DataTypeParser {

  @Override
  public DataType parse(
      CqlIdentifier keyspaceId,
      String toParse,
      Map<CqlIdentifier, UserDefinedType> userTypes,
      InternalDriverContext context) {
    // Top-level is never frozen, it is only set recursively when we encounter the frozen<> keyword
    return parse(toParse, keyspaceId, false, userTypes, context);
  }

  private DataType parse(
      String toParse,
      CqlIdentifier keyspaceId,
      boolean frozen,
      Map<CqlIdentifier, UserDefinedType> userTypes,
      InternalDriverContext context) {

    if (toParse.startsWith("'")) {
      return DataTypes.custom(toParse.substring(1, toParse.length() - 1));
    }

    Parser parser = new Parser(toParse, 0);
    String type = parser.parseTypeName();

    if (type.equalsIgnoreCase(RawColumn.THRIFT_EMPTY_TYPE)) {
      return DataTypes.custom(type);
    }

    DataType nativeType = NATIVE_TYPES_BY_NAME.get(type.toLowerCase(Locale.ROOT));
    if (nativeType != null) {
      return nativeType;
    }

    if (parser.isEOS()) {
      // No parameters => it's a UDT
      CqlIdentifier name = CqlIdentifier.fromCql(type);
      if (userTypes != null) {
        UserDefinedType userType = userTypes.get(name);
        if (userType == null) {
          throw new IllegalStateException(
              String.format("Can't find referenced user type %s", type));
        }
        return userType.copy(frozen);
      } else {
        return new ShallowUserDefinedType(keyspaceId, name, frozen);
      }
    }

    List<String> parameters = parser.parseTypeParameters();
    if (type.equalsIgnoreCase("list")) {
      if (parameters.size() != 1) {
        throw new IllegalArgumentException(
            String.format("Expecting single parameter for list, got %s", parameters));
      }
      DataType elementType = parse(parameters.get(0), keyspaceId, false, userTypes, context);
      return DataTypes.listOf(elementType, frozen);
    }

    if (type.equalsIgnoreCase("set")) {
      if (parameters.size() != 1) {
        throw new IllegalArgumentException(
            String.format("Expecting single parameter for set, got %s", parameters));
      }
      DataType elementType = parse(parameters.get(0), keyspaceId, false, userTypes, context);
      return DataTypes.setOf(elementType, frozen);
    }

    if (type.equalsIgnoreCase("map")) {
      if (parameters.size() != 2) {
        throw new IllegalArgumentException(
            String.format("Expecting two parameters for map, got %s", parameters));
      }
      DataType keyType = parse(parameters.get(0), keyspaceId, false, userTypes, context);
      DataType valueType = parse(parameters.get(1), keyspaceId, false, userTypes, context);
      return DataTypes.mapOf(keyType, valueType, frozen);
    }

    if (type.equalsIgnoreCase("frozen")) {
      if (parameters.size() != 1) {
        throw new IllegalArgumentException(
            String.format("Expecting single parameter for frozen keyword, got %s", parameters));
      }
      return parse(parameters.get(0), keyspaceId, true, userTypes, context);
    }

    if (type.equalsIgnoreCase("tuple")) {
      if (parameters.isEmpty()) {
        throw new IllegalArgumentException("Expecting at list one parameter for tuple, got none");
      }
      ImmutableList.Builder<DataType> componentTypesBuilder = ImmutableList.builder();
      for (String rawType : parameters) {
        componentTypesBuilder.add(parse(rawType, keyspaceId, false, userTypes, context));
      }
      return new DefaultTupleType(componentTypesBuilder.build(), context);
    }

    if (type.equalsIgnoreCase("vector")) {
      if (parameters.size() != 2) {
        throw new IllegalArgumentException(
            String.format("Expecting two parameters for vector custom type, got %s", parameters));
      }
      DataType subType = parse(parameters.get(0), keyspaceId, false, userTypes, context);
      int dimensions = Integer.parseInt(parameters.get(1));
      return new DefaultVectorType(subType, dimensions);
    }

    throw new IllegalArgumentException("Could not parse type name " + toParse);
  }

  private static class Parser {

    private final String str;

    private int idx;

    Parser(String str, int idx) {
      this.str = str;
      this.idx = idx;
    }

    String parseTypeName() {
      idx = ParseUtils.skipSpaces(str, idx);
      return readNextIdentifier();
    }

    List<String> parseTypeParameters() {
      List<String> list = new ArrayList<>();

      if (isEOS()) {
        return list;
      }

      skipBlankAndComma();

      if (str.charAt(idx) != '<') {
        throw new IllegalStateException();
      }

      ++idx; // skipping '<'

      while (skipBlankAndComma()) {
        if (str.charAt(idx) == '>') {
          ++idx;
          return list;
        }

        String name = parseTypeName();
        String args = readRawTypeParameters();
        list.add(name + args);
      }
      throw new IllegalArgumentException(
          String.format(
              "Syntax error parsing '%s' at char %d: unexpected end of string", str, idx));
    }

    // left idx positioned on the character stopping the read
    private String readNextIdentifier() {
      int startIdx = idx;
      if (str.charAt(startIdx) == '"') { // case-sensitive name included in double quotes
        ++idx;
        // read until closing quote.
        while (!isEOS()) {
          boolean atQuote = str.charAt(idx) == '"';
          ++idx;
          if (atQuote) {
            // if the next character is also a quote, this is an escaped quote, continue reading,
            // otherwise stop.
            if (!isEOS() && str.charAt(idx) == '"') {
              ++idx;
            } else {
              break;
            }
          }
        }
      } else if (str.charAt(startIdx) == '\'') { // custom type name included in single quotes
        ++idx;
        // read until closing quote.
        while (!isEOS() && str.charAt(idx++) != '\'') {
          /* loop */
        }
      } else {
        while (!isEOS()
            && (ParseUtils.isCqlIdentifierChar(str.charAt(idx)) || str.charAt(idx) == '"')) {
          ++idx;
        }
      }
      return str.substring(startIdx, idx);
    }

    // Assumes we have just read a type name and read its potential arguments blindly. I.e. it
    // assumes that either parsing is done or that we're on a '<' and this reads everything up until
    // the corresponding closing '>'. It returns everything read, including the enclosing brackets.
    private String readRawTypeParameters() {
      idx = ParseUtils.skipSpaces(str, idx);

      if (isEOS() || str.charAt(idx) == '>' || str.charAt(idx) == ',') {
        return "";
      }

      if (str.charAt(idx) != '<') {
        throw new IllegalStateException(
            String.format(
                "Expecting char %d of %s to be '<' but '%c' found", idx, str, str.charAt(idx)));
      }

      int i = idx;
      int open = 1;
      boolean inQuotes = false;
      while (open > 0) {
        ++idx;

        if (isEOS()) {
          throw new IllegalStateException("Non closed angle brackets");
        }

        // Only parse for '<' and '>' characters if not within a quoted identifier.
        // Note we don't need to handle escaped quotes ("") in type names here, because they just
        // cause inQuotes to flip to false and immediately back to true
        if (!inQuotes) {
          if (str.charAt(idx) == '"') {
            inQuotes = true;
          } else if (str.charAt(idx) == '<') {
            open++;
          } else if (str.charAt(idx) == '>') {
            open--;
          }
        } else if (str.charAt(idx) == '"') {
          inQuotes = false;
        }
      }
      // we've stopped at the last closing ')' so move past that
      ++idx;
      return str.substring(i, idx);
    }

    // skip all blank and at best one comma, return true if there not EOS
    private boolean skipBlankAndComma() {
      boolean commaFound = false;
      while (!isEOS()) {
        int c = str.charAt(idx);
        if (c == ',') {
          if (commaFound) {
            return true;
          } else {
            commaFound = true;
          }
        } else if (!ParseUtils.isBlank(c)) {
          return true;
        }
        ++idx;
      }
      return false;
    }

    private boolean isEOS() {
      return idx >= str.length();
    }

    @Override
    public String toString() {
      return str.substring(0, idx)
          + "["
          + (idx == str.length() ? "" : str.charAt(idx))
          + "]"
          + str.substring(idx + 1);
    }
  }

  @VisibleForTesting
  static final ImmutableMap<String, DataType> NATIVE_TYPES_BY_NAME =
      new ImmutableMap.Builder<String, DataType>()
          .put("ascii", DataTypes.ASCII)
          .put("bigint", DataTypes.BIGINT)
          .put("blob", DataTypes.BLOB)
          .put("boolean", DataTypes.BOOLEAN)
          .put("counter", DataTypes.COUNTER)
          .put("decimal", DataTypes.DECIMAL)
          .put("double", DataTypes.DOUBLE)
          .put("float", DataTypes.FLOAT)
          .put("inet", DataTypes.INET)
          .put("int", DataTypes.INT)
          .put("text", DataTypes.TEXT)
          .put("varchar", DataTypes.TEXT)
          .put("timestamp", DataTypes.TIMESTAMP)
          .put("date", DataTypes.DATE)
          .put("time", DataTypes.TIME)
          .put("uuid", DataTypes.UUID)
          .put("varint", DataTypes.VARINT)
          .put("timeuuid", DataTypes.TIMEUUID)
          .put("tinyint", DataTypes.TINYINT)
          .put("smallint", DataTypes.SMALLINT)
          .put("duration", DataTypes.DURATION)
          .build();
}
