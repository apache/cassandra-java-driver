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
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.type.DefaultTupleType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.driver.internal.core.type.codec.ParseUtils;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses data types from schema tables, for Cassandra 2.2 and below.
 *
 * <p>In these versions, data types appear as class names, like
 * "org.apache.cassandra.db.marshal.AsciiType" or
 * "org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.Int32Type)".
 *
 * <p>This is modified (and simplified) from Cassandra's {@code TypeParser} class to suit our needs.
 * In particular it's not very efficient, but it doesn't really matter since it's rarely used and
 * never in a critical path.
 */
@ThreadSafe
public class DataTypeClassNameParser implements DataTypeParser {

  private static final Logger LOG = LoggerFactory.getLogger(DataTypeClassNameParser.class);

  @Override
  public DataType parse(
      CqlIdentifier keyspaceId,
      String toParse,
      Map<CqlIdentifier, UserDefinedType> userTypes,
      InternalDriverContext context) {
    // We take keyspaceId as a parameter because of the parent interface, but it's actually unused
    // by this implementation.
    return parse(toParse, userTypes, context, context.getSessionName());
  }

  /**
   * Simplified parse method for external use.
   *
   * <p>This is intended for use in Cassandra's UDF implementation (the current version uses the
   * similar method from driver 3).
   */
  public DataType parse(String toParse, AttachmentPoint attachmentPoint) {
    return parse(
        toParse,
        null, // No caching of user types: nested types will always be fully re-parsed
        attachmentPoint,
        "parser");
  }

  private DataType parse(
      String toParse,
      Map<CqlIdentifier, UserDefinedType> userTypes,
      AttachmentPoint attachmentPoint,
      String logPrefix) {
    boolean frozen = false;
    if (isReversed(toParse)) {
      // Just skip the ReversedType part, we don't care
      toParse = getNestedClassName(toParse);
    } else if (toParse.startsWith("org.apache.cassandra.db.marshal.FrozenType")) {
      frozen = true;
      toParse = getNestedClassName(toParse);
    }

    Parser parser = new Parser(toParse, 0);
    String next = parser.parseNextName();

    if (next.startsWith("org.apache.cassandra.db.marshal.ListType")) {
      DataType elementType =
          parse(parser.getTypeParameters().get(0), userTypes, attachmentPoint, logPrefix);
      return DataTypes.listOf(elementType, frozen);
    }

    if (next.startsWith("org.apache.cassandra.db.marshal.SetType")) {
      DataType elementType =
          parse(parser.getTypeParameters().get(0), userTypes, attachmentPoint, logPrefix);
      return DataTypes.setOf(elementType, frozen);
    }

    if (next.startsWith("org.apache.cassandra.db.marshal.MapType")) {
      List<String> parameters = parser.getTypeParameters();
      DataType keyType = parse(parameters.get(0), userTypes, attachmentPoint, logPrefix);
      DataType valueType = parse(parameters.get(1), userTypes, attachmentPoint, logPrefix);
      return DataTypes.mapOf(keyType, valueType, frozen);
    }

    if (frozen)
      LOG.warn(
          "[{}] Got o.a.c.db.marshal.FrozenType for something else than a collection, "
              + "this driver version might be too old for your version of Cassandra",
          logPrefix);

    if (next.startsWith("org.apache.cassandra.db.marshal.UserType")) {
      ++parser.idx; // skipping '('

      CqlIdentifier keyspace = CqlIdentifier.fromInternal(parser.readOne());
      parser.skipBlankAndComma();
      String typeName =
          TypeCodecs.TEXT.decode(
              Bytes.fromHexString("0x" + parser.readOne()), attachmentPoint.getProtocolVersion());
      if (typeName == null) {
        throw new AssertionError("Type name cannot be null, this is a server bug");
      }
      CqlIdentifier typeId = CqlIdentifier.fromInternal(typeName);
      Map<String, String> nameAndTypeParameters = parser.getNameAndTypeParameters();

      // Avoid re-parsing if we already have the definition
      if (userTypes != null && userTypes.containsKey(typeId)) {
        // copy as frozen since C* 2.x UDTs are always frozen.
        return userTypes.get(typeId).copy(true);
      } else {
        UserDefinedTypeBuilder builder = new UserDefinedTypeBuilder(keyspace, typeId);
        parser.skipBlankAndComma();
        for (Map.Entry<String, String> entry : nameAndTypeParameters.entrySet()) {
          CqlIdentifier fieldName = CqlIdentifier.fromInternal(entry.getKey());
          DataType fieldType = parse(entry.getValue(), userTypes, attachmentPoint, logPrefix);
          builder.withField(fieldName, fieldType);
        }
        // Create a frozen UserType since C* 2.x UDTs are always frozen.
        return builder.frozen().withAttachmentPoint(attachmentPoint).build();
      }
    }

    if (next.startsWith("org.apache.cassandra.db.marshal.TupleType")) {
      List<String> rawTypes = parser.getTypeParameters();
      ImmutableList.Builder<DataType> componentTypesBuilder = ImmutableList.builder();
      for (String rawType : rawTypes) {
        componentTypesBuilder.add(parse(rawType, userTypes, attachmentPoint, logPrefix));
      }
      return new DefaultTupleType(componentTypesBuilder.build(), attachmentPoint);
    }

    DataType type = NATIVE_TYPES_BY_CLASS_NAME.get(next);
    return type == null ? DataTypes.custom(toParse) : type;
  }

  static boolean isReversed(String toParse) {
    return toParse.startsWith("org.apache.cassandra.db.marshal.ReversedType");
  }

  private static String getNestedClassName(String className) {
    Parser p = new Parser(className, 0);
    p.parseNextName();
    List<String> l = p.getTypeParameters();
    if (l.size() != 1) {
      throw new IllegalStateException();
    }
    className = l.get(0);
    return className;
  }

  static class Parser {

    private final String str;
    private int idx;

    Parser(String str, int idx) {
      this.str = str;
      this.idx = idx;
    }

    String parseNextName() {
      skipBlank();
      return readNextIdentifier();
    }

    private String readOne() {
      String name = parseNextName();
      String args = readRawArguments();
      return name + args;
    }

    // Assumes we have just read a class name and read it's potential arguments
    // blindly. I.e. it assume that either parsing is done or that we're on a '('
    // and this reads everything up until the corresponding closing ')'. It
    // returns everything read, including the enclosing parenthesis.
    private String readRawArguments() {
      skipBlank();

      if (isEOS() || str.charAt(idx) == ')' || str.charAt(idx) == ',') {
        return "";
      }

      if (str.charAt(idx) != '(') {
        throw new IllegalStateException(
            String.format(
                "Expecting char %d of %s to be '(' but '%c' found", idx, str, str.charAt(idx)));
      }

      int i = idx;
      int open = 1;
      while (open > 0) {
        ++idx;

        if (isEOS()) {
          throw new IllegalStateException("Non closed parenthesis");
        }

        if (str.charAt(idx) == '(') {
          open++;
        } else if (str.charAt(idx) == ')') {
          open--;
        }
      }
      // we've stopped at the last closing ')' so move past that
      ++idx;
      return str.substring(i, idx);
    }

    List<String> getTypeParameters() {
      List<String> list = new ArrayList<>();

      if (isEOS()) {
        return list;
      }

      if (str.charAt(idx) != '(') {
        throw new IllegalStateException();
      }

      ++idx; // skipping '('

      while (skipBlankAndComma()) {
        if (str.charAt(idx) == ')') {
          ++idx;
          return list;
        }
        list.add(readOne());
      }
      throw new IllegalArgumentException(
          String.format(
              "Syntax error parsing '%s' at char %d: unexpected end of string", str, idx));
    }

    Map<String, String> getCollectionsParameters() {
      if (isEOS()) {
        return Collections.emptyMap();
      }
      if (str.charAt(idx) != '(') {
        throw new IllegalStateException();
      }
      ++idx; // skipping '('
      return getNameAndTypeParameters();
    }

    // Must be at the start of the first parameter to read
    private Map<String, String> getNameAndTypeParameters() {
      // The order of the hashmap matters for UDT
      Map<String, String> map = new LinkedHashMap<>();

      while (skipBlankAndComma()) {
        if (str.charAt(idx) == ')') {
          ++idx;
          return map;
        }

        String bbHex = readNextIdentifier();
        String name = null;
        try {
          name =
              TypeCodecs.TEXT.decode(
                  Bytes.fromHexString("0x" + bbHex), DefaultProtocolVersion.DEFAULT);
        } catch (NumberFormatException e) {
          throwSyntaxError(e.getMessage());
        }

        skipBlank();
        if (str.charAt(idx) != ':') {
          throwSyntaxError("expecting ':' token");
        }

        ++idx;
        skipBlank();
        map.put(name, readOne());
      }
      throw new IllegalArgumentException(
          String.format(
              "Syntax error parsing '%s' at char %d: unexpected end of string", str, idx));
    }

    private void throwSyntaxError(String msg) {
      throw new IllegalArgumentException(
          String.format("Syntax error parsing '%s' at char %d: %s", str, idx, msg));
    }

    private boolean isEOS() {
      return isEOS(str, idx);
    }

    private static boolean isEOS(String str, int i) {
      return i >= str.length();
    }

    private void skipBlank() {
      idx = skipBlank(str, idx);
    }

    private static int skipBlank(String str, int i) {
      while (!isEOS(str, i) && ParseUtils.isBlank(str.charAt(i))) {
        ++i;
      }
      return i;
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

    // left idx positioned on the character stopping the read
    private String readNextIdentifier() {
      int i = idx;
      while (!isEOS() && ParseUtils.isCqlIdentifierChar(str.charAt(idx))) {
        ++idx;
      }
      return str.substring(i, idx);
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
  static ImmutableMap<String, DataType> NATIVE_TYPES_BY_CLASS_NAME =
      new ImmutableMap.Builder<String, DataType>()
          .put("org.apache.cassandra.db.marshal.AsciiType", DataTypes.ASCII)
          .put("org.apache.cassandra.db.marshal.LongType", DataTypes.BIGINT)
          .put("org.apache.cassandra.db.marshal.BytesType", DataTypes.BLOB)
          .put("org.apache.cassandra.db.marshal.BooleanType", DataTypes.BOOLEAN)
          .put("org.apache.cassandra.db.marshal.CounterColumnType", DataTypes.COUNTER)
          .put("org.apache.cassandra.db.marshal.DecimalType", DataTypes.DECIMAL)
          .put("org.apache.cassandra.db.marshal.DoubleType", DataTypes.DOUBLE)
          .put("org.apache.cassandra.db.marshal.FloatType", DataTypes.FLOAT)
          .put("org.apache.cassandra.db.marshal.InetAddressType", DataTypes.INET)
          .put("org.apache.cassandra.db.marshal.Int32Type", DataTypes.INT)
          .put("org.apache.cassandra.db.marshal.UTF8Type", DataTypes.TEXT)
          .put("org.apache.cassandra.db.marshal.TimestampType", DataTypes.TIMESTAMP)
          .put("org.apache.cassandra.db.marshal.SimpleDateType", DataTypes.DATE)
          .put("org.apache.cassandra.db.marshal.TimeType", DataTypes.TIME)
          .put("org.apache.cassandra.db.marshal.UUIDType", DataTypes.UUID)
          .put("org.apache.cassandra.db.marshal.IntegerType", DataTypes.VARINT)
          .put("org.apache.cassandra.db.marshal.TimeUUIDType", DataTypes.TIMEUUID)
          .put("org.apache.cassandra.db.marshal.ByteType", DataTypes.TINYINT)
          .put("org.apache.cassandra.db.marshal.ShortType", DataTypes.SMALLINT)
          .put("org.apache.cassandra.db.marshal.DurationType", DataTypes.DURATION)
          .build();
}
