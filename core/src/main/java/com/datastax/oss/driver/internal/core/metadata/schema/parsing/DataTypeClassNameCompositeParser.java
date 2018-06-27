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
package com.datastax.oss.driver.internal.core.metadata.schema.parsing;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class DataTypeClassNameCompositeParser extends DataTypeClassNameParser {

  public ParseResult parseWithComposite(
      String className,
      CqlIdentifier keyspaceId,
      Map<CqlIdentifier, UserDefinedType> userTypes,
      InternalDriverContext context) {
    Parser parser = new Parser(className, 0);

    String next = parser.parseNextName();
    if (!isComposite(next)) {
      return new ParseResult(parse(keyspaceId, className, userTypes, context), isReversed(next));
    }

    List<String> subClassNames = parser.getTypeParameters();
    int count = subClassNames.size();
    String last = subClassNames.get(count - 1);
    Map<String, DataType> collections = new HashMap<>();
    if (isCollection(last)) {
      count--;
      Parser collectionParser = new Parser(last, 0);
      collectionParser.parseNextName(); // skips columnToCollectionType
      Map<String, String> params = collectionParser.getCollectionsParameters();
      for (Map.Entry<String, String> entry : params.entrySet()) {
        collections.put(entry.getKey(), parse(keyspaceId, entry.getValue(), userTypes, context));
      }
    }

    List<DataType> types = new ArrayList<>(count);
    List<Boolean> reversed = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      types.add(parse(keyspaceId, subClassNames.get(i), userTypes, context));
      reversed.add(isReversed(subClassNames.get(i)));
    }

    return new ParseResult(true, types, reversed, collections);
  }

  public static class ParseResult {
    public final boolean isComposite;
    public final List<DataType> types;
    public final List<Boolean> reversed;
    public final Map<String, DataType> collections;

    private ParseResult(DataType type, boolean reversed) {
      this(
          false,
          Collections.singletonList(type),
          Collections.singletonList(reversed),
          Collections.emptyMap());
    }

    private ParseResult(
        boolean isComposite,
        List<DataType> types,
        List<Boolean> reversed,
        Map<String, DataType> collections) {
      this.isComposite = isComposite;
      this.types = types;
      this.reversed = reversed;
      this.collections = collections;
    }
  }

  private static boolean isComposite(String className) {
    return className.startsWith("org.apache.cassandra.db.marshal.CompositeType");
  }

  private static boolean isCollection(String className) {
    return className.startsWith("org.apache.cassandra.db.marshal.ColumnToCollectionType");
  }
}
