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
package com.datastax.oss.driver.internal.querybuilder;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.CqlSnippet;
import java.util.Collection;

public class CqlHelper {

  public static void appendIds(
      Iterable<CqlIdentifier> ids,
      StringBuilder builder,
      String prefix,
      String separator,
      String suffix) {
    boolean first = true;
    for (CqlIdentifier id : ids) {
      if (first) {
        if (prefix != null) {
          builder.append(prefix);
        }
        first = false;
      } else {
        builder.append(separator);
      }
      builder.append(id.asCql(true));
    }
    if (!first && suffix != null) {
      builder.append(suffix);
    }
  }

  public static void append(
      Iterable<? extends CqlSnippet> snippets,
      StringBuilder builder,
      String prefix,
      String separator,
      String suffix) {
    boolean first = true;
    for (CqlSnippet snippet : snippets) {
      if (first) {
        if (prefix != null) {
          builder.append(prefix);
        }
        first = false;
      } else {
        builder.append(separator);
      }
      snippet.appendTo(builder);
    }
    if (!first && suffix != null) {
      builder.append(suffix);
    }
  }

  public static void qualify(CqlIdentifier keyspace, CqlIdentifier element, StringBuilder builder) {
    if (keyspace != null) {
      builder.append(keyspace.asCql(true)).append('.');
    }
    builder.append(element.asCql(true));
  }

  public static void buildPrimaryKey(
      Collection<CqlIdentifier> partitionKeyColumns,
      Collection<CqlIdentifier> clusteringKeyColumns,
      StringBuilder builder) {
    builder.append("PRIMARY KEY(");
    boolean firstKey = true;

    if (partitionKeyColumns.size() > 1) {
      builder.append('(');
    }
    for (CqlIdentifier partitionColumn : partitionKeyColumns) {
      if (firstKey) {
        firstKey = false;
      } else {
        builder.append(',');
      }
      builder.append(partitionColumn.asCql(true));
    }
    if (partitionKeyColumns.size() > 1) {
      builder.append(')');
    }

    for (CqlIdentifier clusteringColumn : clusteringKeyColumns) {
      builder.append(',').append(clusteringColumn.asCql(true));
    }
    builder.append(')');
  }
}
