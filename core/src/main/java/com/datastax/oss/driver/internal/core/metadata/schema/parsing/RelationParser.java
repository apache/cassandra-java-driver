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

import com.datastax.dse.driver.api.core.metadata.DseNodeProperties;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.Describable;
import com.datastax.oss.driver.api.core.metadata.schema.RelationMetadata;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.schema.ScriptBuilder;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaRows;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import java.util.Map;
import net.jcip.annotations.ThreadSafe;

// Shared code for table and view parsing
@ThreadSafe
public abstract class RelationParser {

  protected final SchemaRows rows;
  protected final InternalDriverContext context;
  protected final String logPrefix;

  protected RelationParser(SchemaRows rows, InternalDriverContext context) {
    this.rows = rows;
    this.context = context;
    this.logPrefix = context.getSessionName();
  }

  protected Map<CqlIdentifier, Object> parseOptions(AdminRow row) {
    ImmutableMap.Builder<CqlIdentifier, Object> builder = ImmutableMap.builder();
    for (Map.Entry<String, TypeCodec<?>> entry : OPTION_CODECS.entrySet()) {
      String name = entry.getKey();
      CqlIdentifier id = CqlIdentifier.fromInternal(name);
      TypeCodec<?> codec = entry.getValue();

      if (name.equals("caching") && row.isString("caching")) {
        // C* <=2.2, caching is stored as a string, and also appears as a string in the WITH clause.
        builder.put(id, row.getString(name));
      } else if (name.equals("compaction_strategy_class")) {
        // C* <=2.2, compaction options split in two columns
        String strategyClass = row.getString(name);
        if (strategyClass != null) {
          builder.put(
              CqlIdentifier.fromInternal("compaction"),
              ImmutableMap.<String, String>builder()
                  .put("class", strategyClass)
                  .putAll(
                      SimpleJsonParser.parseStringMap(row.getString("compaction_strategy_options")))
                  .build());
        }
      } else if (name.equals("compression_parameters")) {
        // C* <=2.2, compression stored as a string
        String compressionParameters = row.getString(name);
        if (compressionParameters != null) {
          builder.put(
              CqlIdentifier.fromInternal("compression"),
              ImmutableMap.copyOf(SimpleJsonParser.parseStringMap(row.getString(name))));
        }
      } else if (!isDeprecatedInCassandra4(name)) {
        // Default case, read the value in a generic fashion
        Object value = row.get(name, codec);
        if (value != null) {
          builder.put(id, value);
        }
      }
    }
    return builder.build();
  }

  /**
   * Handle a few oddities in Cassandra 4: some options still appear in system_schema.tables, but
   * they are not valid in CREATE statements anymore. We need to exclude them from our metadata,
   * otherwise {@link Describable#describe(boolean)} will generate invalid CQL.
   */
  private boolean isDeprecatedInCassandra4(String name) {
    return isCassandra4OrAbove()
        && (name.equals("read_repair_chance")
            || name.equals("dclocal_read_repair_chance")
            // default_time_to_live is not allowed in CREATE MATERIALIZED VIEW statements
            || (name.equals("default_time_to_live") && (this instanceof ViewParser)));
  }

  private boolean isCassandra4OrAbove() {
    Node node = rows.getNode();
    return !node.getExtras().containsKey(DseNodeProperties.DSE_VERSION)
        && node.getCassandraVersion() != null
        && node.getCassandraVersion().nextStable().compareTo(Version.V4_0_0) >= 0;
  }

  public static void appendOptions(Map<CqlIdentifier, Object> options, ScriptBuilder builder) {
    for (Map.Entry<CqlIdentifier, Object> entry : options.entrySet()) {
      CqlIdentifier name = entry.getKey();
      Object value = entry.getValue();
      String formattedValue;
      if (name.asInternal().equals("caching") && value instanceof String) {
        formattedValue = TypeCodecs.TEXT.format((String) value);
      } else {
        @SuppressWarnings("unchecked")
        TypeCodec<Object> codec =
            (TypeCodec<Object>) RelationParser.OPTION_CODECS.get(name.asInternal());
        formattedValue = codec.format(value);
      }
      String optionName = name.asCql(true);
      if ("local_read_repair_chance".equals(optionName)) {
        // Another small quirk in C* <= 2.2
        optionName = "dclocal_read_repair_chance";
      }
      builder.andWith().append(optionName).append(" = ").append(formattedValue);
    }
  }

  public static final TypeCodec<Map<String, String>> MAP_OF_TEXT_TO_TEXT =
      TypeCodecs.mapOf(TypeCodecs.TEXT, TypeCodecs.TEXT);
  private static final TypeCodec<Map<String, ByteBuffer>> MAP_OF_TEXT_TO_BLOB =
      TypeCodecs.mapOf(TypeCodecs.TEXT, TypeCodecs.BLOB);
  /**
   * The columns of the system table that are turned into entries in {@link
   * RelationMetadata#getOptions()}.
   */
  public static final ImmutableMap<String, TypeCodec<?>> OPTION_CODECS =
      ImmutableMap.<String, TypeCodec<?>>builder()
          .put("additional_write_policy", TypeCodecs.TEXT)
          .put("bloom_filter_fp_chance", TypeCodecs.DOUBLE)
          // In C* <= 2.2, this is a string, not a map (this is special-cased in parseOptions):
          .put("caching", MAP_OF_TEXT_TO_TEXT)
          .put("cdc", TypeCodecs.BOOLEAN)
          .put("comment", TypeCodecs.TEXT)
          .put("compaction", MAP_OF_TEXT_TO_TEXT)
          // In C*<=2.2, must read from this column and another one called
          // 'compaction_strategy_options' (this is special-cased in parseOptions):
          .put("compaction_strategy_class", TypeCodecs.TEXT)
          .put("compression", MAP_OF_TEXT_TO_TEXT)
          // In C*<=2.2, must parse this column into a map (this is special-cased in parseOptions):
          .put("compression_parameters", TypeCodecs.TEXT)
          .put("crc_check_chance", TypeCodecs.DOUBLE)
          .put("dclocal_read_repair_chance", TypeCodecs.DOUBLE)
          .put("default_time_to_live", TypeCodecs.INT)
          .put("extensions", MAP_OF_TEXT_TO_BLOB)
          .put("gc_grace_seconds", TypeCodecs.INT)
          .put("local_read_repair_chance", TypeCodecs.DOUBLE)
          .put("max_index_interval", TypeCodecs.INT)
          .put("memtable_flush_period_in_ms", TypeCodecs.INT)
          .put("min_index_interval", TypeCodecs.INT)
          .put("read_repair", TypeCodecs.TEXT)
          .put("read_repair_chance", TypeCodecs.DOUBLE)
          .put("speculative_retry", TypeCodecs.TEXT)
          .build();
}
