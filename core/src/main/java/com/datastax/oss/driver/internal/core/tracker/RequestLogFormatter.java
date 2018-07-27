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
package com.datastax.oss.driver.internal.core.tracker;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.util.NanoTime;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class RequestLogFormatter {

  private static final String FURTHER_VALUES_TRUNCATED = "...<further values truncated>]";
  private static final String TRUNCATED = "...<truncated>";

  private final DriverContext context;

  public RequestLogFormatter(DriverContext context) {
    this.context = context;
  }

  public StringBuilder logBuilder(String logPrefix, Node node) {
    return new StringBuilder("[").append(logPrefix).append("][").append(node).append("] ");
  }

  public void appendSuccessDescription(StringBuilder builder) {
    builder.append("Success ");
  }

  public void appendSlowDescription(StringBuilder builder) {
    builder.append("Slow ");
  }

  public void appendErrorDescription(StringBuilder builder) {
    builder.append("Error ");
  }

  public void appendLatency(long latencyNanos, StringBuilder builder) {
    builder.append('(').append(NanoTime.format(latencyNanos)).append(") ");
  }

  public void appendRequest(
      Request request,
      int maxQueryLength,
      boolean showValues,
      int maxValues,
      int maxValueLength,
      StringBuilder builder) {
    appendStats(request, builder);
    appendQueryString(request, maxQueryLength, builder);
    if (showValues) {
      appendValues(request, maxValues, maxValueLength, true, builder);
    }
  }

  protected void appendStats(Request request, StringBuilder builder) {
    int valueCount = countBoundValues(request);
    if (request instanceof BatchStatement) {
      BatchStatement statement = (BatchStatement) request;
      builder
          .append('[')
          .append(statement.size())
          .append(" statements, ")
          .append(valueCount)
          .append(" values] ");
    } else {
      builder.append('[').append(valueCount).append(" values] ");
    }
  }

  protected int countBoundValues(Request request) {
    if (request instanceof BatchStatement) {
      int count = 0;
      for (BatchableStatement<?> child : (BatchStatement) request) {
        count += countBoundValues(child);
      }
      return count;
    } else if (request instanceof BoundStatement) {
      return ((BoundStatement) request).getPreparedStatement().getVariableDefinitions().size();
    } else if (request instanceof SimpleStatement) {
      SimpleStatement statement = (SimpleStatement) request;
      return Math.max(statement.getPositionalValues().size(), statement.getNamedValues().size());
    } else {
      return 0;
    }
  }

  protected int appendQueryString(Request request, int limit, StringBuilder builder) {
    if (request instanceof BatchStatement) {
      BatchStatement batch = (BatchStatement) request;
      limit = append("BEGIN", limit, builder);
      if (batch.getBatchType() == DefaultBatchType.UNLOGGED) {
        limit = append(" UNLOGGED", limit, builder);
      } else if (batch.getBatchType() == DefaultBatchType.COUNTER) {
        limit = append(" COUNTER", limit, builder);
      }
      limit = append(" BATCH ", limit, builder);
      for (BatchableStatement<?> child : batch) {
        limit = appendQueryString(child, limit, builder);
        if (limit < 0) {
          break;
        }
        limit = append("; ", limit, builder);
      }
      limit = append("APPLY BATCH", limit, builder);
      return limit;
    } else if (request instanceof BoundStatement) {
      BoundStatement statement = (BoundStatement) request;
      return append(statement.getPreparedStatement().getQuery(), limit, builder);
    } else if (request instanceof SimpleStatement) {
      SimpleStatement statement = (SimpleStatement) request;
      return append(statement.getQuery(), limit, builder);
    } else {
      return append(request.toString(), limit, builder);
    }
  }

  /**
   * @return the number of values that can still be appended after this, or -1 if the max was
   *     reached by this call.
   */
  protected int appendValues(
      Request request,
      int maxValues,
      int maxValueLength,
      boolean addSeparator,
      StringBuilder builder) {
    if (request instanceof BatchStatement) {
      BatchStatement batch = (BatchStatement) request;
      for (BatchableStatement<?> child : batch) {
        maxValues = appendValues(child, maxValues, maxValueLength, addSeparator, builder);
        if (addSeparator) {
          addSeparator = false;
        }
        if (maxValues < 0) {
          return -1;
        }
      }
    } else if (request instanceof BoundStatement) {
      BoundStatement statement = (BoundStatement) request;
      ColumnDefinitions definitions = statement.getPreparedStatement().getVariableDefinitions();
      List<ByteBuffer> values = statement.getValues();
      assert definitions.size() == values.size();
      if (definitions.size() > 0) {
        if (addSeparator) {
          builder.append(' ');
        }
        builder.append('[');
        for (int i = 0; i < definitions.size(); i++) {
          if (i > 0) {
            builder.append(", ");
          }
          maxValues -= 1;
          if (maxValues < 0) {
            builder.append(FURTHER_VALUES_TRUNCATED);
            return -1;
          }
          builder.append(definitions.get(i).getName().asCql(true)).append('=');
          if (!statement.isSet(i)) {
            builder.append("<UNSET>");
          } else {
            ByteBuffer value = values.get(i);
            DataType type = definitions.get(i).getType();
            appendValue(value, type, maxValueLength, builder);
          }
        }
        builder.append(']');
      }
    } else if (request instanceof SimpleStatement) {
      SimpleStatement statement = (SimpleStatement) request;
      if (!statement.getPositionalValues().isEmpty()) {
        if (addSeparator) {
          builder.append(' ');
        }
        builder.append('[');
        int i = 0;
        for (Object value : statement.getPositionalValues()) {
          if (i > 0) {
            builder.append(", ");
          }
          maxValues -= 1;
          if (maxValues < 0) {
            builder.append(FURTHER_VALUES_TRUNCATED);
            return -1;
          }
          builder.append('v').append(i).append('=');
          appendValue(value, maxValueLength, builder);
          i += 1;
        }
        builder.append(']');
      } else if (!statement.getNamedValues().isEmpty()) {
        if (addSeparator) {
          builder.append(' ');
        }
        builder.append('[');
        int i = 0;
        for (Map.Entry<CqlIdentifier, Object> entry : statement.getNamedValues().entrySet()) {
          if (i > 0) {
            builder.append(", ");
          }
          maxValues -= 1;
          if (maxValues < 0) {
            builder.append(FURTHER_VALUES_TRUNCATED);
            return -1;
          }
          builder.append(entry.getKey().asCql(true)).append('=');
          appendValue(entry.getValue(), maxValueLength, builder);
          i += 1;
        }
        builder.append(']');
      }
    }
    return maxValues;
  }

  protected void appendValue(ByteBuffer raw, DataType type, int maxLength, StringBuilder builder) {
    TypeCodec<Object> codec = context.getCodecRegistry().codecFor(type);
    if (type.equals(DataTypes.BLOB)) {
      // For very large buffers, apply the limit before converting into a string
      int maxBufferLength = Math.max((maxLength - 2) / 2, 0);
      boolean bufferTooLarge = raw.remaining() > maxBufferLength;
      if (bufferTooLarge) {
        raw = (ByteBuffer) raw.duplicate().limit(maxBufferLength);
      }
      Object value = codec.decode(raw, context.getProtocolVersion());
      append(codec.format(value), maxLength, builder);
      if (bufferTooLarge) {
        builder.append(TRUNCATED);
      }
    } else {
      Object value = codec.decode(raw, context.getProtocolVersion());
      append(codec.format(value), maxLength, builder);
    }
  }

  protected void appendValue(Object value, int maxLength, StringBuilder builder) {
    TypeCodec<Object> codec = context.getCodecRegistry().codecFor(value);
    if (value instanceof ByteBuffer) {
      // For very large buffers, apply the limit before converting into a string
      ByteBuffer buffer = (ByteBuffer) value;
      int maxBufferLength = Math.max((maxLength - 2) / 2, 0);
      boolean bufferTooLarge = buffer.remaining() > maxBufferLength;
      if (bufferTooLarge) {
        buffer = (ByteBuffer) buffer.duplicate().limit(maxBufferLength);
      }
      append(codec.format(buffer), maxLength, builder);
      if (bufferTooLarge) {
        builder.append(TRUNCATED);
      }
    } else {
      append(codec.format(value), maxLength, builder);
    }
  }

  /**
   * @return the number of characters that can still be appended after this, or -1 if this call hit
   *     the limit.
   */
  protected int append(String value, int limit, StringBuilder builder) {
    if (limit < 0) {
      // Small simplification to avoid having to check the limit every time when we do a sequence of
      // simple calls, like BEGIN... UNLOGGED... BATCH. If the first call hits the limit, the next
      // ones will be ignored.
      return limit;
    } else if (value.length() <= limit) {
      builder.append(value);
      return limit - value.length();
    } else {
      builder.append(value.substring(0, limit)).append(TRUNCATED);
      return -1;
    }
  }
}
