package com.datastax.oss.driver.api.core.metadata;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;

/** Simple keyspace name and table name pair. */
public class KeyspaceTableNamePair {
  @NonNull private final CqlIdentifier keyspace;
  @NonNull private final CqlIdentifier tableName;

  public KeyspaceTableNamePair(@NonNull CqlIdentifier keyspace, @NonNull CqlIdentifier tableName) {
    this.keyspace = keyspace;
    this.tableName = tableName;
  }

  @NonNull
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @NonNull
  public CqlIdentifier getTableName() {
    return tableName;
  }

  @Override
  public String toString() {
    return "KeyspaceTableNamePair{"
        + "keyspace='"
        + keyspace
        + '\''
        + ", tableName='"
        + tableName
        + '\''
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || !(o instanceof KeyspaceTableNamePair)) return false;
    KeyspaceTableNamePair that = (KeyspaceTableNamePair) o;
    return keyspace.equals(that.keyspace) && tableName.equals(that.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyspace, tableName);
  }
}
