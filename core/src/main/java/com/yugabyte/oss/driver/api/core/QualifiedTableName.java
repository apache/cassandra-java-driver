// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//
package com.yugabyte.oss.driver.api.core;

/**
 * The qualified name of a table, which includes the name of the containing keyspace in addition to
 * the name of the table.
 */
public class QualifiedTableName {

  private final String keyspaceName;
  private final String tableName;

  /** Creates a new {@code QualifiedTableName}. */
  public QualifiedTableName(String keyspaceName, String tableName) {
    this.keyspaceName = keyspaceName;
    this.tableName = tableName;
  }

  /**
   * Return the keyspace name for this table.
   *
   * @return the keyspace name
   */
  public String getKeyspaceName() {
    return keyspaceName;
  }

  /**
   * Return the (unqualified) name of this table.
   *
   * @return the table name
   */
  public String getTableName() {
    return tableName;
  }

  @Override
  public String toString() {
    return keyspaceName + "." + tableName;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof QualifiedTableName) {
      QualifiedTableName otherQualifiedTableName = (QualifiedTableName) other;
      return keyspaceName.equals(otherQualifiedTableName.keyspaceName)
          && tableName.equals(otherQualifiedTableName.tableName);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }
}
