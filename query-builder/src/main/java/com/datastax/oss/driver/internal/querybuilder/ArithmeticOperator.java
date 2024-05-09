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
package com.datastax.oss.driver.internal.querybuilder;

import edu.umd.cs.findbugs.annotations.NonNull;

public enum ArithmeticOperator {
  OPPOSITE("-", 2, 2),
  PRODUCT("*", 2, 2),
  QUOTIENT("/", 2, 3),
  REMAINDER("%", 2, 3),
  SUM("+", 1, 1),
  DIFFERENCE("-", 1, 2),
  ;

  private final String symbol;
  private final int precedenceLeft;
  private final int precedenceRight;

  ArithmeticOperator(String symbol, int precedenceLeft, int precedenceRight) {
    this.symbol = symbol;
    this.precedenceLeft = precedenceLeft;
    this.precedenceRight = precedenceRight;
  }

  @NonNull
  public String getSymbol() {
    return symbol;
  }

  public int getPrecedenceLeft() {
    return precedenceLeft;
  }

  public int getPrecedenceRight() {
    return precedenceRight;
  }
}
