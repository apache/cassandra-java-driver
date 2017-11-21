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
package com.datastax.oss.driver.internal.core.util;

import com.datastax.oss.protocol.internal.ProtocolConstants;

public class ProtocolUtils {
  /**
   * Formats a message opcode for logs and error messages.
   *
   * <p>Note that the reason why we don't use enums is because the driver can be extended with
   * custom opcodes.
   */
  public static String opcodeString(int opcode) {
    switch (opcode) {
      case ProtocolConstants.Opcode.ERROR:
        return "ERROR";
      case ProtocolConstants.Opcode.STARTUP:
        return "STARTUP";
      case ProtocolConstants.Opcode.READY:
        return "READY";
      case ProtocolConstants.Opcode.AUTHENTICATE:
        return "AUTHENTICATE";
      case ProtocolConstants.Opcode.OPTIONS:
        return "OPTIONS";
      case ProtocolConstants.Opcode.SUPPORTED:
        return "SUPPORTED";
      case ProtocolConstants.Opcode.QUERY:
        return "QUERY";
      case ProtocolConstants.Opcode.RESULT:
        return "RESULT";
      case ProtocolConstants.Opcode.PREPARE:
        return "PREPARE";
      case ProtocolConstants.Opcode.EXECUTE:
        return "EXECUTE";
      case ProtocolConstants.Opcode.REGISTER:
        return "REGISTER";
      case ProtocolConstants.Opcode.EVENT:
        return "EVENT";
      case ProtocolConstants.Opcode.BATCH:
        return "BATCH";
      case ProtocolConstants.Opcode.AUTH_CHALLENGE:
        return "AUTH_CHALLENGE";
      case ProtocolConstants.Opcode.AUTH_RESPONSE:
        return "AUTH_RESPONSE";
      case ProtocolConstants.Opcode.AUTH_SUCCESS:
        return "AUTH_SUCCESS";
      default:
        return "0x" + Integer.toHexString(opcode);
    }
  }

  /**
   * Formats an error code for logs and error messages.
   *
   * <p>Note that the reason why we don't use enums is because the driver can be extended with
   * custom codes.
   */
  public static String errorCodeString(int errorCode) {
    switch (errorCode) {
      case ProtocolConstants.ErrorCode.SERVER_ERROR:
        return "SERVER_ERROR";
      case ProtocolConstants.ErrorCode.PROTOCOL_ERROR:
        return "PROTOCOL_ERROR";
      case ProtocolConstants.ErrorCode.AUTH_ERROR:
        return "AUTH_ERROR";
      case ProtocolConstants.ErrorCode.UNAVAILABLE:
        return "UNAVAILABLE";
      case ProtocolConstants.ErrorCode.OVERLOADED:
        return "OVERLOADED";
      case ProtocolConstants.ErrorCode.IS_BOOTSTRAPPING:
        return "IS_BOOTSTRAPPING";
      case ProtocolConstants.ErrorCode.TRUNCATE_ERROR:
        return "TRUNCATE_ERROR";
      case ProtocolConstants.ErrorCode.WRITE_TIMEOUT:
        return "WRITE_TIMEOUT";
      case ProtocolConstants.ErrorCode.READ_TIMEOUT:
        return "READ_TIMEOUT";
      case ProtocolConstants.ErrorCode.READ_FAILURE:
        return "READ_FAILURE";
      case ProtocolConstants.ErrorCode.FUNCTION_FAILURE:
        return "FUNCTION_FAILURE";
      case ProtocolConstants.ErrorCode.WRITE_FAILURE:
        return "WRITE_FAILURE";
      case ProtocolConstants.ErrorCode.SYNTAX_ERROR:
        return "SYNTAX_ERROR";
      case ProtocolConstants.ErrorCode.UNAUTHORIZED:
        return "UNAUTHORIZED";
      case ProtocolConstants.ErrorCode.INVALID:
        return "INVALID";
      case ProtocolConstants.ErrorCode.CONFIG_ERROR:
        return "CONFIG_ERROR";
      case ProtocolConstants.ErrorCode.ALREADY_EXISTS:
        return "ALREADY_EXISTS";
      case ProtocolConstants.ErrorCode.UNPREPARED:
        return "UNPREPARED";
      default:
        return "0x" + Integer.toHexString(errorCode);
    }
  }
}
