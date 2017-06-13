/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.core.channel;

import com.datastax.oss.driver.api.core.InvalidKeyspaceException;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.auth.Authenticator;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.connection.ConnectionInitException;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.util.ProtocolUtils;
import com.datastax.oss.driver.internal.core.util.concurrent.UncaughtExceptions;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.AuthResponse;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Register;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.AuthChallenge;
import com.datastax.oss.protocol.internal.response.AuthSuccess;
import com.datastax.oss.protocol.internal.response.Authenticate;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.SetKeyspace;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.google.common.base.Charsets;
import io.netty.channel.ChannelHandlerContext;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the sequence of internal requests that we send on a channel before it's ready to accept
 * user requests.
 */
class ProtocolInitHandler extends ConnectInitHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ProtocolInitHandler.class);
  private static final Query CLUSTER_NAME_QUERY =
      new Query("SELECT cluster_name FROM system.local");

  private final InternalDriverContext internalDriverContext;
  private final long timeoutMillis;
  private final ProtocolVersion initialProtocolVersion;
  private final DriverChannelOptions options;
  // might be null if this is the first channel to this cluster
  private final String expectedClusterName;

  ProtocolInitHandler(
      InternalDriverContext internalDriverContext,
      ProtocolVersion protocolVersion,
      String expectedClusterName,
      DriverChannelOptions options) {

    this.internalDriverContext = internalDriverContext;

    DriverConfigProfile defaultConfig = internalDriverContext.config().defaultProfile();

    this.timeoutMillis =
        defaultConfig.getDuration(CoreDriverOption.CONNECTION_INIT_QUERY_TIMEOUT).toMillis();
    this.initialProtocolVersion = protocolVersion;
    this.expectedClusterName = expectedClusterName;
    this.options = options;
  }

  @Override
  protected void onRealConnect(ChannelHandlerContext ctx) {
    new InitRequest(ctx).send();
  }

  private enum Step {
    STARTUP,
    GET_CLUSTER_NAME,
    SET_KEYSPACE,
    AUTH_RESPONSE,
    REGISTER,
  }

  private class InitRequest extends ChannelHandlerRequest {
    // This class is a finite-state automaton, that sends a different query depending on the step
    // in the initialization sequence.
    private Step step;
    private Authenticator authenticator;
    private ByteBuffer authReponseToken;

    InitRequest(ChannelHandlerContext ctx) {
      super(ctx, timeoutMillis);
      this.step = Step.STARTUP;
    }

    @Override
    String describe() {
      return "init query " + step;
    }

    @Override
    Message getRequest() {
      switch (step) {
        case STARTUP:
          return new Startup();
        case GET_CLUSTER_NAME:
          return CLUSTER_NAME_QUERY;
        case SET_KEYSPACE:
          return new Query("USE " + options.keyspace.asCql());
        case AUTH_RESPONSE:
          return new AuthResponse(authReponseToken);
        case REGISTER:
          return new Register(options.eventTypes);
        default:
          throw new AssertionError("unhandled step: " + step);
      }
    }

    @Override
    void onResponse(Message response) {
      LOG.trace(
          "[{} {}] received response opcode={}",
          channel,
          step,
          ProtocolUtils.opcodeString(response.opcode));
      try {
        if (step == Step.STARTUP && response instanceof Ready) {
          step = Step.GET_CLUSTER_NAME;
          send();
        } else if (step == Step.STARTUP && response instanceof Authenticate) {
          Authenticate authenticate = (Authenticate) response;
          authenticator = buildAuthenticator(channel.remoteAddress(), authenticate.authenticator);
          authenticator
              .initialResponse()
              .whenCompleteAsync(
                  (token, error) -> {
                    if (error != null) {
                      fail(
                          new AuthenticationException(
                              channel.remoteAddress(), "authenticator threw an exception", error));
                    } else {
                      step = Step.AUTH_RESPONSE;
                      authReponseToken = token;
                      send();
                    }
                  },
                  channel.eventLoop())
              .exceptionally(UncaughtExceptions::log);
        } else if (step == Step.AUTH_RESPONSE && response instanceof AuthChallenge) {
          ByteBuffer challenge = ((AuthChallenge) response).token;
          authenticator
              .evaluateChallenge(challenge)
              .whenCompleteAsync(
                  (token, error) -> {
                    if (error != null) {
                      fail(
                          new AuthenticationException(
                              channel.remoteAddress(), "authenticator threw an exception", error));
                    } else {
                      step = Step.AUTH_RESPONSE;
                      authReponseToken = token;
                      send();
                    }
                  },
                  channel.eventLoop())
              .exceptionally(UncaughtExceptions::log);
        } else if (step == Step.AUTH_RESPONSE && response instanceof AuthSuccess) {
          ByteBuffer token = ((AuthSuccess) response).token;
          authenticator
              .onAuthenticationSuccess(token)
              .whenCompleteAsync(
                  (ignored, error) -> {
                    if (error != null) {
                      fail(
                          new AuthenticationException(
                              channel.remoteAddress(), "authenticator threw an exception", error));
                    } else {
                      step = Step.GET_CLUSTER_NAME;
                      send();
                    }
                  },
                  channel.eventLoop())
              .exceptionally(UncaughtExceptions::log);
        } else if (step == Step.AUTH_RESPONSE
            && response instanceof Error
            && ((Error) response).code == ProtocolConstants.ErrorCode.AUTH_ERROR) {
          fail(
              new AuthenticationException(
                  channel.remoteAddress(),
                  String.format("server replied '%s'", ((Error) response).message)));
        } else if (step == Step.GET_CLUSTER_NAME && response instanceof Rows) {
          Rows rows = (Rows) response;
          List<ByteBuffer> row = rows.data.poll();
          String actualClusterName = getString(row, 0);
          if (expectedClusterName != null && !expectedClusterName.equals(actualClusterName)) {
            fail(
                new ClusterNameMismatchException(
                    channel.remoteAddress(), actualClusterName, expectedClusterName));
          } else {
            if (expectedClusterName == null) {
              // Store the actual name so that it can be retrieved from the factory
              channel.attr(DriverChannel.CLUSTER_NAME_KEY).set(actualClusterName);
            }
            if (options.keyspace != null) {
              step = Step.SET_KEYSPACE;
              send();
            } else if (!options.eventTypes.isEmpty()) {
              step = Step.REGISTER;
              send();
            } else {
              setConnectSuccess();
            }
          }
        } else if (step == Step.SET_KEYSPACE && response instanceof SetKeyspace) {
          if (!options.eventTypes.isEmpty()) {
            step = Step.REGISTER;
            send();
          } else {
            setConnectSuccess();
          }
        } else if (step == Step.REGISTER && response instanceof Ready) {
          setConnectSuccess();
        } else if (response instanceof Error) {
          Error error = (Error) response;
          // Testing for a specific string is a tad fragile but Cassandra doesn't give us a more precise error
          // code.
          // C* 2.1 reports a server error instead of protocol error, see CASSANDRA-9451.
          if (step == Step.STARTUP
              && (error.code == ProtocolConstants.ErrorCode.PROTOCOL_ERROR
                  || error.code == ProtocolConstants.ErrorCode.SERVER_ERROR)
              && error.message.contains("Invalid or unsupported protocol version")) {
            fail(
                UnsupportedProtocolVersionException.forSingleAttempt(
                    channel.remoteAddress(), initialProtocolVersion));
          } else if (step == Step.SET_KEYSPACE
              && error.code == ProtocolConstants.ErrorCode.INVALID) {
            fail(new InvalidKeyspaceException(error.message));
          } else {
            failOnUnexpected(error);
          }
        } else {
          failOnUnexpected(response);
        }
      } catch (AuthenticationException e) {
        fail(e);
      }
    }

    @Override
    void fail(String message, Throwable cause) {
      Throwable finalException =
          (message == null) ? cause : new ConnectionInitException(message, cause);
      setConnectFailure(finalException);
    }

    private Authenticator buildAuthenticator(SocketAddress address, String authenticator) {
      return internalDriverContext
          .authProvider()
          .map(p -> p.newAuthenticator(address, authenticator))
          .orElseThrow(
              () ->
                  new AuthenticationException(
                      address,
                      String.format(
                          "Host %s requires authentication (%s), but no authenticator configured",
                          address, authenticator)));
    }
  }

  // TODO we'll probably need a lightweight ResultSet implementation for internal uses, but this is good for now
  private String getString(List<ByteBuffer> row, int i) {
    ByteBuffer bytes = row.get(i);
    if (bytes == null) {
      return null;
    } else if (bytes.remaining() == 0) {
      return "";
    } else {
      return new String(Bytes.getArray(bytes), Charsets.UTF_8);
    }
  }
}
