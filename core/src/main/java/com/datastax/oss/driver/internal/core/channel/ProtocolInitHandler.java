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
package com.datastax.oss.driver.internal.core.channel;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.InvalidKeyspaceException;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.auth.Authenticator;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.ConnectionInitException;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.internal.core.DefaultProtocolFeature;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.protocol.BytesToSegmentDecoder;
import com.datastax.oss.driver.internal.core.protocol.FrameToSegmentEncoder;
import com.datastax.oss.driver.internal.core.protocol.SegmentToBytesEncoder;
import com.datastax.oss.driver.internal.core.protocol.SegmentToFrameDecoder;
import com.datastax.oss.driver.internal.core.util.ProtocolUtils;
import com.datastax.oss.driver.internal.core.util.concurrent.UncaughtExceptions;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode;
import com.datastax.oss.protocol.internal.request.AuthResponse;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Register;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.AuthChallenge;
import com.datastax.oss.protocol.internal.response.AuthSuccess;
import com.datastax.oss.protocol.internal.response.Authenticate;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.oss.protocol.internal.response.Supported;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.SetKeyspace;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import net.jcip.annotations.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the sequence of internal requests that we send on a channel before it's ready to accept
 * user requests.
 */
@NotThreadSafe
class ProtocolInitHandler extends ConnectInitHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ProtocolInitHandler.class);
  private static final Query CLUSTER_NAME_QUERY =
      new Query("SELECT cluster_name FROM system.local");

  private final InternalDriverContext context;
  private final long timeoutMillis;
  private final ProtocolVersion initialProtocolVersion;
  private final DriverChannelOptions options;
  // might be null if this is the first channel to this cluster
  private final String expectedClusterName;
  private final EndPoint endPoint;
  private final HeartbeatHandler heartbeatHandler;
  private String logPrefix;
  private ChannelHandlerContext ctx;
  private final boolean querySupportedOptions;

  /**
   * @param querySupportedOptions whether to send OPTIONS as the first message, to request which
   *     protocol options the channel supports. If this is true, the options will be stored as a
   *     channel attribute, and exposed via {@link DriverChannel#getOptions()}.
   */
  ProtocolInitHandler(
      InternalDriverContext context,
      ProtocolVersion protocolVersion,
      String expectedClusterName,
      EndPoint endPoint,
      DriverChannelOptions options,
      HeartbeatHandler heartbeatHandler,
      boolean querySupportedOptions) {

    this.context = context;
    this.endPoint = endPoint;

    DriverExecutionProfile defaultConfig = context.getConfig().getDefaultProfile();

    this.timeoutMillis =
        defaultConfig.getDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT).toMillis();
    this.initialProtocolVersion = protocolVersion;
    this.expectedClusterName = expectedClusterName;
    this.options = options;
    this.heartbeatHandler = heartbeatHandler;
    this.querySupportedOptions = querySupportedOptions;
    this.logPrefix = options.ownerLogPrefix + "|connecting...";
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    super.channelActive(ctx);
    String channelId = ctx.channel().toString();
    this.logPrefix = options.ownerLogPrefix + "|" + channelId.substring(1, channelId.length() - 1);
  }

  @Override
  protected void onRealConnect(ChannelHandlerContext ctx) {
    LOG.debug("[{}] Starting channel initialization", logPrefix);
    this.ctx = ctx;
    new InitRequest(ctx).send();
  }

  @Override
  protected boolean setConnectSuccess() {
    boolean result = super.setConnectSuccess();
    if (result) {
      // add heartbeat to pipeline now that protocol is initialized.
      ctx.pipeline()
          .addBefore(
              ChannelFactory.INFLIGHT_HANDLER_NAME,
              ChannelFactory.HEARTBEAT_HANDLER_NAME,
              heartbeatHandler);
    }
    return result;
  }

  private enum Step {
    OPTIONS,
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
    private int stepNumber = 0;
    private Message request;
    private Authenticator authenticator;
    private ByteBuffer authResponseToken;

    InitRequest(ChannelHandlerContext ctx) {
      super(ctx, timeoutMillis);
      this.step = querySupportedOptions ? Step.OPTIONS : Step.STARTUP;
    }

    @Override
    String describe() {
      return String.format(
          "[%s] Protocol initialization request, step %d (%s)", logPrefix, stepNumber, request);
    }

    @Override
    Message getRequest() {
      switch (step) {
        case OPTIONS:
          return request = Options.INSTANCE;
        case STARTUP:
          return request = new Startup(context.getStartupOptions());
        case GET_CLUSTER_NAME:
          return request = CLUSTER_NAME_QUERY;
        case SET_KEYSPACE:
          return request = new Query("USE " + options.keyspace.asCql(false));
        case AUTH_RESPONSE:
          return request = new AuthResponse(authResponseToken);
        case REGISTER:
          return request = new Register(options.eventTypes);
        default:
          throw new AssertionError("unhandled step: " + step);
      }
    }

    @Override
    void send() {
      stepNumber++;
      super.send();
    }

    @Override
    void onResponse(Message response) {
      LOG.debug(
          "[{}] step {} received response opcode={}",
          logPrefix,
          step,
          ProtocolUtils.opcodeString(response.opcode));
      try {
        if (step == Step.OPTIONS && response instanceof Supported) {
          channel.attr(DriverChannel.OPTIONS_KEY).set(((Supported) response).options);
          step = Step.STARTUP;
          send();
        } else if (step == Step.STARTUP && response instanceof Ready) {
          maybeSwitchToModernFraming();
          context.getAuthProvider().ifPresent(provider -> provider.onMissingChallenge(endPoint));
          step = Step.GET_CLUSTER_NAME;
          send();
        } else if (step == Step.STARTUP && response instanceof Authenticate) {
          maybeSwitchToModernFraming();
          Authenticate authenticate = (Authenticate) response;
          authenticator = buildAuthenticator(endPoint, authenticate.authenticator);
          authenticator
              .initialResponse()
              .whenCompleteAsync(
                  (token, error) -> {
                    if (error != null) {
                      fail(
                          new AuthenticationException(
                              endPoint,
                              String.format(
                                  "Authenticator.initialResponse(): stage completed exceptionally (%s)",
                                  error),
                              error));
                    } else {
                      step = Step.AUTH_RESPONSE;
                      authResponseToken = token;
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
                              endPoint,
                              String.format(
                                  "Authenticator.evaluateChallenge(): stage completed exceptionally (%s)",
                                  error),
                              error));
                    } else {
                      step = Step.AUTH_RESPONSE;
                      authResponseToken = token;
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
                              endPoint,
                              String.format(
                                  "Authenticator.onAuthenticationSuccess(): stage completed exceptionally (%s)",
                                  error),
                              error));
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
                  endPoint,
                  String.format(
                      "server replied with '%s' to AuthResponse request",
                      ((Error) response).message)));
        } else if (step == Step.GET_CLUSTER_NAME && response instanceof Rows) {
          Rows rows = (Rows) response;
          List<ByteBuffer> row = Objects.requireNonNull(rows.getData().poll());
          String actualClusterName = getString(row, 0);
          if (expectedClusterName != null && !expectedClusterName.equals(actualClusterName)) {
            fail(
                new ClusterNameMismatchException(endPoint, actualClusterName, expectedClusterName));
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
          // Testing for a specific string is a tad fragile but Cassandra doesn't give us a more
          // precise error code.
          // C* 2.1 reports a server error instead of protocol error, see CASSANDRA-9451.
          boolean firstRequest =
              (step == Step.OPTIONS && querySupportedOptions) || step == Step.STARTUP;
          boolean serverOrProtocolError =
              error.code == ErrorCode.PROTOCOL_ERROR || error.code == ErrorCode.SERVER_ERROR;
          boolean badProtocolVersionMessage =
              error.message.contains("Invalid or unsupported protocol version")
                  // JAVA-2925: server is behind driver and considers the proposed version as beta
                  || error.message.contains("Beta version of the protocol used");
          if (firstRequest && serverOrProtocolError && badProtocolVersionMessage) {
            fail(
                UnsupportedProtocolVersionException.forSingleAttempt(
                    endPoint, initialProtocolVersion));
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
      } catch (Throwable t) {
        fail(String.format("%s: unexpected exception (%s)", describe(), t), t);
      }
    }

    @Override
    void fail(String message, Throwable cause) {
      Throwable finalException =
          (message == null) ? cause : new ConnectionInitException(message, cause);
      setConnectFailure(finalException);
    }

    private Authenticator buildAuthenticator(EndPoint endPoint, String authenticator) {
      return context
          .getAuthProvider()
          .map(p -> p.newAuthenticator(endPoint, authenticator))
          .orElseThrow(
              () ->
                  new AuthenticationException(
                      endPoint,
                      String.format(
                          "Node %s requires authentication (%s), but no authenticator configured",
                          endPoint, authenticator)));
    }

    @Override
    public String toString() {
      return "init query " + step;
    }
  }

  /**
   * Rearranges the pipeline to deal with the new framing structure in protocol v5 and above. The
   * first messages still use the legacy format, we only do this after a successful response to the
   * first STARTUP message.
   */
  private void maybeSwitchToModernFraming() {
    if (context
        .getProtocolVersionRegistry()
        .supports(initialProtocolVersion, DefaultProtocolFeature.MODERN_FRAMING)) {

      ChannelPipeline pipeline = ctx.pipeline();

      // We basically add one conversion step in the middle: frames <-> *segments* <-> bytes
      // Outbound:
      pipeline.replace(
          ChannelFactory.FRAME_TO_BYTES_ENCODER_NAME,
          ChannelFactory.FRAME_TO_SEGMENT_ENCODER_NAME,
          new FrameToSegmentEncoder(
              context.getPrimitiveCodec(), context.getFrameCodec(), logPrefix));
      pipeline.addBefore(
          ChannelFactory.FRAME_TO_SEGMENT_ENCODER_NAME,
          ChannelFactory.SEGMENT_TO_BYTES_ENCODER_NAME,
          new SegmentToBytesEncoder(context.getSegmentCodec()));

      // Inbound:
      pipeline.replace(
          ChannelFactory.BYTES_TO_FRAME_DECODER_NAME,
          ChannelFactory.BYTES_TO_SEGMENT_DECODER_NAME,
          new BytesToSegmentDecoder(context.getSegmentCodec()));
      pipeline.addAfter(
          ChannelFactory.BYTES_TO_SEGMENT_DECODER_NAME,
          ChannelFactory.SEGMENT_TO_FRAME_DECODER_NAME,
          new SegmentToFrameDecoder(context.getFrameCodec(), logPrefix));
    }
  }

  private String getString(List<ByteBuffer> row, int i) {
    return TypeCodecs.TEXT.decode(row.get(i), DefaultProtocolVersion.DEFAULT);
  }
}
