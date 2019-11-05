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
package com.datastax.dse.driver.api.core;

import com.datastax.dse.driver.api.core.session.DseProgrammaticArguments;
import com.datastax.dse.driver.api.core.type.codec.DseTypeCodecs;
import com.datastax.dse.driver.internal.core.auth.DseProgrammaticPlainTextAuthProvider;
import com.datastax.dse.driver.internal.core.config.typesafe.DefaultDseDriverConfigLoader;
import com.datastax.dse.driver.internal.core.context.DseDriverContext;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.util.Loggers;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import net.jcip.annotations.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public abstract class DseSessionBuilderBase<
        SelfT extends DseSessionBuilderBase<SelfT, SessionT>, SessionT>
    extends SessionBuilder<SelfT, SessionT> {

  private static final Logger LOG = LoggerFactory.getLogger(DseSessionBuilderBase.class);

  protected DseProgrammaticArguments.Builder dseProgrammaticArgumentsBuilder =
      DseProgrammaticArguments.builder();

  protected DseSessionBuilderBase() {
    try {
      Class.forName("com.esri.core.geometry.ogc.OGCGeometry");
      programmaticArgumentsBuilder.addTypeCodecs(
          DseTypeCodecs.LINE_STRING,
          DseTypeCodecs.POINT,
          DseTypeCodecs.POLYGON,
          DseTypeCodecs.DATE_RANGE);
    } catch (ClassNotFoundException | LinkageError error) {
      Loggers.warnWithException(
          LOG, "Could not register Geo codecs; ESRI API might be missing from classpath", error);
    }
  }

  /**
   * Sets the configuration loader to use.
   *
   * <p>Note that this loader must produce a configuration that includes the DSE-specific options:
   * if you're using one of the built-in implementations provided by the driver, use the static
   * factory methods from {@link DriverConfigLoader}.
   *
   * <p>If you don't call this method, the builder will use the default implementation, based on the
   * Typesafe config library. More precisely, configuration properties are loaded and merged from
   * the following (first-listed are higher priority):
   *
   * <ul>
   *   <li>system properties
   *   <li>{@code application.conf} (all resources on classpath with this name)
   *   <li>{@code application.json} (all resources on classpath with this name)
   *   <li>{@code application.properties} (all resources on classpath with this name)
   *   <li>{@code dse-reference.conf} (all resources on classpath with this name). In particular,
   *       this will load the {@code dse-reference.conf} included in the core DSE driver JAR, that
   *       defines default options for all DSE-specific mandatory options.
   *   <li>{@code reference.conf} (all resources on classpath with this name). In particular, this
   *       will load the {@code reference.conf} included in the core driver JAR, that defines
   *       default options for all mandatory options.
   * </ul>
   *
   * The resulting configuration is expected to contain a {@code datastax-java-driver} section.
   *
   * <p>This default loader will honor the reload interval defined by the option {@code
   * basic.config-reload-interval}.
   *
   * @see <a href="https://github.com/typesafehub/config#standard-behavior">Typesafe config's
   *     standard loading behavior</a>
   */
  @NonNull
  @Override
  public SelfT withConfigLoader(@Nullable DriverConfigLoader configLoader) {
    // overridden only to customize the javadocs
    return super.withConfigLoader(configLoader);
  }

  /**
   * Configures the session to use DSE plaintext authentication with the given username and
   * password.
   *
   * <p>This methods calls {@link #withAuthProvider(AuthProvider)} to register a special provider
   * implementation. Therefore calling it overrides the configuration (that is, the {@code
   * advanced.auth-provider.class} option will be ignored).
   *
   * <p>Note that this approach holds the credentials in clear text in memory, which makes them
   * vulnerable to an attacker who is able to perform memory dumps. If this is not acceptable for
   * you, consider writing your own {@link AuthProvider} implementation (the internal class {@code
   * PlainTextAuthProviderBase} is a good starting point), and providing it either with {@link
   * #withAuthProvider(AuthProvider)} or via the configuration ({@code
   * advanced.auth-provider.class}).
   */
  @NonNull
  @Override
  public SelfT withAuthCredentials(@NonNull String username, @NonNull String password) {
    return withAuthCredentials(username, password, "");
  }

  /**
   * Configures the session to use DSE plaintext authentication with the given username and
   * password, and perform proxy authentication with the given authorization id.
   *
   * <p>This methods calls {@link #withAuthProvider(AuthProvider)} to register a special provider
   * implementation. Therefore calling it overrides the configuration (that is, the {@code
   * advanced.auth-provider.class} option will be ignored).
   *
   * <p>Note that this approach holds the credentials in clear text in memory, which makes them
   * vulnerable to an attacker who is able to perform memory dumps. If this is not acceptable for
   * you, consider writing your own {@link AuthProvider} implementation (the internal class {@code
   * PlainTextAuthProviderBase} is a good starting point), and providing it either with {@link
   * #withAuthProvider(AuthProvider)} or via the configuration ({@code
   * advanced.auth-provider.class}).
   */
  @NonNull
  public SelfT withAuthCredentials(
      @NonNull String username, @NonNull String password, @NonNull String authorizationId) {
    return withAuthProvider(
        new DseProgrammaticPlainTextAuthProvider(username, password, authorizationId));
  }

  @Override
  protected DriverContext buildContext(
      DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {

    // Preserve backward compatibility with the deprecated method:
    @SuppressWarnings("deprecation")
    DriverContext legacyApiContext =
        buildContext(
            configLoader,
            programmaticArguments.getTypeCodecs(),
            programmaticArguments.getNodeStateListener(),
            programmaticArguments.getSchemaChangeListener(),
            programmaticArguments.getRequestTracker(),
            programmaticArguments.getLocalDatacenters(),
            programmaticArguments.getNodeFilters(),
            programmaticArguments.getClassLoader());
    if (legacyApiContext != null) {
      return legacyApiContext;
    }

    return new DseDriverContext(
        configLoader, programmaticArguments, dseProgrammaticArgumentsBuilder.build());
  }

  @Deprecated
  @Override
  protected DriverContext buildContext(
      DriverConfigLoader configLoader,
      List<TypeCodec<?>> typeCodecs,
      NodeStateListener nodeStateListener,
      SchemaChangeListener schemaChangeListener,
      RequestTracker requestTracker,
      Map<String, String> localDatacenters,
      Map<String, Predicate<Node>> nodeFilters,
      ClassLoader classLoader) {
    return super.buildContext(
        configLoader,
        typeCodecs,
        nodeStateListener,
        schemaChangeListener,
        requestTracker,
        localDatacenters,
        nodeFilters,
        classLoader);
  }

  @NonNull
  @Override
  protected DriverConfigLoader defaultConfigLoader() {
    return new DefaultDseDriverConfigLoader();
  }
}
