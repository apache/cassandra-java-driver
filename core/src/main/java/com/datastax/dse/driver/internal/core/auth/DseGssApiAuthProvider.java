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
package com.datastax.dse.driver.internal.core.auth;

import com.datastax.dse.driver.api.core.auth.DseGssApiAuthProviderBase;
import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import net.jcip.annotations.ThreadSafe;

/**
 * {@link AuthProvider} that provides GSSAPI authenticator instances for clients to connect to DSE
 * clusters secured with {@code DseAuthenticator}.
 *
 * <p>To activate this provider an {@code auth-provider} section must be included in the driver
 * configuration, for example:
 *
 * <pre>
 * dse-java-driver {
 *  auth-provider {
 *      class = com.datastax.dse.driver.internal.core.auth.DseGssApiAuthProvider
 *      login-configuration {
 *          principal = "user principal here ex cassandra@DATASTAX.COM"
 *          useKeyTab = "true"
 *          refreshKrb5Config = "true"
 *          keyTab = "Path to keytab file here"
 *      }
 *   }
 * }
 * </pre>
 *
 * <h2>Kerberos Authentication</h2>
 *
 * Keytab and ticket cache settings are specified using a standard JAAS configuration file. The
 * location of the file can be set using the <code>java.security.auth.login.config</code> system
 * property or by adding a <code>login.config.url.n</code> entry in the <code>java.security</code>
 * properties file. Alternatively a login-configuration section can be included in the driver
 * configuration.
 *
 * <p>See the following documents for further details:
 *
 * <ol>
 *   <li><a
 *       href="https://docs.oracle.com/javase/6/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html">JAAS
 *       Login Configuration File</a>;
 *   <li><a
 *       href="https://docs.oracle.com/javase/6/docs/jre/api/security/jaas/spec/com/sun/security/auth/module/Krb5LoginModule.html">Krb5LoginModule
 *       options</a>;
 *   <li><a
 *       href="http://docs.oracle.com/javase/6/docs/technotes/guides/security/jaas/tutorials/GeneralAcnOnly.html">JAAS
 *       Authentication Tutorial</a> for more on JAAS in general.
 * </ol>
 *
 * <h3>Authentication using ticket cache</h3>
 *
 * Run <code>kinit</code> to obtain a ticket and populate the cache before connecting. JAAS config:
 *
 * <pre>
 * DseClient {
 *   com.sun.security.auth.module.Krb5LoginModule required
 *     useTicketCache=true
 *     renewTGT=true;
 * };
 * </pre>
 *
 * <h3>Authentication using a keytab file</h3>
 *
 * To enable authentication using a keytab file, specify its location on disk. If your keytab
 * contains more than one principal key, you should also specify which one to select. This
 * information can also be specified in the driver config, under the login-configuration section.
 *
 * <pre>
 * DseClient {
 *     com.sun.security.auth.module.Krb5LoginModule required
 *       useKeyTab=true
 *       keyTab="/path/to/file.keytab"
 *       principal="user@MYDOMAIN.COM";
 * };
 * </pre>
 *
 * <h2>Specifying SASL protocol name</h2>
 *
 * The SASL protocol name used by this auth provider defaults to "<code>
 * {@value #DEFAULT_SASL_SERVICE_NAME}</code>".
 *
 * <p><strong>Important</strong>: the SASL protocol name should match the username of the Kerberos
 * service principal used by the DSE server. This information is specified in the dse.yaml file by
 * the {@code service_principal} option under the <a
 * href="https://docs.datastax.com/en/dse/5.1/dse-admin/datastax_enterprise/config/configDseYaml.html#configDseYaml__refKerbSupport">kerberos_options</a>
 * section, and <em>may vary from one DSE installation to another</em> – especially if you installed
 * DSE with an automated package installer.
 *
 * <p>For example, if your dse.yaml file contains the following:
 *
 * <pre>{@code
 * kerberos_options:
 *     ...
 *     service_principal: cassandra/my.host.com@MY.REALM.COM
 * }</pre>
 *
 * The correct SASL protocol name to use when authenticating against this DSE server is "{@code
 * cassandra}".
 *
 * <p>Should you need to change the SASL protocol name, use one of the methods below:
 *
 * <ol>
 *   <li>Specify the service name in the driver config.
 *       <pre>
 * dse-java-driver {
 *   auth-provider {
 *     class = com.datastax.dse.driver.internal.core.auth.DseGssApiAuthProvider
 *     service = "alternate"
 *   }
 * }
 * </pre>
 *   <li>Specify the service name with the {@code dse.sasl.service} system property when starting
 *       your application, e.g. {@code -Ddse.sasl.service=cassandra}.
 * </ol>
 *
 * If a non-null SASL service name is provided to the aforementioned config, that name takes
 * precedence over the contents of the {@code dse.sasl.service} system property.
 *
 * <p>Should internal sasl properties need to be set such as qop. This can be accomplished by
 * including a sasl-properties in the driver config, for example:
 *
 * <pre>
 * dse-java-driver {
 *   auth-provider {
 *     class = com.datastax.dse.driver.internal.core.auth.DseGssApiAuthProvider
 *     sasl-properties {
 *       javax.security.sasl.qop = "auth-conf"
 *     }
 *   }
 * }
 * </pre>
 */
@ThreadSafe
public class DseGssApiAuthProvider extends DseGssApiAuthProviderBase {

  private final DriverExecutionProfile config;

  public DseGssApiAuthProvider(DriverContext context) {
    super(context.getSessionName());

    this.config = context.getConfig().getDefaultProfile();
  }

  @NonNull
  @Override
  protected GssApiOptions getOptions(
      @NonNull EndPoint endPoint, @NonNull String serverAuthenticator) {
    // A login configuration is always necessary, throw an exception if that option is missing.
    AuthUtils.validateConfigPresent(
        config,
        DseGssApiAuthProvider.class.getName(),
        endPoint,
        DseDriverOption.AUTH_PROVIDER_LOGIN_CONFIGURATION);

    GssApiOptions.Builder optionsBuilder = GssApiOptions.builder();

    if (config.isDefined(DseDriverOption.AUTH_PROVIDER_AUTHORIZATION_ID)) {
      optionsBuilder.withAuthorizationId(
          config.getString(DseDriverOption.AUTH_PROVIDER_AUTHORIZATION_ID));
    }
    if (config.isDefined(DseDriverOption.AUTH_PROVIDER_SERVICE)) {
      optionsBuilder.withSaslProtocol(config.getString(DseDriverOption.AUTH_PROVIDER_SERVICE));
    }
    if (config.isDefined(DseDriverOption.AUTH_PROVIDER_SASL_PROPERTIES)) {
      for (Map.Entry<String, String> entry :
          config.getStringMap(DseDriverOption.AUTH_PROVIDER_SASL_PROPERTIES).entrySet()) {
        optionsBuilder.addSaslProperty(entry.getKey(), entry.getValue());
      }
    }
    Map<String, String> loginConfigurationMap =
        config.getStringMap(DseDriverOption.AUTH_PROVIDER_LOGIN_CONFIGURATION);
    optionsBuilder.withLoginConfiguration(loginConfigurationMap);
    return optionsBuilder.build();
  }
}
