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
package com.datastax.dse.driver.api.core.auth;

import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * {@link AuthProvider} that provides GSSAPI authenticator instances for clients to connect to DSE
 * clusters secured with {@code DseAuthenticator}, in a programmatic way.
 *
 * <p>To use this provider the corresponding GssApiOptions must be passed into the provider
 * directly, for example:
 *
 * <pre>
 *     DseGssApiAuthProviderBase.GssApiOptions.Builder builder =
 *         DseGssApiAuthProviderBase.GssApiOptions.builder();
 *     Map&lt;String, String&gt; loginConfig =
 *         ImmutableMap.of(
 *             "principal",
 *             "user principal here ex cassandra@DATASTAX.COM",
 *             "useKeyTab",
 *             "true",
 *             "refreshKrb5Config",
 *             "true",
 *             "keyTab",
 *             "Path to keytab file here");
 *
 *     builder.withLoginConfiguration(loginConfig);
 *
 *     CqlSession session =
 *         CqlSession.builder()
 *             .withAuthProvider(new ProgrammaticDseGssApiAuthProvider(builder.build()))
 *             .build();
 * </pre>
 *
 * or alternatively
 *
 * <pre>
 *     DseGssApiAuthProviderBase.GssApiOptions.Builder builder =
 *         DseGssApiAuthProviderBase.GssApiOptions.builder().withSubject(subject);
 *     CqlSession session =
 *         CqlSession.builder()
 *             .withAuthProvider(new ProgrammaticDseGssApiAuthProvider(builder.build()))
 *             .build();
 * </pre>
 *
 * <h2>Kerberos Authentication</h2>
 *
 * Keytab and ticket cache settings are specified using a standard JAAS configuration file. The
 * location of the file can be set using the <code>java.security.auth.login.config</code> system
 * property or by adding a <code>login.config.url.n</code> entry in the <code>java.security</code>
 * properties file. Alternatively a login-configuration, or subject can be provided to the provider
 * via the GssApiOptions (see above).
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
 * <p>Should you need to change the SASL protocol name specify it in the GssApiOptions, use the
 * method below:
 *
 * <pre>
 *     DseGssApiAuthProviderBase.GssApiOptions.Builder builder =
 *         DseGssApiAuthProviderBase.GssApiOptions.builder();
 *     builder.withSaslProtocol("alternate");
 *     DseGssApiAuthProviderBase.GssApiOptions options = builder.build();
 * </pre>
 *
 * <p>Should internal sasl properties need to be set such as qop. This can also be accomplished by
 * setting it in the GssApiOptions:
 *
 * <pre>
 *   DseGssApiAuthProviderBase.GssApiOptions.Builder builder =
 *         DseGssApiAuthProviderBase.GssApiOptions.builder();
 *     builder.addSaslProperty("javax.security.sasl.qop", "auth-conf");
 *     DseGssApiAuthProviderBase.GssApiOptions options = builder.build();
 * </pre>
 *
 * @see <a
 *     href="http://docs.datastax.com/en/dse/5.1/dse-admin/datastax_enterprise/security/securityTOC.html">Authenticating
 *     a DSE cluster with Kerberos</a>
 */
public class ProgrammaticDseGssApiAuthProvider extends DseGssApiAuthProviderBase {
  private final GssApiOptions options;

  public ProgrammaticDseGssApiAuthProvider(GssApiOptions options) {
    super("Programmatic-Kerberos");
    this.options = options;
  }

  @NonNull
  @Override
  protected GssApiOptions getOptions(
      @NonNull EndPoint endPoint, @NonNull String serverAuthenticator) {
    return options;
  }
}
