/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.hive.metastore.thrift;

import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.authentication.HiveMetastoreAuthentication;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.PrestoException;
import org.apache.thrift.transport.TTransportException;

import javax.inject.Inject;
import javax.net.ssl.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Optional;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ThriftMetastoreClientFactory
{
    private final Optional<SSLContext> sslContext;
    private final Optional<HostAndPort> socksProxy;
    private final int timeoutMillis;
    private final HiveMetastoreAuthentication metastoreAuthentication;
    private final String hostname;

    public ThriftMetastoreClientFactory(
            Optional<SSLContext> sslContext,
            Optional<HostAndPort> socksProxy,
            Duration timeout,
            HiveMetastoreAuthentication metastoreAuthentication,
            String hostname)
    {
        this.sslContext = requireNonNull(sslContext, "sslContext is null");
        this.socksProxy = requireNonNull(socksProxy, "socksProxy is null");
        this.timeoutMillis = toIntExact(timeout.toMillis());
        this.metastoreAuthentication = requireNonNull(metastoreAuthentication, "metastoreAuthentication is null");
        this.hostname = requireNonNull(hostname, "hostname is null");
    }

    @Inject
    public ThriftMetastoreClientFactory(
        ThriftMetastoreConfig config,
        HiveMetastoreAuthentication metastoreAuthentication,
        NodeManager nodeManager)
    {
        this(
            buildSslContext(config.isSslEnabled(), config.getKey(), config.getKeyPassword(), config.getTrustCertificate()),
            Optional.ofNullable(config.getSocksProxy()),
            config.getMetastoreTimeout(),
            metastoreAuthentication,
            nodeManager.getCurrentNode().getHost());
    }

    public ThriftMetastoreClient create(HostAndPort address, Optional<String> delegationToken)
            throws TTransportException
    {
        return new ThriftHiveMetastoreClient(
            Transport.create(address, sslContext, socksProxy, timeoutMillis, metastoreAuthentication, delegationToken),
            hostname);
    }

    private static Optional<SSLContext> buildSslContext(boolean sslEnabled, File key, String keyPassword, File trustCertificate) {
        if (!sslEnabled || (key == null && trustCertificate == null)) {
            return Optional.empty();
        }

        try {
            KeyManager[] keyManagers = null;
            if (key != null) {
                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(keyStore, keyManagerPassword);
                keyManagers = keyManagerFactory.getKeyManagers();
            }

            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);

            TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
            if (trustManagers.length != 1 || !(trustManagers[0] instanceof X509TrustManager)) {
                throw new RuntimeException("Unexpected default trust managers:" + Arrays.toString(trustManagers));
            }

            SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(keyMangers, trustManagers, null);
            return Optional.of(sslContext);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }
}
