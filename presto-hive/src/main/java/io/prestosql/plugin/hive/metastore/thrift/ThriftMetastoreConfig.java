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
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.util.RetryDriver;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class ThriftMetastoreConfig
{
    private Duration metastoreTimeout = new Duration(10, TimeUnit.SECONDS);
    private HostAndPort socksProxy;
    private int maxRetries = RetryDriver.DEFAULT_MAX_ATTEMPTS - 1;
    private double backoffScaleFactor = RetryDriver.DEFAULT_SCALE_FACTOR;
    private Duration minBackoffDelay = RetryDriver.DEFAULT_SLEEP_TIME;
    private Duration maxBackoffDelay = RetryDriver.DEFAULT_SLEEP_TIME;
    private Duration maxRetryTime = RetryDriver.DEFAULT_MAX_RETRY_TIME;
    private boolean impersonationEnabled;
    private boolean deleteFilesOnDrop;
    private Duration maxWaitForTransactionLock = new Duration(10, TimeUnit.MINUTES);

    private boolean tlsEnabled;
    private File keystorePath;
    private String keystorePassword;
    private File truststorePath;
    private String truststorePassword;

    @NotNull
    public Duration getMetastoreTimeout()
    {
        return metastoreTimeout;
    }

    @Config("hive.metastore-timeout")
    public ThriftMetastoreConfig setMetastoreTimeout(Duration metastoreTimeout)
    {
        this.metastoreTimeout = metastoreTimeout;
        return this;
    }

    public HostAndPort getSocksProxy()
    {
        return socksProxy;
    }

    @Config("hive.metastore.thrift.client.socks-proxy")
    public ThriftMetastoreConfig setSocksProxy(HostAndPort socksProxy)
    {
        this.socksProxy = socksProxy;
        return this;
    }

    @Min(0)
    public int getMaxRetries()
    {
        return maxRetries;
    }

    @Config("hive.metastore.thrift.client.max-retries")
    @ConfigDescription("Maximum number of retry attempts for metastore requests")
    public ThriftMetastoreConfig setMaxRetries(int maxRetries)
    {
        this.maxRetries = maxRetries;
        return this;
    }

    public double getBackoffScaleFactor()
    {
        return backoffScaleFactor;
    }

    @Config("hive.metastore.thrift.client.backoff-scale-factor")
    @ConfigDescription("Scale factor for metastore request retry delay")
    public ThriftMetastoreConfig setBackoffScaleFactor(double backoffScaleFactor)
    {
        this.backoffScaleFactor = backoffScaleFactor;
        return this;
    }

    @NotNull
    public Duration getMaxRetryTime()
    {
        return maxRetryTime;
    }

    @Config("hive.metastore.thrift.client.max-retry-time")
    @ConfigDescription("Total time limit for a metastore request to be retried")
    public ThriftMetastoreConfig setMaxRetryTime(Duration maxRetryTime)
    {
        this.maxRetryTime = maxRetryTime;
        return this;
    }

    public Duration getMinBackoffDelay()
    {
        return minBackoffDelay;
    }

    @Config("hive.metastore.thrift.client.min-backoff-delay")
    @ConfigDescription("Minimum delay between metastore request retries")
    public ThriftMetastoreConfig setMinBackoffDelay(Duration minBackoffDelay)
    {
        this.minBackoffDelay = minBackoffDelay;
        return this;
    }

    public Duration getMaxBackoffDelay()
    {
        return maxBackoffDelay;
    }

    @Config("hive.metastore.thrift.client.max-backoff-delay")
    @ConfigDescription("Maximum delay between metastore request retries")
    public ThriftMetastoreConfig setMaxBackoffDelay(Duration maxBackoffDelay)
    {
        this.maxBackoffDelay = maxBackoffDelay;
        return this;
    }

    public boolean isImpersonationEnabled()
    {
        return impersonationEnabled;
    }

    @Config("hive.metastore.thrift.impersonation.enabled")
    @LegacyConfig("hive.metastore.impersonation-enabled")
    @ConfigDescription("Should end user be impersonated when communicating with metastore")
    public ThriftMetastoreConfig setImpersonationEnabled(boolean impersonationEnabled)
    {
        this.impersonationEnabled = impersonationEnabled;
        return this;
    }

    public boolean isDeleteFilesOnDrop()
    {
        return deleteFilesOnDrop;
    }

    @Config("hive.metastore.thrift.delete-files-on-drop")
    @ConfigDescription("Delete files on drop in case the metastore doesn't do it")
    public ThriftMetastoreConfig setDeleteFilesOnDrop(boolean deleteFilesOnDrop)
    {
        this.deleteFilesOnDrop = deleteFilesOnDrop;
        return this;
    }

    public Duration getMaxWaitForTransactionLock()
    {
        return maxWaitForTransactionLock;
    }

    @Config("hive.metastore.thrift.txn-lock-max-wait")
    @ConfigDescription("Maximum time to wait to acquire hive transaction lock")
    public ThriftMetastoreConfig setMaxWaitForTransactionLock(Duration maxWaitForTransactionLock)
    {
        this.maxWaitForTransactionLock = maxWaitForTransactionLock;
        return this;
    }

    public boolean isTlsEnabled()
    {
        return tlsEnabled;
    }

    @Config("hive.metastore.thrift.client.tls.enabled")
    @ConfigDescription("Whether TLS security is enabled")
    public ThriftMetastoreConfig setTlsEnabled(boolean tlsEnabled)
    {
        this.tlsEnabled = tlsEnabled;
        return this;
    }

    public File getKeystorePath()
    {
        return keystorePath;
    }

    @Config("hive.metastore.thrift.client.tls.keystore.path")
    @ConfigDescription("Path to the PEM or JKS key store")
    public ThriftMetastoreConfig setKeystorePath(File keystorePath)
    {
        this.keystorePath = keystorePath;
        return this;
    }

    public String getKeystorePassword()
    {
        return keystorePassword;
    }

    @Config("hive.metastore.thrift.client.tls.keystore.password")
    @ConfigDescription("Password for the key store")
    public ThriftMetastoreConfig setKeystorePassword(String keystorePassword)
    {
        this.keystorePassword = keystorePassword;
        return this;
    }

    public File getTruststorePath()
    {
        return truststorePath;
    }

    @Config("hive.metastore.thrift.client.tls.truststore.path")
    @ConfigDescription("Path to the PEM or JKS trust store")
    public ThriftMetastoreConfig setTruststorePath(File truststorePath)
    {
        this.truststorePath = truststorePath;
        return this;
    }

    public String getTruststorePassword()
    {
        return truststorePassword;
    }

    @Config("hive.metastore.thrift.client.tls.truststore.password")
    @ConfigDescription("Password for the trust store")
    public ThriftMetastoreConfig setTruststorePassword(String truststorePassword)
    {
        this.truststorePassword = truststorePassword;
        return this;
    }
}
