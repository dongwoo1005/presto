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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.util.RetryDriver;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.Objects.requireNonNull;

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

    private boolean sslEnabled;
    private List<String> ciphers = ImmutableList.of();

//    private File trustCertificate;
//    private File key;
//    private String keyPassword;

    private long sessionCacheSize;
    private Duration sessionTimeout = new Duration(1, DAYS);

    private File keystore;
    private String keystorePassword;
    private File truststore;
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

    public boolean isSslEnabled() {
        return sslEnabled;
    }

    @Config("hive.metastore.thrift.client.ssl.enabled")
    @ConfigDescription("")
    public ThriftMetastoreConfig setSslEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
        return this;
    }

    public File getTrustCertificate() {
        return trustCertificate;
    }

    @Config("hive.metastore.thrift.client.ssl.trust-certificate")
    @ConfigDescription("")
    public ThriftMetastoreConfig setTrustCertificate(File trustCertificate) {
        this.trustCertificate = trustCertificate;
        return this;
    }

    public File getKey() {
        return key;
    }

    @Config("hive.metastore.thrift.client.ssl.key")
    @ConfigDescription("")
    public ThriftMetastoreConfig setKey(File key) {
        this.key = key;
        return this;
    }

    public String getKeyPassword() {
        return keyPassword;
    }

    @Config("hive.metastore.thrift.client.ssl.key-password")
    @ConfigDescription("")
    public ThriftMetastoreConfig setKeyPassword(String keyPassword) {
        this.keyPassword = keyPassword;
        return this;
    }

    public long getSessionCacheSize() {
        return sessionCacheSize;
    }

    @Config("hive.metastore.thrift.client.ssl.session-cache-size")
    @ConfigDescription("")
    public ThriftMetastoreConfig setSessionCacheSize(long sessionCacheSize) {
        this.sessionCacheSize = sessionCacheSize;
        return this;
    }

    public Duration getSessionTimeout() {
        return sessionTimeout;
    }

    @Config("hive.metastore.thrift.client.ssl.session-timeout")
    @ConfigDescription("")
    public ThriftMetastoreConfig setSessionTimeout(Duration sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
        return this;
    }

    public List<String> getCiphers() {
        return ciphers;
    }

    @Config("hive.metastore.thrift.client.ssl.ciphers")
    @ConfigDescription("")
    public ThriftMetastoreConfig setCiphers(String ciphers) {
        this.ciphers = Splitter
            .on(',')
            .trimResults()
            .omitEmptyStrings()
            .splitToList(requireNonNull(ciphers, "ciphers is null"));
        return this;
    }
}
