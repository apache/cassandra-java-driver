package com.datastax.driver.core;

public class EncryptionOptions
{
    private boolean enabled;
    private String keystore = "conf/.keystore";
    private String keystorePassword = "cassandra";
    private String truststore = "conf/.truststore";
    private String truststorePassword = "cassandra";
    private String[] cipherSuites = {"TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA"};
    private String protocol = "TLS";
    private String algorithm = "SunX509";
    private String storeType = "JKS";

    public boolean isEnabled()
    {
        return enabled;
    }

    public void setEnabled(boolean enabled)
    {
        this.enabled = enabled;
    }

    public String getKeystore()
    {
        return keystore;
    }

    public void setKeystore(String keystore)
    {
        this.keystore = keystore;
    }

    public String getKeystorePassword()
    {
        return keystorePassword;
    }

    public void setKeystorePassword(String keystorePassword)
    {
        this.keystorePassword = keystorePassword;
    }

    public String[] getCipherSuites()
    {
        return cipherSuites;
    }

    public void setCipherSuites(String[] cipherSuites)
    {
        this.cipherSuites = cipherSuites;
    }

    public String getProtocol()
    {
        return protocol;
    }

    public void setProtocol(String protocol)
    {
        this.protocol = protocol;
    }

    public String getAlgorithm()
    {
        return algorithm;
    }

    public void setAlgorithm(String algorithm)
    {
        this.algorithm = algorithm;
    }

    public String getStoreType()
    {
        return storeType;
    }

    public void setStoreType(String storeType)
    {
        this.storeType = storeType;
    }

    public String getTruststore()
    {
        return truststore;
    }

    public void setTruststore(String truststore)
    {
        this.truststore = truststore;
    }

    public String getTruststorePassword()
    {
        return truststorePassword;
    }

    public void setTruststorePassword(String truststorePassword)
    {
        this.truststorePassword = truststorePassword;
    }
}
