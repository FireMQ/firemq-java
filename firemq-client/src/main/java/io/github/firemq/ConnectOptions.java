package io.github.firemq;

import java.time.Duration;
import java.util.Objects;
import javax.net.ssl.SSLContext;

/** How to connect. Start from {@link #fromUrl(String)}. */
public final class ConnectOptions {
    private ConnectUrl url;
    private int heartbeat = 30;
    private String connectionName = "";
    private Duration connectTimeout = Duration.ofSeconds(10);
    private byte[] tlsCaPem;
    private boolean tlsInsecure;
    private SSLContext sslContext;

    public ConnectOptions(ConnectUrl url) {
        this.url = Objects.requireNonNull(url, "url");
    }

    /** Options for a URL such as {@code fire://user:pass@host:5673/vhost}, with defaults for the rest. */
    public static ConnectOptions fromUrl(String url) {
        return new ConnectOptions(ConnectUrl.parse(url));
    }

    public ConnectUrl url() {
        return url;
    }

    public ConnectOptions url(ConnectUrl url) {
        this.url = Objects.requireNonNull(url, "url");
        return this;
    }

    /** Heartbeat interval in seconds. 0 disables heartbeats. Default 30. */
    public int heartbeat() {
        return heartbeat;
    }

    public ConnectOptions heartbeat(int seconds) {
        if (seconds < 0) {
            throw new IllegalArgumentException("heartbeat must be >= 0");
        }
        this.heartbeat = seconds;
        return this;
    }

    /** Shown in the management UI. */
    public String connectionName() {
        return connectionName;
    }

    public ConnectOptions connectionName(String name) {
        this.connectionName = name == null ? "" : name;
        return this;
    }

    /** Timeout for the TCP connect, the TLS handshake and the protocol handshake. Default 10 seconds. */
    public Duration connectTimeout() {
        return connectTimeout;
    }

    public ConnectOptions connectTimeout(Duration timeout) {
        this.connectTimeout = Objects.requireNonNull(timeout, "timeout");
        return this;
    }

    /** Extra trusted CA certificates (PEM) for {@code fires://}, for example a self-signed one. */
    public byte[] tlsCaPem() {
        return tlsCaPem;
    }

    public ConnectOptions tlsCaPem(byte[] pem) {
        this.tlsCaPem = pem;
        return this;
    }

    /** Skip certificate and host name verification. Only for local development. */
    public boolean tlsInsecure() {
        return tlsInsecure;
    }

    public ConnectOptions tlsInsecure(boolean insecure) {
        this.tlsInsecure = insecure;
        return this;
    }

    /**
     * A fully configured {@link SSLContext} for {@code fires://}, for example one with a client
     * certificate. When set, {@link #tlsCaPem(byte[])} is ignored; host name verification still
     * applies unless {@link #tlsInsecure(boolean)} is set.
     */
    public SSLContext sslContext() {
        return sslContext;
    }

    public ConnectOptions sslContext(SSLContext context) {
        this.sslContext = context;
        return this;
    }
}
