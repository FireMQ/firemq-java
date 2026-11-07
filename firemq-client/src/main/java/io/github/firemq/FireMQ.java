package io.github.firemq;

/**
 * Entry point of the FireMQ Java client.
 *
 * <pre>{@code
 * try (Client client = FireMQ.connect("fire://guest:guest@localhost:5673")) {
 *     client.declareQueue("hello");
 *     client.publish("", "hello", "Hello World!");
 * }
 * }</pre>
 */
public final class FireMQ {
    /** Version of this client library. */
    public static final String VERSION = "0.1.0";

    private FireMQ() {}

    /**
     * Connects with a URL such as {@code fire://user:pass@host:5673/vhost} or
     * {@code fires://host} for TLS. User and password default to {@code guest}, the vhost to
     * {@code /}.
     */
    public static Client connect(String url) {
        return Client.connect(url);
    }

    /** Connects with explicit options, for example a heartbeat interval or TLS settings. */
    public static Client connect(ConnectOptions options) {
        return Client.connect(options);
    }
}
