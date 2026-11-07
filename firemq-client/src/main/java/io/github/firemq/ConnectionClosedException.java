package io.github.firemq;

/**
 * The connection is closed: the application closed it, the broker closed it, or the network
 * failed. {@link #code()} is the broker's close or error code when it sent one, such as 320 when
 * the broker shuts down.
 */
public class ConnectionClosedException extends FireMQException {
    private static final long serialVersionUID = 1L;

    private final int code;

    public ConnectionClosedException(String message) {
        this(message, 0, null);
    }

    public ConnectionClosedException(String message, int code) {
        this(message, code, null);
    }

    public ConnectionClosedException(String message, Throwable cause) {
        this(message, 0, cause);
    }

    public ConnectionClosedException(String message, int code, Throwable cause) {
        super("connection closed: " + message, cause);
        this.code = code;
    }

    @Override
    public int code() {
        return code;
    }
}
