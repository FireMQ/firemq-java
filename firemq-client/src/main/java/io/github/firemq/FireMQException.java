package io.github.firemq;

/**
 * Base class of every error the client raises.
 *
 * <p>Used directly for I/O and protocol errors. The subclasses cover broker refusals
 * ({@link ServerException}), rejected publishes ({@link NackedException}), unroutable mandatory
 * publishes ({@link UnroutableException}) and closed connections
 * ({@link ConnectionClosedException}).
 */
public class FireMQException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public FireMQException(String message) {
        super(message);
    }

    public FireMQException(String message, Throwable cause) {
        super(message, cause);
    }

    /** The AMQP reply code when the broker sent one, such as 404 or 406; otherwise 0. */
    public int code() {
        return 0;
    }
}
