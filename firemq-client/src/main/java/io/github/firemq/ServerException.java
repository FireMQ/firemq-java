package io.github.firemq;

/** The broker refused a request. {@link #code()} is an AMQP reply code such as 404 or 406. */
public class ServerException extends FireMQException {
    private static final long serialVersionUID = 1L;

    private final int code;
    private final String serverMessage;

    public ServerException(int code, String serverMessage) {
        super(serverMessage + " (code " + code + ")");
        this.code = code;
        this.serverMessage = serverMessage;
    }

    @Override
    public int code() {
        return code;
    }

    /** The broker's reason, without the code. */
    public String serverMessage() {
        return serverMessage;
    }
}
