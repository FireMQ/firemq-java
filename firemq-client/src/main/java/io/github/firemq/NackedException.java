package io.github.firemq;

/** The broker rejected a publish, for example because a queue with {@code x-overflow: reject-publish} was full. */
public class NackedException extends FireMQException {
    private static final long serialVersionUID = 1L;

    private final int code;
    private final String serverMessage;

    public NackedException(int code, String serverMessage) {
        super("publish rejected: " + serverMessage + " (code " + code + ")");
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
