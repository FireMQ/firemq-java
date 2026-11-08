package io.github.firemq;

/** A mandatory publish reached no queue. The broker returned it with code 312 (no-route). */
public class UnroutableException extends FireMQException {
    private static final long serialVersionUID = 1L;

    private final String exchange;
    private final String routingKey;

    public UnroutableException(String exchange, String routingKey) {
        super("message returned: no queue is bound to exchange '" + exchange + "' with key '" + routingKey + "'");
        this.exchange = exchange;
        this.routingKey = routingKey;
    }

    @Override
    public int code() {
        return 312;
    }

    public String exchange() {
        return exchange;
    }

    public String routingKey() {
        return routingKey;
    }
}
