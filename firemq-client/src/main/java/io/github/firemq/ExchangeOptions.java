package io.github.firemq;

import java.util.LinkedHashMap;
import java.util.Map;

/** Options for {@link Client#declareExchange(String, String, ExchangeOptions)}. Exchanges are durable by default. */
public final class ExchangeOptions {
    private boolean durable = true;
    private boolean autoDelete;
    private boolean internal;
    private boolean passive;
    private final Map<String, Object> args = new LinkedHashMap<>();

    public boolean durable() {
        return durable;
    }

    /** Survives a broker restart. Default true. */
    public ExchangeOptions durable(boolean durable) {
        this.durable = durable;
        return this;
    }

    public boolean autoDelete() {
        return autoDelete;
    }

    /** Deleted when its last binding is removed. */
    public ExchangeOptions autoDelete(boolean autoDelete) {
        this.autoDelete = autoDelete;
        return this;
    }

    public boolean internal() {
        return internal;
    }

    /** Cannot be published to directly, only through exchange-to-exchange bindings. */
    public ExchangeOptions internal(boolean internal) {
        this.internal = internal;
        return this;
    }

    public boolean passive() {
        return passive;
    }

    /** Only check that the exchange exists. */
    public ExchangeOptions passive(boolean passive) {
        this.passive = passive;
        return this;
    }

    public Map<String, Object> args() {
        return args;
    }

    /** Adds an exchange argument, for example {@code alternate-exchange}. */
    public ExchangeOptions arg(String key, Object value) {
        args.put(key, value);
        return this;
    }

    public ExchangeOptions args(Map<String, ?> args) {
        if (args != null) {
            this.args.putAll(args);
        }
        return this;
    }
}
