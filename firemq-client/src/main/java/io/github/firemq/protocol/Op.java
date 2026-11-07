package io.github.firemq.protocol;

/** Op codes of the FireMQ protocol, version 1. */
public final class Op {
    public static final int HELLO = 0x01;
    public static final int WELCOME = 0x02;
    public static final int PING = 0x03;
    public static final int PONG = 0x04;
    public static final int CLOSE = 0x05;
    public static final int OK = 0x06;
    public static final int ERROR = 0x07;
    public static final int FLOW = 0x08;
    public static final int DECLARE_EXCHANGE = 0x10;
    public static final int DELETE_EXCHANGE = 0x11;
    public static final int DECLARE_QUEUE = 0x12;
    public static final int DELETE_QUEUE = 0x13;
    public static final int PURGE_QUEUE = 0x14;
    public static final int BIND = 0x15;
    public static final int UNBIND = 0x16;
    public static final int PUBLISH = 0x20;
    public static final int CONFIRM = 0x21;
    public static final int RETURN = 0x22;
    public static final int SUBSCRIBE = 0x30;
    public static final int UNSUBSCRIBE = 0x31;
    public static final int CREDIT = 0x32;
    public static final int DELIVER = 0x33;
    public static final int CANCELLED = 0x34;
    public static final int ACK = 0x35;
    public static final int NACK = 0x36;
    public static final int GET = 0x37;
    public static final int GET_OK = 0x38;

    private Op() {}

    /** The op's name as written in the spec, for example {@code declare_queue}. */
    public static String name(int op) {
        switch (op) {
            case HELLO: return "hello";
            case WELCOME: return "welcome";
            case PING: return "ping";
            case PONG: return "pong";
            case CLOSE: return "close";
            case OK: return "ok";
            case ERROR: return "error";
            case FLOW: return "flow";
            case DECLARE_EXCHANGE: return "declare_exchange";
            case DELETE_EXCHANGE: return "delete_exchange";
            case DECLARE_QUEUE: return "declare_queue";
            case DELETE_QUEUE: return "delete_queue";
            case PURGE_QUEUE: return "purge_queue";
            case BIND: return "bind";
            case UNBIND: return "unbind";
            case PUBLISH: return "publish";
            case CONFIRM: return "confirm";
            case RETURN: return "return";
            case SUBSCRIBE: return "subscribe";
            case UNSUBSCRIBE: return "unsubscribe";
            case CREDIT: return "credit";
            case DELIVER: return "deliver";
            case CANCELLED: return "cancelled";
            case ACK: return "ack";
            case NACK: return "nack";
            case GET: return "get";
            case GET_OK: return "get_ok";
            default: return "unknown";
        }
    }

    /** True when frames with this op carry a message body as payload. */
    public static boolean hasPayload(int op) {
        return op == PUBLISH || op == RETURN || op == DELIVER || op == GET_OK;
    }
}
