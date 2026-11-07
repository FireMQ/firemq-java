package io.github.firemq.conformance;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.firemq.Client;
import io.github.firemq.ConnectionClosedException;
import io.github.firemq.ExchangeOptions;
import io.github.firemq.FireMQ;
import io.github.firemq.FireMQException;
import io.github.firemq.Message;
import io.github.firemq.MessageProperties;
import io.github.firemq.NackedException;
import io.github.firemq.PublishOptions;
import io.github.firemq.QueueInfo;
import io.github.firemq.QueueOptions;
import io.github.firemq.ServerException;
import io.github.firemq.SubscribeOptions;
import io.github.firemq.Subscription;
import io.github.firemq.UnroutableException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Driver for the FireMQ SDK conformance suite: reads one JSON command per line on stdin and writes
 * exactly one JSON reply per line on stdout. Logs go to stderr.
 */
public final class Driver {
    private static final ObjectMapper JSON = new ObjectMapper().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
    private static final JsonNodeFactory NODES = JsonNodeFactory.instance;

    /** An error reply with a conformance {@code kind}. */
    private static final class DriverError extends Exception {
        private static final long serialVersionUID = 1L;
        final String kind;
        final Integer code;

        DriverError(String kind, Integer code, String message) {
            super(message);
            this.kind = kind;
            this.code = code;
        }
    }

    private Client client;
    private final Map<String, Subscription> subs = new HashMap<>();
    private final Map<String, Message> pending = new HashMap<>();
    private long nextTag = 1;

    public static void main(String[] args) throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        PrintWriter out = new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), false);
        // Nothing but replies may reach stdout.
        System.setOut(System.err);
        Driver driver = new Driver();
        String line;
        while ((line = in.readLine()) != null) {
            if (line.isBlank()) {
                continue;
            }
            out.println(JSON.writeValueAsString(driver.handle(line)));
            out.flush();
        }
        driver.closeClient();
    }

    ObjectNode handle(String line) {
        ObjectNode reply = NODES.objectNode();
        JsonNode cmd;
        try {
            cmd = JSON.readTree(line);
        } catch (Exception e) {
            reply.putNull("id");
            return fail(reply, new DriverError("other", null, "invalid JSON: " + e.getMessage()));
        }
        reply.set("id", cmd.get("id"));
        try {
            reply.put("ok", true);
            reply.set("result", run(cmd));
            return reply;
        } catch (DriverError e) {
            return fail(reply, e);
        } catch (Exception e) {
            return fail(reply, classify(e));
        }
    }

    private static ObjectNode fail(ObjectNode reply, DriverError e) {
        reply.remove("result");
        reply.put("ok", false);
        ObjectNode err = reply.putObject("error");
        err.put("kind", e.kind);
        if (e.code != null) {
            err.put("code", e.code);
        }
        err.put("message", String.valueOf(e.getMessage()));
        return reply;
    }

    private static DriverError classify(Exception e) {
        if (e instanceof UnroutableException) {
            return new DriverError("unroutable", 312, e.getMessage());
        }
        if (e instanceof NackedException) {
            return new DriverError("nacked", ((NackedException) e).code(), e.getMessage());
        }
        if (e instanceof ServerException) {
            return new DriverError("server", ((ServerException) e).code(), e.getMessage());
        }
        if (e instanceof ConnectionClosedException) {
            int code = ((ConnectionClosedException) e).code();
            return new DriverError("closed", code == 0 ? null : code, e.getMessage());
        }
        return new DriverError("other", null, e.getClass().getSimpleName() + ": " + e.getMessage());
    }

    private JsonNode run(JsonNode cmd) throws Exception {
        String op = cmd.path("op").asText();
        ObjectNode result = NODES.objectNode();
        switch (op) {
            case "hello":
                result.put("sdk", "firemq-java");
                result.put("version", FireMQ.VERSION);
                result.putArray("capabilities");
                return result;
            case "connect":
                closeClient();
                client = FireMQ.connect(cmd.path("url").asText());
                return result;
            case "close":
                closeClient();
                return result;
            case "declare_queue": {
                QueueOptions o = new QueueOptions()
                        .durable(cmd.path("durable").asBoolean())
                        .exclusive(cmd.path("exclusive").asBoolean())
                        .autoDelete(cmd.path("auto_delete").asBoolean())
                        .args(toMap(cmd.get("args")));
                QueueInfo q = client().declareQueue(cmd.path("name").asText(), o);
                result.put("name", q.name());
                result.put("messages", q.messages());
                result.put("consumers", q.consumers());
                return result;
            }
            case "delete_queue":
                result.put("messages", client().deleteQueue(cmd.path("name").asText()));
                return result;
            case "purge_queue":
                result.put("messages", client().purgeQueue(cmd.path("name").asText()));
                return result;
            case "declare_exchange":
                client().declareExchange(cmd.path("name").asText(), cmd.path("type").asText(),
                        new ExchangeOptions().durable(cmd.path("durable").asBoolean()));
                return result;
            case "delete_exchange":
                client().deleteExchange(cmd.path("name").asText());
                return result;
            case "bind_queue":
                client().bindQueue(cmd.path("queue").asText(), cmd.path("exchange").asText(), cmd.path("key").asText(),
                        toMap(cmd.get("args")));
                return result;
            case "unbind_queue":
                client().unbindQueue(cmd.path("queue").asText(), cmd.path("exchange").asText(), cmd.path("key").asText());
                return result;
            case "bind_exchange":
                client().bindExchange(cmd.path("destination").asText(), cmd.path("source").asText(),
                        cmd.path("key").asText());
                return result;
            case "publish": {
                byte[] body;
                JsonNode b64 = cmd.get("body_base64");
                if (b64 != null && !b64.isNull()) {
                    body = Base64.getDecoder().decode(b64.asText());
                } else {
                    JsonNode text = cmd.get("body");
                    body = text == null || text.isNull() ? new byte[0] : text.asText().getBytes(StandardCharsets.UTF_8);
                }
                PublishOptions o = new PublishOptions()
                        .persistent(cmd.path("persistent").asBoolean())
                        .mandatory(cmd.path("mandatory").asBoolean())
                        .properties(MessageProperties.fromMap(toMap(cmd.get("properties"))))
                        .headers(toMap(cmd.get("headers")));
                client().publish(cmd.path("exchange").asText(), cmd.path("key").asText(), body, o);
                return result;
            }
            case "subscribe": {
                SubscribeOptions o = new SubscribeOptions().noAck(cmd.path("no_ack").asBoolean());
                int prefetch = cmd.path("prefetch").asInt(0);
                if (prefetch > 0) {
                    o.prefetch(prefetch);
                }
                Subscription s = client().subscribe(cmd.path("queue").asText(), o);
                String id = "sub-" + s.id();
                subs.put(id, s);
                result.put("sub", id);
                return result;
            }
            case "next": {
                Subscription s = subscription(cmd.path("sub").asText());
                Message m = s.next(Duration.ofMillis(cmd.path("timeout_ms").asLong(0)));
                result.set("message", m == null ? NODES.nullNode() : message(m));
                return result;
            }
            case "ack":
                pendingMessage(cmd.path("tag").asText()).ack();
                return result;
            case "nack":
                pendingMessage(cmd.path("tag").asText()).nack(cmd.path("requeue").asBoolean());
                return result;
            case "unsubscribe": {
                String id = cmd.path("sub").asText();
                subscription(id).cancel();
                subs.remove(id);
                return result;
            }
            case "get": {
                Message m = client().get(cmd.path("queue").asText(), cmd.path("no_ack").asBoolean());
                result.set("message", m == null ? NODES.nullNode() : message(m));
                return result;
            }
            default:
                throw new DriverError("unsupported", null, "op '" + op + "' is not supported by firemq-java");
        }
    }

    private Client client() throws DriverError {
        if (client == null) {
            throw new DriverError("closed", null, "not connected");
        }
        return client;
    }

    private Subscription subscription(String id) throws DriverError {
        Subscription s = subs.get(id);
        if (s == null) {
            throw new DriverError("other", null, "unknown subscription '" + id + "'");
        }
        return s;
    }

    private Message pendingMessage(String tag) throws DriverError {
        Message m = pending.remove(tag);
        if (m == null) {
            throw new DriverError("other", null, "unknown tag '" + tag + "'");
        }
        return m;
    }

    private void closeClient() {
        if (client != null) {
            try {
                client.close();
            } catch (FireMQException e) {
                System.err.println("close: " + e.getMessage());
            }
        }
        client = null;
        subs.clear();
        pending.clear();
    }

    private ObjectNode message(Message m) {
        String tag = Long.toString(nextTag++);
        pending.put(tag, m);
        ObjectNode n = NODES.objectNode();
        String text = utf8(m.body());
        if (text == null) {
            n.putNull("body");
        } else {
            n.put("body", text);
        }
        n.put("body_base64", Base64.getEncoder().encodeToString(m.body()));
        n.put("exchange", m.exchange());
        n.put("routing_key", m.routingKey());
        n.put("redelivered", m.redelivered());
        n.put("tag", tag);
        n.set("properties", toJson(m.properties().toMap()));
        n.set("headers", toJson(m.headers()));
        return n;
    }

    private static String utf8(byte[] body) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(body))
                    .toString();
        } catch (CharacterCodingException e) {
            return null;
        }
    }

    /** JSON object to a map of plain Java values; integers stay integers. */
    private static Map<String, Object> toMap(JsonNode n) {
        Map<String, Object> m = new LinkedHashMap<>();
        if (n != null && n.isObject()) {
            n.fields().forEachRemaining(e -> m.put(e.getKey(), toJava(e.getValue())));
        }
        return m;
    }

    private static Object toJava(JsonNode n) {
        if (n == null || n.isNull()) {
            return null;
        }
        if (n.isBoolean()) {
            return n.booleanValue();
        }
        if (n.isIntegralNumber()) {
            return n.canConvertToLong() ? (Object) n.longValue() : n.bigIntegerValue();
        }
        if (n.isNumber()) {
            return n.doubleValue();
        }
        if (n.isTextual()) {
            return n.textValue();
        }
        if (n.isArray()) {
            List<Object> l = new ArrayList<>();
            n.forEach(e -> l.add(toJava(e)));
            return l;
        }
        if (n.isObject()) {
            return toMap(n);
        }
        return n.asText();
    }

    private static JsonNode toJson(Object v) {
        if (v == null) {
            return NODES.nullNode();
        }
        if (v instanceof Boolean) {
            return NODES.booleanNode((Boolean) v);
        }
        if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) {
            return NODES.numberNode(((Number) v).longValue());
        }
        if (v instanceof BigInteger) {
            return NODES.numberNode((BigInteger) v);
        }
        if (v instanceof Float) {
            return NODES.numberNode((Float) v);
        }
        if (v instanceof Double) {
            return NODES.numberNode((Double) v);
        }
        if (v instanceof String) {
            return NODES.textNode((String) v);
        }
        if (v instanceof byte[]) {
            return NODES.textNode(Base64.getEncoder().encodeToString((byte[]) v));
        }
        if (v instanceof Map) {
            ObjectNode o = NODES.objectNode();
            for (Map.Entry<?, ?> e : ((Map<?, ?>) v).entrySet()) {
                o.set(String.valueOf(e.getKey()), toJson(e.getValue()));
            }
            return o;
        }
        if (v instanceof List) {
            ArrayNode a = NODES.arrayNode();
            for (Object o : (List<?>) v) {
                a.add(toJson(o));
            }
            return a;
        }
        return NODES.textNode(String.valueOf(v));
    }
}
