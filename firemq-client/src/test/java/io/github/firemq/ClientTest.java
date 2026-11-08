package io.github.firemq;

import static io.github.firemq.FakeBroker.map;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.firemq.protocol.Frame;
import io.github.firemq.protocol.Op;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Client behaviour against a scripted broker. */
@Timeout(20)
class ClientTest {
    private static Throwable failure(CompletableFuture<?> f) throws Exception {
        ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(5, TimeUnit.SECONDS));
        return e.getCause();
    }

    @Test
    void handshakeSendsHelloAndReadsWelcome() throws Exception {
        try (FakeBroker b = new FakeBroker(); Client c = b.connect()) {
            assertEquals(1, b.hello.getLong("ver"));
            assertEquals("/", b.hello.getString("vhost"));
            assertEquals(Map.of("mech", "plain", "user", "guest", "pass", "guest"), b.hello.getMap("auth"));
            assertEquals(30, b.hello.getLong("heartbeat"));
            assertEquals("java", b.hello.getMap("client").get("lang"));
            assertEquals(List.of("confirms", "credit"), b.hello.getList("caps"));
            assertEquals("test-conn", c.server().connectionId());
            assertEquals(1 << 20, c.server().maxFrame());
            assertTrue(c.server().capabilities().contains("mandatory"));
        }
    }

    @Test
    void refusedLoginRaisesServerException() throws Exception {
        try (FakeBroker b = new FakeBroker()) {
            CompletableFuture<Client> c = CompletableFuture.supplyAsync(() -> FireMQ.connect(b.url()));
            b.accept();
            b.send(Op.ERROR, map("code", 403, "msg", "ACCESS_REFUSED", "fatal", true));
            Throwable e = failure(c);
            assertInstanceOf(ServerException.class, e);
            assertEquals(403, ((ServerException) e).code());
        }
    }

    @Test
    void requestsGetTheirReplies() throws Exception {
        try (FakeBroker b = new FakeBroker(); Client c = b.connect()) {
            CompletableFuture<QueueInfo> q = c.declareQueueAsync("", QueueOptions.temporary());
            Frame d = b.expect(Op.DECLARE_QUEUE);
            assertFalse(d.has("name"));
            assertTrue(d.getBool("exclusive"));
            assertTrue(d.getBool("auto_delete"));
            assertFalse(d.has("durable"));
            b.send(Op.OK, map("rid", d.getLong("rid"), "name", "amq.gen-1", "messages", 0, "consumers", 0));
            assertEquals(new QueueInfo("amq.gen-1", 0, 0), q.get(5, TimeUnit.SECONDS));

            CompletableFuture<Void> x = c.declareExchangeAsync("logs", "fanout", new ExchangeOptions());
            Frame dx = b.expect(Op.DECLARE_EXCHANGE);
            assertEquals("fanout", dx.getString("type"));
            assertTrue(dx.getBool("durable"));
            b.send(Op.ERROR, map("rid", dx.getLong("rid"), "code", 406, "msg", "PRECONDITION_FAILED"));
            Throwable e = failure(x);
            assertInstanceOf(ServerException.class, e);
            assertEquals(406, ((FireMQException) e).code());

            CompletableFuture<Void> bind = c.bindExchangeAsync("down", "up", "audit", null);
            Frame bf = b.expect(Op.BIND);
            assertEquals("up", bf.getString("exchange"));
            assertEquals("down", bf.getString("to_exchange"));
            assertFalse(bf.has("queue"));
            b.ok(bf);
            bind.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void cumulativeConfirmsAndIndividualNacks() throws Exception {
        try (FakeBroker b = new FakeBroker(); Client c = b.connect()) {
            CompletableFuture<Void> p1 = c.publishAsync("", "q", "one");
            CompletableFuture<Void> p2 = c.publishAsync("", "q", "two");
            CompletableFuture<Void> p3 = c.publishAsync("", "q", "three");
            for (int seq = 1; seq <= 3; seq++) {
                Frame p = b.expect(Op.PUBLISH);
                assertEquals(seq, p.getLong("seq"));
                assertTrue(p.getBool("persistent"));
                assertEquals("q", p.getString("key"));
            }
            b.send(Op.CONFIRM, map("seq", 2, "nack", true, "code", 406, "msg", "queue 'q' is full"));
            b.send(Op.CONFIRM, map("upto", 3));
            p1.get(5, TimeUnit.SECONDS);
            p3.get(5, TimeUnit.SECONDS);
            Throwable e = failure(p2);
            assertInstanceOf(NackedException.class, e);
            assertEquals(406, ((NackedException) e).code());
        }
    }

    @Test
    void returnBeforeConfirmFailsWithUnroutable() throws Exception {
        try (FakeBroker b = new FakeBroker(); Client c = b.connect()) {
            CompletableFuture<Void> p =
                    c.publishAsync("orders", "nowhere", "lost", new PublishOptions().mandatory(true));
            Frame f = b.expect(Op.PUBLISH);
            assertTrue(f.getBool("mandatory"));
            b.send(Op.RETURN, map("seq", f.getLong("seq"), "code", 312, "msg", "NO_ROUTE", "exchange", "orders",
                    "key", "nowhere"), "lost".getBytes(StandardCharsets.UTF_8));
            b.send(Op.CONFIRM, map("upto", f.getLong("seq")));
            Throwable e = failure(p);
            assertInstanceOf(UnroutableException.class, e);
            assertEquals("orders", ((UnroutableException) e).exchange());
            assertEquals("nowhere", ((UnroutableException) e).routingKey());
            assertEquals(312, ((UnroutableException) e).code());
        }
    }

    @Test
    void blockingPublishThrowsTypedErrors() throws Exception {
        try (FakeBroker b = new FakeBroker(); Client c = b.connect()) {
            CompletableFuture<Void> p = CompletableFuture.runAsync(() -> c.publish("", "full", "x"));
            Frame f = b.expect(Op.PUBLISH);
            b.send(Op.CONFIRM, map("seq", f.getLong("seq"), "nack", true, "code", 406, "msg", "full"));
            assertInstanceOf(NackedException.class, failure(p));
        }
    }

    @Test
    void unconfirmedPublishesCarryNoSequence() throws Exception {
        try (FakeBroker b = new FakeBroker(); Client c = b.connect()) {
            c.publish("", "q", "fire and forget", new PublishOptions().confirm(false).persistent(false));
            Frame f = b.expect(Op.PUBLISH);
            assertFalse(f.has("seq"));
            assertFalse(f.has("persistent"));
            c.publishAsync("", "q", "confirmed").thenRun(() -> {});
            assertEquals(1, b.expect(Op.PUBLISH).getLong("seq"));
        }
    }

    @Test
    void propertiesAndHeadersAreSent() throws Exception {
        try (FakeBroker b = new FakeBroker(); Client c = b.connect()) {
            PublishOptions o = new PublishOptions()
                    .properties(new MessageProperties().contentType("application/json").expiration(30000L).priority(3))
                    .header("trace", "abc")
                    .header("attempt", 2);
            c.publishAsync("orders", "order.created", "{}", o);
            Frame f = b.expect(Op.PUBLISH);
            assertEquals(map("content_type", "application/json", "expiration", 30000L, "priority", 3L), f.getMap("props"));
            assertEquals(map("trace", "abc", "attempt", 2L), f.getMap("headers"));
            assertEquals("{}", new String(f.payload(), StandardCharsets.UTF_8));
        }
    }

    @Test
    void creditIsToppedUpInHalfWindowBatches() throws Exception {
        try (FakeBroker b = new FakeBroker(); Client c = b.connect()) {
            CompletableFuture<Subscription> sf = c.subscribeAsync("jobs", new SubscribeOptions().prefetch(4));
            Frame s = b.expect(Op.SUBSCRIBE);
            assertEquals(4, s.getLong("credit"));
            assertEquals("jobs", s.getString("queue"));
            b.ok(s);
            Subscription sub = sf.get(5, TimeUnit.SECONDS);
            long id = s.getLong("sub");
            for (int tag = 1; tag <= 4; tag++) {
                b.send(Op.DELIVER, map("sub", id, "tag", tag, "key", "jobs", "props", map("message_id", "m" + tag),
                        "headers", map("n", tag)), ("m" + tag).getBytes(StandardCharsets.UTF_8));
            }
            Message m1 = sub.next(Duration.ofSeconds(5));
            assertEquals("m1", m1.text());
            assertEquals("jobs", m1.routingKey());
            assertEquals("m1", m1.properties().messageId());
            assertEquals(1L, m1.headers().get("n"));
            m1.ack();
            m1.ack(); // settles once
            assertEquals(1, b.expect(Op.ACK).getLong("tag"));
            Message m2 = sub.next(Duration.ofSeconds(5));
            m2.nack(true);
            Frame nack = b.expect(Op.NACK);
            assertEquals(2, nack.getLong("tag"));
            assertTrue(nack.getBool("requeue"));
            Frame credit = b.expect(Op.CREDIT);
            assertEquals(id, credit.getLong("sub"));
            assertEquals(2, credit.getLong("credit"));
        }
    }

    @Test
    void noAckCreditIsGrantedOnReceipt() throws Exception {
        try (FakeBroker b = new FakeBroker(); Client c = b.connect()) {
            CompletableFuture<Subscription> sf =
                    c.subscribeAsync("jobs", new SubscribeOptions().prefetch(2).noAck(true));
            Frame s = b.expect(Op.SUBSCRIBE);
            assertTrue(s.getBool("no_ack"));
            b.ok(s);
            Subscription sub = sf.get(5, TimeUnit.SECONDS);
            b.send(Op.DELIVER, map("sub", s.getLong("sub"), "tag", 1), new byte[] {1});
            Message m = sub.next(Duration.ofSeconds(5));
            assertTrue(m.settled());
            m.ack(); // no-op
            Frame credit = b.expect(Op.CREDIT);
            assertEquals(1, credit.getLong("credit"));
        }
    }

    @Test
    void nextAsyncAndIteration() throws Exception {
        try (FakeBroker b = new FakeBroker(); Client c = b.connect()) {
            CompletableFuture<Subscription> sf = c.subscribeAsync("jobs", new SubscribeOptions().noAck(true));
            Frame s = b.expect(Op.SUBSCRIBE);
            b.ok(s);
            Subscription sub = sf.get(5, TimeUnit.SECONDS);
            CompletableFuture<Message> abandoned = sub.nextAsync();
            CompletableFuture<Message> waiting = sub.nextAsync();
            abandoned.cancel(false);
            b.send(Op.DELIVER, map("sub", s.getLong("sub"), "tag", 1), "a".getBytes(StandardCharsets.UTF_8));
            assertEquals("a", waiting.get(5, TimeUnit.SECONDS).text());
            b.send(Op.DELIVER, map("sub", s.getLong("sub"), "tag", 2), "b".getBytes(StandardCharsets.UTF_8));
            b.send(Op.CANCELLED, map("sub", s.getLong("sub"), "code", 404, "msg", "queue 'jobs' was deleted"));
            StringBuilder seen = new StringBuilder();
            for (Message m : sub) {
                seen.append(m.text());
            }
            assertEquals("b", seen.toString());
            assertFalse(sub.isActive());
            assertNull(sub.next(Duration.ofMillis(10)));
            assertNull(sub.nextAsync().get(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void callbackSubscriptionsRunTheHandler() throws Exception {
        try (FakeBroker b = new FakeBroker(); Client c = b.connect()) {
            CompletableFuture<String> got = new CompletableFuture<>();
            CompletableFuture<Subscription> sf = CompletableFuture.supplyAsync(
                    () -> c.subscribe("jobs", new SubscribeOptions(), m -> {
                        if (m.text().equals("boom")) {
                            throw new IllegalStateException("handler failure");
                        }
                        m.ack();
                        got.complete(m.text());
                    }));
            Frame s = b.expect(Op.SUBSCRIBE);
            b.ok(s);
            Subscription sub = sf.get(5, TimeUnit.SECONDS);
            b.send(Op.DELIVER, map("sub", s.getLong("sub"), "tag", 1), "boom".getBytes(StandardCharsets.UTF_8));
            Frame nack = b.expect(Op.NACK);
            assertTrue(nack.getBool("requeue"));
            b.send(Op.DELIVER, map("sub", s.getLong("sub"), "tag", 2), "fine".getBytes(StandardCharsets.UTF_8));
            assertEquals("fine", got.get(5, TimeUnit.SECONDS));
            assertEquals(2, b.expect(Op.ACK).getLong("tag"));

            CompletableFuture<Void> cancel = sub.cancelAsync();
            Frame u = b.expect(Op.UNSUBSCRIBE);
            assertEquals(s.getLong("sub"), u.getLong("sub"));
            b.ok(u);
            cancel.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void getReturnsAMessageOrNull() throws Exception {
        try (FakeBroker b = new FakeBroker(); Client c = b.connect()) {
            CompletableFuture<Message> empty = c.getAsync("q", true);
            Frame g = b.expect(Op.GET);
            assertTrue(g.getBool("no_ack"));
            b.send(Op.OK, map("rid", g.getLong("rid"), "empty", true));
            assertNull(empty.get(5, TimeUnit.SECONDS));

            CompletableFuture<Message> one = c.getAsync("q", false);
            Frame g2 = b.expect(Op.GET);
            b.send(Op.GET_OK, map("rid", g2.getLong("rid"), "tag", 9, "key", "q", "remaining", 3, "redelivered", true),
                    "work".getBytes(StandardCharsets.UTF_8));
            Message m = one.get(5, TimeUnit.SECONDS);
            assertEquals("work", m.text());
            assertEquals(9, m.deliveryTag());
            assertTrue(m.redelivered());
            m.ack();
            assertEquals(9, b.expect(Op.ACK).getLong("tag"));
        }
    }

    @Test
    void pingIsAnsweredWithPong() throws Exception {
        try (FakeBroker b = new FakeBroker(); Client c = b.connect()) {
            b.send(Op.PING, Map.of());
            assertEquals(Op.PONG, b.read().op());
        }
    }

    @Test
    void brokerCloseFailsPendingWork() throws Exception {
        try (FakeBroker b = new FakeBroker(); Client c = b.connect()) {
            CompletableFuture<QueueInfo> q = c.declareQueueAsync("jobs", new QueueOptions());
            CompletableFuture<Void> p = c.publishAsync("", "jobs", "x");
            b.expect(Op.DECLARE_QUEUE);
            b.expect(Op.PUBLISH);
            b.send(Op.CLOSE, map("code", 320, "msg", "shutting down"));
            assertEquals(Op.CLOSE, b.read().op());
            Throwable e = failure(q);
            assertInstanceOf(ConnectionClosedException.class, e);
            assertEquals(320, ((ConnectionClosedException) e).code());
            assertInstanceOf(ConnectionClosedException.class, failure(p));
            assertTrue(c.isClosed());
            assertThrows(ConnectionClosedException.class, () -> c.publish("", "jobs", "y"));
        }
    }

    @Test
    void clientCloseWaitsForTheBrokersClose() throws Exception {
        try (FakeBroker b = new FakeBroker()) {
            Client c = b.connect();
            CompletableFuture<Void> closing = CompletableFuture.runAsync(c::close);
            Frame close = b.expect(Op.CLOSE);
            assertEquals(200, close.getLong("code"));
            b.send(Op.CLOSE, map("code", 200));
            closing.get(5, TimeUnit.SECONDS);
            assertTrue(c.isClosed());
            c.close(); // idempotent
        }
    }

    @Test
    void fatalErrorsCloseTheConnection() throws Exception {
        try (FakeBroker b = new FakeBroker(); Client c = b.connect()) {
            b.send(Op.ERROR, map("code", 501, "msg", "FRAME_ERROR", "fatal", true));
            CompletableFuture<QueueInfo> q = c.declareQueueAsync("jobs", new QueueOptions());
            Throwable e = failure(q);
            assertInstanceOf(ConnectionClosedException.class, e);
            assertEquals(501, ((FireMQException) e).code());
        }
    }

    @Test
    void oversizedMessagesAreRefusedLocally() throws Exception {
        try (FakeBroker b = new FakeBroker(); Client c = b.connect()) {
            FireMQException e = assertThrows(FireMQException.class, () -> c.publish("", "q", new byte[2 << 20]));
            assertTrue(e.getMessage().contains("too large"), e.getMessage());
            // The sequence number was not used up.
            c.publishAsync("", "q", "small");
            assertEquals(1, b.expect(Op.PUBLISH).getLong("seq"));
        }
    }

    @Test
    void idleClientsSendPingsAndDetectDeadBrokers() throws Exception {
        try (FakeBroker b = new FakeBroker(); Client c = b.connect(1)) {
            assertEquals(Op.PING, b.readRaw().op());
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
            while (!c.isClosed() && System.nanoTime() < deadline) {
                Thread.sleep(50);
            }
            assertTrue(c.isClosed(), "the client should give up on a silent broker after 2 heartbeats");
        }
    }

    @Test
    void webSocketUrlsAreRefused() {
        FireMQException e = assertThrows(FireMQException.class, () -> FireMQ.connect("ws://localhost:15672/fire"));
        assertTrue(e.getMessage().contains("WebSocket"));
    }

    @Test
    void unreachableBrokersFailFast() {
        FireMQException e = assertThrows(FireMQException.class,
                () -> FireMQ.connect(ConnectOptions.fromUrl("fire://127.0.0.1:1").connectTimeout(Duration.ofSeconds(2))));
        assertTrue(e.getMessage().contains("127.0.0.1:1"), e.getMessage());
    }
}
