package io.github.firemq.examples;

import io.github.firemq.Client;
import io.github.firemq.FireMQ;
import io.github.firemq.Message;
import io.github.firemq.MessageProperties;
import io.github.firemq.PublishOptions;
import io.github.firemq.QueueOptions;
import io.github.firemq.SubscribeOptions;
import io.github.firemq.Subscription;
import java.time.Duration;
import java.util.UUID;

/**
 * Request/reply: a server computes Fibonacci numbers from {@code rpc_queue.java}; the client sends
 * requests with a private reply queue and a correlation id.
 */
public final class Rpc {
    public static void main(String[] args) {
        try (Client client = FireMQ.connect(Examples.url())) {
            client.declareQueue("rpc_queue.java");
            client.purgeQueue("rpc_queue.java");

            // Server
            Subscription server = client.subscribe("rpc_queue.java", new SubscribeOptions().prefetch(1), request -> {
                int n = Integer.parseInt(request.text());
                System.out.println(" [server] fib(" + n + ")");
                MessageProperties props = new MessageProperties().correlationId(request.properties().correlationId());
                client.publish("", request.properties().replyTo(), Long.toString(fib(n)),
                        new PublishOptions().properties(props));
                request.ack();
            });

            // Client
            String replyQueue = client.declareQueue("", QueueOptions.temporary()).name();
            try (Subscription replies = client.subscribe(replyQueue, new SubscribeOptions().noAck(true))) {
                for (int n : new int[] {10, 20, 30}) {
                    String correlationId = UUID.randomUUID().toString();
                    MessageProperties props = new MessageProperties().replyTo(replyQueue).correlationId(correlationId);
                    System.out.println(" [client] Requesting fib(" + n + ")");
                    client.publish("", "rpc_queue.java", Integer.toString(n), new PublishOptions().properties(props));
                    Message reply = awaitReply(replies, correlationId);
                    System.out.println(" [client] Got " + reply.text());
                }
            }
            server.cancel();
        }
    }

    private static Message awaitReply(Subscription replies, String correlationId) {
        while (true) {
            Message m = replies.next(Duration.ofSeconds(10));
            if (m == null) {
                throw new IllegalStateException("no reply for " + correlationId);
            }
            if (correlationId.equals(m.properties().correlationId())) {
                return m;
            }
        }
    }

    private static long fib(int n) {
        return n < 2 ? n : fib(n - 1) + fib(n - 2);
    }
}
