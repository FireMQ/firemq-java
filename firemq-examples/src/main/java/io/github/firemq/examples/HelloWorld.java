package io.github.firemq.examples;

import io.github.firemq.Client;
import io.github.firemq.FireMQ;
import io.github.firemq.Message;
import io.github.firemq.Subscription;
import java.time.Duration;

/** Sends one message to a queue and receives it. */
public final class HelloWorld {
    public static void main(String[] args) {
        try (Client client = FireMQ.connect(Examples.url())) {
            client.declareQueue("hello.java");
            client.purgeQueue("hello.java");

            client.publish("", "hello.java", "Hello World!");
            System.out.println(" [x] Sent 'Hello World!'");

            try (Subscription sub = client.subscribe("hello.java")) {
                Message msg = sub.next(Duration.ofSeconds(5));
                if (msg == null) {
                    throw new IllegalStateException("no message arrived");
                }
                System.out.println(" [x] Received '" + msg.text() + "'");
                msg.ack();
            }
        }
    }
}
