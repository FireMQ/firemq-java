package io.github.firemq.examples;

import io.github.firemq.Client;
import io.github.firemq.ExchangeOptions;
import io.github.firemq.FireMQ;
import io.github.firemq.QueueOptions;
import io.github.firemq.Subscription;
import java.util.List;
import java.util.Map;

/** Routes log lines by severity through a direct exchange. */
public final class Routing {
    public static void main(String[] args) {
        Map<String, String> logs = Map.of(
                "info", "service started",
                "warning", "disk at 80%",
                "error", "payment failed");
        try (Client client = FireMQ.connect(Examples.url())) {
            client.declareExchange("direct_logs.java", "direct", new ExchangeOptions());

            // One consumer writes errors to disk; the other prints everything.
            String errors = client.declareQueue("", QueueOptions.temporary()).name();
            client.bindQueue(errors, "direct_logs.java", "error");
            String all = client.declareQueue("", QueueOptions.temporary()).name();
            for (String severity : List.of("info", "warning", "error")) {
                client.bindQueue(all, "direct_logs.java", severity);
            }
            Subscription errorSub = client.subscribe(errors);
            Subscription allSub = client.subscribe(all);

            for (String severity : List.of("info", "warning", "error")) {
                client.publish("direct_logs.java", severity, logs.get(severity));
                System.out.println(" [x] Sent " + severity + ": '" + logs.get(severity) + "'");
            }

            System.out.println(" [errors] Received " + Examples.take(errorSub, 1));
            System.out.println(" [all] Received " + Examples.take(allSub, 3));
            errorSub.cancel();
            allSub.cancel();
        }
    }
}
