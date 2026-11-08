# firemq-java

Java client for [FireMQ](https://github.com/FireMQ/firemq), with Kotlin coroutine extensions.

The client speaks the [FireMQ protocol](https://github.com/FireMQ/firemq/blob/main/spec/fire-protocol.md) over TCP (`fire://`) and TLS (`fires://`). It passes all 19 scenarios of the FireMQ SDK conformance suite.

Defaults favour safety: queues and exchanges are durable, messages are persistent, and `publish` returns only after the broker confirms the message.

## Install

Java 17 or later. The client depends only on `org.msgpack:msgpack-core`.

Maven:

```xml
<dependency>
  <groupId>io.github.firemq</groupId>
  <artifactId>firemq-client</artifactId>
  <version>0.1.0</version>
</dependency>

<!-- Optional: Kotlin coroutine extensions -->
<dependency>
  <groupId>io.github.firemq</groupId>
  <artifactId>firemq-kotlin</artifactId>
  <version>0.1.0</version>
</dependency>
```

Gradle (Kotlin DSL):

```kotlin
dependencies {
    implementation("io.github.firemq:firemq-client:0.1.0")
    implementation("io.github.firemq:firemq-kotlin:0.1.0") // optional
}
```

## Quickstart

Start a broker with `firemq --dev`, then:

```java
import io.github.firemq.*;

try (Client client = FireMQ.connect("fire://guest:guest@localhost:5673")) {
    client.declareQueue("jobs");
    client.publish("", "jobs", "hello");   // returns when the broker confirms

    try (Subscription jobs = client.subscribe("jobs")) {
        for (Message msg : jobs) {
            System.out.println(msg.text());
            msg.ack();
        }
    }
}
```

The same in Kotlin, with coroutines:

```kotlin
import io.github.firemq.kotlin.*
import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
    val client = connectAwait("fire://guest:guest@localhost:5673")
    client.declareQueueAwait("jobs")
    client.publishAwait("", "jobs", "hello")

    client.subscribeFlow("jobs").collect { msg ->
        println(msg.text())
        msg.ack()
    }
}
```

## Connecting

```
fire://user:password@host:5673/vhost
fires://user:password@host:5674/vhost
```

The user and password default to `guest`, the vhost to `/`, and the port to 5673 (`fire`) or 5674 (`fires`). The vhost is the URL path, percent-decoded: `fire://host/prod%2Feu` connects to vhost `prod/eu`. WebSocket URLs (`ws://`, `wss://`) parse, but the Java client refuses them; WebSocket is for browsers.

For more control, use `ConnectOptions`:

```java
Client client = FireMQ.connect(ConnectOptions.fromUrl("fire://localhost")
        .heartbeat(10)                         // seconds; 0 disables heartbeats. Default 30
        .connectionName("billing-worker")      // shown in the management UI
        .connectTimeout(Duration.ofSeconds(5)));
```

The client sends a ping when it has sent nothing for half the negotiated heartbeat interval, and closes the connection when the broker has been silent for twice the interval.

A `Client` is thread-safe. Share one per application and close it when done.

## API

Every blocking method has an `...Async` variant that returns a `CompletableFuture`. Those futures complete on a callback pool, never on the connection's I/O thread, so it is safe to block in their callbacks.

### Queues and exchanges

```java
QueueInfo q = client.declareQueue("jobs");                        // durable
QueueInfo tmp = client.declareQueue("", QueueOptions.temporary()); // server-named, exclusive, auto-delete
client.declareQueue("orders", new QueueOptions()
        .arg("x-message-ttl", 60_000)
        .arg("x-dead-letter-exchange", "dlx"));
long purged = client.purgeQueue("jobs");
long deleted = client.deleteQueue("jobs");

client.declareExchange("logs", "topic");                          // direct, fanout, topic or headers
client.declareExchange("events", "fanout", new ExchangeOptions().durable(false));
client.deleteExchange("events");

client.bindQueue("orders", "logs", "order.*");
client.bindQueue("pdfs", "docs", "", Map.of("x-match", "all", "format", "pdf"));
client.unbindQueue("orders", "logs", "order.*");
client.bindExchange("audit", "logs", "#");                        // destination, source, key
```

`QueueInfo` carries the queue's name (the generated one for server-named queues), its ready message count and its consumer count.

### Publishing

```java
client.publish("", "jobs", "hello");                 // default exchange routes to the queue named by the key
client.publish("logs", "order.created", jsonBytes, new PublishOptions()
        .mandatory(true)                              // fail if no queue receives it
        .properties(new MessageProperties()
                .contentType("application/json")
                .correlationId("req-1")
                .expiration(30_000L))                // milliseconds
        .header("trace", "abc")
        .header("attempt", 2));

CompletableFuture<Void> confirmed = client.publishAsync("", "jobs", "hello");
```

`publish` blocks until the broker confirms the message: written to disk when it is persistent and routed to a durable queue, held in memory otherwise. `publishAsync` returns at once; its future completes on the confirm. Set `PublishOptions.confirm(false)` to skip confirms.

`PublishOptions.persistent` defaults to true. Header values can be `null`, booleans, integers, floats, strings, `byte[]`, lists and maps; `Instant` is sent as seconds since the epoch.

While the broker pauses publishers because of a memory or disk alarm, `publish` waits until it resumes them. `client.isPaused()` reports the state.

### Consuming

```java
try (Subscription sub = client.subscribe("jobs", new SubscribeOptions().prefetch(50))) {
    Message msg = sub.next(Duration.ofSeconds(5));   // null on timeout
    if (msg != null) {
        handle(msg.body());
        msg.ack();                                  // or msg.nack(true) to requeue
    }
}
```

A subscription buffers at most `prefetch` messages (default 100). The client grants that much credit when it subscribes and grants more in batches of half the window as you acknowledge or reject messages. With `noAck(true)` the broker treats messages as acknowledged when it sends them, and credit is granted as your code takes them from the subscription.

There are four ways to take messages:

- `next()` blocks until a message arrives or the subscription ends, then returns `null`.
- `next(Duration)` returns `null` on timeout too; `isActive()` tells the two apart.
- `nextAsync()` returns a `CompletableFuture<Message>`.
- Iterating with `for (Message m : sub)` stops when the subscription ends.

To run a handler on a dedicated thread instead:

```java
Subscription sub = client.subscribe("jobs", new SubscribeOptions(), msg -> {
    process(msg);
    msg.ack();
});
// later
sub.cancel();
```

If the handler throws, the client logs the error and nacks the message with requeue.

A subscription ends when you cancel or close it, when the broker cancels it (for example because the queue was deleted), or when the connection closes. Messages already buffered can still be taken, and unacknowledged messages can still be acknowledged until the connection closes. The broker requeues unacknowledged messages when the connection closes.

`get` takes a single message, or returns `null` when the queue is empty:

```java
Message msg = client.get("jobs", false);   // noAck = false: acknowledge it yourself
```

### Messages

`Message` exposes `body()`, `text()` (UTF-8), `exchange()`, `routingKey()`, `properties()`, `headers()`, `redelivered()` and `deliveryTag()`. In headers, integers decode as `Long`, binary as `byte[]`, arrays as `List` and tables as `Map`.

`ack()` and `nack(requeue)` settle a message once; later calls do nothing. A message nacked without requeue goes to the queue's dead-letter exchange, if it has one, and is dropped otherwise.

## Kotlin

`firemq-kotlin` adds suspending versions of the client methods and `Flow` subscriptions. Each `...Await` function suspends instead of blocking a thread and throws the same exceptions as the Java method.

| Java | Kotlin |
|---|---|
| `FireMQ.connect(url)` | `connectAwait(url)` |
| `client.declareQueue(name, opts)` | `client.declareQueueAwait(name, opts)` |
| `client.publish(exchange, key, body, opts)` | `client.publishAwait(exchange, key, body, opts)` |
| `client.get(queue, noAck)` | `client.getAwait(queue, noAck)` |
| `client.subscribe(queue, opts)` | `client.subscribeFlow(queue, opts)` returns `Flow<Message>` |
| `sub.next()` | `sub.nextAwait()` |
| `client.close()` | `client.closeAwait()` |

`subscribeFlow` subscribes when collected and cancels the subscription when the collector stops:

```kotlin
val first10 = client.subscribeFlow("jobs", SubscribeOptions().prefetch(10))
    .take(10)
    .onEach { it.ack() }
    .toList()
```

## Errors

All exceptions extend `FireMQException`, which is unchecked. `code()` returns the AMQP reply code when the broker sent one, and 0 otherwise.

| Exception | When | `code()` |
|---|---|---|
| `ServerException` | The broker refused a request: missing queue (404), redeclaring with other properties (406), bad credentials (403), and so on. | the reply code |
| `UnroutableException` | A `mandatory` publish reached no queue. | 312 |
| `NackedException` | The broker rejected a publish, for example because a `reject-publish` queue was full. | usually 406 |
| `ConnectionClosedException` | The connection is closed, by you, by the broker or by a network failure. Pending requests and confirms fail with it. | the broker's code, if any |
| `FireMQException` | I/O, TLS and protocol errors. | 0 |

A `ServerException` for one request leaves the connection usable. Invalid URLs throw `IllegalArgumentException`.

## TLS

Use a `fires://` URL. The client verifies the broker's certificate against the JVM's trusted CAs and checks the host name.

```java
// A broker with a certificate from a public CA
Client client = FireMQ.connect("fires://app:secret@mq.example.com");

// A self-signed or private CA
byte[] ca = Files.readAllBytes(Path.of("ca.pem"));
Client client = FireMQ.connect(ConnectOptions.fromUrl("fires://mq.internal").tlsCaPem(ca));

// Your own SSLContext, for example with a client certificate
Client client = FireMQ.connect(ConnectOptions.fromUrl("fires://mq.internal").sslContext(sslContext));

// Local development only: skip certificate and host name checks
Client client = FireMQ.connect(ConnectOptions.fromUrl("fires://localhost").tlsInsecure(true));
```

## Examples

[`firemq-examples`](firemq-examples/src/main/java/io/github/firemq/examples) has the six RabbitMQ tutorials as runnable classes: `HelloWorld`, `WorkQueues`, `PublishSubscribe`, `Routing`, `Topics` and `Rpc`. Their queues and exchanges end in `.java`. They connect to `FIREMQ_URL`, or to `fire://guest:guest@localhost:5673` when it is not set.

```bash
mvn -DskipTests package
java -cp firemq-examples/target/firemq-examples.jar io.github.firemq.examples.HelloWorld
```

## Development

The build needs JDK 21 and Maven 3.9 (the client itself targets Java 17). Without a local JDK, use Docker:

```bash
docker run --rm -v "$PWD":/w -w /w -v "$HOME/.m2":/root/.m2 maven:3.9-eclipse-temurin-21 mvn -B verify
```

| Module | Contents |
|---|---|
| `firemq-client` | The Java client (`io.github.firemq`) and the protocol codec (`io.github.firemq.protocol`). |
| `firemq-kotlin` | Coroutine extensions (`io.github.firemq.kotlin`). |
| `firemq-conformance-driver` | Driver for the FireMQ conformance suite. Not published. |
| `firemq-examples` | Runnable examples. Not published. |

`mvn verify` runs the unit tests: the codec against every frame in the spec's test vectors, and the client against a scripted in-process broker. Integration tests run too when `FIREMQ_URL` is set:

```bash
firemq --ephemeral &
FIREMQ_URL=fire://guest:guest@localhost:5673 mvn -B verify
```

Set `FIREMQ_TLS_URL` (a `fires://` URL) and `FIREMQ_TLS_CA` (a PEM file) to include the TLS test.

To run the conformance suite, build the driver and point the FireMQ runner at it:

```bash
mvn -B -DskipTests package
firemq-conformance run --driver "java -cp firemq-conformance-driver/target/firemq-conformance-driver.jar io.github.firemq.conformance.Driver"
```

## Releasing

Pushing a tag such as `v0.1.0` runs `.github/workflows/publish.yml`, which deploys `firemq-parent`, `firemq-client` and `firemq-kotlin` to Maven Central through the Sonatype Central Portal. The tag must match the version in `pom.xml`. To publish a tag that already exists, run the workflow by hand and pass the tag.

The workflow needs four secrets and skips the deploy until all of them are set:

| Secret | Contents |
|---|---|
| `MAVEN_CENTRAL_USERNAME`, `MAVEN_CENTRAL_PASSWORD` | A Central Portal user token for the `io.github.firemq` namespace. |
| `MAVEN_GPG_PRIVATE_KEY` | The ASCII-armored private signing key. Its public key must be on a key server Central checks, such as `keyserver.ubuntu.com`. |
| `MAVEN_GPG_PASSPHRASE` | The passphrase of that key. |

The `release` profile adds the sources and javadoc jars, the GPG signatures and the Central upload. To check the release artifacts locally without signing or uploading:

```bash
mvn -B -P release -Dgpg.skip -DskipTests -DskipUpload=true deploy
```

The bundle that would be uploaded is written to `target/central-publishing/central-bundle.zip`.

## License

Apache-2.0. See [LICENSE](LICENSE).
