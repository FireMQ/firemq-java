package io.github.firemq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ConnectUrlTest {
    @Test
    void defaultsFillIn() {
        ConnectUrl u = ConnectUrl.parse("fire://localhost");
        assertEquals(ConnectUrl.Transport.TCP, u.transport());
        assertEquals("localhost", u.host());
        assertEquals(5673, u.port());
        assertEquals("/", u.vhost());
        assertEquals("guest", u.user());
        assertEquals("guest", u.password());
    }

    @Test
    void emptyHostIsLocalhost() {
        assertEquals("localhost", ConnectUrl.parse("fire://").host());
        assertEquals(5673, ConnectUrl.parse("fire://:5673/").port());
    }

    @Test
    void credentialsAndVhostAreDecoded() {
        ConnectUrl u = ConnectUrl.parse("fires://app:p%40ss@mq.example.com:7000/prod%2Feu");
        assertEquals(ConnectUrl.Transport.TLS, u.transport());
        assertEquals("mq.example.com", u.host());
        assertEquals(7000, u.port());
        assertEquals("app", u.user());
        assertEquals("p@ss", u.password());
        assertEquals("prod/eu", u.vhost());
    }

    @Test
    void tlsDefaultPort() {
        assertEquals(5674, ConnectUrl.parse("fires://mq.example.com").port());
    }

    @Test
    void userWithoutPasswordKeepsDefaultPassword() {
        ConnectUrl u = ConnectUrl.parse("fire://app@localhost");
        assertEquals("app", u.user());
        assertEquals("guest", u.password());
    }

    @Test
    void vhostFromQueryWhenPathIsEmpty() {
        assertEquals("dev", ConnectUrl.parse("fire://localhost?vhost=dev").vhost());
        assertEquals("/", ConnectUrl.parse("fire://localhost/").vhost());
    }

    @Test
    void websocketUrlsUseQueryVhost() {
        ConnectUrl u = ConnectUrl.parse("ws://localhost:15672/fire?vhost=dev");
        assertEquals(ConnectUrl.Transport.WS, u.transport());
        assertEquals("/fire", u.path());
        assertEquals("dev", u.vhost());
        ConnectUrl w = ConnectUrl.parse("wss://mq.example.com");
        assertEquals(15671, w.port());
        assertEquals("/fire", w.path());
        assertEquals(15672, ConnectUrl.parse("ws://localhost").port());
    }

    @Test
    void otherSchemesAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> ConnectUrl.parse("amqp://localhost"));
        assertThrows(IllegalArgumentException.class, () -> ConnectUrl.parse("localhost:5673"));
    }

    @Test
    void badPortsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> ConnectUrl.parse("fire://localhost:abc"));
        assertThrows(IllegalArgumentException.class, () -> ConnectUrl.parse("fire://localhost:70000"));
    }

    @Test
    void ipv6HostsAreBracketedInAuthority() {
        ConnectUrl u = ConnectUrl.parse("fire://[::1]:5673");
        assertEquals("::1", u.host());
        assertEquals("[::1]:5673", u.authority());
        assertEquals(5673, ConnectUrl.parse("fire://[::1]").port());
    }

    @Test
    void hostsWithUnderscoresParse() {
        assertEquals("my_broker.internal", ConnectUrl.parse("fire://my_broker.internal:1234").host());
    }

    @Test
    void toStringHidesThePassword() {
        String s = ConnectUrl.parse("fire://app:secret@localhost/prod").toString();
        assertFalse(s.contains("secret"), s);
        assertEquals("fire://app:***@localhost:5673/prod", s);
    }

    @Test
    void invalidUrlMessagesHideThePassword() {
        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> ConnectUrl.parse("fire://app:secret@localhost:x"));
        assertFalse(e.getMessage().contains("secret"), e.getMessage());
    }
}
