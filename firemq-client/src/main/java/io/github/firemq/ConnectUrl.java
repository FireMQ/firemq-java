package io.github.firemq;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Objects;

/**
 * A parsed {@code fire://}, {@code fires://}, {@code ws://} or {@code wss://} connection URL.
 *
 * <pre>
 * fire://user:password@host:5673/vhost
 * </pre>
 *
 * Defaults: user and password {@code guest}, vhost {@code /}, ports 5673 ({@code fire}), 5674
 * ({@code fires}), 15672 ({@code ws}) and 15671 ({@code wss}). For {@code fire} and {@code fires}
 * the path is the percent-decoded vhost. For {@code ws} and {@code wss} the path is the WebSocket
 * endpoint ({@code /fire} by default) and the vhost is the {@code vhost} query parameter.
 */
public final class ConnectUrl {
    /** How a client reaches the broker. */
    public enum Transport {
        TCP,
        TLS,
        WS,
        WSS
    }

    public static final int DEFAULT_PORT = 5673;
    public static final int DEFAULT_TLS_PORT = 5674;
    public static final int DEFAULT_WS_PORT = 15672;
    public static final int DEFAULT_WSS_PORT = 15671;

    private final Transport transport;
    private final String host;
    private final int port;
    private final String vhost;
    private final String user;
    private final String password;
    private final String path;

    public ConnectUrl(Transport transport, String host, int port, String vhost, String user, String password, String path) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
        this.vhost = Objects.requireNonNull(vhost, "vhost");
        this.user = Objects.requireNonNull(user, "user");
        this.password = Objects.requireNonNull(password, "password");
        this.path = path == null ? "" : path;
    }

    /**
     * Parses a connection URL and fills in defaults.
     *
     * @throws IllegalArgumentException when the URL is malformed or uses another scheme
     */
    public static ConnectUrl parse(String input) {
        Objects.requireNonNull(input, "url");
        int sep = input.indexOf("://");
        if (sep <= 0) {
            throw new IllegalArgumentException("invalid URL '" + redact(input) + "': expected scheme://host");
        }
        String scheme = input.substring(0, sep).toLowerCase(Locale.ROOT);
        Transport transport;
        int defaultPort;
        switch (scheme) {
            case "fire":
                transport = Transport.TCP;
                defaultPort = DEFAULT_PORT;
                break;
            case "fires":
                transport = Transport.TLS;
                defaultPort = DEFAULT_TLS_PORT;
                break;
            case "ws":
                transport = Transport.WS;
                defaultPort = DEFAULT_WS_PORT;
                break;
            case "wss":
                transport = Transport.WSS;
                defaultPort = DEFAULT_WSS_PORT;
                break;
            default:
                throw new IllegalArgumentException("unsupported scheme '" + scheme + "'; use fire, fires, ws or wss");
        }

        String rest = input.substring(sep + 3);
        int hash = rest.indexOf('#');
        if (hash >= 0) {
            rest = rest.substring(0, hash);
        }
        int authorityEnd = rest.length();
        for (int i = 0; i < rest.length(); i++) {
            char c = rest.charAt(i);
            if (c == '/' || c == '?') {
                authorityEnd = i;
                break;
            }
        }
        String authority = rest.substring(0, authorityEnd);
        String pathAndQuery = rest.substring(authorityEnd);
        String rawPath = pathAndQuery;
        String query = "";
        int q = pathAndQuery.indexOf('?');
        if (q >= 0) {
            rawPath = pathAndQuery.substring(0, q);
            query = pathAndQuery.substring(q + 1);
        }

        String user = "guest";
        String password = "guest";
        int at = authority.lastIndexOf('@');
        if (at >= 0) {
            String userInfo = authority.substring(0, at);
            authority = authority.substring(at + 1);
            int colon = userInfo.indexOf(':');
            String rawUser = colon >= 0 ? userInfo.substring(0, colon) : userInfo;
            if (!rawUser.isEmpty()) {
                user = percentDecode(rawUser, false);
            }
            if (colon >= 0) {
                password = percentDecode(userInfo.substring(colon + 1), false);
            }
        }

        String host;
        String portText = null;
        if (authority.startsWith("[")) {
            int close = authority.indexOf(']');
            if (close < 0) {
                throw new IllegalArgumentException("invalid URL '" + redact(input) + "': unterminated IPv6 address");
            }
            host = authority.substring(1, close);
            String after = authority.substring(close + 1);
            if (after.startsWith(":")) {
                portText = after.substring(1);
            } else if (!after.isEmpty()) {
                throw new IllegalArgumentException("invalid URL '" + redact(input) + "': unexpected text after IPv6 address");
            }
        } else {
            int colon = authority.lastIndexOf(':');
            if (colon >= 0) {
                host = authority.substring(0, colon);
                portText = authority.substring(colon + 1);
            } else {
                host = authority;
            }
        }
        if (host.isEmpty()) {
            host = "localhost";
        }
        int port = defaultPort;
        if (portText != null && !portText.isEmpty()) {
            try {
                port = Integer.parseInt(portText);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("invalid URL '" + redact(input) + "': invalid port '" + portText + "'");
            }
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("invalid URL '" + redact(input) + "': port " + port + " is out of range");
            }
        }

        String queryVhost = null;
        if (!query.isEmpty()) {
            for (String pair : query.split("&")) {
                int eq = pair.indexOf('=');
                String k = percentDecode(eq >= 0 ? pair.substring(0, eq) : pair, true);
                if (k.equals("vhost")) {
                    queryVhost = percentDecode(eq >= 0 ? pair.substring(eq + 1) : "", true);
                    break;
                }
            }
        }

        String vhost;
        String path;
        if (transport == Transport.TCP || transport == Transport.TLS) {
            String raw = rawPath.startsWith("/") ? rawPath.substring(1) : rawPath;
            if (raw.isEmpty()) {
                vhost = queryVhost != null ? queryVhost : "/";
            } else {
                vhost = percentDecode(raw, false);
            }
            path = "";
        } else {
            path = rawPath.isEmpty() || rawPath.equals("/") ? "/fire" : rawPath;
            vhost = queryVhost != null ? queryVhost : "/";
        }
        return new ConnectUrl(transport, host, port, vhost, user, password, path);
    }

    private static String percentDecode(String s, boolean plusIsSpace) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(s.length());
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < bytes.length; i++) {
            byte b = bytes[i];
            if (b == '%' && i + 2 < bytes.length) {
                int hi = Character.digit(bytes[i + 1], 16);
                int lo = Character.digit(bytes[i + 2], 16);
                if (hi >= 0 && lo >= 0) {
                    out.write(hi * 16 + lo);
                    i += 2;
                    continue;
                }
            }
            if (plusIsSpace && b == '+') {
                out.write(' ');
                continue;
            }
            out.write(b);
        }
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }

    private static String redact(String url) {
        int sep = url.indexOf("://");
        int at = url.lastIndexOf('@');
        if (sep < 0 || at < sep) {
            return url;
        }
        String userInfo = url.substring(sep + 3, at);
        int colon = userInfo.indexOf(':');
        if (colon < 0) {
            return url;
        }
        return url.substring(0, sep + 3) + userInfo.substring(0, colon) + ":***" + url.substring(at);
    }

    public Transport transport() {
        return transport;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public String vhost() {
        return vhost;
    }

    public String user() {
        return user;
    }

    public String password() {
        return password;
    }

    /** WebSocket endpoint path; empty for {@code fire} and {@code fires}. */
    public String path() {
        return path;
    }

    /** {@code host:port}, with IPv6 hosts bracketed. */
    public String authority() {
        return host.contains(":") ? "[" + host + "]:" + port : host + ":" + port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectUrl)) {
            return false;
        }
        ConnectUrl u = (ConnectUrl) o;
        return port == u.port && transport == u.transport && host.equals(u.host) && vhost.equals(u.vhost)
                && user.equals(u.user) && password.equals(u.password) && path.equals(u.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transport, host, port, vhost, user, password, path);
    }

    /** The URL with the password masked. */
    @Override
    public String toString() {
        String scheme;
        switch (transport) {
            case TLS:
                scheme = "fires";
                break;
            case WS:
                scheme = "ws";
                break;
            case WSS:
                scheme = "wss";
                break;
            default:
                scheme = "fire";
        }
        return scheme + "://" + user + ":***@" + authority()
                + (transport == Transport.TCP || transport == Transport.TLS ? "/" + (vhost.equals("/") ? "" : vhost) : path + "?vhost=" + vhost);
    }
}
