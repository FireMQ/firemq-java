package io.github.firemq;

import java.util.List;

/**
 * What the broker reported in {@code welcome}.
 *
 * @param name server name, {@code firemq}
 * @param version server version
 * @param connectionId the connection's name in the management API
 * @param heartbeat negotiated heartbeat interval in seconds; 0 when disabled
 * @param maxFrame largest frame the server accepts, in bytes
 * @param capabilities capabilities the server supports, such as {@code confirms}
 */
public record ServerInfo(String name, String version, String connectionId, int heartbeat, int maxFrame,
        List<String> capabilities) {}
