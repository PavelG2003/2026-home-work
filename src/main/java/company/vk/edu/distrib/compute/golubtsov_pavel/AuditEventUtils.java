package company.vk.edu.distrib.compute.golubtsov_pavel;

import company.vk.edu.distrib.compute.AuditEvent;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

final class AuditEventUtils {
    private static final String SEPARATOR = "\t";

    private AuditEventUtils() {
    }

    static String encode(AuditEvent event) {
        return event.method()
                + SEPARATOR
                + Base64.getEncoder().encodeToString(event.id().getBytes(StandardCharsets.UTF_8))
                + SEPARATOR
                + event.timestamp();
    }

    static AuditEvent decode(String data) {
        String[] parts = data.split(SEPARATOR, 3);
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid audit event: " + data);
        }

        String id = new String(Base64.getDecoder().decode(parts[1]), StandardCharsets.UTF_8);
        return new AuditEvent(parts[0], id, Long.parseLong(parts[2]));
    }
}
