import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConfigLoader {

    public enum BroadcastMode {
        RELIABLE,
        ATOMIC_SEQUENCER,
        ATOMIC_LAMPORT
    }

    public static class Config {
        public List<NodeInfo> nodes = new ArrayList<>();
        public String leaderId = "nodo1";
        public BroadcastMode broadcastMode = BroadcastMode.ATOMIC_SEQUENCER;
        public int heartbeatIntervalMs = 2000;
        public int heartbeatTimeoutMs = 6000;
        public int ackTimeoutMs = 3000;
        public int retransmitIntervalMs = 1500;
    }

    public static Config load(Path path) throws IOException {
        String json = Files.readString(path);
        Config config = new Config();

        Matcher nodeMatcher = Pattern.compile(
            "\\{\\s*\"id\"\\s*:\\s*\"([^\"]+)\"\\s*,\\s*\"host\"\\s*:\\s*\"([^\"]+)\"\\s*,\\s*\"port\"\\s*:\\s*(\\d+)\\s*\\}"
        ).matcher(json);

        while (nodeMatcher.find()) {
            config.nodes.add(new NodeInfo(
                nodeMatcher.group(1),
                nodeMatcher.group(2),
                Integer.parseInt(nodeMatcher.group(3))
            ));
        }

        config.leaderId = extractString(json, "leader_id", config.leaderId);
        String mode = extractString(json, "broadcast_mode", "ATOMIC_SEQUENCER");
        config.broadcastMode = BroadcastMode.valueOf(mode);
        config.heartbeatIntervalMs = extractInt(json, "heartbeat_interval_ms", 2000);
        config.heartbeatTimeoutMs = extractInt(json, "heartbeat_timeout_ms", 6000);
        config.ackTimeoutMs = extractInt(json, "ack_timeout_ms", 3000);
        config.retransmitIntervalMs = extractInt(json, "retransmit_interval_ms", 1500);

        if (config.nodes.size() < 3) {
            throw new IllegalArgumentException("Configuracao minima: 3 nos.");
        }
        return config;
    }

    private static String extractString(String json, String key, String defaultValue) {
        Pattern p = Pattern.compile("\"" + key + "\"\\s*:\\s*\"([^\"]+)\"");
        Matcher m = p.matcher(json);
        return m.find() ? m.group(1) : defaultValue;
    }

    private static int extractInt(String json, String key, int defaultValue) {
        Pattern p = Pattern.compile("\"" + key + "\"\\s*:\\s*(\\d+)");
        Matcher m = p.matcher(json);
        return m.find() ? Integer.parseInt(m.group(1)) : defaultValue;
    }
}
