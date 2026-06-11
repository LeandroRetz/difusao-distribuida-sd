import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Formato JSON unificado para troca de mensagens na rede.
 */
public class Mensagem {

    public static final String BROADCAST = "BROADCAST";
    public static final String ACK = "ACK";
    public static final String NACK = "NACK";
    public static final String HEARTBEAT = "HEARTBEAT";
    public static final String SEQUENCE = "SEQUENCE";
    public static final String ELECTION = "ELECTION";
    public static final String LEADER = "LEADER";

    public String msgId;
    public String senderId;
    public String type;
    public int sequenceNumber;
    public int lamportClock;
    public String payload;
    public long timestamp;

    public Mensagem() {
    }

    public Mensagem(String msgId, String senderId, String type, int sequenceNumber,
                    int lamportClock, String payload) {
        this.msgId = msgId;
        this.senderId = senderId;
        this.type = type;
        this.sequenceNumber = sequenceNumber;
        this.lamportClock = lamportClock;
        this.payload = payload == null ? "" : payload;
        this.timestamp = System.currentTimeMillis() / 1000L;
    }

    public String toJson() {
        return String.format(
            "{\"msg_id\":\"%s\",\"sender_id\":\"%s\",\"type\":\"%s\","
                + "\"sequence_number\":%d,\"lamport_clock\":%d,"
                + "\"payload\":\"%s\",\"timestamp\":%d}",
            escape(msgId), escape(senderId), escape(type),
            sequenceNumber, lamportClock, escape(payload), timestamp
        );
    }

    public static Mensagem fromJson(String json) {
        Mensagem m = new Mensagem();
        m.msgId = extractString(json, "msg_id");
        m.senderId = extractString(json, "sender_id");
        m.type = extractString(json, "type");
        m.sequenceNumber = extractInt(json, "sequence_number");
        m.lamportClock = extractInt(json, "lamport_clock");
        m.payload = extractString(json, "payload");
        m.timestamp = extractLong(json, "timestamp");
        return m;
    }

    private static String escape(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String extractString(String json, String key) {
        Pattern p = Pattern.compile("\"" + key + "\"\\s*:\\s*\"((?:\\\\.|[^\"\\\\])*)\"");
        Matcher m = p.matcher(json);
        if (m.find()) {
            return unescape(m.group(1));
        }
        return "";
    }

    private static int extractInt(String json, String key) {
        Pattern p = Pattern.compile("\"" + key + "\"\\s*:\\s*(-?\\d+)");
        Matcher m = p.matcher(json);
        if (m.find()) {
            return Integer.parseInt(m.group(1));
        }
        return 0;
    }

    private static long extractLong(String json, String key) {
        Pattern p = Pattern.compile("\"" + key + "\"\\s*:\\s*(\\d+)");
        Matcher m = p.matcher(json);
        if (m.find()) {
            return Long.parseLong(m.group(1));
        }
        return 0L;
    }

    private static String unescape(String value) {
        return value.replace("\\\"", "\"").replace("\\\\", "\\");
    }

    @Override
    public String toString() {
        return msgId + "(" + type + ")";
    }
}
