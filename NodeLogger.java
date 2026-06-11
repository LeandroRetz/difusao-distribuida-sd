import java.text.SimpleDateFormat;
import java.util.Date;

public final class NodeLogger {

    private final String nodeId;
    private final SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");

    public NodeLogger(String nodeId) {
        this.nodeId = nodeId;
    }

    public synchronized void log(String state, String message) {
        String time = format.format(new Date());
        System.out.printf("[%s] [%s] [%s] -> %s%n", time, nodeId, state, message);
    }
}
