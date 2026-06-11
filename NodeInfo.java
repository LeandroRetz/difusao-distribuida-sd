public class NodeInfo {
    public String id;
    public String host;
    public int port;

    public NodeInfo(String id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }

    @Override
    public String toString() {
        return id + "@" + host + ":" + port;
    }
}
