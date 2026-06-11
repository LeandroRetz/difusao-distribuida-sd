import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class NetworkManager implements AutoCloseable {

    private final String nodeId;
    private final NodeLogger logger;
    private final Map<String, PrintWriter> writers = new ConcurrentHashMap<>();
    private final Set<String> connectedPeers = ConcurrentHashMap.newKeySet();
    private final LinkedBlockingQueue<String> outboundQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<String> inboundQueue = new LinkedBlockingQueue<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final Thread sendThread;
    private ServerSocket serverSocket;
    private volatile boolean running = true;
    private volatile boolean simulateDelay = false;
    private volatile boolean simulateLoss = false;

    public NetworkManager(String nodeId, int localPort, NodeLogger logger) throws IOException {
        this.nodeId = nodeId;
        this.logger = logger;
        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(localPort));
        this.sendThread = new Thread(this::sendLoop, nodeId + "-send");
    }

    public void start() {
        executor.submit(this::acceptLoop);
        sendThread.start();
        logger.log("INIT", "Servidor Socket ativo na porta " + serverSocket.getLocalPort());
    }

    public void connectTo(NodeInfo peer) {
        if (peer.id.equals(nodeId) || nodeId.compareTo(peer.id) > 0) {
            return;
        }
        executor.submit(() -> connectLoop(peer));
    }

    private void connectLoop(NodeInfo peer) {
        while (running) {
            if (connectedPeers.contains(peer.id)) {
                return;
            }
            try {
                Socket socket = new Socket();
                socket.connect(new InetSocketAddress(peer.host, peer.port), 3000);
                socket.setTcpNoDelay(true);
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                setupConnection(peer.id, socket, true);
                readLoop(peer.id, reader);
                return;
            } catch (IOException e) {
                sleep(1000);
            }
        }
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                socket.setTcpNoDelay(true);
                executor.submit(() -> acceptConnection(socket));
            } catch (IOException e) {
                if (running) {
                    logger.log("ERROR", "Falha no accept: " + e.getMessage());
                }
            }
        }
    }

    private void acceptConnection(Socket socket) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String peerId = reader.readLine();
            if (peerId == null || peerId.isBlank()) {
                socket.close();
                return;
            }
            peerId = peerId.trim();
            if (connectedPeers.contains(peerId)) {
                socket.close();
                return;
            }
            setupConnection(peerId, socket, false);
            readLoop(peerId, reader);
        } catch (IOException e) {
            if (running) {
                logger.log("NETWORK", "Accept encerrado: " + e.getMessage());
            }
        }
    }

    private void setupConnection(String peerId, Socket socket, boolean outbound) throws IOException {
        PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
        if (outbound) {
            writer.println(nodeId);
            writer.flush();
        }
        writers.put(peerId, writer);
        connectedPeers.add(peerId);
        logger.log("NETWORK", (outbound ? "Conectado a " : "Peer ") + peerId
            + (outbound ? "" : " conectou") + " [peers=" + connectedPeers.size() + "]");
    }

    private void readLoop(String peerId, BufferedReader reader) {
        try {
            String line;
            while (running && (line = reader.readLine()) != null) {
                if (!line.isBlank()) {
                    inboundQueue.put(line);
                }
            }
        } catch (Exception e) {
            if (running) {
                logger.log("NETWORK", "Leitura com " + peerId + " encerrada.");
            }
        } finally {
            writers.remove(peerId);
            connectedPeers.remove(peerId);
            if (running) {
                executor.submit(() -> reconnect(peerId));
            }
        }
    }

    private void reconnect(String peerId) {
        sleep(1500);
        // Reconexao e tratada pelo connectLoop inicial; peers reiniciam manualmente no laboratorio.
    }

    private void sendLoop() {
        while (running) {
            try {
                String json = outboundQueue.poll(200, TimeUnit.MILLISECONDS);
                if (json == null) {
                    continue;
                }
                if (simulateDelay) {
                    int delay = 2000 + (int) (Math.random() * 3000);
                    logger.log("DELAY", "Injetando atraso de " + delay + "ms no envio.");
                    Thread.sleep(delay);
                }
                for (Map.Entry<String, PrintWriter> entry : writers.entrySet()) {
                    entry.getValue().println(json);
                    if (entry.getValue().checkError()) {
                        logger.log("NETWORK", "Erro ao enviar para " + entry.getKey());
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    public void enqueueSend(String json) {
        outboundQueue.offer(json);
    }

    public void sendTo(String peerId, String json) {
        PrintWriter writer = writers.get(peerId);
        if (writer != null) {
            writer.println(json);
        }
    }

    public String pollInbound(long timeoutMs) throws InterruptedException {
        return inboundQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public boolean isPeerConnected(String peerId) {
        return connectedPeers.contains(peerId);
    }

    public int connectedCount() {
        return connectedPeers.size();
    }

    public void setSimulateDelay(boolean simulateDelay) {
        this.simulateDelay = simulateDelay;
    }

    public boolean isSimulateDelay() {
        return simulateDelay;
    }

    public void setSimulateLoss(boolean simulateLoss) {
        this.simulateLoss = simulateLoss;
    }

    public boolean isSimulateLoss() {
        return simulateLoss;
    }

    public boolean shouldDropPacket() {
        return simulateLoss && Math.random() < 0.20;
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        running = false;
        try {
            serverSocket.close();
        } catch (IOException ignored) {
        }
        executor.shutdownNow();
    }
}
