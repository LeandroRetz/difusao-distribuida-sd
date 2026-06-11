import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * No distribuido com Difusao Confiavel e Difusao Atomica (Sequenciador + Lamport).
 */
public class DistributedNode {

    private final ConfigLoader.Config config;
    private final NodeInfo self;
    private final List<NodeInfo> peers;
    private final NodeLogger logger;
    private final NetworkManager network;

    private volatile ConfigLoader.BroadcastMode mode;
    private volatile String leaderId;
    private volatile boolean electionInProgress = false;
    private volatile long lastLeaderHeartbeat = System.currentTimeMillis();

    private final AtomicInteger lamportClock = new AtomicInteger(0);
    private final AtomicInteger localMsgCounter = new AtomicInteger(0);
    private final AtomicInteger sequenceCounter = new AtomicInteger(0);
    private final AtomicInteger nextDeliverSequence = new AtomicInteger(1);
    private final AtomicInteger deliverPosition = new AtomicInteger(0);

    private final Set<String> delivered = ConcurrentHashMap.newKeySet();
    private final Set<String> seen = ConcurrentHashMap.newKeySet();
    private final Map<String, Mensagem> pendingBroadcasts = new ConcurrentHashMap<>();
    private final Map<String, Long> pendingAckDeadline = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> acksReceived = new ConcurrentHashMap<>();
    private final Map<Integer, Mensagem> sequenceBuffer = new ConcurrentHashMap<>();
    private final Map<String, Mensagem> lamportBuffer = new ConcurrentHashMap<>();
    private final Map<String, Long> lamportFirstSeen = new ConcurrentHashMap<>();
    private static final long LAMPORT_STABILITY_MS = 7000;

    private final PriorityQueue<Mensagem> lamportQueue = new PriorityQueue<>(
        Comparator.comparingInt((Mensagem m) -> m.lamportClock)
            .thenComparing(m -> m.senderId)
            .thenComparing(m -> m.msgId)
    );

    private final ExecutorService messagePool = Executors.newCachedThreadPool();
    private final AtomicBoolean running = new AtomicBoolean(true);

    public DistributedNode(ConfigLoader.Config config, String nodeId) throws Exception {
        this.config = config;
        this.mode = config.broadcastMode;
        this.leaderId = config.leaderId;
        this.self = config.nodes.stream()
            .filter(n -> n.id.equals(nodeId))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("No desconhecido: " + nodeId));
        this.peers = config.nodes.stream()
            .filter(n -> !n.id.equals(nodeId))
            .toList();
        this.logger = new NodeLogger(self.id);
        this.network = new NetworkManager(self.id, self.port, logger);
    }

    public void start() throws Exception {
        network.start();
        for (NodeInfo peer : peers) {
            network.connectTo(peer);
        }
        waitForMesh();

        new Thread(this::inboundLoop, self.id + "-inbound").start();
        new Thread(this::deliveryLoop, self.id + "-delivery").start();
        new Thread(this::heartbeatLoop, self.id + "-heartbeat").start();
        new Thread(this::retransmitLoop, self.id + "-retransmit").start();
        new Thread(this::leaderWatchdogLoop, self.id + "-watchdog").start();

        logger.log("INIT", "Modo=" + mode + " Lider=" + leaderId);
        runConsole();
    }

    private void waitForMesh() throws InterruptedException {
        int needed = peers.size();
        for (int i = 0; i < 30 && network.connectedCount() < needed; i++) {
            Thread.sleep(500);
        }
        logger.log("NETWORK", "Peers conectados: " + network.connectedCount() + "/" + needed);
    }

    private void inboundLoop() {
        while (running.get()) {
            try {
                String json = network.pollInbound(200);
                if (json != null) {
                    handleIncomingSafe(json);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void handleIncomingSafe(String json) {
        try {
            if (network.shouldDropPacket()) {
                logger.log("OMISSION", "Pacote descartado artificialmente (20%).");
                return;
            }
            Mensagem msg = Mensagem.fromJson(json);
            messagePool.submit(() -> processMessage(msg));
        } catch (Exception e) {
            logger.log("ERROR", "JSON invalido: " + e.getMessage());
        }
    }

    private void processMessage(Mensagem msg) {
        switch (msg.type) {
            case Mensagem.BROADCAST -> onBroadcast(msg);
            case Mensagem.ACK -> onAck(msg);
            case Mensagem.NACK -> onNack(msg);
            case Mensagem.SEQUENCE -> onSequence(msg);
            case Mensagem.HEARTBEAT -> onHeartbeat(msg);
            case Mensagem.ELECTION -> onElection(msg);
            case Mensagem.LEADER -> onLeaderAnnounce(msg);
            default -> logger.log("WARN", "Tipo desconhecido: " + msg.type);
        }
    }

    private void onBroadcast(Mensagem msg) {
        lamportClock.updateAndGet(c -> Math.max(c, msg.lamportClock) + 1);

        if (delivered.contains(msg.msgId)) {
            sendAck(msg);
            return;
        }

        if (seen.add(msg.msgId)) {
            logger.log("RECEIVE", "Recebida " + msg.msgId + " do " + msg.senderId);
            pendingBroadcasts.put(msg.msgId, msg);
            sendAck(msg);
            rebroadcast(msg);
        } else {
            sendAck(msg);
        }

        if (mode == ConfigLoader.BroadcastMode.RELIABLE) {
            tryDeliverReliable(msg);
            return;
        }

        if (mode == ConfigLoader.BroadcastMode.ATOMIC_LAMPORT) {
            enqueueLamport(msg);
            return;
        }

        if (mode == ConfigLoader.BroadcastMode.ATOMIC_SEQUENCER) {
            if (isLeader()) {
                assignSequence(msg);
            } else {
                logger.log("ORDERING", "Aguardando numero de sequencia para " + msg.msgId + "...");
            }
        }
    }

    private void onAck(Mensagem msg) {
        String originalId = msg.payload;
        acksReceived.computeIfAbsent(originalId, k -> ConcurrentHashMap.newKeySet())
            .add(msg.senderId);
    }

    private void onNack(Mensagem msg) {
        String originalId = msg.payload;
        Mensagem original = pendingBroadcasts.get(originalId);
        if (original != null) {
            logger.log("RETRANSMIT", "NACK recebido para " + originalId + ". Retransmitindo.");
            network.enqueueSend(original.toJson());
        }
    }

    private void onSequence(Mensagem msg) {
        if (mode != ConfigLoader.BroadcastMode.ATOMIC_SEQUENCER) {
            return;
        }
        logger.log("ORDERING", "Recebida sequencia #" + msg.sequenceNumber + " para " + msg.msgId);
        sequenceBuffer.put(msg.sequenceNumber, msg);
        sequenceCounter.updateAndGet(current -> Math.max(current, msg.sequenceNumber));
    }

    private void onHeartbeat(Mensagem msg) {
        if (msg.senderId.equals(leaderId)) {
            lastLeaderHeartbeat = System.currentTimeMillis();
        }
    }

    private void onElection(Mensagem msg) {
        logger.log("ELECTION", "Mensagem de eleicao recebida de " + msg.senderId);
        if (self.id.compareTo(msg.senderId) > 0) {
            startElection();
        } else {
            Mensagem answer = controlMessage(Mensagem.LEADER, self.id, 0, self.id);
            network.enqueueSend(answer.toJson());
        }
    }

    private void onLeaderAnnounce(Mensagem msg) {
        leaderId = msg.payload;
        electionInProgress = false;
        lastLeaderHeartbeat = System.currentTimeMillis();
        logger.log("LEADER", "Novo lider reconhecido: " + leaderId);

        int announcedSeq = msg.sequenceNumber;
        if (announcedSeq > sequenceCounter.get()) {
            sequenceCounter.set(announcedSeq);
            nextDeliverSequence.set(Math.max(nextDeliverSequence.get(), announcedSeq));
        }

        if (isLeader()) {
            logger.log("LEADER", "Assumindo sequenciamento de mensagens pendentes.");
            for (Mensagem pending : new ArrayList<>(pendingBroadcasts.values())) {
                if (sequenceBuffer.values().stream().noneMatch(m -> m.msgId.equals(pending.msgId))) {
                    assignSequence(pending);
                }
            }
        }
    }

    public void broadcast(String payload) {
        int clock = lamportClock.incrementAndGet();
        int n = localMsgCounter.incrementAndGet();
        String msgId = self.id + "_" + n;
        Mensagem msg = new Mensagem(msgId, self.id, Mensagem.BROADCAST, 0, clock, payload);

        seen.add(msgId);
        pendingBroadcasts.put(msgId, msg);
        pendingAckDeadline.put(msgId, System.currentTimeMillis() + config.ackTimeoutMs);
        acksReceived.put(msgId, ConcurrentHashMap.newKeySet());

        logger.log("BROADCAST", "Enviando " + msgId + ": \"" + payload + "\"");
        network.enqueueSend(msg.toJson());
        processMessage(msg);
    }

    private void rebroadcast(Mensagem msg) {
        logger.log("RETRANSMIT", "Retransmitindo " + msg.msgId + " para garantir acordo.");
        network.enqueueSend(msg.toJson());
    }

    private void sendAck(Mensagem original) {
        Mensagem ack = controlMessage(Mensagem.ACK, original.msgId, 0, original.msgId);
        network.sendTo(original.senderId, ack.toJson());
        network.enqueueSend(ack.toJson());
    }

    private synchronized void assignSequence(Mensagem msg) {
        if (delivered.contains(msg.msgId)) {
            return;
        }
        boolean alreadySequenced = sequenceBuffer.values().stream()
            .anyMatch(m -> m.msgId.equals(msg.msgId));
        if (alreadySequenced) {
            return;
        }

        int seq = sequenceCounter.incrementAndGet();
        Mensagem sequenced = new Mensagem(
            msg.msgId, self.id, Mensagem.SEQUENCE, seq, lamportClock.get(), msg.payload
        );
        logger.log("SEQUENCE", "Atribuido #" + seq + " para " + msg.msgId);
        sequenceBuffer.put(seq, sequenced);
        network.enqueueSend(sequenced.toJson());
        onSequence(sequenced);
    }

    private void enqueueLamport(Mensagem msg) {
        synchronized (lamportQueue) {
            if (!lamportBuffer.containsKey(msg.msgId)) {
                lamportBuffer.put(msg.msgId, msg);
                lamportFirstSeen.put(msg.msgId, System.currentTimeMillis());
                lamportQueue.add(msg);
                logger.log("ORDERING", "Aguardando ordenacao Lamport(" + msg.lamportClock + ") para " + msg.msgId + "...");
            }
        }
    }

    private void tryDeliverReliable(Mensagem msg) {
        Set<String> acks = acksReceived.getOrDefault(msg.msgId, Set.of());
        if (acks.size() >= peers.size()) {
            deliver(msg, null);
        } else {
            logger.log("ORDERING", "Aguardando ACKs (" + acks.size() + "/" + peers.size()
                + ") para " + msg.msgId + "...");
        }
    }

    private void deliveryLoop() {
        while (running.get()) {
            try {
                if (mode == ConfigLoader.BroadcastMode.ATOMIC_SEQUENCER) {
                    deliverSequencerOrder();
                } else if (mode == ConfigLoader.BroadcastMode.ATOMIC_LAMPORT) {
                    deliverLamportOrder();
                }
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void deliverSequencerOrder() {
        int expected = nextDeliverSequence.get();
        Mensagem msg = sequenceBuffer.get(expected);
        if (msg == null) {
            return;
        }
        sequenceBuffer.remove(expected);
        nextDeliverSequence.incrementAndGet();
        if (delivered.contains(msg.msgId)) {
            return;
        }
        deliver(msg, expected);
    }

    private void deliverLamportOrder() {
        synchronized (lamportQueue) {
            while (!lamportQueue.isEmpty()) {
                Mensagem next = lamportQueue.peek();
                if (next == null) {
                    break;
                }
                if (canDeliverLamport(next)) {
                    lamportQueue.poll();
                    lamportBuffer.remove(next.msgId);
                    lamportFirstSeen.remove(next.msgId);
                    deliver(next, null);
                } else {
                    break;
                }
            }
        }
    }

    private boolean canDeliverLamport(Mensagem candidate) {
        for (Mensagem other : lamportBuffer.values()) {
            if (other.msgId.equals(candidate.msgId)) {
                continue;
            }
            if (compareLamport(other, candidate) < 0) {
                return false;
            }
        }
        long age = System.currentTimeMillis() - lamportFirstSeen.getOrDefault(candidate.msgId, 0L);
        if (age < LAMPORT_STABILITY_MS) {
            return false;
        }
        return true;
    }

    private int compareLamport(Mensagem a, Mensagem b) {
        int cmp = Integer.compare(a.lamportClock, b.lamportClock);
        if (cmp != 0) {
            return cmp;
        }
        cmp = a.senderId.compareTo(b.senderId);
        if (cmp != 0) {
            return cmp;
        }
        return a.msgId.compareTo(b.msgId);
    }

    private void deliver(Mensagem msg, Integer sequencePosition) {
        if (!delivered.add(msg.msgId)) {
            return;
        }
        String seqInfo = sequencePosition != null
            ? " na posicao #" + sequencePosition
            : " na posicao #" + deliverPosition.incrementAndGet();
        logger.log("DELIVER", "Mensagem " + msg.msgId + " entregue" + seqInfo
            + ". Conteudo: \"" + msg.payload + "\"");
        pendingBroadcasts.remove(msg.msgId);
        pendingAckDeadline.remove(msg.msgId);
    }

    private void heartbeatLoop() {
        while (running.get()) {
            try {
                if (isLeader()) {
                    Mensagem hb = controlMessage(Mensagem.HEARTBEAT, "ping", 0, "alive");
                    network.enqueueSend(hb.toJson());
                    lastLeaderHeartbeat = System.currentTimeMillis();
                }
                Thread.sleep(config.heartbeatIntervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void leaderWatchdogLoop() {
        while (running.get()) {
            try {
                Thread.sleep(1000);
                if (mode != ConfigLoader.BroadcastMode.ATOMIC_SEQUENCER) {
                    continue;
                }
                long elapsed = System.currentTimeMillis() - lastLeaderHeartbeat;
                if (!isLeader() && elapsed > config.heartbeatTimeoutMs && !electionInProgress) {
                    logger.log("TIMEOUT", "Lider " + leaderId + " nao responde. Iniciando eleicao Bully.");
                    startElection();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void startElection() {
        if (electionInProgress) {
            return;
        }
        electionInProgress = true;
        logger.log("ELECTION", "Iniciando protocolo Bully a partir de " + self.id);

        List<String> higher = peers.stream()
            .map(p -> p.id)
            .filter(id -> id.compareTo(self.id) > 0)
            .sorted()
            .toList();

        if (higher.isEmpty()) {
            announceLeader();
            return;
        }

        Mensagem election = controlMessage(Mensagem.ELECTION, "election", 0, self.id);
        network.enqueueSend(election.toJson());

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (electionInProgress && higher.stream().noneMatch(id -> id.equals(leaderId))) {
            announceLeader();
        }
    }

    private void announceLeader() {
        leaderId = self.id;
        electionInProgress = false;
        lastLeaderHeartbeat = System.currentTimeMillis();
        int nextSeq = Math.max(sequenceCounter.get(), nextDeliverSequence.get() - 1);
        Mensagem leaderMsg = controlMessage(Mensagem.LEADER, "leader", nextSeq, self.id);
        network.enqueueSend(leaderMsg.toJson());
        logger.log("LEADER", "Este no assumiu a lideranca: " + self.id + " (seq=" + nextSeq + ")");
        onLeaderAnnounce(leaderMsg);
    }

    private void retransmitLoop() {
        while (running.get()) {
            try {
                long now = System.currentTimeMillis();
                for (Map.Entry<String, Long> entry : pendingAckDeadline.entrySet()) {
                    String msgId = entry.getKey();
                    if (now < entry.getValue()) {
                        continue;
                    }
                    Mensagem msg = pendingBroadcasts.get(msgId);
                    if (msg == null || delivered.contains(msgId)) {
                        pendingAckDeadline.remove(msgId);
                        continue;
                    }
                    Set<String> acks = acksReceived.getOrDefault(msgId, Set.of());
                    if (acks.size() < peers.size()) {
                        logger.log("RETRANSMIT", "Timeout ACK para " + msgId
                            + " (" + acks.size() + "/" + peers.size() + "). Retransmitindo.");
                        network.enqueueSend(msg.toJson());
                        pendingAckDeadline.put(msgId, now + config.retransmitIntervalMs);
                    } else {
                        pendingAckDeadline.remove(msgId);
                    }
                }
                Thread.sleep(config.retransmitIntervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private Mensagem controlMessage(String type, String msgId, int seq, String payload) {
        return new Mensagem(msgId, self.id, type, seq, lamportClock.get(), payload);
    }

    private boolean isLeader() {
        return self.id.equals(leaderId);
    }

    private void runConsole() {
        logger.log("READY", "Comandos: send <texto> | drop | delay | crash | mode <reliable|sequencer|lamport> | status");
        try (var scanner = new java.util.Scanner(System.in)) {
            while (running.get() && scanner.hasNextLine()) {
                String line = scanner.nextLine().trim();
                if (line.isEmpty()) {
                    continue;
                }
                if (line.startsWith("send ")) {
                    broadcast(line.substring(5).trim());
                } else if (line.equals("drop")) {
                    boolean enabled = !network.isSimulateLoss();
                    network.setSimulateLoss(enabled);
                    logger.log("TEST", "Omissao de pacotes (20%): " + enabled);
                } else if (line.equals("delay")) {
                    boolean next = !network.isSimulateDelay();
                    network.setSimulateDelay(next);
                    logger.log("TEST", "Atraso de pacotes (2-5s): " + next);
                } else if (line.equals("crash")) {
                    logger.log("CRASH", "Simulando colapso. Encerrando processo.");
                    shutdown();
                    System.exit(0);
                } else if (line.startsWith("mode ")) {
                    switch (line.substring(5).trim().toLowerCase()) {
                        case "reliable" -> mode = ConfigLoader.BroadcastMode.RELIABLE;
                        case "sequencer" -> mode = ConfigLoader.BroadcastMode.ATOMIC_SEQUENCER;
                        case "lamport" -> mode = ConfigLoader.BroadcastMode.ATOMIC_LAMPORT;
                        default -> logger.log("WARN", "Modos: reliable, sequencer, lamport");
                    }
                    logger.log("CONFIG", "Modo alterado para " + mode);
                } else if (line.equals("status")) {
                    logger.log("STATUS", "lider=" + leaderId + " entregues=" + delivered.size()
                        + " prox_seq=" + nextDeliverSequence.get());
                } else if (line.equals("elect")) {
                    startElection();
                } else {
                    logger.log("HELP", "send <texto> | drop | delay | crash | mode <...> | status | elect");
                }
            }
        }
    }

    private void shutdown() {
        running.set(false);
        messagePool.shutdownNow();
        network.close();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: java DistributedNode <config.json> <node_id>");
            System.out.println("Ex.: java DistributedNode config.json nodo1");
            return;
        }
        ConfigLoader.Config config = ConfigLoader.load(Path.of(args[0]));
        DistributedNode node = new DistributedNode(config, args[1]);
        node.start();
    }
}
