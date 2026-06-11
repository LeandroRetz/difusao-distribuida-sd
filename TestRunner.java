import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Executa cenarios automatizados para validacao da atividade.
 */
public class TestRunner {

    public static void main(String[] args) throws Exception {
        Path dir = Path.of(".").toAbsolutePath().normalize();
        killExisting();

        List<Process> processes = new ArrayList<>();
        processes.add(startNode(dir, "nodo1"));
        Thread.sleep(1500);
        processes.add(startNode(dir, "nodo2"));
        Thread.sleep(1500);
        processes.add(startNode(dir, "nodo3"));
        Thread.sleep(3000);

        System.out.println("=== Cenario 1: Operacao Normal ===");
        send(processes.get(0), "send Msg-A de nodo1");
        Thread.sleep(500);
        send(processes.get(1), "send Msg-B de nodo2");
        Thread.sleep(500);
        send(processes.get(2), "send Msg-C de nodo3");
        Thread.sleep(4000);
        printDelivers(dir, "nodo1");
        printDelivers(dir, "nodo2");
        printDelivers(dir, "nodo3");

        System.out.println("\n=== Cenario 2: Resiliencia a Omissao ===");
        send(processes.get(1), "drop");
        send(processes.get(0), "send Omissao-1");
        send(processes.get(2), "send Omissao-2");
        Thread.sleep(6000);
        printDelivers(dir, "nodo1");

        System.out.println("\n=== Cenario 3: Queda do Lider ===");
        send(processes.get(0), "send Antes-da-queda");
        Thread.sleep(1000);
        processes.get(0).destroyForcibly();
        Thread.sleep(1000);
        send(processes.get(1), "send Apos-queda-lider");
        Thread.sleep(5000);
        printDelivers(dir, "nodo2");
        printDelivers(dir, "nodo3");

        System.out.println("\n=== Cenario 4: Atraso Temporario ===");
        send(processes.get(2), "delay");
        send(processes.get(2), "send Atrasada");
        Thread.sleep(500);
        send(processes.get(1), "send Nova-1");
        send(processes.get(1), "send Nova-2");
        Thread.sleep(12000);
        printDelivers(dir, "nodo2");

        for (Process p : processes) {
            if (p.isAlive()) {
                p.destroyForcibly();
            }
        }
        System.out.println("\nTestes concluidos. Verifique logs/test_*.log");
    }

    private static Process startNode(Path dir, String nodeId) throws IOException {
        Path log = dir.resolve("logs/test_" + nodeId + ".log");
        Files.createDirectories(dir.resolve("logs"));
        ProcessBuilder pb = new ProcessBuilder(
            "java", "DistributedNode", "config.json", nodeId
        );
        pb.directory(dir.toFile());
        pb.redirectInput(ProcessBuilder.Redirect.PIPE);
        pb.redirectOutput(log.toFile());
        pb.redirectError(ProcessBuilder.Redirect.appendTo(log.toFile()));
        Process process = pb.start();
        if (!process.isAlive()) {
            throw new IllegalStateException("No " + nodeId + " encerrou na inicializacao.");
        }
        return process;
    }

    private static void send(Process process, String command) throws IOException {
        if (!process.isAlive()) {
            throw new IOException("Processo encerrado antes de enviar: " + command);
        }
        process.getOutputStream().write((command + "\n").getBytes());
        process.getOutputStream().flush();
        System.out.println(">> " + command);
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void printDelivers(Path dir, String nodeId) throws IOException {
        Path log = dir.resolve("logs/test_" + nodeId + ".log");
        if (!Files.exists(log)) {
            System.out.println(nodeId + ": log nao encontrado");
            return;
        }
        System.out.println("--- DELIVER " + nodeId + " ---");
        Files.lines(log)
            .filter(l -> l.contains("[DELIVER]"))
            .forEach(System.out::println);
    }

    private static void killExisting() throws Exception {
        Process p = new ProcessBuilder("pkill", "-f", "DistributedNode config.json").start();
        p.waitFor(2, TimeUnit.SECONDS);
        Thread.sleep(1000);
    }
}
