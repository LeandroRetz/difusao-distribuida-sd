# Instruções de Execução — Difusão Confiável e Atômica

Sistema distribuído em **Java (Sockets)** com 3 nós configuráveis, implementando **Reliable Broadcast** e **Atomic Broadcast**.

---

## Pré-requisitos

- **Java JDK 17+** (testado com Java 21)
- **3 terminais** (ou uso dos scripts em background)
- Portas livres: `9011`, `9012`, `9013` (configuráveis em `config.json`)

Verifique a versão do Java:

```bash
java -version
javac -version
```

---

## 1. Compilar o projeto

Entre na pasta do projeto e compile todos os arquivos `.java`:

```bash
cd /home/leandro/Downloads/sd
javac *.java
```

Se a compilação for bem-sucedida, serão gerados os arquivos `.class` na mesma pasta.

---

## 2. Configuração dos nós

O arquivo `config.json` define a topologia. Exemplo padrão:

```json
{
  "nodes": [
    {"id": "nodo1", "host": "127.0.0.1", "port": 9011},
    {"id": "nodo2", "host": "127.0.0.1", "port": 9012},
    {"id": "nodo3", "host": "127.0.0.1", "port": 9013}
  ],
  "leader_id": "nodo1",
  "broadcast_mode": "ATOMIC_SEQUENCER",
  "heartbeat_interval_ms": 2000,
  "heartbeat_timeout_ms": 6000,
  "ack_timeout_ms": 3000,
  "retransmit_interval_ms": 1500
}
```

| Campo | Descrição |
|-------|-----------|
| `nodes` | Lista de nós (mínimo 3), cada um com `id`, `host` e `port` |
| `leader_id` | Nó líder inicial (modo sequenciador) |
| `broadcast_mode` | `RELIABLE`, `ATOMIC_SEQUENCER` ou `ATOMIC_LAMPORT` |
| `heartbeat_interval_ms` | Intervalo entre heartbeats do líder |
| `heartbeat_timeout_ms` | Timeout para detectar queda do líder |
| `ack_timeout_ms` | Timeout antes de retransmitir por falta de ACK |
| `retransmit_interval_ms` | Intervalo entre tentativas de retransmissão |

> **Dica:** Se alguma porta estiver em uso, altere as portas no `config.json` e reinicie os nós.

---

## 3. Executar manualmente (recomendado para demonstração)

Abra **3 terminais** separados. Em cada um:

**Terminal 1 — nodo1 (líder inicial):**
```bash
cd /home/leandro/Downloads/sd
java DistributedNode config.json nodo1
```

**Terminal 2 — nodo2:**
```bash
cd /home/leandro/Downloads/sd
java DistributedNode config.json nodo2
```

**Terminal 3 — nodo3:**
```bash
cd /home/leandro/Downloads/sd
java DistributedNode config.json nodo3
```

Aguarde alguns segundos até aparecer nos logs:
```
[HH:MM:SS] [nodoX] [NETWORK] -> Peers conectados: 2/2
[HH:MM:SS] [nodoX] [READY] -> Comandos: send <texto> | drop | delay | crash | ...
```

A partir daí, digite os comandos diretamente em cada terminal.

---

## 4. Executar com scripts (background)

```bash
cd /home/leandro/Downloads/sd
chmod +x start_nodes.sh stop_nodes.sh
./start_nodes.sh
```

Os logs ficam em:
- `logs/nodo1.log`
- `logs/nodo2.log`
- `logs/nodo3.log`

Acompanhe em tempo real:
```bash
tail -f logs/nodo1.log
tail -f logs/nodo2.log
tail -f logs/nodo3.log
```

Para encerrar todos os nós:
```bash
./stop_nodes.sh
```

> **Nota:** O script em background não aceita comandos interativos (`send`, `drop`, etc.). Para a demonstração dos cenários, use os **3 terminais manuais** ou o `TestRunner`.

---

## 5. Comandos disponíveis

Digite no terminal de qualquer nó:

| Comando | Descrição |
|---------|-----------|
| `send <texto>` | Envia uma mensagem via broadcast |
| `drop` | Liga/desliga perda artificial de 20% dos pacotes |
| `delay` | Liga/desliga atraso de 2–5 segundos no envio |
| `crash` | Simula falha por colapso (encerra o processo) |
| `mode reliable` | Ativa Difusão Confiável |
| `mode sequencer` | Ativa Difusão Atômica com sequenciador + Bully |
| `mode lamport` | Ativa Difusão Atômica com relógios de Lamport |
| `elect` | Força eleição de novo líder (Bully) |
| `status` | Exibe líder, mensagens entregues e próxima sequência |

### Exemplos

```text
send Olá, mundo!
send Mensagem concorrente do nodo2
drop
send Teste com perda de pacotes
delay
send Mensagem atrasada
mode lamport
status
crash
```

---

## 6. Formato dos logs

Cada linha segue o padrão:

```
[TIMESTAMP] [ID_NÓ] [ESTADO] -> Mensagem
```

Estados principais:

| Estado | Significado |
|--------|-------------|
| `INIT` | Nó iniciado |
| `NETWORK` | Conexão estabelecida com peer |
| `BROADCAST` | Mensagem enviada |
| `RECEIVE` | Mensagem recebida de outro nó |
| `ORDERING` | Aguardando ordenação (sequência ou Lamport) |
| `SEQUENCE` | Líder atribuiu número de sequência |
| `DELIVER` | Mensagem entregue à aplicação |
| `RETRANSMIT` | Retransmissão por acordo ou timeout de ACK |
| `OMISSION` | Pacote descartado (simulação de perda) |
| `DELAY` | Atraso artificial injetado |
| `ELECTION` / `LEADER` | Protocolo Bully em andamento |
| `TIMEOUT` | Líder não respondeu ao heartbeat |
| `CRASH` | Simulação de colapso |

Exemplo de fluxo atômico:
```
[10:15:01] [nodo2] [RECEIVE] -> Recebida nodo1_5 do nodo1
[10:15:02] [nodo2] [ORDERING] -> Aguardando numero de sequencia para nodo1_5...
[10:15:03] [nodo2] [DELIVER] -> Mensagem nodo1_5 entregue na posicao #12. Conteudo: "Teste"
```

---

## 7. Cenários de teste para a apresentação

### Cenário 1 — Operação normal

1. Inicie os 3 nós.
2. Envie mensagens concorrentes de nós diferentes:
   ```text
   # Terminal nodo1:  send Msg-1
   # Terminal nodo2:  send Msg-2
   # Terminal nodo3:  send Msg-3
   ```
3. Compare os logs `[DELIVER]` nos 3 terminais — a **ordem deve ser idêntica** em todos.

### Cenário 2 — Resiliência à omissão

1. Em um nó, ative: `drop`
2. Envie várias mensagens de nós diferentes.
3. Observe nos logs as linhas `[RETRANSMIT]` e `[OMISSION]`.
4. Confirme que todas as mensagens foram entregues **uma única vez** (sem duplicata no `DELIVER`).

### Cenário 3 — Queda do líder

1. Confirme que `nodo1` é o líder (`status`).
2. Envie uma mensagem: `send Antes-da-queda`
3. No terminal do **nodo1**, digite: `crash`
4. Nos outros nós, observe `[TIMEOUT]`, `[ELECTION]` e `[LEADER]`.
5. Envie nova mensagem de outro nó: `send Apos-queda`
6. Verifique que as mensagens pendentes foram entregues na ordem correta.

### Cenário 4 — Atraso temporário

1. No **nodo3**, ative: `delay`
2. Envie: `send Mensagem-atrasada`
3. Nos **nodo1** e **nodo2**, envie novas mensagens imediatamente.
4. Observe o estado `[ORDERING]` no nó atrasado e a reordenação no buffer antes do `[DELIVER]`.

---

## 8. Teste automatizado

Para validar todos os cenários de uma vez:

```bash
cd /home/leandro/Downloads/sd
javac *.java
java TestRunner
```

Os logs detalhados ficam em `logs/test_nodo1.log`, `logs/test_nodo2.log` e `logs/test_nodo3.log`.

---

## 9. Solução de problemas

| Problema | Solução |
|----------|---------|
| `Endereço já em uso` | Porta ocupada — mate o processo antigo ou altere as portas no `config.json` |
| Nós não se conectam | Aguarde 3–5 s após iniciar todos; verifique firewall e IPs no config |
| Ordem diferente entre nós | Confirme que todos estão no mesmo modo (`mode sequencer` ou `mode lamport`) |
| Processo não encerra | `pkill -f "DistributedNode config.json"` ou `./stop_nodes.sh` |

Verificar processos ativos:
```bash
ps aux | grep DistributedNode
```

Verificar portas em uso:
```bash
ss -tlnp | grep 901
```

---

## 10. Estrutura do projeto

```
sd/
├── config.json           # Configuração da topologia
├── DistributedNode.java  # Nó principal (protocolos + console)
├── NetworkManager.java     # Camada TCP (sockets, filas, threads)
├── Mensagem.java           # Formato JSON das mensagens
├── ConfigLoader.java       # Leitura do config.json
├── NodeInfo.java           # Dados de um nó
├── NodeLogger.java         # Formatação de logs
├── TestRunner.java         # Testes automatizados dos 4 cenários
├── start_nodes.sh          # Inicia os 3 nós em background
├── stop_nodes.sh           # Encerra os nós em background
└── INSTRUCOES.md           # Este arquivo
```

---

## Referência rápida

```bash
# Compilar
javac *.java

# Rodar (um nó por terminal)
java DistributedNode config.json nodo1
java DistributedNode config.json nodo2
java DistributedNode config.json nodo3

# Teste automatizado
java TestRunner
```

---

## Publicar no GitHub

O repositório Git já está inicializado nesta pasta. Para criar o repositório remoto e enviar o código:

```bash
cd /home/leandro/Downloads/sd
./push_github.sh
```

Na primeira execução, o script abrirá o fluxo de login do GitHub no navegador (`gh auth login`). Após autenticar, o repositório será criado em:

`https://github.com/LeandroRetz/difusao-distribuida-sd`

Para usar outro nome de repositório ou usuário:

```bash
./push_github.sh meu-repo-outro-nome MeuUsuarioGitHub
```
