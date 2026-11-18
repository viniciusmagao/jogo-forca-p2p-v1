# Parte 1 — Comunicação e Rede (PyP2P-Forca)

Implementação em **Python 3.10+**, usando apenas a biblioteca padrão. A pasta `src/rendezvous/` do professor **não foi modificada**. Este pacote resolve a **camada de rede do cliente**; não implementa regras da forca, lobby de verdade ou interface completa do jogo.

## Arquivos e responsabilidades

| Arquivo | O que faz |
| --- | --- |
| `__init__.py` | Exporta a API pública `NetworkConfig`, `P2PNetwork` e `RendezvousClient`. |
| `protocolo.py` | Serializa um JSON por linha (`\n`, UTF-8, até 32 KiB), gera IDs UUID e valida campos. |
| `rendezvous_client.py` | Realiza `REGISTER`, `DISCOVER`, `UNREGISTER`; abre socket TCP **novo** em cada operação. |
| `peer_connection.py` | Armazena `StreamReader`/`StreamWriter` e protege escritas concorrentes. |
| `heartbeat.py` | Envia `PING`, recebe `PONG` via roteamento de sessão, detecta perdas. |
| `reconnect.py` | Guarda estado do peer e política de backoff exponencial. |
| `p2p_client.py` | Coordena servidor TCP, conexões, `HELLO`, `BYE`, descoberta periódica e API do jogo. |

Arquivos externos: `examples/demo_rede.py` (CLI mínima demonstrativa) e `tests/test_comunicacao_rede.py` (testes automatizados contra o servidor original local).

## Instalação e execução no Linux Mint

Não há dependências de `pip`. Na **raiz do projeto**:

```bash
python3 --version
python3 -m unittest discover -s tests -v
```

Para testar o Rendezvous original localmente, no **terminal 1**:

```bash
python3 src/rendezvous/main.py --host 127.0.0.1 --port 8080
```

Em **outros dois terminais**, na mesma pasta do projeto:

```bash
# terminal 2
python3 -m examples.demo_rede --name alice --port 8081 --rdv-host 127.0.0.1
# terminal 3
python3 -m examples.demo_rede --name bob --port 8082 --rdv-host 127.0.0.1
```

Em alice e bob, digite `/discover`, `/reconnect` e `/conn`; `/rtt` mostra o RTT médio após os PINGs. Em alice, digite `/games bob@CIC` para **enviar uma mensagem real `GAME_LIST` por TCP**; o exemplo de bob responde `GAME_LIST_OK` com lista vazia, sem criar uma partida. Use `/quit` para enviar `BYE` e `UNREGISTER`.

Para demonstrar PING/PONG rapidamente (apenas no laboratório), acrescente `--ping-interval 2` **em ambos os terminais**. **Não reduza `--discover-interval` drasticamente quando usar o servidor público**: ele limita requisições por IP.

Servidor fornecido na especificação: `pyp2p.mfcaetano.cc:8080`. Para usá-lo, omita `--rdv-host 127.0.0.1`. A disponibilidade externa depende da rede; não foi verificada nestes testes locais.

## Protocolo de integração

**Rendezvous:** JSON por linha, uma conexão curta para cada comando. `REGISTER` informa `name`, `namespace`, `port` e `ttl`; `DISCOVER` recebe lista com IP/porta; `UNREGISTER` remove o registro.

**Peer-peer:** conexão TCP persistente; início por `HELLO/HELLO_OK` com `features:["hangman"]` e `version:"1.0"`. Todos os peers de um mesmo namespace participam da malha. Conexões duplicadas são resolvidas mantendo **a iniciada pelo menor `peer_id` lexicográfico**. Um `PING` é enviado por ambos os lados a cada 30 s (padrão), o `PONG` repete `msg_id` e `timestamp`, e após 3 PINGs sem PONG a ligação fica `STALE`. `BYE/BYE_OK` fazem encerramento limpo. O jogo usa **a mesma conexão**, sem abrir outra.

Exemplo de pacote de jogo: 

```json
{"type":"GUESS","msg_id":"550e8400-e29b-41d4-a716-446655440000","ttl":1,"src":"bob@CIC","dst":"alice@CIC","game_id":"alice@CIC/3f9a2c1b","letter":"a"}
```

O transporte preenche automaticamente `type`, `msg_id`, `ttl`, `src` e `dst` quando se chama `network.send()`.

## Como integrar com as partes 2 e 3

Os colegas **não** precisam implementar sockets TCP nem PING/PONG. Basta importar a API pública:

```python
import asyncio
from comunicacao_rede import NetworkConfig, P2PNetwork

async def main():
    net = None

    async def ao_receber(peer_id: str, mensagem: dict):
        # Despachar para GameHost / GamePlayer conforme "type".
        print("Recebida de", peer_id, mensagem)
        if mensagem["type"] == "GAME_LIST":
            # Demonstração apenas: substituir pela lista real da Parte 2.
            await net.send(peer_id, "GAME_LIST_OK",
                           msg_id=mensagem["msg_id"], games=[])

    async def ao_mudar(evento: str, peer_id: str):
        # "CONNECTED", "STALE" ou "GONE".
        print("Conexão:", evento, peer_id)
        # A Parte 2 remove jogador desconectado; a Parte 3 detecta queda do host.

    net = P2PNetwork(
        NetworkConfig(name="alice", namespace="CIC", listen_port=8081),
        message_handler=ao_receber,
        peer_handler=ao_mudar,
    )
    await net.start()
    try:
        await net.discover_once()  # descoberta manual (além da periódica)
        print(net.connections_status())
        # Exemplo: após encontrar peer conectado e uma partida válida:
        # await net.send("bob@CIC", "JOIN", game_id="bob@CIC/3f9a2c1b")
    finally:
        # ANTES de stop(), a Parte 2 manda GAME_OVER ABORTED e a Parte 3 manda LEAVE.
        await net.stop()

asyncio.run(main())
```

**Funções públicas:**

| Chamada | Retorno / comportamento |
| --- | --- |
| `await net.start()` | Abre escuta, tenta `REGISTER`, inicia descoberta e renovação. Pode iniciar offline com aviso. |
| `await net.discover_once()` | Retorna lista de `dict` de outros peers; filtra o próprio registro. Falha de RDV retorna `[]` e escreve aviso no log, sem derrubar conexões existentes. |
| `await net.connect(peer_id)` | `bool`; tenta TCP + handshake de peer descoberto. |
| `await net.reconnect_now()` | Libera contadores de backoff e refaz `DISCOVER`; serve ao comando `/reconnect`. |
| `await net.send(peer_id, "GUESS", game_id=id, letter="a")` | Retorna `msg_id` enviado; `ConnectionError` se não conectado e `ValueError` para tipo inválido. |
| `await net.send(peer_id, "GUESS_RESULT", msg_id=msg_id_recebido, ...)` | Reutiliza o `msg_id` da requisição, conforme protocolo. |
| `net.connections_status()` | Lista de dicts: `peer_id`, `direction`, `state`, `rtt_ms`. |
| `net.peers` | Mapa de objetos `PeerInfo` (estado, IP, porta, tentativas, RTT). |
| `await net.stop()` | `BYE/BYE_OK`, fecha sockets, interrompe tarefas e tenta `UNREGISTER`. |

**Divisão:** a **Parte 2** cria `GAME_LIST_OK`, recebe `JOIN`, valida `GUESS`, produz `GAME_START`, `STATE` e `GAME_OVER`. A **Parte 3** implementa os comandos do usuário, dispara `JOIN`/`LEAVE`/`GUESS` e exibe `STATE`. Esta Parte 1 **não** decide regra, turno nem vitória. Para envio de respostas como `JOIN_OK` e `GUESS_RESULT`, reutilize `msg_id=mensagem["msg_id"]`.

**Importante:** o protocolo usa `asyncio`; os callbacks da aplicação podem ser funções `async` ou normais, mas lógica pesada e `input()` bloqueante devem ser executados fora do event loop, por exemplo `asyncio.to_thread`. Não façam callbacks bloquearem por dezenas de segundos. Dois peers precisam compartilhar o mesmo `namespace` para serem descobertos.

## Limitações e erros previstos

- `RendezvousError`: conexão indisponível, resposta inválida ou mensagem `status: ERROR` do servidor; a API de baixo nível levanta a exceção, enquanto `P2PNetwork` registra aviso e continua.
- `ConnectionError`: peer não conectado; interface/host devem avisar usuário e reagir conforme estado da partida.
- `ProtocolError`: JSON malformado, campo obrigatório inválido ou pacote acima de 32 KiB; mensagens peer inválidas recebem `ERROR` quando há uma sessão estabelecida.
- `OSError` (ex.: `Connection refused`): endereço/porta errados, firewall, peer offline ou NAT; a camada de rede tenta novamente com espera exponencial.
- Servidor público **não atravessa NAT**; para peers em redes externas diferentes pode ser necessário redirecionar as portas, usar uma rede VPN ou executar testes na mesma LAN. Não há relay.
- Ao atingir `max_reconnect_attempts`, o peer fica `GONE` até que seja identificado novamente como participante novo ou seja reiniciado; não fica em reconexão infinita.
- Após o host cair, o módulo de rede emite `STALE`/`GONE`; **cancelar a partida** é responsabilidade da Parte 3. Ao um jogador cair, **remover e redistribuir `STATE`** é responsabilidade da Parte 2.

## Critérios de verificação

Rode `python3 -m unittest discover -s tests -v` antes de fazer commit. Os testes cobrem protocolo, registro, descoberta, três peers sem duplicação, mensagem de integração, PING/PONG e RTT, 3 PINGs ausentes, reconexão após socket abortado e queda do Rendezvous com conexões peer ainda válidas. **Os testes do jogo real contra `forca-bot` e `pyp2p-forca` dependem das Partes 2 e 3**.
