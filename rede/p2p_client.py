"""
API de comunicação para ser consumida pelas 
Partes 2 (host) e 3 (jogador).

"""
import asyncio
import inspect
import logging
import time
from dataclasses import dataclass

from .heartbeat import heartbeat_loop
from .peer_connection import PeerConnection
from .protocolo import (
                        VERSION, 
                        ProtocolError,
                        new_id,
                        now_utc,
                        read_line,
                        validate_envelope,
                        encode_line
                        )
from .rendezvous_client import (
                                RendezvousClient,
                                RendezvousError
                                )
from .reconnect import PeerInfo

log = logging.getLogger("comunicacao_rede")

GAME_TYPES = frozenset({
    "GAME_LIST",
    "GAME_LIST_OK",
    "JOIN",
    "JOIN_OK",
    "JOIN_ERR",
    "LEAVE",
    "LEAVE_OK",
    "GAME_START",
    "STATE",
    "GUESS",
    "GUESS_RESULT",
    "GAME_OVER",
})


@dataclass
class NetworkConfig:
    name: str
    namespace: str = "CIC"
    listen_host: str = "0.0.0.0"
    listen_port: int = 8081
    rdv_host: str = "pyp2p.mfcaetano.cc"
    rdv_port: int = 8080
    register_ttl: int = 7200
    discover_interval: float = 20
    ping_interval: float = 30
    ping_miss_limit: int = 3
    max_reconnect_attempts: int = 5
    base_backoff: float = 2
    max_backoff: float = 60

    @property
    #------------------------------------------------------------
    # Monta o id do peer
    def peer_id(self):
        return f"{self.name}@{self.namespace}"

    #------------------------------------------------------------
    # VALIDA campos
    def validate(self):
        if not (self.name and self.namespace and len(self.name) <= 64 and len(self.namespace) <= 64):
            raise ValueError("Name e namespace devem ter 1 a 64 caracteres")
        if not (1 <= self.listen_port <= 65535 and 1 <= self.rdv_port <= 65535):
            raise ValueError("portas inválidas")
        if not (1 <= self.register_ttl <= 86400):
            raise ValueError("TTL inválido")
        if self.discover_interval <= 0 or self.ping_interval <= 0 or self.ping_miss_limit <= 0:
            raise ValueError("Intervalos e limite de PING devem ser positivos")
        if self.max_reconnect_attempts < 1 or self.base_backoff <= 0 or self.max_backoff <= 0:
            raise ValueError("Parâmetros de reconexão inválidos")


class P2PNetwork:
    """
    Rede P2P independente da lógica do jogo.

    message_handler(peer_id, message): callback para mensagens 
    da partida.
    peer_handler(event, peer_id): 'CONNECTED', 'STALE' ou 'GONE'.
    Callbacks podem ser funções normais ou async.

    """

    def __init__(
                self, 
                config: NetworkConfig, 
                message_handler=None, 
                peer_handler=None
                ):
        config.validate()
        self.config = config
        self.message_handler = message_handler
        self.peer_handler = peer_handler
        self.rdv = RendezvousClient(config.rdv_host, config.rdv_port)
        self.peers: dict[str, PeerInfo] = {}
        self.connections: dict[str, PeerConnection] = {}
        self.server = None
        self.running = False
        self._tasks: list[asyncio.Task] = []
        self._conn_lock = asyncio.Lock()
        self._connecting: set[str] = set()
        self._registered = False

    @property
    def peer_id(self):
        return self.config.peer_id

    async def _notify(self, callback, *args):
        if callback is None:
            return
        try:
            result = callback(*args)
            if inspect.isawaitable(result):
                await result
        except Exception:
            log.exception("Callback da aplicação falhou: %s", args)

    #------------------------------------------------------------
    # Como assim start passando o self?
    async def start(self):
        """
        Abre escuta TCP antes de REGISTER; 
        inicia tarefas periódicas.
        
        """
        if self.running:
            return
        self.server = await asyncio.start_server(self._accept, self.config.listen_host,
                                                 self.config.listen_port, limit=32770)
        self.running = True
        try:
            result = await asyncio.to_thread(self.rdv.register, self.config.name,
                                             self.config.namespace, self.config.listen_port,
                                             self.config.register_ttl)
            self._registered = True
            log.info("[RDV] registered %s as %s:%s", self.peer_id,
                     result.get("ip"), result.get("port"))
        except RendezvousError as exc:
            log.warning("[RDV] inicialização sem registro: %s (tentará renovar)", exc)
        self._tasks = [asyncio.create_task(self._discover_loop()),
                       asyncio.create_task(self._refresh_loop())]
        await self.discover_once()

    async def stop(self):
        """
        Desliga a rede; 
        o host/jogador deve enviar LEAVE/GAME_OVER ANTES.
        
        """
        if not self.running:
            return
        self.running = False
        
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        # Aguarda BYE_OK por no máximo dois segundos antes de fechar cada socket.
        active = list(self.connections.values())
        
        for conn in active:
            try:
                await conn.send(self._make_message("BYE", conn.peer_id, reason="shutdown"))
            except (OSError, ConnectionError):
                pass
        await asyncio.gather(*(self._wait_bye(c) for c in active), return_exceptions=True)
        
        for conn in active:
            await self._drop(conn, stale=False, reason="shutdown")
        
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        
        if self._registered:
            try:
                await asyncio.to_thread(self.rdv.unregister, self.config.name,
                                        self.config.namespace, self.config.listen_port)
                log.info("[RDV] UNREGISTER %s", self.peer_id)
            except RendezvousError as exc:
                log.warning("[RDV] UNREGISTER falhou: %s", exc)
        self._registered = False

    async def _wait_bye(self, connection):
        try:
            await asyncio.wait_for(connection.bye_waiter.wait(), timeout=2)
        except asyncio.TimeoutError:
            pass

    def _make_message(self, kind: str, dst: str, *, msg_id=None, **payload):
        
        if kind in {"HELLO", "HELLO_OK", "PING", "PONG"}:
            raise ValueError("Mensagens de sessão são gerenciadas internamente")
        
        if any(k in payload for k in {"type", "src", "dst", "ttl", "msg_id"}):
            raise ValueError("Campos reservados não podem ser sobrescritos")
        
        return {"type": kind, "msg_id": msg_id or new_id(),
                "src": self.peer_id, "dst": dst, "ttl": 1, **payload}

    async def send(self, peer_id: str, message_type: str, *, msg_id=None, **payload) -> str:
        """
        Envia mensagem de jogo; 
        retorna o msg_id para correlacionar a resposta.

        """
        if message_type not in GAME_TYPES:
            raise ValueError(f"Tipo de jogo desconhecido: {message_type}")
        conn = self.connections.get(peer_id)
        
        if conn is None or conn.writer.is_closing():
            raise ConnectionError(f"Peer não conectado: {peer_id}")
        
        if "hangman" not in conn.features:
            raise ConnectionError(f"Peer {peer_id} não anuncia hangman")
        
        msg = self._make_message(message_type, peer_id, msg_id=msg_id, **payload)
        await conn.send(msg)
        return msg["msg_id"]

    async def discover_once(self) -> list[dict]:
        """
        Consulta RDV, reconcilia PeerTable e tenta conectar 
        os peers novos.
        
        """
        if not self.running:
            return []
        try:
            found = await asyncio.to_thread(self.rdv.discover, self.config.namespace)
        except RendezvousError as exc:
            log.warning("[RDV] DISCOVER falhou: %s", exc)
            return []  # Não considera os peers GONE se o RDV estiver indisponível.
        valid = {}
        
        for obj in found:
            if not isinstance(obj, dict):
                continue
            if obj.get("namespace") != self.config.namespace:
                continue
            key = f"{obj.get('name')}@{obj.get('namespace')}"
            if key == self.peer_id:
                continue
            if not isinstance(obj.get("ip"), str) or not isinstance(obj.get("port"), int):
                continue
            valid[key] = obj
            if key not in self.peers:
                self.peers[key] = PeerInfo(key, obj["ip"], obj["port"])
            else:
                info = self.peers[key]
                if info.state == "GONE" and (info.ip != obj["ip"] or info.port != obj["port"]):
                    info.state, info.attempts = "DISCOVERED", 0
                info.ip, info.port = obj["ip"], obj["port"]
        
        for key, info in list(self.peers.items()):
            if key not in valid and info.state != "GONE":
                info.state = "GONE"
                active = self.connections.get(key)
                if active:
                    await self._drop(active, stale=False, reason="Não consta mais no Rendezvous")
                await self._notify(self.peer_handler, "GONE", key)
        
        for key, info in list(self.peers.items()):
            if key in valid:
                current = self.connections.get(key)
                needs_preferred = (current is not None and self.peer_id < key
                                   and not current.outbound)
                if (needs_preferred or (current is None and info.state in {"DISCOVERED", "STALE"})) \
                        and time.monotonic() >= info.next_retry:
                    await self.connect(key)
        
        return [obj for obj in valid.values()]
    
    #------------------------------------------------------------
    # RECONECTA MANUALMENTE
    async def reconnect_now(self) -> list[dict]:
        """
        Comando manual /reconnect: 
        libera peers esgotados e tenta novamente.
        
        """
        for info in self.peers.values():
            if info.state in {"GONE", "STALE"}:
                info.state = "DISCOVERED"
                info.attempts = 0
                info.next_retry = 0.0
        return await self.discover_once()

    #------------------------------------------------------------
    # LOOP DE DESCOBERTA
    async def _discover_loop(self):
        while self.running:
            try:
                await asyncio.sleep(self.config.discover_interval)
                await self.discover_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Erro no ciclo DISCOVER")


    async def _refresh_loop(self):
        while self.running:
            try:
                await asyncio.sleep(max(1, self.config.register_ttl / 2) if self._registered
                                    else max(2, self.config.discover_interval))
                await asyncio.to_thread(self.rdv.register, self.config.name,
                                        self.config.namespace, self.config.listen_port,
                                        self.config.register_ttl)
                self._registered = True
                log.info("[RDV] registro renovado: %s", self.peer_id)
            except asyncio.CancelledError:
                raise
            except RendezvousError as exc:
                log.warning("[RDV] falha ao renovar: %s", exc)

    #------------------------------------------------------------
    # HELLO: envia
    async def connect(self, peer_id: str) -> bool:
        """
        Conecta via TCP e faz HELLO; 
        retorna True quando há conexão ativa.
        
        """
        info = self.peers.get(peer_id)
        current = self.connections.get(peer_id)
        if not info or peer_id in self._connecting:
            return bool(current)
        if current and (current.outbound or self.peer_id > peer_id):
            return True
        if info.state == "GONE" and current is None:
            return False
        self._connecting.add(peer_id)
        if current is None:
            info.state = "CONNECTING"
        
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(info.ip, info.port, limit=32770), 4)
            
            # Aqui omite scr/dst
            hello = {
                    "type": "HELLO",
                    "msg_id": new_id(),
                    "ttl": 1,
                    "peer_id": self.peer_id,
                    "version": VERSION,
                    "features": ["hangman"]
                    }
            writer.write(encode_line(hello))
            await writer.drain()
            reply = await asyncio.wait_for(read_line(reader), 4)
            validate_envelope(reply, session=True)

            if (reply.get("type") != "HELLO_OK" or reply.get("msg_id") != hello["msg_id"]
                    or reply.get("peer_id") != peer_id or reply.get("version") != VERSION
                    or not isinstance(reply.get("features"), list)):
                raise ProtocolError("HELLO_OK inválido")
            conn = PeerConnection(peer_id, reader, writer, True, reply["features"])

            if await self._activate(conn):
                asyncio.create_task(self._recv_loop(conn))
            else:
                await self._duplicate_close(conn)
            
            return peer_id in self.connections
        
        except asyncio.CancelledError:
            if "writer" in locals():
                writer.close()
            raise
        except (OSError, asyncio.TimeoutError, ProtocolError, EOFError) as exc:
            log.warning("[PeerServer] conexão com %s falhou: %s", peer_id, exc)
            if peer_id not in self.connections:
                info.failed(self.config.base_backoff, self.config.max_backoff,
                            self.config.max_reconnect_attempts)
            else:
                info.connected()
            # Se ocorreu antes da criação de PeerConnection, fechar writer.
            if "writer" in locals():
                writer.close()
            return False
        finally:
            self._connecting.discard(peer_id)

    #------------------------------------------------------------
    # HELLO_OK: recebe HELLO, valida HELLO, envia HELLO_OK
    async def _accept(
                        self, 
                        reader: asyncio.StreamReader, 
                        writer: asyncio.StreamWriter
                    ):
        try:
            hello = await asyncio.wait_for(read_line(reader), 5)
            validate_envelope(hello, session=True)
            
            key = hello.get("peer_id")
            
            if (
                hello.get("type") != "HELLO" 
                or hello.get("version") != VERSION
                or not isinstance(key, str) or key == self.peer_id
                or not key.endswith("@" + self.config.namespace)
                or not isinstance(hello.get("features"), list)
                ):
                raise ProtocolError("HELLO inválido")

            # Aqui omite scr/dst 
            reply = {
                    "type": "HELLO_OK",
                    "msg_id": hello["msg_id"],
                    "ttl": 1,
                    "peer_id": self.peer_id,
                    "version": VERSION,
                    "features": ["hangman"]
                    }
            writer.write(encode_line(reply))
            await writer.drain()

            if key not in self.peers:
                peername = writer.get_extra_info("peername")
                self.peers[key] = PeerInfo(key, peername[0], peername[1])
            conn = PeerConnection(key, reader, writer, False, hello["features"])
            if await self._activate(conn):
                await self._recv_loop(conn)
            else:
                await self._duplicate_close(conn)
        except (OSError, asyncio.TimeoutError, ProtocolError, EOFError) as exc:
            log.warning("[PeerServer] HELLO recebido inválido: %s", exc)
            writer.close()

    async def _activate(self, connection: PeerConnection) -> bool:
        peer_id = connection.peer_id
        old = None
        async with self._conn_lock:
            current = self.connections.get(peer_id)
            # É mantida a conexão iniciada pelo menor peer_id.
            preferred_outbound = self.peer_id < peer_id
            if current is not None:
                if current.outbound == preferred_outbound:
                    return False
                if connection.outbound != preferred_outbound:
                    return False
                old = current
            self.connections[peer_id] = connection
            if peer_id not in self.peers:
                addr = connection.writer.get_extra_info("peername")
                self.peers[peer_id] = PeerInfo(peer_id, addr[0], addr[1])
            self.peers[peer_id].connected()
        if old:
            await self._duplicate_close(old)
        connection.heartbeat_task = asyncio.create_task(heartbeat_loop(self, connection))
        log.info("[PeerServer] %s conectado (%s)", peer_id,
                 "outbound" if connection.outbound else "inbound")
        await self._notify(self.peer_handler, "CONNECTED", peer_id)
        return True

    async def _duplicate_close(self, connection):
        try:
            await connection.send(self._make_message("BYE", connection.peer_id,
                                                     reason="duplicate"))
            # A leitura da conexão antiga pode estar em _recv_loop.
            if connection.reader_running:
                await self._wait_bye(connection)
            else:
                await self._wait_bye_from_socket(connection)
        except (OSError, ConnectionError):
            pass
        await self._drop(connection, stale=False, reason="duplicate")

    async def _wait_bye_from_socket(self, conn):
        try:
            msg = await asyncio.wait_for(read_line(conn.reader), timeout=2)
            if msg.get("type") == "BYE_OK":
                conn.bye_waiter.set()
        except (OSError, EOFError, ProtocolError, asyncio.TimeoutError):
            pass


    #------------------------------------------------------------
    # PONG: envio
    # BYE / BYE_OK: Encerramento controlado de uma conexão.
    async def _recv_loop(self, connection):
        connection.reader_running = True
        graceful = False
        try:
            while self.running and not connection.writer.is_closing():
                try:
                    data = await read_line(connection.reader)
                except ProtocolError:
                    await self._error(connection, "bad_json", "Linha JSON inválida")
                    break
                try:
                    validate_envelope(data, session=data.get("type") in {"PING", "PONG"})
                except ProtocolError as exc:
                    if str(exc) == "bad_ttl":
                        continue  # ttl diferente de 1 deve ser descartado.
                    await self._error(connection, "bad_request", str(exc), data.get("msg_id"))
                    continue
                kind = data["type"]

                # Envia o PONG
                if kind in {"PING", "PONG"}:
                    if not isinstance(data.get("timestamp"), str):
                        await self._error(connection, "bad_request", "timestamp faltando", data["msg_id"])
                        continue
                    if kind == "PING":
                        await connection.send({
                                                "type": "PONG",
                                                "msg_id": data["msg_id"],
                                                "timestamp": data["timestamp"],
                                                "ttl": 1
                                                })
                    else:
                        pending = connection.pending_pings.pop(data["msg_id"], None)
                        if pending and pending[1] == data["timestamp"]:
                            connection.missed = 0
                            rtt = (time.monotonic() - pending[0]) * 1000
                            self.peers[connection.peer_id].rtts_ms.append(rtt)
                            log.debug("[KeepAlive] %s RTT %.1f ms", connection.peer_id, rtt)
                    continue
                    
                if data.get("src") != connection.peer_id:
                    await self._error(connection, "bad_request", "src diferente do peer conectado", data["msg_id"])
                    continue
                if data.get("dst") != self.peer_id:
                    await self._error(connection, "not_for_me", "dst diferente do peer local", data["msg_id"])
                    continue

                # BYE / BYE_OK
                if kind == "BYE_OK":
                    connection.bye_waiter.set()
                elif kind == "BYE":
                    graceful = True
                    await connection.send(self._make_message(
                                                            "BYE_OK",
                                                            connection.peer_id,
                                                            msg_id=data["msg_id"]
                                                            ))
                    return

                elif kind == "ERROR":
                    log.warning("[PeerServer] ERROR de %s: %s", connection.peer_id, data.get("code"))
                elif kind in GAME_TYPES:
                    if "hangman" not in connection.features:
                        await self._error(connection, "invalid_state", "Peer sem feature hangman", data["msg_id"])
                    elif self.message_handler is None:
                        await self._error(connection, "invalid_state", "Lógica do jogo não conectada", data["msg_id"])
                    else:
                        await self._notify(self.message_handler, connection.peer_id, data)
                else:
                    await self._error(connection, "unknown_type", kind, data["msg_id"])
        except (EOFError, OSError, ConnectionError) as exc:
            log.info("[PeerServer] %s caiu: %s", connection.peer_id, exc)
        finally:
            connection.reader_running = False
            await self._drop(connection, stale=self.running and not graceful, reason="socket fechado")

    async def _error(self, connection, code, description, ref_msg_id=None):
        try:
            msg = self._make_message("ERROR", connection.peer_id, code=code,
                                     message=description, ref_msg_id=ref_msg_id)
            await connection.send(msg)
        except (OSError, ConnectionError):
            pass

    async def _drop(self, connection, *, stale: bool, reason: str):
        peer_id = connection.peer_id
        async with self._conn_lock:
            active = self.connections.get(peer_id) is connection
            if active:
                del self.connections[peer_id]
                info = self.peers.get(peer_id)
                if info:
                    if stale:
                        info.failed(self.config.base_backoff, self.config.max_backoff,
                                    self.config.max_reconnect_attempts)
                    else:
                        info.state = "GONE"
        hb = connection.heartbeat_task
        if active and hb and hb is not asyncio.current_task():
            hb.cancel()
        await connection.close()
        if active:
            log.info("[PeerServer] %s desconectado (%s)", peer_id, reason)
            await self._notify(self.peer_handler, "STALE" if stale else "GONE", peer_id)

    def connections_status(self) -> list[dict]:
        """
        Retorna dados para o comando /conn e /rtt da Parte 3.
        
        """
        return [{"peer_id": key,
                 "direction": "outbound" if c.outbound else "inbound",
                 "state": "CONNECTED",
                 "rtt_ms": self.peers[key].rtt_avg_ms}
                for key, c in sorted(self.connections.items())]
