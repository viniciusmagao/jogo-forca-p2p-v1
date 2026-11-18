"""
O MessageRouter não deve implementar novamente as regras da forca. 
Ele apenas coordena a comunicação entre os módulos.

O MessageRouter não precisa manipular diretamente sockets, streams
ou locks. Toda essa responsabilidade já está encapsulada em 
PeerConnection, e o P2PNetwork disponibiliza o método send() 
para enviar mensagens.

MessageRouter: não vamos acessar writer, reader ou chamar 
encode_line() diretamente. Toda comunicação será feita por 
network.send()

"""

import asyncio
import inspect
import logging

from .game_host import GameHost
from .game_player import GamePlayer


logger = logging.getLogger(__name__)


class MessageRouter:

    def __init__(
        self,
        network,
        game_host: GameHost | None = None,
        game_player: GamePlayer | None = None,
        event_handler=None
    ):
        self.network = network

        self.host = game_host
        self.player = game_player or GamePlayer(network.peer_id)

        self.event_handler = event_handler

        self._turn_task = None
        self._turn_token = 0

        self._finished_task = None

        self.network.message_handler = self.handle_message
        self.network.peer_handler = self.handle_peer_event

    async def _notify(self, event: str, **data):
        if self.event_handler is None:
            return

        try:
            result = self.event_handler(event, data)

            if inspect.isawaitable(result):
                await result

        except Exception:
            logger.exception("Erro ao notificar evento: %s", event)

    async def _send(
        self,
        peer_id: str,
        message_type: str,
        *,
        msg_id: str | None = None,
        **payload
    ):
        try:
            return await self.network.send(
                peer_id,
                message_type,
                msg_id=msg_id,
                **payload
            )

        except Exception:
            logger.exception(
                "Falha ao enviar %s para %s",
                message_type,
                peer_id
            )
            return None

    async def _broadcast(
        self,
        players: list[str],
        message_type: str,
        **payload
    ):
        results = await asyncio.gather(
            *(
                self._send(player_id, message_type, **payload)
                for player_id in players
            ),
            return_exceptions=True
        )

        for player_id, result in zip(players, results):
            if isinstance(result, Exception):
                logger.error(
                    "Falha ao enviar %s para %s: %s",
                    message_type,
                    player_id,
                    result
                )

    def set_host(self, game_host: GameHost):
        if self.host is not None and self.host.status in (
            "OPEN",
            "RUNNING",
            "FINISHED"
        ):
            raise RuntimeError("already_hosting_game")

        if self.player.status != "IDLE":
            raise RuntimeError("already_in_game")

        self.host = game_host

    def _validate_host_game(self, game_id: str) -> bool:
        return (
            self.host is not None
            and self.host.game_id == game_id
        )

    async def request_game_list(self, peer_id: str | None = None):
        if peer_id is not None:
            return await self._send(peer_id, "GAME_LIST")

        peers = list(self.network.connections.keys())

        await asyncio.gather(
            *(
                self._send(peer_id, "GAME_LIST")
                for peer_id in peers
            ),
            return_exceptions=True
        )

    async def join_game(self, host_peer_id: str, game_id: str):
        self.player.begin_join(host_peer_id, game_id)

        result = await self._send(
            host_peer_id,
            "JOIN",
            game_id=game_id
        )

        if result is None:
            self.player.reset()
            raise ConnectionError("host_unreachable")

    async def leave_game(self):
        if self.player.status not in ("JOINED", "PLAYING"):
            raise RuntimeError("not_in_game")

        host_peer_id = self.player.host_peer_id
        game_id = self.player.game_id

        result = await self._send(
            host_peer_id,
            "LEAVE",
            game_id=game_id
        )

        if result is None:
            raise ConnectionError("host_unreachable")

    async def send_guess(
        self,
        *,
        letter: str | None = None,
        word: str | None = None
    ):
        guess_data = self.player.build_guess(
            letter=letter,
            word=word
        )

        result = await self._send(
            self.player.host_peer_id,
            "GUESS",
            **guess_data
        )

        if result is None:
            raise ConnectionError("host_unreachable")

        return result

    async def start_game(self):
        if self.host is None:
            raise RuntimeError("not_hosting_game")

        if self.host.status != "OPEN":
            raise RuntimeError("game_not_open")

        self.host.start_game()

        game_start_data = self.host.build_game_start_data()
        state_data = self.host.build_state_data()

        players = self.host.players.copy()

        await self._broadcast(
            players,
            "GAME_START",
            **game_start_data
        )

        await self._broadcast(
            players,
            "STATE",
            **state_data
        )

        self._schedule_turn_timeout()

    async def cancel_game(self):
        if self.host is None:
            raise RuntimeError("not_hosting_game")

        if self.host.status != "OPEN":
            raise RuntimeError("game_not_open")

        players = self.host.players.copy()

        game_over_data = self.host.abort_game("host_shutdown")
        game_over_data["game_id"] = self.host.game_id

        await self._broadcast(
            players,
            "GAME_OVER",
            **game_over_data
        )

        self._cancel_turn_timeout()
        self._schedule_finished_cleanup()

    async def handle_message(self, peer_id: str, message: dict):
        if not isinstance(message, dict):
            logger.warning("Mensagem inválida recebida de %s", peer_id)
            return

        message_type = message.get("type")

        handlers = {
            "GAME_LIST": self._handle_game_list,
            "GAME_LIST_OK": self._handle_game_list_ok,
            "JOIN": self._handle_join,
            "JOIN_OK": self._handle_join_ok,
            "JOIN_ERR": self._handle_join_err,
            "LEAVE": self._handle_leave,
            "LEAVE_OK": self._handle_leave_ok,
            "GAME_START": self._handle_game_start,
            "STATE": self._handle_state,
            "GUESS": self._handle_guess,
            "GUESS_RESULT": self._handle_guess_result,
            "GAME_OVER": self._handle_game_over,
        }

        handler = handlers.get(message_type)

        if handler is None:
            logger.warning(
                "Tipo de mensagem desconhecido: %s",
                message_type
            )
            return

        try:
            await handler(peer_id, message)

        except Exception:
            logger.exception(
                "Erro ao processar mensagem %s recebida de %s",
                message_type,
                peer_id
            )

    async def _handle_game_list(self, peer_id: str, message: dict):
        games = []

        if self.host is not None:
            if self.host.status in ("OPEN", "RUNNING", "FINISHED"):
                game_start_data = self.host.build_game_start_data()

                games.append({
                    "game_id": self.host.game_id,
                    "host": self.network.peer_id,
                    "status": self.host.status,
                    "players": self.host.players.copy(),
                    "max_players": self.host.max_players,
                    "lives": (
                        self.host.game.remaining_lives
                        if self.host.status == "RUNNING"
                        else game_start_data["lives"]
                    ),
                    "word_length": game_start_data["word_length"]
                })

        await self._send(
            peer_id,
            "GAME_LIST_OK",
            msg_id=message.get("msg_id"),
            games=games
        )

    async def _handle_game_list_ok(self, peer_id: str, message: dict):
        games = message.get("games")

        if not isinstance(games, list):
            logger.warning(
                "GAME_LIST_OK inválido recebido de %s",
                peer_id
            )
            return

        await self._notify(
            "GAME_LIST_OK",
            peer_id=peer_id,
            games=games
        )

    async def _handle_join(self, peer_id: str, message: dict):
        game_id = message.get("game_id")
        msg_id = message.get("msg_id")

        if not isinstance(game_id, str):
            return

        if not self._validate_host_game(game_id):
            await self._send(
                peer_id,
                "JOIN_ERR",
                msg_id=msg_id,
                game_id=game_id,
                reason="unknown_game"
            )
            return

        rejoin = False

        if self.host.status == "OPEN":
            accepted, reason = self.host.add_player(peer_id)

        elif self.host.status == "RUNNING":
            accepted, reason = self.host.rejoin_player(peer_id)
            rejoin = accepted

        else:
            accepted = False
            reason = "game_finished"

        if not accepted:
            await self._send(
                peer_id,
                "JOIN_ERR",
                msg_id=msg_id,
                game_id=game_id,
                reason=reason
            )
            return

        await self._send(
            peer_id,
            "JOIN_OK",
            msg_id=msg_id,
            game_id=game_id,
            players=self.host.players.copy(),
            rejoin=rejoin
        )

        if rejoin:
            state_data = self.host.build_state_data()

            await self._send(
                peer_id,
                "STATE",
                **state_data
            )

            self._schedule_turn_timeout()

        await self._notify(
            "PLAYER_JOINED",
            peer_id=peer_id,
            game_id=game_id,
            rejoin=rejoin,
            players=self.host.players.copy()
        )

    async def _handle_join_ok(self, peer_id: str, message: dict):
        accepted = self.player.handle_join_ok(peer_id, message)

        if not accepted:
            return

        await self._notify(
            "JOIN_OK",
            peer_id=peer_id,
            game_id=self.player.game_id,
            players=self.player.players.copy(),
            rejoin=self.player.rejoin
        )

    async def _handle_join_err(self, peer_id: str, message: dict):
        game_id = self.player.game_id
        reason = message.get("reason")

        accepted = self.player.handle_join_err(peer_id, message)

        if not accepted:
            return

        await self._notify(
            "JOIN_ERR",
            peer_id=peer_id,
            game_id=game_id,
            reason=reason
        )

    async def _handle_leave(self, peer_id: str, message: dict):
        game_id = message.get("game_id")
        msg_id = message.get("msg_id")

        if not isinstance(game_id, str):
            return

        if not self._validate_host_game(game_id):
            return

        accepted, reason = self.host.remove_player(peer_id)

        if not accepted:
            logger.warning(
                "Não foi possível remover %s da partida: %s",
                peer_id,
                reason
            )
            return

        await self._send(
            peer_id,
            "LEAVE_OK",
            msg_id=msg_id,
            game_id=game_id
        )

        if self.host.status == "RUNNING":
            state_data = self.host.build_state_data()

            await self._broadcast(
                self.host.players.copy(),
                "STATE",
                **state_data
            )

            self._schedule_turn_timeout()

        elif self.host.result == "ABORTED":
            self._cancel_turn_timeout()
            self._schedule_finished_cleanup()

        await self._notify(
            "PLAYER_LEFT",
            peer_id=peer_id,
            game_id=game_id,
            players=self.host.players.copy()
        )

    async def _handle_leave_ok(self, peer_id: str, message: dict):
        game_id = self.player.game_id

        accepted = self.player.handle_leave_ok(peer_id, message)

        if not accepted:
            return

        await self._notify(
            "LEAVE_OK",
            peer_id=peer_id,
            game_id=game_id
        )

    async def _handle_game_start(self, peer_id: str, message: dict):
        accepted = self.player.handle_game_start(peer_id, message)

        if not accepted:
            logger.warning(
                "GAME_START inválido recebido de %s",
                peer_id
            )
            return

        await self._notify(
            "GAME_START",
            peer_id=peer_id,
            game_id=self.player.game_id,
            players=self.player.players.copy(),
            lives=self.player.lives,
            word_length=self.player.word_length,
            turn_timeout=self.player.turn_timeout
        )

    async def _handle_state(self, peer_id: str, message: dict):
        accepted = self.player.apply_state(peer_id, message)

        if not accepted:
            logger.debug(
                "STATE descartado de %s: seq=%s",
                peer_id,
                message.get("seq")
            )
            return

        await self._notify(
            "STATE",
            peer_id=peer_id,
            state=self.player.get_state()
        )

    async def _handle_guess(self, peer_id: str, message: dict):
        game_id = message.get("game_id")
        msg_id = message.get("msg_id")

        if not isinstance(game_id, str):
            return

        if not self._validate_host_game(game_id):
            return

        result = self.host.process_guess(
            peer_id,
            letter=message.get("letter"),
            word=message.get("word")
        )

        guess_result = result["guess_result"]

        await self._send(
            peer_id,
            "GUESS_RESULT",
            msg_id=msg_id,
            game_id=game_id,
            **guess_result
        )

        if result["state"] is not None:
            await self._broadcast(
                self.host.players.copy(),
                "STATE",
                **result["state"]
            )

        if result["game_over"] is not None:
            game_over_data = result["game_over"].copy()
            game_over_data["game_id"] = game_id

            await self._broadcast(
                self.host.players.copy(),
                "GAME_OVER",
                **game_over_data
            )

            self._cancel_turn_timeout()
            self._schedule_finished_cleanup()

        elif result["state"] is not None:
            self._schedule_turn_timeout()

        await self._notify(
            "GUESS_RESULT",
            peer_id=peer_id,
            game_id=game_id,
            result=guess_result
        )

    async def _handle_guess_result(self, peer_id: str, message: dict):
        accepted = self.player.handle_guess_result(peer_id, message)

        if not accepted:
            return

        await self._notify(
            "GUESS_RESULT",
            peer_id=peer_id,
            game_id=self.player.game_id,
            result=self.player.last_guess_result.copy()
        )

    async def _handle_game_over(self, peer_id: str, message: dict):
        accepted = self.player.handle_game_over(peer_id, message)

        if not accepted:
            return

        result = self.player.last_game_result.copy()

        if not result["hash_matches"]:
            logger.warning(
                "host cheated: hash mismatch"
            )

        await self._notify(
            "GAME_OVER",
            peer_id=peer_id,
            result=result
        )

    async def handle_peer_event(self, event: str, peer_id: str):
        if event == "STALE" or event == "GONE":
            await self._handle_peer_unreachable(peer_id)

        elif event == "CONNECTED":
            await self._handle_peer_connected(peer_id)

    async def _handle_peer_unreachable(self, peer_id: str):
        if self.host is not None:
            if (
                self.host.status == "RUNNING"
                and peer_id in self.host.players
            ):
                game_id = self.host.game_id

                accepted, reason = self.host.remove_player(peer_id)

                if accepted:
                    if self.host.status == "RUNNING":
                        state_data = self.host.build_state_data()

                        await self._broadcast(
                            self.host.players.copy(),
                            "STATE",
                            **state_data
                        )

                        self._schedule_turn_timeout()

                    elif self.host.result == "ABORTED":
                        self._cancel_turn_timeout()
                        self._schedule_finished_cleanup()

                    await self._notify(
                        "PLAYER_UNREACHABLE",
                        peer_id=peer_id,
                        game_id=game_id,
                        reason=reason,
                        players=self.host.players.copy()
                    )

        if self.player.status in ("JOINED", "PLAYING"):
            if self.player.host_peer_id == peer_id:
                game_id = self.player.game_id

                self.player.handle_host_unreachable(peer_id)

                await self._notify(
                    "HOST_UNREACHABLE",
                    peer_id=peer_id,
                    game_id=game_id,
                    result=self.player.last_game_result.copy()
                )

    async def _handle_peer_connected(self, peer_id: str):
        if self.player.status not in ("JOINED", "PLAYING"):
            return

        if self.player.host_peer_id != peer_id:
            return

        if self.player.game_id is None:
            return

        await self._send(
            peer_id,
            "JOIN",
            game_id=self.player.game_id
        )

    def _cancel_turn_timeout(self):
        self._turn_token += 1

        if self._turn_task is not None:
            self._turn_task.cancel()
            self._turn_task = None

    def _schedule_turn_timeout(self):
        self._cancel_turn_timeout()

        if self.host is None or self.host.status != "RUNNING":
            return

        token = self._turn_token
        current_turn = self.host.players[self.host.current_turn_index]
        turn_timeout = self.host.turn_timeout

        self._turn_task = asyncio.create_task(
            self._wait_turn_timeout(
                token,
                current_turn,
                turn_timeout
            )
        )

    async def _wait_turn_timeout(
        self,
        token: int,
        current_turn: str,
        turn_timeout: int
    ):
        try:
            await asyncio.sleep(turn_timeout)

            if token != self._turn_token:
                return

            if self.host is None or self.host.status != "RUNNING":
                return

            if not self.host.players:
                return

            current_player = self.host.players[
                self.host.current_turn_index
            ]

            if current_player != current_turn:
                return

            state_data = self.host.handle_timeout()

            await self._broadcast(
                self.host.players.copy(),
                "STATE",
                **state_data
            )

            await self._notify(
                "TURN_TIMEOUT",
                peer_id=current_turn,
                game_id=self.host.game_id,
                state=state_data
            )

            self._schedule_turn_timeout()

        except asyncio.CancelledError:
            pass

        except Exception:
            logger.exception("Erro durante timeout de turno")

    def _schedule_finished_cleanup(self):
        if self._finished_task is not None:
            self._finished_task.cancel()

        self._finished_task = asyncio.create_task(
            self._wait_finished_cleanup()
        )

    async def _wait_finished_cleanup(self):
        try:
            await asyncio.sleep(60)

            if self.host is not None and self.host.status == "FINISHED":
                self.host.status = "IDLE"

                await self._notify(
                    "GAME_EXPIRED",
                    game_id=self.host.game_id
                )

                self.host = None

        except asyncio.CancelledError:
            pass

    async def shutdown(self):
        if self.host is not None:
            if self.host.status in ("OPEN", "RUNNING"):
                players = self.host.players.copy()

                game_over_data = self.host.abort_game("host_shutdown")
                game_over_data["game_id"] = self.host.game_id

                await self._broadcast(
                    players,
                    "GAME_OVER",
                    **game_over_data
                )

        self._cancel_turn_timeout()

        if self._finished_task is not None:
            self._finished_task.cancel()
            self._finished_task = None