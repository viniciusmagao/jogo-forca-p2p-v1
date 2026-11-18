import asyncio
import shlex

from jogo.game_host import GameHost
from rede.p2p_client import NetworkConfig, P2PNetwork
from jogo.message_router import MessageRouter


class CLI:

    def __init__(self, network, router):
        self.network = network
        self.router = router
        self.running = True
        self.games = {}

    async def notify(self, event, data):
        print(f"\n[EVENTO] {event}")

        if event == "GAME_LIST":
            peer_id = data.get("peer_id")
            games = data.get("games", [])

            self.games[peer_id] = games

            if not games:
                print(f"Nenhuma partida encontrada em {peer_id}.")
            else:
                for game in games:
                    print(
                        f"  Game ID: {game.get('game_id')}\n"
                        f"  Host: {game.get('host')}\n"
                        f"  Status: {game.get('status')}\n"
                        f"  Jogadores: {game.get('players')}\n"
                        f"  Vagas: {game.get('max_players')}\n"
                        f"  Vidas: {game.get('lives')}\n"
                        f"  Tamanho da palavra: {game.get('word_length')}\n"
                    )

        elif event == "STATE":
            state = data.get("state", data)
            self.display_game_state(state)

        elif event == "GAME_OVER":
            result = data.get("result")
            print(f"Resultado da partida: {result}")
            print(f"Palavra: {data.get('word')}")
            print(f"Vencedor: {data.get('winner')}")
            print(f"Pontuação: {data.get('scores')}")

        elif event == "GAME_START":
            print("A partida começou!")

        elif event == "GUESS_RESULT":
            result = data.get("result", data)

            if result.get("accepted"):
                print("Palpite aceito.")
                print(f"Correto: {result.get('correct')}")
            else:
                print(
                    "Palpite rejeitado: "
                    f"{result.get('reason')}"
                )

        elif event == "HOST_UNREACHABLE":
            print("O host ficou indisponível.")

        elif event == "PLAYER_JOINED":
            print(f"Jogador entrou: {data.get('peer_id')}")

        elif event == "PLAYER_LEFT":
            print(f"Jogador saiu: {data.get('peer_id')}")

        else:
            print(data)

    def display_game_state(self, state):
        print("\n========== ESTADO DA PARTIDA ==========")
        print(f"Partida: {state.get('game_id')}")
        print(f"Palavra: {state.get('masked')}")
        print(f"Letras erradas: {state.get('wrong_letters')}")
        print(f"Vidas restantes: {state.get('lives')}")
        print(f"Jogadores: {state.get('players')}")
        print(f"Vez de: {state.get('turn')}")
        print(f"Última jogada: {state.get('last_guess')}")
        print("=======================================\n")

    def display_help(self):
        print(
            """
========== COMANDOS DISPONÍVEIS ==========

/help
    Exibe os comandos disponíveis.

/peers
    Descobre e lista os peers disponíveis.

/conn
    Lista as conexões ativas.

/discover
    Atualiza a descoberta de peers.

/create <palavra> [max_jogadores] [vidas] [timeout]
    Cria uma partida.

/start
    Inicia a partida hospedada.

/games
    Solicita a lista de partidas disponíveis.

/join <peer_id> <game_id>
    Entra em uma partida.

/leave
    Sai da partida atual.

/guess <letra>
    Envia um palpite de letra.

/word <palavra>
    Envia um palpite de palavra.

/status
    Exibe o estado da partida atual.

/cancel
    Cancela a partida hospedada.

/quit
    Encerra a aplicação.

==========================================
"""
        )

    async def show_peers(self):
        peers = await self.network.discover_once()

        print("\n========== PEERS DESCOBERTOS ==========")

        if not peers:
            print("Nenhum peer encontrado.")
        else:
            for peer in peers:
                if isinstance(peer, dict):
                    print(peer)
                else:
                    print(peer)

        print("=======================================\n")

    def show_connections(self):
        connections = self.network.connections_status()

        print("\n========== CONEXÕES ATIVAS ==========")

        if not connections:
            print("Nenhuma conexão ativa.")
        else:
            for connection in connections:
                print(connection)

        print("=====================================\n")

    async def show_games(self):
        self.games.clear()
        await self.router.request_game_list()

        print(
            "\nSolicitação de lista de partidas enviada "
            "aos peers conectados."
        )

    def create_game(self, args):
        if len(args) < 1 or len(args) > 4:
            print(
                "Uso: /create <palavra> "
                "[max_jogadores] [vidas] [timeout]"
            )
            return

        if self.router.host is not None:
            print("Você já está hospedando uma partida.")
            return

        if self.router.player.status != "IDLE":
            print("Você já está participando de uma partida.")
            return

        word = args[0]
        max_players = int(args[1]) if len(args) >= 2 else 4
        lives = int(args[2]) if len(args) >= 3 else 6
        turn_timeout = int(args[3]) if len(args) >= 4 else 30

        host = GameHost(
            host_peer_id=self.network.peer_id,
            word=word,
            max_players=max_players,
            lives=lives,
            turn_timeout=turn_timeout
        )

        self.router.set_host(host)

        print("\nPartida criada com sucesso!")
        print(f"Game ID: {host.game_id}")
        print(f"Máximo de jogadores: {host.max_players}")
        print(f"Vidas: {host.game.initial_lives}")
        print(f"Timeout: {host.turn_timeout} segundos")
        print("Aguardando jogadores...")

    async def start_game(self):
        if self.router.host is None:
            print("Você não está hospedando uma partida.")
            return

        await self.router.start_game()
        print("Solicitação de início da partida processada.")

    async def join_game(self, args):
        if len(args) != 2:
            print("Uso: /join <peer_id> <game_id>")
            return

        host_peer_id, game_id = args

        await self.router.join_game(host_peer_id, game_id)

        print(f"Solicitação para entrar em {game_id} enviada.")

    async def leave_game(self):
        await self.router.leave_game()
        print("Solicitação para sair da partida enviada.")

    async def send_letter(self, args):
        if len(args) != 1:
            print("Uso: /guess <letra>")
            return

        if len(args[0]) != 1:
            print("Informe apenas uma letra.")
            return

        await self.router.send_guess(letter=args[0])

        print("Palpite enviado.")

    async def send_word(self, args):
        if len(args) != 1:
            print("Uso: /word <palavra>")
            return

        await self.router.send_guess(word=args[0])

        print("Palpite enviado.")

    def show_status(self):
        if self.router.player.status != "IDLE":
            print("\n========== ESTADO DO JOGADOR ==========")
            print(self.router.player.get_state())
            print("=======================================\n")
            return

        if self.router.host is not None:
            host = self.router.host

            print("\n========== ESTADO DO HOST ==========")
            print(f"Game ID: {host.game_id}")
            print(f"Status: {host.status}")
            print(f"Jogadores: {host.players}")
            print(f"Vidas: {host.game.get_state()['remaining_lives']}")
            print(f"Turno: {host.current_turn_index}")
            print(f"Pontuações: {host.scores}")
            print("====================================\n")
            return

        print("Você não está participando nem hospedando uma partida.")

    async def cancel_game(self):
        if self.router.host is None:
            print("Você não está hospedando uma partida.")
            return

        await self.router.cancel_game()
        print("Solicitação de cancelamento processada.")

    async def execute_command(self, command):
        try:
            parts = shlex.split(command)

            if not parts:
                return

            cmd = parts[0].lower()
            args = parts[1:]

            if cmd == "/help":
                self.display_help()

            elif cmd == "/peers":
                await self.show_peers()

            elif cmd == "/conn":
                self.show_connections()

            elif cmd == "/discover":
                await self.show_peers()

            elif cmd == "/create":
                self.create_game(args)

            elif cmd == "/start":
                await self.start_game()

            elif cmd == "/games":
                await self.show_games()

            elif cmd == "/join":
                await self.join_game(args)

            elif cmd == "/leave":
                await self.leave_game()

            elif cmd == "/guess":
                await self.send_letter(args)

            elif cmd == "/word":
                await self.send_word(args)

            elif cmd == "/status":
                self.show_status()

            elif cmd == "/cancel":
                await self.cancel_game()

            elif cmd == "/quit":
                self.running = False

            else:
                print("Comando desconhecido. Digite /help.")

        except ValueError as error:
            print(f"Argumento inválido: {error}")

        except RuntimeError as error:
            print(f"Operação não permitida: {error}")

        except ConnectionError as error:
            print(f"Erro de conexão: {error}")

        except Exception as error:
            print(f"Erro ao executar comando: {error}")

    async def run(self):
        print("\n======================================")
        print("          PY P2P - FORCA")
        print("======================================")
        print(f"Peer ID: {self.network.peer_id}")
        print("Digite /help para ver os comandos.")

        while self.running:
            try:
                command = await asyncio.to_thread(
                    input,
                    "\nPyP2P-Forca> "
                )

                await self.execute_command(command)

            except (EOFError, KeyboardInterrupt):
                self.running = False

        await self.shutdown()

    async def shutdown(self):
        print("\nEncerrando aplicação...")

        try:
            if self.router.player.status in ("JOINED", "PLAYING"):
                await self.router.leave_game()

            await self.router.shutdown()

        except Exception as error:
            print(f"Erro ao encerrar o jogo: {error}")

        try:
            await self.network.stop()
        except Exception as error:
            print(f"Erro ao encerrar a rede: {error}")

        print("Aplicação encerrada.")


async def main():
    name = input("Digite seu nome: ").strip()
    namespace = input("Digite seu namespace [CIC]: ").strip() or "CIC"

    try:
        listen_port = int(input("Digite a porta TCP [8081] (nao pode ser a mesma porta): ") or "8081")
    except ValueError:
        print("Porta inválida.")
        return

    config = NetworkConfig(
        name=name,
        namespace=namespace,
        listen_port=listen_port
    )

    config.validate()

    network = P2PNetwork(config)

    router = MessageRouter(
        network=network,
        event_handler=None
    )

    cli = CLI(network, router)
    router.event_handler = cli.notify

    try:
        await network.start()
        await cli.run()

    except KeyboardInterrupt:
        print("\nInterrupção recebida.")

    finally:
        if cli.running:
            await cli.shutdown()


if __name__ == "__main__":
    asyncio.run(main())