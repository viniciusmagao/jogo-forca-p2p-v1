"""
Lógica de quem hospeda uma partida

O GameHost será responsável por controlar a partida e 
preparar os dados das mensagens, mas não terá nenhuma 
dependência de conexão TCP.

Uma decisão importante será fazer os métodos de 
processamento retornarem os dados necessários para o 
MessageRouter. Assim, por exemplo, ao receber um palpite,
o host poderá produzir um GUESS_RESULT, um STATE e, se 
a partida terminar, um GAME_OVER.
"""

import uuid

from .hangman import HangmanGame, calculate_word_hash


class GameHost:

    # Construtor
    def __init__(
        self,
        host_peer_id: str,
        word: str,
        max_players: int = 4,
        lives: int = 6,
        turn_timeout: int = 30
    ):
        # Valida o identificador do host
        if not isinstance(host_peer_id, str) or not host_peer_id.strip():
            raise ValueError("O ID do host deve ser uma string não vazia")

        # Valida a quantidade máxima de jogadores
        if (
            not isinstance(max_players, int)
            or isinstance(max_players, bool)
            or max_players <= 0
        ):
            raise ValueError("O número máximo de jogadores deve ser um inteiro positivo")

        # Valida o timeout
        if (
            not isinstance(turn_timeout, (int, float))
            or isinstance(turn_timeout, bool)
            or turn_timeout <= 0
        ):
            raise ValueError("O timeout deve ser um número positivo")

        # Identificação da partida
        self.host_peer_id = host_peer_id
        self.game_id = f"{host_peer_id}/{uuid.uuid4().hex[:8]}"

        # Configurações e lógica da forca
        self.game = HangmanGame(word, lives)

        self.max_players = max_players
        self.turn_timeout = turn_timeout

        # Estado inicial da partida
        self.status = "OPEN"

        # Jogadores e controle de turnos
        self.players = []
        self.current_turn_index = 0

        # Controle dos estados enviados
        self.seq = 0

        # Pontuação e última jogada
        self.scores = {}
        self.last_guess = None

        # Informações de encerramento
        self.result = None
        self.abort_reason = None
        self.winner = None

    
    #------------------------------------------------------------
    # Entrada de jogadores na partida
    def add_player(self, peer_id: str) -> tuple[bool, str | None]:

        # Valida o identificador do jogador
        if not isinstance(peer_id, str) or not peer_id:
            return False, "invalid_peer"

        # Impede que o host participe da própria partida
        if peer_id == self.host_peer_id:
            return False, "host_cannot_join"

        # Novos jogadores só podem entrar em partidas abertas
        if self.status != "OPEN":
            if self.status == "RUNNING":
                return False, "game_running"

            return False, "game_finished"

        # Impede a entrada duplicada
        if peer_id in self.players:
            return False, "already_joined"

        # Verifica o limite de participantes
        if len(self.players) >= self.max_players:
            return False, "game_full"

        # Adiciona o jogador e inicializa sua pontuação
        self.players.append(peer_id)
        self.scores[peer_id] = 0

        return True, None


    #------------------------------------------------------------
    # Remove jogador
    def remove_player(self, peer_id: str) -> tuple[bool, str | None]:

        # Verifica se a partida permite remoção de jogadores
        if self.status not in ("OPEN", "RUNNING"):
            return False, "game_not_running"

        # Valida o identificador
        if not isinstance(peer_id, str) or not peer_id:
            return False, "invalid_peer"

        # Verifica se o jogador participa da partida
        if peer_id not in self.players:
            return False, "player_not_found"

        # Guarda a posição do jogador antes de removê-lo
        removed_index = self.players.index(peer_id)

        # Remove o jogador da lista
        self.players.remove(peer_id)

        # Em partidas ainda não iniciadas, não há pontuação a preservar
        if self.status == "OPEN":
            self.scores.pop(peer_id, None)

        # Se ainda existem jogadores, ajusta o índice do turno
        if self.players:

            if removed_index < self.current_turn_index:
                self.current_turn_index -= 1

            elif self.current_turn_index >= len(self.players):
                self.current_turn_index = 0

        # Se não restou nenhum jogador, aborta a partida
        else:
            self.current_turn_index = 0
            self.abort_game("no_players")

        return True, None


    #------------------------------------------------------------
    # INICIO DO JOGO
    def start_game(self) -> tuple[bool, str | None]:

        # Só permite iniciar uma partida que esteja aberta
        if self.status != "OPEN":
            return False, "game_not_open"

        # Exige pelo menos um jogador
        if not self.players:
            return False, "no_players"

        # Define o primeiro jogador como responsável pelo primeiro turno
        self.current_turn_index = 0

        # Reinicia o contador de estados
        self.seq = 0

        # Atualiza o status da partida
        self.status = "RUNNING"

        return True, None


    """
    Vamos preparar os dados da mensagem GAME_START, mantendo
    a separação que definimos: o GameHost monta os dados e o
    MessageRouter será responsável pelo envio pela conexão TCP.
    """
    #------------------------------------------------------------
    # Constroi a msg de inicio de jogo
    def build_game_start_data(self) -> dict:

        # Só permite gerar os dados após o início da partida
        if self.status != "RUNNING":
            raise RuntimeError("A partida não está em andamento")

        # Monta os dados da mensagem GAME_START
        return {
            "game_id": self.game_id,
            "players": self.players.copy(),
            "lives": self.game.initial_lives,
            "word_length": len(self.game.word),
            "word_hash": calculate_word_hash(self.game.word),
            "turn_timeout": self.turn_timeout
        }


    #------------------------------------------------------------
    # Assim que o host inicia a partida, ele deve enviar um 
    # primeiro STATE com seq = 1, contendo o estado inicial do jogo
    def build_state_data(self) -> dict:
        if self.status != "RUNNING":
            raise RuntimeError("game_not_running")

        if not self.players:
            raise RuntimeError("no_players")

        if not 0 <= self.current_turn_index < len(self.players):
            raise RuntimeError("invalid_turn_index")

        game_state = self.game.get_state()
        self.seq += 1

        return {
            "game_id": self.game_id,
            "seq": self.seq,
            "masked": game_state["masked_word"],
            "wrong_letters": game_state["wrong_letters"],
            "lives": game_state["remaining_lives"],
            "turn": self.players[self.current_turn_index],
            "players": self.players.copy(),
            "last_guess": self.last_guess.copy() if self.last_guess else None
        }


    #------------------------------------------------------------
    # Avança o índice do turno e retorna ao primeiro jogador 
    # quando chega ao final da lista
    def _advance_turn(self):

        # Não há turno para avançar se não existirem jogadores
        if not self.players:
            self.current_turn_index = 0
            return

        # Avança para o próximo jogador, retornando ao início da lista
        self.current_turn_index = (
            self.current_turn_index + 1
        ) % len(self.players)


    #------------------------------------------------------------
    # PROCESSAMENTO DO PALPITE
    def process_guess(
                        self,
                        peer_id: str,
                        *,
                        letter: str | None = None,
                        word: str | None = None
                    ) -> dict:

        if self.status != "RUNNING":
            return {
                "guess_result": {
                    "accepted": False,
                    "reason": "game_not_running"
                },
                "state": None,
                "game_over": None
            }

        if peer_id not in self.players:
            return {
                "guess_result": {
                    "accepted": False,
                    "reason": "player_not_in_game"
                },
                "state": None,
                "game_over": None
            }

        current_player = self.players[self.current_turn_index]

        if peer_id != current_player:
            return {
                "guess_result": {
                    "accepted": False,
                    "reason": "not_your_turn"
                },
                "state": None,
                "game_over": None
            }

        if (letter is None and word is None) or (letter is not None and word is not None):
            return {
                "guess_result": {
                    "accepted": False,
                    "reason": "invalid_guess"
                },
                "state": None,
                "game_over": None
            }

        if letter is not None:
            guess_result = self.game.guess_letter(letter)

            current_guess = {
                "player": peer_id,
                "letter": letter,
                "correct": guess_result.is_correct
            }

            points = guess_result.word_revealed_count

        else:
            hidden_before = self.game.get_masked_word().count("_")

            guess_result = self.game.guess_word(word)

            current_guess = {
                "player": peer_id,
                "word": word,
                "correct": guess_result.is_correct
            }

            points = hidden_before if guess_result.is_correct else 0

        if not guess_result.is_accepted:
            return {
                "guess_result": {
                    "accepted": False,
                    "reason": guess_result.reason
                },
                "state": None,
                "game_over": None
            }

        self.last_guess = current_guess
        self.scores[peer_id] += points

        if self.game.is_won() or self.game.is_lost():
            state_data = self.build_state_data()

            if self.game.is_won():
                self.result = "WON"
                self.winner = peer_id
            else:
                self.result = "LOST"
                self.winner = None

            self.status = "FINISHED"

            return {
                "guess_result": {
                    "accepted": True,
                    "correct": guess_result.is_correct
                },
                "state": state_data,
                "game_over": self.build_game_over_data()
            }

        self._advance_turn()
        state_data = self.build_state_data()

        return {
            "guess_result": {
                "accepted": True,
                "correct": guess_result.is_correct
            },
            "state": state_data,
            "game_over": None
        }

    #------------------------------------------------------------
    # GAME OVER
    def build_game_over_data(self) -> dict:

        if self.result not in ("WON", "LOST", "ABORTED"):
            raise RuntimeError("A partida ainda não possui resultado")

        data = {
            "result": self.result,
            "word": self.game.word,
            "winner": self.winner,
            "scores": self.scores.copy()
        }

        if self.result == "ABORTED":
            data["reason"] = self.abort_reason

        return data
    #------------------------------------------------------------
    # Reentrada de jogador na partida
    def rejoin_player(self, peer_id: str) -> tuple[bool, str | None]:

        # Valida o identificador
        if not isinstance(peer_id, str) or not peer_id:
            return False, "invalid_peer"

        # Impede que o host participe da própria partida
        if peer_id == self.host_peer_id:
            return False, "host_cannot_join"

        # A reentrada só é permitida durante uma partida em andamento
        if self.status == "OPEN":
            return False, "game_not_running"

        if self.status in ("FINISHED", "IDLE"):
            return False, "game_finished"

        # Verifica se o jogador já está participando
        if peer_id in self.players:
            return False, "already_joined"

        # Verifica se o jogador já participou anteriormente
        if peer_id not in self.scores:
            return False, "game_running"

        # Verifica se existe espaço para o retorno
        if len(self.players) >= self.max_players:
            return False, "game_full"

        # Adiciona o jogador ao final da ordem de turnos
        self.players.append(peer_id)

        return True, None


    #------------------------------------------------------------
    # Jogador que perde a vez de jogar
    def handle_timeout(self) -> dict:

        # Verifica se a partida está em andamento
        if self.status != "RUNNING":
            return {
                "accepted": False,
                "reason": "game_not_running",
                "state": None
            }

        # Verifica se existem jogadores
        if not self.players:
            return {
                "accepted": False,
                "reason": "no_players",
                "state": None
            }

        # Identifica o jogador cujo turno expirou
        current_player = self.players[self.current_turn_index]

        # Registra o timeout
        self.last_guess = {
            "player": current_player,
            "timeout": True
        }

        # Passa a vez para o próximo jogador
        self._advance_turn()

        # Gera o novo estado da partida
        state_data = self.build_state_data()

        return {
            "accepted": True,
            "state": state_data
        }

    #------------------------------------------------------------
    # ABORTA a partida
    def abort_game(self, reason: str) -> dict:

        # Verifica se a partida pode ser encerrada
        if self.status not in ("OPEN", "RUNNING"):
            return {
                "accepted": False,
                "reason": "game_not_running",
                "game_over": None
            }

        # Valida o motivo do encerramento
        if reason not in ("no_players", "host_shutdown"):
            return {
                "accepted": False,
                "reason": "invalid_abort_reason",
                "game_over": None
            }

        # Atualiza as informações do encerramento
        self.status = "IDLE"
        self.result = "ABORTED"
        self.abort_reason = reason
        self.winner = None

        # Monta os dados da mensagem GAME_OVER
        game_over_data = self.build_game_over_data()

        return {
            "accepted": True,
            "game_over": game_over_data
        }
