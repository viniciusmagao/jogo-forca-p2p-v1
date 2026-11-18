"""
Lógica de quem joga uma partida

"""

from .hangman import calculate_word_hash


class GamePlayer:

    def __init__(self, peer_id: str):
        if not isinstance(peer_id, str) or not peer_id.strip():
            raise ValueError("invalid_peer_id")

        self.peer_id = peer_id

        self.status = "IDLE"

        self.host_peer_id = None
        self.game_id = None

        self.players = []

        self.lives = None
        self.word_length = None
        self.word_hash = None
        self.turn_timeout = None

        self.seq = 0
        self.masked = None
        self.wrong_letters = []
        self.turn = None
        self.last_guess = None

        self.last_guess_result = None
        self.last_game_result = None

        self.rejoin = False

    def begin_join(self, host_peer_id: str, game_id: str) -> None:
        if self.status != "IDLE":
            raise RuntimeError("already_in_game")

        if not isinstance(host_peer_id, str) or not host_peer_id.strip():
            raise ValueError("invalid_host_peer_id")

        if not isinstance(game_id, str) or not game_id.strip():
            raise ValueError("invalid_game_id")

        if host_peer_id == self.peer_id:
            raise ValueError("host_cannot_join")

        self.host_peer_id = host_peer_id
        self.game_id = game_id

        self.status = "JOINED"
        self.rejoin = False

    def handle_join_ok(self, host_peer_id: str, data: dict) -> bool:
        if not isinstance(data, dict):
            return False

        if self.status not in ("JOINED", "PLAYING"):
            return False

        if host_peer_id != self.host_peer_id:
            return False

        if data.get("game_id") != self.game_id:
            return False

        players = data.get("players")
        rejoin = data.get("rejoin")

        if not isinstance(players, list):
            return False

        if not all(isinstance(player, str) for player in players):
            return False

        if self.peer_id not in players:
            return False

        if not isinstance(rejoin, bool):
            return False

        self.players = players.copy()
        self.rejoin = rejoin

        if rejoin:
            self.status = "PLAYING"

        return True

    def handle_join_err(self, host_peer_id: str, data: dict) -> bool:
        if not isinstance(data, dict):
            return False

        if self.status != "JOINED":
            return False

        if host_peer_id != self.host_peer_id:
            return False

        if data.get("game_id") != self.game_id:
            return False

        reason = data.get("reason")

        if not isinstance(reason, str):
            return False

        self.reset()

        return True

    def handle_game_start(self, host_peer_id: str, data: dict) -> bool:
        if not isinstance(data, dict):
            return False

        if self.status != "JOINED":
            return False

        if host_peer_id != self.host_peer_id:
            return False

        if data.get("game_id") != self.game_id:
            return False

        players = data.get("players")
        lives = data.get("lives")
        word_length = data.get("word_length")
        word_hash = data.get("word_hash")
        turn_timeout = data.get("turn_timeout")

        if not isinstance(players, list) or not players:
            return False

        if not all(isinstance(player, str) for player in players):
            return False

        if len(players) != len(set(players)):
            return False

        if self.peer_id not in players:
            return False

        if not isinstance(lives, int) or isinstance(lives, bool) or lives <= 0:
            return False

        if not isinstance(word_length, int) or isinstance(word_length, bool):
            return False

        if not 3 <= word_length <= 20:
            return False

        if not isinstance(word_hash, str):
            return False

        if len(word_hash) != 64:
            return False

        if any(char not in "0123456789abcdef" for char in word_hash):
            return False

        if not isinstance(turn_timeout, int) or isinstance(turn_timeout, bool):
            return False

        if turn_timeout <= 0:
            return False

        self.players = players.copy()

        self.lives = lives
        self.word_length = word_length
        self.word_hash = word_hash
        self.turn_timeout = turn_timeout

        self.seq = 0
        self.masked = None
        self.wrong_letters = []
        self.turn = None
        self.last_guess = None

        self.last_guess_result = None
        self.last_game_result = None

        self.status = "PLAYING"

        return True

    def apply_state(self, host_peer_id: str, data: dict) -> bool:
        if not isinstance(data, dict):
            return False

        if self.status != "PLAYING":
            return False

        if host_peer_id != self.host_peer_id:
            return False

        if data.get("game_id") != self.game_id:
            return False

        seq = data.get("seq")
        masked = data.get("masked")
        wrong_letters = data.get("wrong_letters")
        lives = data.get("lives")
        turn = data.get("turn")
        players = data.get("players")
        last_guess = data.get("last_guess")

        if not isinstance(seq, int) or isinstance(seq, bool) or seq <= self.seq:
            return False

        if not isinstance(masked, str):
            return False

        if len(masked) != self.word_length:
            return False

        if any(char != "_" and not ("a" <= char <= "z") for char in masked):
            return False

        if not isinstance(wrong_letters, list):
            return False

        if not all(
            isinstance(letter, str)
            and len(letter) == 1
            and "a" <= letter <= "z"
            for letter in wrong_letters
        ):
            return False

        if len(wrong_letters) != len(set(wrong_letters)):
            return False

        if not isinstance(lives, int) or isinstance(lives, bool):
            return False

        if lives < 0 or lives > self.lives:
            return False

        if not isinstance(turn, str):
            return False

        if not isinstance(players, list) or not players:
            return False

        if not all(isinstance(player, str) for player in players):
            return False

        if len(players) != len(set(players)):
            return False

        if self.peer_id not in players:
            return False

        if turn not in players:
            return False

        if last_guess is not None:
            if not isinstance(last_guess, dict):
                return False

            if not isinstance(last_guess.get("player"), str):
                return False

            if last_guess["player"] not in players:
                return False

            guess_fields = [
                field for field in ("letter", "word", "timeout")
                if field in last_guess
            ]

            if len(guess_fields) != 1:
                return False

            if "letter" in last_guess:
                letter = last_guess["letter"]

                if (
                    not isinstance(letter, str)
                    or len(letter) != 1
                    or not ("a" <= letter <= "z")
                    or not isinstance(last_guess.get("correct"), bool)
                ):
                    return False

            elif "word" in last_guess:
                word = last_guess["word"]

                if (
                    not isinstance(word, str)
                    or not word
                    or not isinstance(last_guess.get("correct"), bool)
                ):
                    return False

            elif "timeout" in last_guess:
                if last_guess["timeout"] is not True:
                    return False

        self.seq = seq
        self.masked = masked
        self.wrong_letters = wrong_letters.copy()
        self.lives = lives
        self.turn = turn
        self.players = players.copy()
        self.last_guess = last_guess.copy() if last_guess else None

        return True

    def build_guess(self, *, letter: str = None, word: str = None) -> dict:
        if self.status != "PLAYING":
            raise RuntimeError("game_not_running")

        if self.turn != self.peer_id:
            raise RuntimeError("not_your_turn")

        if (letter is None and word is None) or (
            letter is not None and word is not None
        ):
            raise ValueError("invalid_guess")

        if letter is not None:
            if (
                not isinstance(letter, str)
                or len(letter) != 1
                or not ("a" <= letter <= "z")
            ):
                raise ValueError("invalid_letter")

            return {
                "game_id": self.game_id,
                "letter": letter
            }

        if not isinstance(word, str) or not word:
            raise ValueError("invalid_word")

        return {
            "game_id": self.game_id,
            "word": word
        }

    def handle_guess_result(self, host_peer_id: str, data: dict) -> bool:
        if not isinstance(data, dict):
            return False

        if self.status != "PLAYING":
            return False

        if host_peer_id != self.host_peer_id:
            return False

        if data.get("game_id") != self.game_id:
            return False

        accepted = data.get("accepted")

        if not isinstance(accepted, bool):
            return False

        if accepted:
            correct = data.get("correct")

            if not isinstance(correct, bool):
                return False

            self.last_guess_result = {
                "accepted": True,
                "correct": correct
            }

        else:
            reason = data.get("reason")

            if not isinstance(reason, str):
                return False

            self.last_guess_result = {
                "accepted": False,
                "reason": reason
            }

        return True

    def handle_game_over(self, host_peer_id: str, data: dict) -> bool:
        if not isinstance(data, dict):
            return False

        if self.status != "PLAYING":
            return False

        if host_peer_id != self.host_peer_id:
            return False

        if data.get("game_id") != self.game_id:
            return False

        result = data.get("result")
        word = data.get("word")
        winner = data.get("winner")
        scores = data.get("scores")

        if result not in ("WON", "LOST", "ABORTED"):
            return False

        if not isinstance(word, str):
            return False

        if not isinstance(winner, str) and winner is not None:
            return False

        if not isinstance(scores, dict):
            return False

        for player, score in scores.items():
            if not isinstance(player, str):
                return False

            if not isinstance(score, int) or isinstance(score, bool) or score < 0:
                return False

        if result == "WON" and winner is None:
            return False

        if result in ("LOST", "ABORTED") and winner is not None:
            return False

        if result == "ABORTED":
            reason = data.get("reason")

            if reason not in ("host_shutdown", "no_players"):
                return False

        hash_matches = calculate_word_hash(word) == self.word_hash

        self.last_game_result = {
            "game_id": self.game_id,
            "result": result,
            "word": word,
            "winner": winner,
            "scores": scores.copy(),
            "hash_matches": hash_matches,
            "reason": data.get("reason")
        }

        self.reset()

        return True

    def handle_host_unreachable(self, host_peer_id: str) -> bool:
        if self.status not in ("JOINED", "PLAYING"):
            return False

        if host_peer_id != self.host_peer_id:
            return False

        self.last_game_result = {
            "game_id": self.game_id,
            "result": "ABORTED",
            "reason": "host_unreachable",
            "hash_matches": None
        }

        self.reset()

        return True

    def handle_leave_ok(self, host_peer_id: str, data: dict) -> bool:
        if not isinstance(data, dict):
            return False

        if self.status not in ("JOINED", "PLAYING"):
            return False

        if host_peer_id != self.host_peer_id:
            return False

        if data.get("game_id") != self.game_id:
            return False

        self.reset()

        return True

    def reset(self) -> None:
        self.status = "IDLE"

        self.host_peer_id = None
        self.game_id = None

        self.players = []

        self.lives = None
        self.word_length = None
        self.word_hash = None
        self.turn_timeout = None

        self.seq = 0
        self.masked = None
        self.wrong_letters = []
        self.turn = None
        self.last_guess = None

        self.last_guess_result = None
        self.rejoin = False

    def get_state(self) -> dict:
        return {
            "status": self.status,
            "peer_id": self.peer_id,
            "host_peer_id": self.host_peer_id,
            "game_id": self.game_id,
            "players": self.players.copy(),
            "lives": self.lives,
            "word_length": self.word_length,
            "turn_timeout": self.turn_timeout,
            "seq": self.seq,
            "masked": self.masked,
            "wrong_letters": self.wrong_letters.copy(),
            "turn": self.turn,
            "last_guess": self.last_guess.copy() if self.last_guess else None,
            "last_guess_result": (
                self.last_guess_result.copy()
                if self.last_guess_result else None
            ),
            "last_game_result": (
                self.last_game_result.copy()
                if self.last_game_result else None
            ),
            "rejoin": self.rejoin
        }