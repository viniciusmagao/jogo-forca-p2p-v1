"""
Controle de estados e política de reconexão
sem laços agressivos.

"""
from dataclasses import dataclass, field
from collections import deque
import time


@dataclass
class PeerInfo:
    peer_id: str
    ip: str
    port: int
    state: str = "DISCOVERED"
    attempts: int = 0
    next_retry: float = 0.0
    rtts_ms: deque = field(default_factory=lambda: deque(maxlen=10))

    @property
    def rtt_avg_ms(self):
        return sum(self.rtts_ms) / len(self.rtts_ms) if self.rtts_ms else None

    def failed(
                self, 
                base_backoff=2, 
                max_backoff=60, 
                max_attempts=5
                ):
        self.attempts += 1
        if self.attempts >= max_attempts:
            self.state = "GONE"
        else:
            self.state = "STALE"
            self.next_retry = time.monotonic() + min(max_backoff, base_backoff * 2 ** (self.attempts - 1))

    def connected(self):
        self.state = "CONNECTED"
        self.attempts = 0
        self.next_retry = 0
