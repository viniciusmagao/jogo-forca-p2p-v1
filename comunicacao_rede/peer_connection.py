"""
CONEXAO PERSISTENTE ENTRE 2 PEERS
Representa a conexão TCP persistente entre dois peers.

"""

import asyncio
from dataclasses import dataclass, field
from .protocolo import encode_line


@dataclass
class PeerConnection:
    peer_id: str
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    outbound: bool
    features: list[str]
    pending_pings: dict = field(default_factory=dict)
    missed: int = 0
    reader_running: bool = False
    heartbeat_task: asyncio.Task | None = None
    bye_waiter: asyncio.Event = field(default_factory=asyncio.Event)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def send(self, message: dict):
        """
        Serialize um JSON por linha e proteja writes 
        concorrentes no mesmo TCP.
        """
        async with self.lock:
            if self.writer.is_closing():
                raise ConnectionError(f"Conexão encerrada: {self.peer_id}")
            self.writer.write(encode_line(message))
            await self.writer.drain()

    async def close(self):
        self.writer.close()
        try:
            await asyncio.wait_for(self.writer.wait_closed(), timeout=1)
        except (OSError, asyncio.TimeoutError):
            pass
