"""
PING / PONG

perdas consecutivas e média de RTT.

"""
import asyncio
import time
from .protocolo import new_id, now_utc


async def heartbeat_loop(network, connection):
    """
    Envia PING periodicamente; 
    três ciclos sem resposta causam STALE.
    
    """
    while network.running and not connection.writer.is_closing():
        await asyncio.sleep(network.config.ping_interval)
        if not network.running or connection.writer.is_closing():
            break
        
        # Contabiliza PINGs dos intervalos anteriores ainda não respondidos
        if connection.pending_pings:
            connection.missed += 1
            connection.pending_pings.clear()
            if connection.missed >= network.config.ping_miss_limit:
                await network._drop(connection, stale=True, reason="PING sem PONG")
                break
        msg_id = new_id()
        timestamp = now_utc()

        connection.pending_pings[msg_id] = (time.monotonic(), timestamp)

        try:
            await connection.send({
                                    "type": "PING",
                                    "msg_id": msg_id,
                                    "timestamp": timestamp,
                                    "ttl": 1
                                })
        except (OSError, ConnectionError):
            await network._drop(connection, stale=True, reason="Falha ao enviar PING")
            break
