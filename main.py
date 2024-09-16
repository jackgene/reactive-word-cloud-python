import asyncio
import sys
import tomllib
from datetime import timedelta
from typing import Literal

import reactivex as rx
import reactive_word_cloud.user_input as user_input
from reactivex import Observable
from reactivex import operators as ops
from reactivex.scheduler import ThreadPoolScheduler
from reactivex.subject import BehaviorSubject
from websockets.asyncio.server import ServerConnection, serve

from reactive_word_cloud.config import *
from reactive_word_cloud.model import DebuggingCounts, SenderAndText
from reactive_word_cloud.service import WordCloudService


async def start_server():
    with open('config.toml', 'rb') as f:
        config: dict[str, Any] = tomllib.load(f)

    counts: Observable[DebuggingCounts] = BehaviorSubject(
        DebuggingCounts(history=[], counts_by_word={})
    )
    user_input_msgs: Observable[SenderAndText]
    if len(sys.argv) > 1 and sys.argv[1] == 'kafka':
        user_input_msgs = user_input.from_kafka(KafkaConfig.from_dict(config['kafka']))
    else:
        user_input_msgs = user_input.from_websockets(WebSocketsConfig.from_dict(config['websockets']))
    WordCloudService(
        WordCloudConfig.from_dict(config['word_cloud'])
    ).debugging_word_counts(
        user_input_msgs
    ).subscribe(counts, scheduler=ThreadPoolScheduler(1))

    port: int = HttpConfig.from_dict(config['http']).port
    conns: int = 0
    async def handle(ws_conn: ServerConnection):
        nonlocal conns
        conns += 1
        print(f'+1 websocket connection (={conns})')

        async def raise_on_close() -> Literal["done"]:
            try: await ws_conn.recv()
            except: pass
            return "done"

        def publish(counts: DebuggingCounts):
            async def _publish(counts: DebuggingCounts):
                await ws_conn.send(counts.to_json())
            return rx.from_future(asyncio.ensure_future(_publish(counts)))

        closed: Observable[Literal["done"]] = rx.repeat_value(()) >> ops.flat_map(
            rx.from_future(asyncio.ensure_future(raise_on_close()))
        )

        publisher: Observable[int] = counts \
            >> ops.debounce(timedelta(milliseconds=100)) \
            >> ops.flat_map(publish) \
            >> ops.merge(closed) \
            >> ops.take_while(lambda msg: msg != "done")
        await publisher
        await ws_conn.close()

        conns -= 1
        print(f'-1 websocket connection (={conns})')

    async with serve(handle, '0.0.0.0', port) as server:
        print(f'server listening on port {port}')
        await asyncio.get_running_loop().create_future()  # run forever

if __name__ == '__main__':
    asyncio.run(start_server())
