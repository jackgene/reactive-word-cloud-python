import asyncio
import logging
import sys
import tomllib
from asyncio import CancelledError, Task
from datetime import timedelta

import reactivex as rx
import reactive_word_cloud.user_input as user_input
from reactivex import Observable
from reactivex import operators as ops
from reactivex.subject import BehaviorSubject
from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed

from reactive_word_cloud.config import *
from reactive_word_cloud.model import DebuggingCounts, SenderAndText
from reactive_word_cloud.service import WordCloudService


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


async def start_server() -> None:
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
    service: WordCloudService = WordCloudService(WordCloudConfig.from_dict(config['word_cloud']))
    async def update_counts() -> None:
        updater: Observable[DebuggingCounts] = user_input_msgs \
            >> service.debugging_word_counts \
            >> ops.do(counts)
        await updater
    update_counts_task: Task[None] = asyncio.create_task(update_counts())

    port: int = HttpConfig.from_dict(config['http']).port
    conns: int = 0
    async def handle(ws_conn: ServerConnection) -> None:
        nonlocal conns
        conns += 1
        logging.info(f'+1 websocket connection (={conns})')

        async def raise_on_close():
            while True:
                try: await ws_conn.recv()
                except ConnectionClosed: raise
                except: pass

        def publish(counts: DebuggingCounts):
            async def _publish(counts: DebuggingCounts):
                await ws_conn.send(counts.to_json())
            return rx.from_future(asyncio.create_task(_publish(counts)))

        closed: Observable[None] = rx.from_future(
            asyncio.create_task(raise_on_close())
        )

        publisher: Observable[int] = counts \
            >> ops.debounce(timedelta(milliseconds=100)) \
            >> ops.flat_map(publish) \
            >> ops.merge(closed) \
            >> ops.catch(rx.empty()) \
            >> ops.reduce(lambda acc, _: acc + 1, seed=0)
        published: int = await publisher
        await ws_conn.close()

        conns -= 1
        logging.info(f'-1 websocket connection (={conns}, published {published} messages)')

    async with serve(handle, '0.0.0.0', port):
        try:
            await asyncio.get_running_loop().create_future()  # run forever
        except CancelledError:
            update_counts_task.cancel()
            exit(0)

if __name__ == '__main__':
    asyncio.run(start_server())
