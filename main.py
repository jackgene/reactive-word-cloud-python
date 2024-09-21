import tomllib
from datetime import timedelta
from typing import Any

import reactivex as rx
from reactivex import Observable
from reactivex import operators as ops
from reactivex.scheduler import ThreadPoolScheduler
from reactivex.subject import BehaviorSubject
from websockets.sync.server import ServerConnection, serve

from reactive_word_cloud.config import *
from reactive_word_cloud.model import DebuggingCounts
from reactive_word_cloud.service import WordCloudService, user_input_from_kafka


def start_server():
    with open('config.toml', 'rb') as f:
        config: dict[str, Any] = tomllib.load(f)

    counts: Observable[DebuggingCounts] = BehaviorSubject(
        DebuggingCounts(history=[], counts_by_word={})
    )
    WordCloudService(
        WordCloudConfig.from_dict(config['word_cloud'])
    ).debugging_word_counts(
        user_input_from_kafka(KafkaConfig.from_dict(config['kafka']))
    ).subscribe(counts, scheduler=ThreadPoolScheduler(1))

    port: int = HttpConfig.from_dict(config['http']).port
    conns: int = 0
    def handle(ws_conn: ServerConnection):
        nonlocal conns
        conns += 1
        print(f'+1 websocket connection (={conns})')

        def raise_on_close(_: Any) -> Observable[Any]:
            try: ws_conn.recv(timeout=0)
            except TimeoutError: pass
            return rx.empty()

        def publish(counts: DebuggingCounts):
            ws_conn.send(counts.to_json())

        closed: Observable[None] = rx.timer(
            duetime=timedelta(milliseconds=10), period=timedelta(milliseconds=10)
        ) >> ops.concat_map(raise_on_close)

        publisher: Observable[int] = counts \
            >> ops.debounce(timedelta(milliseconds=100)) \
            >> ops.do_action(publish) \
            >> ops.merge(closed) \
            >> ops.catch(rx.empty()) \
            >> ops.reduce(lambda acc, _: acc + 1, seed=0)
        published: int = publisher.run()
        ws_conn.close()

        conns -= 1
        print(f'-1 websocket connection (={conns}, published {published} messages)')

    with serve(handle, '0.0.0.0', port) as server:
        print(f'server listening on port {port}')
        try: server.serve_forever()
        finally: exit(0)

if __name__ == '__main__':
    start_server()
