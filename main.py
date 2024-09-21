from datetime import timedelta
from typing import Any

import reactivex as rx
from reactivex import Observable
from reactivex import operators as ops
from reactivex.scheduler import ThreadPoolScheduler
from reactivex.subject import BehaviorSubject
from websockets.sync.server import ServerConnection, serve

from reactive_word_cloud.model import DebuggingCounts
from reactive_word_cloud.service import user_input, debugging_word_counts


def start_server():
    port: int = 9673
    kafka_conf: dict[str, str] = {
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': 'reactive-word-cloud-python',
        'enable.auto.commit': 'false',
        'auto.offset.reset': 'earliest'
    }
    counts: Observable[DebuggingCounts] = BehaviorSubject(
        DebuggingCounts(history=[], counts_by_word={})
    )
    debugging_word_counts(
        user_input(kafka_conf=kafka_conf, topic_name='word-cloud.chat-message')
    ).subscribe(counts, scheduler=ThreadPoolScheduler(1))

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
