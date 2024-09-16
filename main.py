from datetime import timedelta
from typing import Any, Dict

import reactivex as rx
from reactivex import Observable
from reactivex import operators as ops
from websockets.sync.server import ServerConnection, serve

from reactive_word_cloud.model import DebuggingCounts
from reactive_word_cloud.service.word_cloud import chat_messages, debugging_word_counts


def start_server():
    port: int = 9673
    kafka_conf: Dict[str, str] = {
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': 'reactive-word-cloud-python',
        'enable.auto.commit': 'false',
        'auto.offset.reset': 'earliest'
    }
    counts: Observable[DebuggingCounts] = chat_messages(kafka_conf=kafka_conf, topic_name='word-cloud.chat-message') \
        >> debugging_word_counts \
        >> ops.publish_value(DebuggingCounts(history=[], counts_by_word={})) \
        >> ops.ref_count()
    def handle(ws_conn: ServerConnection):
        print('websocket connection established')
        def raise_on_close(_: Any) -> Observable[Any]:
            try: ws_conn.recv(timeout=0)
            except TimeoutError: pass
            return rx.empty()

        def publish(counts: DebuggingCounts):
            ws_conn.send(counts.to_json())

        closed: Observable[None] = rx.timer(
            duetime=timedelta(milliseconds=10), period=timedelta(milliseconds=10)
        ) >> ops.concat_map(raise_on_close)

        publisher: Observable[DebuggingCounts | None] = counts \
            >> ops.debounce(timedelta(milliseconds=100)) \
            >> ops.do_action(publish) \
            >> ops.merge(closed) \
            >> ops.catch(rx.just(None)) # RxPY gets sad when a stream is empty
        publisher.run()
        ws_conn.close()
        print('websocket connection closed')

    with serve(handle, '0.0.0.0', port) as server:
        print(f'server listening on port {port}')
        try:
            server.serve_forever()
        finally:
            exit(0)

if __name__ == '__main__':
    start_server()