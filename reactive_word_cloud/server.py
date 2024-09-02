from datetime import timedelta
from typing import Any, Dict

import reactivex as rx
from reactivex import Observable
from reactivex import operators as ops
from websockets.sync.server import ServerConnection, serve

from .model import ChatMessage
from .service.word_cloud import chat_messages


def start_server():
    port: int = 9673
    kafka_conf: Dict[str, str] = {
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': 'reactive-word-cloud-python',
        'enable.auto.commit': 'false',
        'auto.offset.reset': 'earliest'
    }
    def handle(ws_conn: ServerConnection):
        def raise_on_close(_: Any) -> Observable[Any]:
            try: ws_conn.recv(timeout=0)
            except TimeoutError: pass
            return rx.empty()

        def publish_chat_message(chat_message: ChatMessage) -> ChatMessage:
            ws_conn.send(chat_message.to_json())
            return chat_message

        closed: Observable[None] = rx.timer(
            duetime=timedelta(milliseconds=10), period=timedelta(milliseconds=10)
        ) >> ops.concat_map(raise_on_close)

        publisher: Observable[Any] = chat_messages(
            kafka_conf=kafka_conf, topic_name='word-cloud.chat-message'
        ) \
            >> ops.map(publish_chat_message) \
            >> ops.merge(closed) \
            >> ops.catch(rx.just(())) # RxPY gets sad when a stream is empty
        publisher.run()
        print('connection closed')

    with serve(handle, 'localhost', port) as server:
        print(f'server listening on port {port}')
        server.serve_forever()

if __name__ == '__main__':
    start_server()
