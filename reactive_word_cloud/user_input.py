import reactivex as rx
import reactivex.operators as ops
import websockets.sync.client as ws_client
from confluent_kafka import Consumer, Message
from reactivex import Observable
from websockets.exceptions import ConnectionClosed
from websockets.sync.connection import Connection

from reactive_word_cloud.config import KafkaConfig, WebSocketsConfig
from reactive_word_cloud.model import SenderAndText


def from_kafka(config: KafkaConfig) -> Observable[SenderAndText]:
    def consume_message(consumer: Consumer) -> rx.Observable[Message]:
        msg: Message | None = consumer.poll(timeout=1.0)
        return rx.just(msg) if msg is not None else rx.empty()

    def not_error(msg: Message) -> bool:
        return not msg.error()

    def extract_chat_message(msg: Message) -> Observable[SenderAndText]:
        value: str | bytes | None = msg.value()
        sender_text: SenderAndText | None
        match value:
            case str():
                sender_text = SenderAndText.from_json(value)
            case bytes():
                sender_text = SenderAndText.from_json(value.decode('utf-8'))
            case _:
                sender_text = None
        if sender_text is not None:
            print(f'consumed: {sender_text.to_json()}')
        return rx.empty() if sender_text is None else rx.just(sender_text)

    consumer: Consumer = Consumer({
        'bootstrap.servers': ','.join(config.bootstrap_servers),
        'group.id': config.group_id,
        'enable.auto.commit': 'true' if config.enable_auto_commit else 'false',
        'auto.offset.reset': config.auto_offset_reset
    })
    consumer.subscribe(config.topic_names)
    return rx.repeat_value(consumer) \
        >> ops.concat_map(consume_message) \
        >> ops.filter(not_error) \
        >> ops.concat_map(extract_chat_message)


def from_websockets(config: WebSocketsConfig) -> Observable[SenderAndText]:
    def consume_message(ws_conn: Connection) -> Observable[str]:
        msg: str | bytes = ws_conn.recv()
        return rx.just(msg) if isinstance(msg, str) else rx.empty()

    def extract_chat_message(json: str) -> Observable[SenderAndText]:
        sender_text: SenderAndText | None = SenderAndText.from_json(json)
        if sender_text is not None:
            print(f'consumed: {sender_text.to_json()}')
        return rx.empty() if sender_text is None else rx.just(sender_text)

    def handle_closure(err: Exception, obs: Observable[SenderAndText]) -> Observable[SenderAndText]:
        return from_websockets(config) >> ops.delay(1) if isinstance(err, ConnectionClosed) else obs

    ws_conn: Connection = ws_client.connect(config.url)
    return rx.repeat_value(ws_conn) \
        >> ops.concat_map(consume_message) \
        >> ops.concat_map(extract_chat_message) \
        >> ops.catch(handle_closure)
