from typing import Dict, Iterable

import reactivex as rx
import reactivex.operators as ops
from confluent_kafka import Consumer, Message
from reactivex import Observable

from ..model import ChatMessage


def make_chat_messages(kafka_conf: Dict[str, str], topic_name: str) -> Observable[ChatMessage]:
    consumer: Consumer = Consumer(kafka_conf)
    def consume_messages() -> Iterable[Message]:
        try:
            consumer.subscribe([topic_name])

            while True:
                msg: Message | None = consumer.poll(timeout=1.0)
                if msg is not None:
                    yield msg
                    # consumer.commit(asynchronous=False)
        finally:
            # Close down consumer to commit final offsets.
            consumer.close()
    def not_error(msg: Message) -> bool:
        return not msg.error()
    def extract_chat_message(msg: Message) -> Iterable[ChatMessage]:
        value: str | bytes | None = msg.value()
        chat_message: ChatMessage | None
        print(f'Extracting from: {value}')
        match value:
            case str():
                chat_message = ChatMessage.from_json(value)
            case bytes():
                chat_message = ChatMessage.from_json(value.decode('utf-8'))
            case _:
                chat_message = None
        print(f'Extracted: {chat_message}')
        if chat_message is None:
            return []
        return [chat_message]
    def debug(chat_message: ChatMessage) -> ChatMessage:
        print(f"fuck: {chat_message}")
        return chat_message
    return rx.from_iterable(
        consume_messages()
    ) >> ops.filter(not_error) >> ops.flat_map(extract_chat_message) >> ops.map(debug) >> ops.share()