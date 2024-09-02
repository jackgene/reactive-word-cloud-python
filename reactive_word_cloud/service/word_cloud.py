from datetime import timedelta
from typing import Dict, Iterable

import reactivex as rx
import reactivex.operators as ops
from confluent_kafka import Consumer, Message
from reactivex import Observable

from ..model import ChatMessage


def chat_messages(kafka_conf: Dict[str, str], topic_name: str) -> Observable[ChatMessage]:
    consumer: Consumer = Consumer(kafka_conf)
    def consume_messages() -> Iterable[Message]:
        try:
            consumer.subscribe([topic_name])

            while True:
                msg: Message | None = consumer.poll(timeout=1.0)
                # consumer.commit(asynchronous=False)
                if msg is not None:
                    yield msg
        finally:
            # Close down consumer to commit final offsets.
            consumer.close()
    def not_error(msg: Message) -> bool:
        return not msg.error()
    def extract_chat_message(msg: Message) -> Observable[ChatMessage]:
        value: str | bytes | None = msg.value()
        chat_message: ChatMessage | None
        match value:
            case str():
                chat_message = ChatMessage.from_json(value)
            case bytes():
                chat_message = ChatMessage.from_json(value.decode('utf-8'))
            case _:
                chat_message = None
        if chat_message is None:
            return rx.empty()
        return rx.just(chat_message)
    return rx.from_iterable(consume_messages()) \
        >> ops.filter(not_error) \
        >> ops.debounce(timedelta(milliseconds=100)) \
        >> ops.concat_map(extract_chat_message)