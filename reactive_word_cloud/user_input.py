import reactivex as rx
import reactivex.operators as ops
from confluent_kafka import Consumer, Message
from reactivex.observable.observable import Observable

from reactive_word_cloud.config import KafkaConfig
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
